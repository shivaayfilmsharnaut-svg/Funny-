import express, { Request, Response } from 'express';
import { createServer as createViteServer } from 'vite';
import fs from 'fs';
import os from 'os';
import path from 'path';
import { execFile } from 'child_process';
import { Readable } from 'stream';
import { promisify } from 'util';
import CryptoJS from 'crypto-js';

const execFileAsync = promisify(execFile);

const app = express();
const PORT = 3000;

app.use((req, res, next) => {
  if (req.path === '/api/media/mux') {
    next();
    return;
  }
  express.json({ limit: '10mb' })(req, res, next);
});
app.use('/api', (req, res, next) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, PUT, PATCH, DELETE, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization, x-api-key');
  if (req.method === 'OPTIONS') {
    res.sendStatus(204);
    return;
  }
  next();
});

// ── Persistent Data Storage ──────────────────────────────────────────────
const DATA_DIR = path.resolve('data');
const CHAT_FILE = path.join(DATA_DIR, 'chats.json');
const FOLLOWS_FILE = path.join(DATA_DIR, 'follows.json');

if (!fs.existsSync(DATA_DIR)) {
  fs.mkdirSync(DATA_DIR, { recursive: true });
}

interface ChatMessage {
  id: string;
  clientId?: string;
  conversationId: string;
  senderId: string;
  senderName: string;
  senderAvatar?: string;
  recipientId?: string;
  recipientName?: string;
  recipientAvatar?: string;
  text: string;
  createdAt: string;
  read: boolean;
  isMine?: boolean;
}

interface ConversationItem {
  id: string;
  conversationId: string;
  peerId: string;
  peerName: string;
  peerAvatar: string;
  lastMessage: string;
  lastMessageAt: string;
  participants: string[];
  unreadCount?: number;
}

interface ChatStore {
  messages: Record<string, ChatMessage[]>;
  conversations: Record<string, {
    conversationId: string;
    participants: string[];
    participantDetails: Record<string, { name: string; avatar: string }>;
    lastMessage: string;
    lastMessageAt: string;
    lastSenderId: string;
  }>;
}

const loadChatStore = (): ChatStore => {
  try {
    if (fs.existsSync(CHAT_FILE)) {
      const raw = fs.readFileSync(CHAT_FILE, 'utf-8');
      return JSON.parse(raw);
    }
  } catch (err) {
    console.warn('[Server] Could not read chat store:', err);
  }
  return { messages: {}, conversations: {} };
};

const saveChatStore = (store: ChatStore) => {
  try {
    fs.writeFileSync(CHAT_FILE, JSON.stringify(store, null, 2), 'utf-8');
  } catch (err) {
    console.warn('[Server] Could not save chat store:', err);
  }
};

let store = loadChatStore();

interface FollowStoreRecord {
  id: string;
  followerId: string;
  followerUserId: string;
  followerEmail?: string;
  followerUid?: string;
  followerName: string;
  followerPic: string;
  followerAliases: string[];
  followeeId: string;
  followeeBackendId?: string;
  followeeEmail?: string;
  followeeName: string;
  followeePic: string;
  followeeAliases: string[];
  createdAt: string;
  updatedAt: string;
}

const normFollowKey = (v: unknown): string =>
  String(v ?? '').trim().replace(/^@+/, '').toLowerCase();

const isSelfFollowRecord = (rec: Partial<FollowStoreRecord>): boolean => {
  const fSet = new Set(
    [
      ...(rec.followerAliases ?? []),
      rec.followerId,
      rec.followerUserId,
      rec.followerEmail,
      rec.followerUid,
    ]
      .map(normFollowKey)
      .filter(Boolean),
  );
  const tList = [
    ...(rec.followeeAliases ?? []),
    rec.followeeId,
    rec.followeeBackendId,
    rec.followeeEmail,
  ]
    .map(normFollowKey)
    .filter(Boolean);
  return tList.some((k) => fSet.has(k));
};

const loadFollowsStore = (): FollowStoreRecord[] => {
  try {
    if (fs.existsSync(FOLLOWS_FILE)) {
      const raw = fs.readFileSync(FOLLOWS_FILE, 'utf-8');
      const parsed = JSON.parse(raw);
      if (Array.isArray(parsed)) {
        return parsed.filter((r) => r && typeof r === 'object' && !isSelfFollowRecord(r));
      }
    }
  } catch (err) {
    console.warn('[Server] Could not read follows store:', err);
  }
  return [];
};

const saveFollowsStore = (records: FollowStoreRecord[]) => {
  try {
    const clean = records.filter((r) => r && !isSelfFollowRecord(r));
    fs.writeFileSync(FOLLOWS_FILE, JSON.stringify(clean, null, 2), 'utf-8');
  } catch (err) {
    console.warn('[Server] Could not save follows store:', err);
  }
};

let followsStore: FollowStoreRecord[] = loadFollowsStore();

// ── SSE Active Listeners Map ─────────────────────────────────────────────
const sseListeners = new Map<string, Set<Response>>();

const normalizeId = (id: string | undefined): string => {
  return String(id ?? '').trim().replace(/^@+/, '').toLowerCase();
};

const notifySseClients = (userId: string, data: any) => {
  const norm = normalizeId(userId);
  const clients = sseListeners.get(norm);
  if (!clients) return;
  const payload = `data: ${JSON.stringify(data)}\n\n`;
  clients.forEach(res => {
    try {
      res.write(payload);
    } catch {
      clients.delete(res);
    }
  });
};

// ── Chat API Routes ──────────────────────────────────────────────────────

// Health check
app.get('/api/health', (_req: Request, res: Response) => {
  res.json({ ok: true, timestamp: new Date().toISOString() });
});

const expandChatAliases = (values: unknown[]): string[] => {
  const out = new Set<string>();
  values.forEach((raw) => {
    const k = normalizeId(String(raw ?? ''));
    if (!k) return;
    out.add(k);
  });
  return Array.from(out);
};

// GET /api/chat/messages?conversationId=...&myId=...&aliases=...
app.get('/api/chat/messages', (req: Request, res: Response) => {
  const convId = String(req.query.conversationId || '').trim();
  const extraConvIds = String(req.query.conversationIds || '')
    .split(',')
    .map((v) => v.trim())
    .filter(Boolean);
  const convIdSet = new Set([convId, ...extraConvIds].filter(Boolean));
  const myId = normalizeId(String(req.query.myId || ''));
  const aliasList = expandChatAliases([
    myId,
    ...String(req.query.aliases || '').split(','),
  ]);
  const mySet = new Set(aliasList);

  if (convIdSet.size === 0) {
    res.json({ success: true, messages: [] });
    return;
  }

  const combined: ChatMessage[] = [];
  const seen = new Set<string>();
  convIdSet.forEach((cid) => {
    (store.messages[cid] || []).forEach((m) => {
      if (seen.has(m.id)) return;
      seen.add(m.id);
      combined.push(m);
    });
  });

  combined.sort((a, b) => (Date.parse(a.createdAt) || 0) - (Date.parse(b.createdAt) || 0));
  const mapped = combined.map((m) => ({
    ...m,
    isMine: mySet.size > 0 ? mySet.has(normalizeId(m.senderId)) : false,
  }));

  res.json({ success: true, messages: mapped });
});

// POST /api/chat/send
app.post('/api/chat/send', (req: Request, res: Response) => {
  const {
    conversationId,
    senderId,
    senderAliases,
    senderName,
    senderAvatar,
    recipientId,
    recipientAliases,
    recipientName,
    recipientAvatar,
    text,
    clientId,
  } = req.body;

  if (!conversationId || !senderId || !text) {
    res.status(400).json({ success: false, error: 'conversationId, senderId and text are required.' });
    return;
  }

  const normSender = normalizeId(senderId);
  const normRecipient = normalizeId(recipientId);
  const allSenderAliases = expandChatAliases([
    normSender,
    ...(Array.isArray(senderAliases) ? senderAliases : []),
  ]);
  const allRecipientAliases = expandChatAliases([
    normRecipient,
    ...(Array.isArray(recipientAliases) ? recipientAliases : []),
  ]);
  const nowIso = new Date().toISOString();

  const newMsg: ChatMessage = {
    id: `msg_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
    clientId: clientId || `client_${Date.now()}`,
    conversationId,
    senderId: normSender,
    senderName: senderName || normSender,
    senderAvatar: senderAvatar || '',
    recipientId: normRecipient,
    recipientName: recipientName || normRecipient,
    recipientAvatar: recipientAvatar || '',
    text: String(text).trim(),
    createdAt: nowIso,
    read: false,
  };

  if (!store.messages[conversationId]) {
    store.messages[conversationId] = [];
  }
  const existingDup = newMsg.clientId
    ? store.messages[conversationId].find((m) => m.clientId === newMsg.clientId)
    : undefined;
  if (!existingDup) {
    store.messages[conversationId].push(newMsg);
  }

  // Update conversation record
  const existingConv = store.conversations[conversationId];
  const participants = Array.from(
    new Set([normSender, normRecipient, ...(existingConv?.participants || [])].filter(Boolean)),
  );
  store.conversations[conversationId] = {
    conversationId,
    participants,
    participantDetails: {
      ...(existingConv?.participantDetails || {}),
      [normSender]: {
        name: senderName || existingConv?.participantDetails?.[normSender]?.name || normSender,
        avatar: senderAvatar || existingConv?.participantDetails?.[normSender]?.avatar || '',
      },
      ...(normRecipient
        ? {
            [normRecipient]: {
              name: recipientName || existingConv?.participantDetails?.[normRecipient]?.name || normRecipient,
              avatar: recipientAvatar || existingConv?.participantDetails?.[normRecipient]?.avatar || '',
            },
          }
        : {}),
    },
    lastMessage: newMsg.text,
    lastMessageAt: nowIso,
    lastSenderId: normSender,
  };

  saveChatStore(store);

  // Real-time Push to all sender and recipient alias SSE streams (deduped per socket)
  const payload = `data: ${JSON.stringify({ type: 'new_message', message: newMsg })}\n\n`;
  const pushedSockets = new Set<Response>();
  [...allSenderAliases, ...allRecipientAliases].forEach((alias) => {
    const clients = sseListeners.get(alias);
    if (!clients) return;
    clients.forEach((clientRes) => {
      if (pushedSockets.has(clientRes)) return;
      pushedSockets.add(clientRes);
      try {
        clientRes.write(payload);
      } catch {
        clients.delete(clientRes);
      }
    });
  });

  res.json({ success: true, message: existingDup || newMsg });
});

// GET /api/chat/conversations?userId=...&aliases=...
app.get('/api/chat/conversations', (req: Request, res: Response) => {
  const userId = normalizeId(String(req.query.userId || ''));
  const aliases = expandChatAliases([
    userId,
    ...String(req.query.aliases || '').split(','),
  ]);
  if (aliases.length === 0) {
    res.json({ success: true, conversations: [] });
    return;
  }
  const mySet = new Set(aliases);

  const results: ConversationItem[] = [];

  Object.values(store.conversations).forEach((conv) => {
    const matchesMe = conv.participants.some((p) => mySet.has(normalizeId(p)));
    if (matchesMe) {
      const peerId = conv.participants.find((p) => !mySet.has(normalizeId(p))) || userId;
      if (!peerId || mySet.has(normalizeId(peerId))) return;
      const peerDetails = conv.participantDetails?.[peerId] || { name: peerId, avatar: '' };
      results.push({
        id: peerId,
        conversationId: conv.conversationId,
        peerId,
        peerName: peerDetails.name || peerId,
        peerAvatar: peerDetails.avatar || '',
        lastMessage: conv.lastMessage,
        lastMessageAt: conv.lastMessageAt,
        participants: conv.participants,
      });
    }
  });

  results.sort((a, b) => Date.parse(b.lastMessageAt) - Date.parse(a.lastMessageAt));
  res.json({ success: true, conversations: results });
});

// GET /api/chat/stream?userId=...&aliases=... (Server-Sent Events)
app.get('/api/chat/stream', (req: Request, res: Response) => {
  const userId = normalizeId(String(req.query.userId || ''));
  const aliasKeys = expandChatAliases([
    userId,
    ...String(req.query.aliases || '').split(','),
  ]);
  if (aliasKeys.length === 0) {
    res.status(400).send('userId is required');
    return;
  }

  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.flushHeaders();

  res.write(`data: ${JSON.stringify({ type: 'connected', userId, aliases: aliasKeys })}\n\n`);

  aliasKeys.forEach((alias) => {
    if (!sseListeners.has(alias)) {
      sseListeners.set(alias, new Set());
    }
    sseListeners.get(alias)!.add(res);
  });

  // Keep alive ping
  const interval = setInterval(() => {
    try {
      res.write(': keepalive\n\n');
    } catch {
      clearInterval(interval);
    }
  }, 20000);

  req.on('close', () => {
    clearInterval(interval);
    aliasKeys.forEach((alias) => {
      sseListeners.get(alias)?.delete(res);
      if (sseListeners.get(alias)?.size === 0) {
        sseListeners.delete(alias);
      }
    });
  });
});

// ── Health & Feed Proxy Endpoints ─────────────────────────────────────────
app.get('/api/health', (_req: Request, res: Response) => {
  return res.status(200).json({ status: 'ok', time: new Date().toISOString() });
});

// ── Persistent Notification Store & Routes ───────────────────────────────
const NOTIFICATIONS_FILE = path.join(DATA_DIR, 'notifications.json');

interface StoredNotificationRecord {
  notificationId: string;
  id: string;
  recipientId: string;
  recipientAliases: string[];
  videoOwnerId?: string;
  senderId: string;
  senderAliases?: string[];
  senderName: string;
  senderAvatar: string;
  fromUserId: string;
  fromUserName: string;
  fromProfilePic: string;
  avatar: string;
  profilePic: string;
  type: 'like' | 'follow' | 'comment' | 'share' | 'save' | string;
  videoId?: string;
  videoThumbnail?: string;
  comment?: string;
  title: string;
  message: string;
  isRead: boolean;
  read: boolean;
  createdAt: string;
}

const normNotifKey = (v: unknown): string =>
  String(v ?? '').trim().replace(/^@+/, '').toLowerCase();

const FAKE_SERVER_NOTIF_ACTORS = new Set([
  'all',
  'local_user',
  'user',
  'newuser',
  'new user',
  'creator',
  'you',
  'usr_anonymous',
  'anonymous',
  'masti user',
  'masti_user',
  'mastiuser',
  'masti creator',
  'someone',
  'a fan',
  'afan',
  'guest',
  'unknown',
  'default',
  'user@masti.in',
]);

const hasRealServerNotifSender = (n: any): boolean => {
  if (!n || typeof n !== 'object') return false;
  const candidates = [
    n.senderId,
    n.fromUserId,
    ...(Array.isArray(n.senderAliases) ? n.senderAliases : []),
    n.senderName,
    n.fromUserName,
  ]
    .map(normNotifKey)
    .filter((k) => Boolean(k) && !FAKE_SERVER_NOTIF_ACTORS.has(k));
  return candidates.length > 0;
};

const normalizeServerNotifVideoId = (val: unknown): string =>
  String(val ?? '')
    .trim()
    .replace(/^db-/i, '')
    .toLowerCase();

const normalizeServerNotifComment = (val: unknown): string =>
  String(val ?? '')
    .trim()
    .replace(/^["'“”]+|["'“”]+$/g, '')
    .replace(/\s+/g, ' ')
    .trim()
    .toLowerCase();

const expandServerNotifTokens = (values: unknown[]): string[] => {
  const out = new Set<string>();
  values.forEach((v) => {
    const k = normNotifKey(v);
    if (!k || FAKE_SERVER_NOTIF_ACTORS.has(k)) return;
    out.add(k);
    if (k.includes('@')) {
      const local = k.split('@')[0]?.trim();
      if (local && !FAKE_SERVER_NOTIF_ACTORS.has(local)) out.add(local);
    }
  });
  return Array.from(out);
};

const collectNotificationRecipientKeys = (n: Partial<StoredNotificationRecord>): string[] =>
  expandServerNotifTokens([
    n.recipientId,
    n.videoOwnerId,
    ...(Array.isArray(n.recipientAliases) ? n.recipientAliases : []),
  ]);

const collectNotificationSenderKeys = (n: Partial<StoredNotificationRecord>): string[] =>
  expandServerNotifTokens([
    (n as any).actorId,
    n.senderId,
    n.fromUserId,
    ...(Array.isArray(n.senderAliases) ? n.senderAliases : []),
  ]);

const dedupeServerNotifications = (records: StoredNotificationRecord[]): StoredNotificationRecord[] => {
  const sorted = [...records].sort(
    (a, b) => (Date.parse(b.createdAt || '') || 0) - (Date.parse(a.createdAt || '') || 0),
  );
  const kept: StoredNotificationRecord[] = [];
  for (const r of sorted) {
    if (!r || typeof r !== 'object' || !hasRealServerNotifSender(r)) continue;
    const rawType = String(r.type || 'activity').trim().toLowerCase();
    const rType = rawType === 'bookmark' ? 'save' : (rawType === 'comments' ? 'comment' : rawType);
    if (rType === 'chat' || rType === 'direct_chat_message' || rType.startsWith('support_')) {
      kept.push(r);
      continue;
    }
    const rVid = rType === 'follow' ? '' : normalizeServerNotifVideoId(r.videoId);
    const rComment = rType === 'comment' ? normalizeServerNotifComment(r.comment || r.message) : '';
    const rSenders = collectNotificationSenderKeys(r);
    const rRecipients = collectNotificationRecipientKeys(r);
    if (rSenders.length === 0 || rRecipients.length === 0) continue;

    const dup = kept.find((existing) => {
      const eRawType = String(existing.type || 'activity').trim().toLowerCase();
      const eType = eRawType === 'bookmark' ? 'save' : (eRawType === 'comments' ? 'comment' : eRawType);
      if (eType !== rType) return false;
      const eVid = eType === 'follow' ? '' : normalizeServerNotifVideoId(existing.videoId);
      if (eType !== 'follow' && eVid !== rVid) return false;
      if (eType === 'comment') {
        const eComment = normalizeServerNotifComment(existing.comment || existing.message);
        if (eComment !== rComment) return false;
      }
      const eSenders = collectNotificationSenderKeys(existing);
      const eRecipients = collectNotificationRecipientKeys(existing);
      return (
        rSenders.some((s) => eSenders.includes(s)) &&
        rRecipients.some((rec) => eRecipients.includes(rec))
      );
    });
    if (!dup) {
      kept.push(r);
    } else if (r.isRead || r.read) {
      dup.isRead = true;
      dup.read = true;
    }
  }
  return kept;
};

const DISMISSED_NOTIFICATIONS_FILE = path.join(DATA_DIR, 'dismissed_notifications.json');

interface ServerDismissedNotifSignature {
  type: string;
  videoId: string;
  comment: string;
  actors: string[];
  title?: string;
  message?: string;
}

interface ServerDismissedNotifStore {
  ids: string[];
  signatures: ServerDismissedNotifSignature[];
}

const buildServerDismissSig = (n: any): ServerDismissedNotifSignature | null => {
  if (!n || typeof n !== 'object') return null;
  const rawType = String(n.type || n.notificationType || 'activity').trim().toLowerCase().replace(/[\s-]+/g, '_');
  const type = rawType === 'bookmark' ? 'save' : (rawType === 'comments' ? 'comment' : rawType);
  if (type === 'chat' || type === 'direct_chat_message') return null;
  const videoId = type === 'follow' ? '' : normalizeServerNotifVideoId(n.videoId || n.mediaId);
  const comment = type === 'comment' ? normalizeServerNotifComment(n.comment || n.message) : '';
  const actors = expandServerNotifTokens([
    n.actorId,
    n.senderId,
    n.fromUserId,
    n.userId,
    n.actorName,
    n.senderName,
    n.fromUserName,
    n.userName,
    ...(Array.isArray(n.senderAliases) ? n.senderAliases : []),
  ]);
  const title = String(n.title || '').trim().toLowerCase();
  const message = String(n.message || '').trim().toLowerCase();
  if (actors.length === 0 && !title && !message) return null;
  return {
    type,
    videoId,
    comment,
    actors,
    ...(type.startsWith('support_') || type === 'admin_message' || type === 'message' ? { title, message } : {}),
  };
};

const loadDismissedNotificationsStore = (): ServerDismissedNotifStore => {
  try {
    if (fs.existsSync(DISMISSED_NOTIFICATIONS_FILE)) {
      const raw = fs.readFileSync(DISMISSED_NOTIFICATIONS_FILE, 'utf-8');
      const parsed = JSON.parse(raw);
      return {
        ids: Array.isArray(parsed?.ids)
          ? parsed.ids.map((id: any) => String(id || '').trim().replace(/^fs_/, '')).filter(Boolean)
          : [],
        signatures: Array.isArray(parsed?.signatures)
          ? parsed.signatures.filter((s: any) => s && typeof s === 'object')
          : [],
      };
    }
  } catch (err) {
    console.warn('[Server] Could not read dismissed notifications store:', err);
  }
  return { ids: [], signatures: [] };
};

const saveDismissedNotificationsStore = (store: ServerDismissedNotifStore) => {
  try {
    fs.writeFileSync(
      DISMISSED_NOTIFICATIONS_FILE,
      JSON.stringify(
        {
          ids: store.ids.slice(-3000),
          signatures: store.signatures.slice(-3000),
        },
        null,
        2,
      ),
      'utf-8',
    );
  } catch (err) {
    console.warn('[Server] Could not save dismissed notifications store:', err);
  }
};

let dismissedNotificationsStore: ServerDismissedNotifStore = loadDismissedNotificationsStore();

const isServerNotifDismissed = (n: any): boolean => {
  if (!n || typeof n !== 'object') return false;
  const idSet = new Set(dismissedNotificationsStore.ids);
  const candidateIds = [
    n.notificationId,
    n.id,
    ...(Array.isArray(n._mergedIds) ? n._mergedIds : []),
  ]
    .map((v) => String(v || '').trim().replace(/^fs_/, ''))
    .filter(Boolean);
  if (candidateIds.some((id) => idSet.has(id))) return true;

  const sig = buildServerDismissSig(n);
  if (!sig) return false;
  return dismissedNotificationsStore.signatures.some((s) => {
    if (!s || s.type !== sig.type) return false;
    if (sig.type.startsWith('support_') || sig.type === 'admin_message' || sig.type === 'message') {
      return Boolean(sig.title || sig.message) && s.title === sig.title && s.message === sig.message;
    }
    if (sig.type !== 'follow' && s.videoId !== sig.videoId) return false;
    if (sig.type === 'comment' && s.comment !== sig.comment) return false;
    if (sig.actors.length === 0 || !Array.isArray(s.actors) || s.actors.length === 0) return false;
    return sig.actors.some((a) => s.actors.includes(a));
  });
};

const loadNotificationsStore = (): StoredNotificationRecord[] => {
  try {
    if (fs.existsSync(NOTIFICATIONS_FILE)) {
      const raw = fs.readFileSync(NOTIFICATIONS_FILE, 'utf-8');
      const parsed = JSON.parse(raw);
      if (Array.isArray(parsed)) {
        return dedupeServerNotifications(
          parsed.filter((r) => r && typeof r === 'object' && hasRealServerNotifSender(r) && !isServerNotifDismissed(r)),
        );
      }
    }
  } catch (err) {
    console.warn('[Server] Could not read notifications store:', err);
  }
  return [];
};

const saveNotificationsStore = (records: StoredNotificationRecord[]) => {
  try {
    fs.writeFileSync(NOTIFICATIONS_FILE, JSON.stringify(records.slice(0, 1000), null, 2), 'utf-8');
  } catch (err) {
    console.warn('[Server] Could not save notifications store:', err);
  }
};

let notificationsStore: StoredNotificationRecord[] = loadNotificationsStore();

app.get('/api/notifications/stream', (req: Request, res: Response) => {
  const rawAliases = String(req.query.aliases || req.query.userId || '').trim();
  const aliasKeys = Array.from(
    new Set(rawAliases.split(',').map(normNotifKey).filter(Boolean)),
  );
  if (aliasKeys.length === 0) {
    res.status(400).send('aliases is required');
    return;
  }

  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.flushHeaders();

  res.write(`data: ${JSON.stringify({ type: 'connected', aliases: aliasKeys })}\n\n`);

  aliasKeys.forEach((alias) => {
    if (!sseListeners.has(alias)) {
      sseListeners.set(alias, new Set());
    }
    sseListeners.get(alias)!.add(res);
  });

  const interval = setInterval(() => {
    try {
      res.write(': keepalive\n\n');
    } catch {
      clearInterval(interval);
    }
  }, 20000);

  req.on('close', () => {
    clearInterval(interval);
    aliasKeys.forEach((alias) => {
      sseListeners.get(alias)?.delete(res);
      if (sseListeners.get(alias)?.size === 0) {
        sseListeners.delete(alias);
      }
    });
  });
});

app.get('/api/notifications', (req: Request, res: Response) => {
  const rawAliases = String(req.query.aliases || req.query.userId || '').trim();
  const queryKeys = Array.from(
    new Set(rawAliases.split(',').map(normNotifKey).filter((k) => k && k !== 'all')),
  );
  if (queryKeys.length === 0) {
    return res.json({ success: true, notifications: [] });
  }
  const querySet = new Set(queryKeys);
  const matching = dedupeServerNotifications(
    notificationsStore.filter((n) => {
      if (!hasRealServerNotifSender(n) || isServerNotifDismissed(n)) return false;
      const recKeys = collectNotificationRecipientKeys(n);
      const isForMe = recKeys.some((k) => querySet.has(k));
      if (!isForMe) return false;
      const senderKeys = collectNotificationSenderKeys(n);
      const isSelf = senderKeys.some((s) => querySet.has(s));
      return !isSelf;
    }),
  );

  return res.json({ success: true, notifications: matching });
});

app.post('/api/notifications', (req: Request, res: Response) => {
  const body = req.body || {};
  const rawType = String(body.type || 'activity').trim().toLowerCase();
  const type = rawType === 'bookmark' ? 'save' : rawType;
  const cleanSender = normNotifKey(body.senderId || body.fromUserId);
  const allSenderAliases = Array.from(
    new Set(
      [cleanSender, body.fromUserId, ...(Array.isArray(body.senderAliases) ? body.senderAliases : [])]
        .map(normNotifKey)
        .filter((k) => Boolean(k) && !FAKE_SERVER_NOTIF_ACTORS.has(k)),
    ),
  );
  const cleanRecipient = normNotifKey(body.recipientId || body.videoOwnerId);
  const allRecipientAliases = Array.from(
    new Set(
      [cleanRecipient, body.videoOwnerId, ...(Array.isArray(body.recipientAliases) ? body.recipientAliases : [])]
        .map(normNotifKey)
        .filter((k) => Boolean(k) && !FAKE_SERVER_NOTIF_ACTORS.has(k)),
    ),
  );

  if (allRecipientAliases.length === 0 || allSenderAliases.length === 0) {
    return res.status(400).json({ success: false, error: 'real sender and recipient aliases are required.' });
  }

  const senderSet = new Set(allSenderAliases);
  if (allRecipientAliases.some((r) => senderSet.has(r))) {
    return res.json({ success: true, skippedSelf: true });
  }

  const normVideoId = type === 'follow' ? '' : normalizeServerNotifVideoId(body.videoId);
  const commentText = String(body.comment ?? '');
  const normCommentText = type === 'comment' ? normalizeServerNotifComment(commentText || body.message) : '';
  const expandedIncomingSenders = new Set(expandServerNotifTokens(allSenderAliases));
  const expandedIncomingRecipients = expandServerNotifTokens(allRecipientAliases);

  // Permanently prevent duplicates for like/save/follow/share and identical comments (even after read or unlike->re-like)
  const existing = notificationsStore.find((n) => {
    const rawNType = String(n.type || '').toLowerCase();
    const nType = rawNType === 'bookmark' ? 'save' : (rawNType === 'comments' ? 'comment' : rawNType);
    if (nType !== type) return false;
    const nSenders = collectNotificationSenderKeys(n);
    const nRecipients = collectNotificationRecipientKeys(n);
    const matchSender = nSenders.some((s) => expandedIncomingSenders.has(s));
    const matchRecipient = nRecipients.some((r) => expandedIncomingRecipients.includes(r));
    if (!matchSender || !matchRecipient) return false;
    const nVid = nType === 'follow' ? '' : normalizeServerNotifVideoId(n.videoId);
    if (nType !== 'follow' && normVideoId !== nVid) return false;
    if (type === 'comment') {
      return normalizeServerNotifComment(n.comment || n.message) === normCommentText;
    }
    return true;
  });

  if (existing) {
    return res.json({ success: true, duplicate: true, notification: existing });
  }

  const notificationId = String(
    body.notificationId ||
      body.id ||
      `notif_${Date.now()}_${Math.random().toString(36).substring(2, 9)}`,
  );
  const displaySender = String(body.senderName || body.fromUserName || `@${allSenderAliases[0]}`);
  const senderAvatar = String(body.senderAvatar || body.fromProfilePic || body.avatar || '');

  const defaultTitle =
    type === 'like' ? `${displaySender} liked your video` :
    type === 'follow' ? `${displaySender} started following you` :
    type === 'comment' ? `${displaySender} commented: "${commentText.slice(0, 30)}"` :
    type === 'save' ? `${displaySender} saved your video` :
    type === 'share' ? `${displaySender} shared your video` :
    `${displaySender} sent you a notification`;

  const defaultMessage =
    type === 'like' ? 'liked your video' :
    type === 'follow' ? 'started following you' :
    type === 'comment' ? (commentText || 'commented on your video') :
    type === 'save' ? 'saved your video' :
    type === 'share' ? 'shared your video' :
    'new activity';

  const record: StoredNotificationRecord = {
    notificationId,
    id: notificationId,
    recipientId: cleanRecipient || allRecipientAliases[0],
    recipientAliases: allRecipientAliases,
    videoOwnerId: body.videoOwnerId ? String(body.videoOwnerId) : (cleanRecipient || allRecipientAliases[0]),
    senderId: cleanSender || allSenderAliases[0],
    senderAliases: allSenderAliases,
    senderName: displaySender,
    senderAvatar,
    fromUserId: `@${cleanSender || allSenderAliases[0]}`,
    fromUserName: displaySender,
    fromProfilePic: senderAvatar,
    avatar: senderAvatar,
    profilePic: senderAvatar,
    type,
    videoId: body.videoId ? String(body.videoId) : '',
    videoThumbnail: body.videoThumbnail ? String(body.videoThumbnail) : '',
    comment: commentText,
    title: String(body.title || defaultTitle),
    message: String(body.message || defaultMessage),
    isRead: false,
    read: false,
    createdAt: String(body.createdAt || new Date().toISOString()),
  };

  notificationsStore.unshift(record);
  if (notificationsStore.length > 1000) {
    notificationsStore = notificationsStore.slice(0, 1000);
  }
  saveNotificationsStore(notificationsStore);

  // Real-time SSE push to all recipient aliases
  const notifiedSockets = new Set<Response>();
  allRecipientAliases.forEach((alias) => {
    const clients = sseListeners.get(alias);
    if (!clients) return;
    clients.forEach((clientRes) => {
      if (notifiedSockets.has(clientRes)) return;
      notifiedSockets.add(clientRes);
      try {
        clientRes.write(`data: ${JSON.stringify({ type: 'new_notification', notification: record })}\n\n`);
      } catch {
        clients.delete(clientRes);
      }
    });
  });

  return res.json({ success: true, notification: record });
});

app.post('/api/notifications/read', (req: Request, res: Response) => {
  const { aliases, notificationIds } = req.body || {};
  const queryKeys = Array.isArray(aliases) ? aliases.map(normNotifKey).filter(Boolean) : [];
  if (queryKeys.length === 0) {
    return res.json({ success: true, updatedCount: 0 });
  }
  const querySet = new Set(queryKeys);
  const targetIdSet = Array.isArray(notificationIds) && notificationIds.length > 0
    ? new Set(notificationIds.map((id: any) => String(id)))
    : null;

  let updatedCount = 0;
  notificationsStore = notificationsStore.map((n) => {
    const nid = String(n.notificationId || n.id || '');
    if (targetIdSet && !targetIdSet.has(nid)) return n;
    const recKeys = collectNotificationRecipientKeys(n);
    if (!recKeys.some((k) => querySet.has(k))) return n;
    if (n.isRead && n.read) return n;
    updatedCount += 1;
    return { ...n, isRead: true, read: true };
  });

  if (updatedCount > 0) {
    saveNotificationsStore(notificationsStore);
  }
  return res.json({ success: true, updatedCount });
});

app.post('/api/notifications/preserve', (req: Request, res: Response) => {
  const { notifications } = req.body || {};
  if (!Array.isArray(notifications) || notifications.length === 0) {
    return res.json({ success: true, preservedCount: 0 });
  }
  const existingIds = new Set(notificationsStore.map((n) => String(n.notificationId || n.id || '').replace(/^fs_/, '')));
  let preservedCount = 0;
  for (const raw of notifications) {
    if (!raw || typeof raw !== 'object' || isServerNotifDismissed(raw) || !hasRealServerNotifSender(raw)) continue;
    const nid = String(raw.notificationId || raw.id || '').trim().replace(/^fs_/, '');
    if (!nid || existingIds.has(nid)) continue;
    const recKeys = collectNotificationRecipientKeys(raw);
    const senderKeys = collectNotificationSenderKeys(raw);
    if (recKeys.length === 0 || senderKeys.length === 0) continue;
    if (recKeys.some((r) => senderKeys.includes(r))) continue;
    existingIds.add(nid);
    notificationsStore.push({
      ...raw,
      notificationId: nid,
      id: nid,
      recipientId: raw.recipientId || recKeys[0],
      recipientAliases: recKeys,
      senderId: raw.senderId || senderKeys[0],
      senderAliases: senderKeys,
    });
    preservedCount += 1;
  }
  if (preservedCount > 0) {
    notificationsStore = dedupeServerNotifications(notificationsStore);
    saveNotificationsStore(notificationsStore);
  }
  return res.json({ success: true, preservedCount });
});

app.post('/api/notifications/dismiss', (req: Request, res: Response) => {
  const { aliases, notificationIds, dismissedItems } = req.body || {};
  const targetIds = new Set<string>(
    Array.isArray(notificationIds)
      ? notificationIds.map((id: any) => String(id || '').trim().replace(/^fs_/, '')).filter(Boolean)
      : [],
  );
  const items = Array.isArray(dismissedItems) ? dismissedItems : [];
  items.forEach((it: any) => {
    const ids = [it?.notificationId, it?.id, ...(Array.isArray(it?._mergedIds) ? it._mergedIds : [])]
      .map((v) => String(v || '').trim().replace(/^fs_/, ''))
      .filter(Boolean);
    ids.forEach((id) => targetIds.add(id));
  });

  if (targetIds.size === 0 && items.length === 0) {
    return res.json({ success: true, removedCount: 0 });
  }

  const queryKeys = Array.isArray(aliases) ? aliases.map(normNotifKey).filter(Boolean) : [];
  const querySet = new Set(queryKeys);

  // Collect signatures from any matching stored records + explicit dismissedItems
  const idSetAfter = new Set<string>(dismissedNotificationsStore.ids);
  targetIds.forEach((id) => idSetAfter.add(id));

  const sigCandidates: any[] = [...items];
  notificationsStore.forEach((n) => {
    const nid = String(n.notificationId || n.id || '').trim().replace(/^fs_/, '');
    if (nid && targetIds.has(nid)) {
      sigCandidates.push(n);
    }
  });

  sigCandidates.forEach((item) => {
    const sig = buildServerDismissSig(item);
    if (!sig) return;
    const dup = dismissedNotificationsStore.signatures.find((e) => {
      if (e.type !== sig.type) return false;
      if (sig.type.startsWith('support_') || sig.type === 'admin_message' || sig.type === 'message') {
        return e.title === sig.title && e.message === sig.message;
      }
      if (sig.type !== 'follow' && e.videoId !== sig.videoId) return false;
      if (sig.type === 'comment' && e.comment !== sig.comment) return false;
      return sig.actors.length > 0 && Array.isArray(e.actors) && sig.actors.some((a) => e.actors.includes(a));
    });
    if (dup) {
      dup.actors = Array.from(new Set([...(dup.actors || []), ...sig.actors]));
    } else {
      dismissedNotificationsStore.signatures.push(sig);
    }
  });
  dismissedNotificationsStore.ids = Array.from(idSetAfter);
  saveDismissedNotificationsStore(dismissedNotificationsStore);

  const beforeLen = notificationsStore.length;
  notificationsStore = notificationsStore.filter((n) => {
    const nid = String(n.notificationId || n.id || '').trim().replace(/^fs_/, '');
    const idMatch = nid && targetIds.has(nid);
    const sigMatch = isServerNotifDismissed(n);
    if (!idMatch && !sigMatch) return true;
    if (querySet.size > 0 && !idMatch) {
      const recKeys = collectNotificationRecipientKeys(n);
      if (recKeys.length > 0 && !recKeys.some((k) => querySet.has(k))) {
        return true;
      }
    }
    return false;
  });

  const removedCount = beforeLen - notificationsStore.length;
  if (removedCount > 0) {
    saveNotificationsStore(notificationsStore);
  }
  return res.json({ success: true, removedCount });
});

// ── Persistent Follow Graph API Routes ────────────────────────────────────
app.get('/api/follows', (req: Request, res: Response) => {
  const rawIdentities = String(req.query.identities || req.query.userId || '').trim();
  const queryKeys = Array.from(
    new Set(rawIdentities.split(',').map(normFollowKey).filter(Boolean)),
  );
  if (queryKeys.length === 0) {
    return res.json({ success: true, followers: [], following: [] });
  }
  const querySet = new Set(queryKeys);
  const followersMap = new Map<string, FollowStoreRecord>();
  const followingMap = new Map<string, FollowStoreRecord>();

  followsStore.forEach((rec) => {
    if (isSelfFollowRecord(rec)) return;
    const fAliases = Array.from(
      new Set(
        [
          ...(rec.followerAliases ?? []),
          rec.followerUserId,
          rec.followerId,
          rec.followerEmail,
          rec.followerUid,
        ]
          .map(normFollowKey)
          .filter(Boolean),
      ),
    );
    const tAliases = Array.from(
      new Set(
        [
          ...(rec.followeeAliases ?? []),
          rec.followeeId,
          rec.followeeBackendId,
          rec.followeeEmail,
        ]
          .map(normFollowKey)
          .filter(Boolean),
      ),
    );

    const matchesFollower = fAliases.some((k) => querySet.has(k));
    const matchesFollowee = tAliases.some((k) => querySet.has(k));

    if (matchesFollowee && !matchesFollower) {
      const fKey = fAliases[0];
      if (fKey && !querySet.has(fKey) && !followersMap.has(fKey)) {
        followersMap.set(fKey, rec);
      }
    }
    if (matchesFollower && !matchesFollowee) {
      const tKey = tAliases[0];
      if (tKey && !querySet.has(tKey) && !followingMap.has(tKey)) {
        followingMap.set(tKey, rec);
      }
    }
  });

  return res.json({
    success: true,
    followers: Array.from(followersMap.values()),
    following: Array.from(followingMap.values()),
  });
});

app.post('/api/follows/sync', (req: Request, res: Response) => {
  const { isFollowing, followerAliases, followeeAliases, record } = req.body || {};
  const fKeys = Array.from(
    new Set(
      [
        ...(Array.isArray(followerAliases) ? followerAliases : []),
        record?.followerUserId,
        record?.followerId,
        record?.followerEmail,
        record?.followerUid,
      ]
        .map(normFollowKey)
        .filter(Boolean),
    ),
  );
  const tKeys = Array.from(
    new Set(
      [
        ...(Array.isArray(followeeAliases) ? followeeAliases : []),
        record?.followeeId,
        record?.followeeBackendId,
        record?.followeeEmail,
      ]
        .map(normFollowKey)
        .filter(Boolean),
    ),
  );

  if (fKeys.length === 0 || tKeys.length === 0) {
    return res.status(400).json({ success: false, error: 'follower and followee aliases are required.' });
  }
  const fSet = new Set(fKeys);
  const tSet = new Set(tKeys);
  if (tKeys.some((k) => fSet.has(k))) {
    // Strip any legacy self-follow match and do not store a self-follow
    followsStore = followsStore.filter((rec) => !isSelfFollowRecord(rec));
    saveFollowsStore(followsStore);
    return res.json({ success: true, skippedSelfFollow: true });
  }

  followsStore = followsStore.filter((rec) => {
    if (isSelfFollowRecord(rec)) return false;
    const recF = [
      ...(rec.followerAliases ?? []),
      rec.followerUserId,
      rec.followerId,
      rec.followerEmail,
      rec.followerUid,
    ].map(normFollowKey);
    const recT = [
      ...(rec.followeeAliases ?? []),
      rec.followeeId,
      rec.followeeBackendId,
      rec.followeeEmail,
    ].map(normFollowKey);
    const samePair = recF.some((k) => fSet.has(k)) && recT.some((k) => tSet.has(k));
    return !samePair;
  });

  if (isFollowing) {
    const nowIso = new Date().toISOString();
    const canonicalFollower = normFollowKey(record?.followerUserId || record?.followerId || fKeys[0]);
    const canonicalFollowee = normFollowKey(record?.followeeId || tKeys[0]);
    const newRecord: FollowStoreRecord = {
      id: `${canonicalFollower}__${canonicalFollowee}`,
      followerId: String(record?.followerId || `@${canonicalFollower}`),
      followerUserId: String(record?.followerUserId || record?.followerId || `@${canonicalFollower}`),
      followerEmail: record?.followerEmail ? String(record.followerEmail) : undefined,
      followerUid: record?.followerUid ? String(record.followerUid) : undefined,
      followerName: String(record?.followerName || record?.followerId || `@${canonicalFollower}`),
      followerPic: String(record?.followerPic || ''),
      followerAliases: fKeys,
      followeeId: String(record?.followeeId || `@${canonicalFollowee}`),
      followeeBackendId: record?.followeeBackendId ? String(record.followeeBackendId) : undefined,
      followeeEmail: record?.followeeEmail ? String(record.followeeEmail) : undefined,
      followeeName: String(record?.followeeName || record?.followeeId || `@${canonicalFollowee}`),
      followeePic: String(record?.followeePic || ''),
      followeeAliases: tKeys,
      createdAt: String(record?.createdAt || nowIso),
      updatedAt: nowIso,
    };
    followsStore.push(newRecord);
  }

  saveFollowsStore(followsStore);
  return res.json({ success: true, count: followsStore.length });
});

app.post('/api/follows/migrate', (req: Request, res: Response) => {
  const incoming = Array.isArray(req.body?.records) ? req.body.records : [];
  let changed = false;
  incoming.forEach((raw: any) => {
    if (!raw || typeof raw !== 'object') return;
    const fKeys = Array.from(
      new Set(
        [raw.followerUserId, raw.followerId, raw.followerEmail, raw.followerUid]
          .map(normFollowKey)
          .filter(Boolean),
      ),
    );
    const tKeys = Array.from(
      new Set(
        [raw.followeeId, raw.followeeBackendId, raw.followeeEmail]
          .map(normFollowKey)
          .filter(Boolean),
      ),
    );
    if (fKeys.length === 0 || tKeys.length === 0) return;
    const fSet = new Set(fKeys);
    if (tKeys.some((k) => fSet.has(k))) return; // Never migrate self-follow
    const tSet = new Set(tKeys);
    const exists = followsStore.some((rec) => {
      const recF = [...(rec.followerAliases ?? []), rec.followerUserId, rec.followerId].map(normFollowKey);
      const recT = [...(rec.followeeAliases ?? []), rec.followeeId, rec.followeeBackendId].map(normFollowKey);
      return recF.some((k) => fSet.has(k)) && recT.some((k) => tSet.has(k));
    });
    if (!exists) {
      const nowIso = new Date().toISOString();
      const canonicalFollower = normFollowKey(raw.followerUserId || raw.followerId || fKeys[0]);
      const canonicalFollowee = normFollowKey(raw.followeeId || tKeys[0]);
      followsStore.push({
        id: `${canonicalFollower}__${canonicalFollowee}`,
        followerId: String(raw.followerId || `@${canonicalFollower}`),
        followerUserId: String(raw.followerUserId || raw.followerId || `@${canonicalFollower}`),
        followerEmail: raw.followerEmail ? String(raw.followerEmail) : undefined,
        followerUid: raw.followerUid ? String(raw.followerUid) : undefined,
        followerName: String(raw.followerName || raw.followerId || `@${canonicalFollower}`),
        followerPic: String(raw.followerPic || ''),
        followerAliases: fKeys,
        followeeId: String(raw.followeeId || `@${canonicalFollowee}`),
        followeeBackendId: raw.followeeBackendId ? String(raw.followeeBackendId) : undefined,
        followeeEmail: raw.followeeEmail ? String(raw.followeeEmail) : undefined,
        followeeName: String(raw.followeeName || raw.followeeId || `@${canonicalFollowee}`),
        followeePic: String(raw.followeePic || ''),
        followeeAliases: tKeys,
        createdAt: String(raw.createdAt || nowIso),
        updatedAt: nowIso,
      });
      changed = true;
    }
  });
  if (changed) saveFollowsStore(followsStore);
  return res.json({ success: true, total: followsStore.length });
});

app.post('/api/follows/rename-owner', (req: Request, res: Response) => {
  const {
    oldIdentities,
    uid,
    email,
    mastiBaseUserId,
    newHandle,
    newName,
    newAvatar,
  } = req.body || {};
  const cleanNewHandle = String(newHandle ?? '').trim();
  const normNew = normFollowKey(cleanNewHandle);
  if (!normNew) {
    return res.status(400).json({ success: false, error: 'newHandle is required.' });
  }
  const handleWithAt = cleanNewHandle.startsWith('@') ? cleanNewHandle : `@${cleanNewHandle}`;
  const ownerAliasSet = new Set(
    [
      ...(Array.isArray(oldIdentities) ? oldIdentities : []),
      uid,
      email,
      mastiBaseUserId,
      handleWithAt,
    ]
      .map(normFollowKey)
      .filter(Boolean),
  );
  const mergedOwnerAliases = Array.from(ownerAliasSet);
  let updatedCount = 0;

  followsStore = followsStore
    .map((rec) => {
      const fAliases = [
        ...(rec.followerAliases ?? []),
        rec.followerUserId,
        rec.followerId,
        rec.followerEmail,
        rec.followerUid,
      ]
        .map(normFollowKey)
        .filter(Boolean);
      const tAliases = [
        ...(rec.followeeAliases ?? []),
        rec.followeeId,
        rec.followeeBackendId,
        rec.followeeEmail,
      ]
        .map(normFollowKey)
        .filter(Boolean);

      const isFollowerOwner = fAliases.some((k) => ownerAliasSet.has(k));
      const isFolloweeOwner = tAliases.some((k) => ownerAliasSet.has(k));
      if (!isFollowerOwner && !isFolloweeOwner) return rec;
      if (isFollowerOwner && isFolloweeOwner) return null;

      updatedCount += 1;
      const next: FollowStoreRecord = { ...rec, updatedAt: new Date().toISOString() };
      if (isFollowerOwner) {
        next.followerId = handleWithAt;
        next.followerUserId = handleWithAt;
        if (newName) next.followerName = String(newName);
        if (newAvatar) next.followerPic = String(newAvatar);
        if (uid) next.followerUid = String(uid);
        if (email) next.followerEmail = String(email);
        next.followerAliases = Array.from(new Set([...fAliases, ...mergedOwnerAliases]));
      }
      if (isFolloweeOwner) {
        next.followeeId = handleWithAt;
        if (newName) next.followeeName = String(newName);
        if (newAvatar) next.followeePic = String(newAvatar);
        if (mastiBaseUserId) next.followeeBackendId = String(mastiBaseUserId);
        if (email) next.followeeEmail = String(email);
        next.followeeAliases = Array.from(new Set([...tAliases, ...mergedOwnerAliases]));
      }
      const nextFKey = normFollowKey(next.followerUserId || next.followerId);
      const nextTKey = normFollowKey(next.followeeId || next.followeeBackendId);
      if (nextFKey && nextTKey) {
        next.id = `${nextFKey}__${nextTKey}`;
      }
      return next;
    })
    .filter((rec): rec is FollowStoreRecord => Boolean(rec) && !isSelfFollowRecord(rec));

  if (updatedCount > 0) saveFollowsStore(followsStore);
  return res.json({ success: true, updatedCount });
});

const VIDEO_SONGS_FILE = path.join(DATA_DIR, 'video_songs.json');

interface StoredVideoSongRecord {
  song: {
    id: string;
    title: string;
    artist: string;
    duration?: string;
    cover?: string;
    audioUrl?: string;
    trimStart?: number;
    trimEnd?: number;
    genre?: string;
    userId?: string;
    isOriginal?: boolean;
  };
  musicVol?: number;
  originalSoundVol?: number;
  updatedAt: string;
}

const loadVideoSongsStore = (): Record<string, StoredVideoSongRecord> => {
  try {
    if (fs.existsSync(VIDEO_SONGS_FILE)) {
      const raw = fs.readFileSync(VIDEO_SONGS_FILE, 'utf-8');
      const parsed = JSON.parse(raw);
      if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) {
        return parsed;
      }
    }
  } catch (err) {
    console.warn('[Server] Could not read video songs store:', err);
  }
  return {};
};

const saveVideoSongsStore = (data: Record<string, StoredVideoSongRecord>) => {
  try {
    fs.writeFileSync(VIDEO_SONGS_FILE, JSON.stringify(data, null, 2), 'utf-8');
  } catch (err) {
    console.warn('[Server] Could not save video songs store:', err);
  }
};

let videoSongsStore = loadVideoSongsStore();

const DELETED_VIDEOS_FILE = path.join(DATA_DIR, 'deleted_videos.json');

const loadDeletedVideosStore = (): Record<string, boolean> => {
  try {
    if (fs.existsSync(DELETED_VIDEOS_FILE)) {
      const raw = fs.readFileSync(DELETED_VIDEOS_FILE, 'utf-8');
      const parsed = JSON.parse(raw);
      if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) {
        return parsed;
      }
    }
  } catch (err) {
    console.warn('[Server] Could not read deleted videos store:', err);
  }
  return {};
};

const saveDeletedVideosStore = (data: Record<string, boolean>) => {
  try {
    fs.writeFileSync(DELETED_VIDEOS_FILE, JSON.stringify(data, null, 2), 'utf-8');
  } catch (err) {
    console.warn('[Server] Could not save deleted videos store:', err);
  }
};

let deletedVideosStore: Record<string, boolean> = loadDeletedVideosStore();

const buildVideoIdentityKeys = (videoId?: unknown, videoUrl?: unknown): string[] => {
  const keys: string[] = [];
  const rawId = String(videoId || '').trim();
  if (rawId) {
    keys.push(rawId);
    const cleanId = rawId.replace(/^db-/, '');
    if (cleanId) {
      keys.push(cleanId);
      keys.push(`db-${cleanId}`);
    }
  }
  const rawUrl = String(videoUrl || '').trim();
  if (rawUrl) {
    keys.push(rawUrl);
    keys.push(rawUrl.split('?')[0]);
  }
  return Array.from(new Set(keys.filter(Boolean)));
};

const isServerVideoDeleted = (item: any): boolean => {
  if (!item || typeof item !== 'object') return false;
  const keys = [
    ...buildVideoIdentityKeys(item.id, item.videoUrl || item.url || item.mediaUrl),
    ...buildVideoIdentityKeys(item.mediaId, item.streamUrl),
  ];
  return keys.some((k) => Boolean(deletedVideosStore[k]));
};

app.get('/api/videos/deleted', (_req: Request, res: Response) => {
  return res.json({ success: true, deletedIds: deletedVideosStore });
});

const handleServerDeleteVideo = async (
  rawId: string,
  videoUrl?: string,
  userId?: string,
  userEmail?: string,
) => {
  const cleanId = String(rawId || '').trim().replace(/^db-/, '');
  const keys = buildVideoIdentityKeys(cleanId || rawId, videoUrl);
  keys.forEach((k) => {
    deletedVideosStore[k] = true;
    if (videoSongsStore[k]) {
      delete videoSongsStore[k];
    }
  });
  saveDeletedVideosStore(deletedVideosStore);
  saveVideoSongsStore(videoSongsStore);

  let mastiBaseDeleted = false;
  if (cleanId) {
    const queryParts: string[] = [];
    if (userId) queryParts.push(`userId=${encodeURIComponent(userId)}`);
    if (userEmail) queryParts.push(`userEmail=${encodeURIComponent(userEmail)}`);
    const qs = queryParts.length > 0 ? `?${queryParts.join('&')}` : '';
    const headers: Record<string, string> = {
      'x-api-key': 'mb_live_rajkali_8789_7b31a6fe89d24643',
      'Content-Type': 'application/json',
    };
    const bodyPayload = JSON.stringify({
      id: cleanId,
      mediaId: cleanId,
      videoId: cleanId,
      ...(userId ? { userId } : {}),
      ...(userEmail ? { userEmail } : {}),
    });

    const targets = [
      `https://masti-basen.lovable.app/api/v1/media/${encodeURIComponent(cleanId)}${qs}`,
      `https://masti-basen.lovable.app/api/videos/${encodeURIComponent(cleanId)}${qs}`,
      `https://masti-basen.lovable.app/api/v1/videos/${encodeURIComponent(cleanId)}${qs}`,
    ];

    const results = await Promise.all(
      targets.map(async (targetUrl) => {
        try {
          const r = await fetch(targetUrl, {
            method: 'DELETE',
            headers,
            body: bodyPayload,
          });
          if (r.ok) return true;
        } catch {}
        return false;
      }),
    );
    mastiBaseDeleted = results.some(Boolean);
  }

  return { cleanId, keys, mastiBaseDeleted };
};

app.post('/api/videos/delete', async (req: Request, res: Response) => {
  try {
    const { videoId, mediaId, id, videoUrl, userId, userEmail } = req.body || {};
    const targetId = String(videoId || mediaId || id || '').trim();
    if (!targetId && !videoUrl) {
      return res.status(400).json({ success: false, error: 'Missing videoId' });
    }
    const result = await handleServerDeleteVideo(
      targetId,
      videoUrl ? String(videoUrl) : undefined,
      userId ? String(userId) : undefined,
      userEmail ? String(userEmail) : undefined,
    );
    return res.json({
      success: true,
      ...result,
      deletedIds: deletedVideosStore,
    });
  } catch (err: any) {
    console.error('[Server] Video delete error:', err);
    return res.status(500).json({ success: false, error: err?.message || 'Delete failed' });
  }
});

app.delete('/api/videos/:id', async (req: Request, res: Response) => {
  try {
    const targetId = String(req.params.id || '').trim();
    const videoUrl = req.query.videoUrl ? String(req.query.videoUrl) : undefined;
    const userId = req.query.userId ? String(req.query.userId) : undefined;
    const userEmail = req.query.userEmail ? String(req.query.userEmail) : undefined;
    const result = await handleServerDeleteVideo(targetId, videoUrl, userId, userEmail);
    return res.json({
      success: true,
      ...result,
      deletedIds: deletedVideosStore,
    });
  } catch (err: any) {
    return res.status(500).json({ success: false, error: err?.message || 'Delete failed' });
  }
});

app.get('/api/videos/song-metadata', (_req: Request, res: Response) => {
  return res.json({ success: true, metadata: videoSongsStore });
});

app.post('/api/videos/song-metadata', (req: Request, res: Response) => {
  try {
    const { videoId, videoUrl, song, musicVol, originalSoundVol } = req.body || {};
    if (!song || typeof song !== 'object') {
      return res.status(400).json({ success: false, error: 'Missing song payload' });
    }
    const record: StoredVideoSongRecord = {
      song,
      musicVol: typeof musicVol === 'number' ? musicVol : undefined,
      originalSoundVol: typeof originalSoundVol === 'number' ? originalSoundVol : undefined,
      updatedAt: new Date().toISOString(),
    };
    const keys: string[] = [];
    if (videoId) {
      const rawId = String(videoId).trim();
      const cleanId = rawId.replace(/^db-/, '');
      if (rawId) keys.push(rawId);
      if (cleanId) {
        keys.push(cleanId);
        keys.push(`db-${cleanId}`);
      }
    }
    if (videoUrl) {
      const rawUrl = String(videoUrl).trim();
      if (rawUrl) {
        keys.push(rawUrl);
        keys.push(rawUrl.split('?')[0]);
      }
    }
    keys.forEach((k) => {
      videoSongsStore[k] = record;
    });
    saveVideoSongsStore(videoSongsStore);
    return res.json({ success: true, keys });
  } catch (err: any) {
    console.error('[Server] Could not save video song metadata:', err);
    return res.status(500).json({ success: false, error: err?.message || 'Failed' });
  }
});

app.post(
  '/api/media/mux',
  express.raw({ type: () => true, limit: '150mb' }),
  async (req: Request, res: Response) => {
    const tmpId = `${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
    const tmpDir = os.tmpdir();
    const mimeHint = String(req.query.mimeType || req.headers['content-type'] || '').toLowerCase();
    const inExt = mimeHint.includes('webm') ? '.webm' : mimeHint.includes('quicktime') ? '.mov' : '.mp4';
    const inVideoPath = path.join(tmpDir, `masti_in_${tmpId}${inExt}`);
    const inAudioPath = path.join(tmpDir, `masti_aud_${tmpId}.audio`);
    const outVideoPath = path.join(tmpDir, `masti_out_${tmpId}.mp4`);

    const cleanup = () => {
      [inVideoPath, inAudioPath, outVideoPath].forEach((p) => {
        try {
          if (fs.existsSync(p)) fs.unlinkSync(p);
        } catch {}
      });
    };

    try {
      let videoBuf: Buffer;
      if (Buffer.isBuffer(req.body)) {
        videoBuf = req.body;
      } else if (req.body instanceof Uint8Array || req.body instanceof ArrayBuffer) {
        videoBuf = Buffer.from(req.body as any);
      } else if (typeof req.body === 'string') {
        videoBuf = Buffer.from(req.body, 'binary');
      } else {
        videoBuf = Buffer.alloc(0);
      }
      if (!videoBuf || videoBuf.length < 100) {
        return res.status(400).json({ success: false, error: 'Empty video body' });
      }
      fs.writeFileSync(inVideoPath, videoBuf);

      const audioUrl = String(req.query.audioUrl || '').trim();
      const trimStart = Math.max(0, Number(req.query.trimStart || 0) || 0);
      const rawTrimEnd = req.query.trimEnd !== undefined ? Number(req.query.trimEnd) : NaN;
      const trimEnd = Number.isFinite(rawTrimEnd) && rawTrimEnd > trimStart ? rawTrimEnd : null;
      const musicVol = Math.max(0, Math.min(100, req.query.musicVol !== undefined ? Number(req.query.musicVol) : 100)) / 100;
      const originalSoundVol = Math.max(0, Math.min(100, req.query.originalSoundVol !== undefined ? Number(req.query.originalSoundVol) : 0)) / 100;

      let segments: Array<{ start: number; end: number }> = [];
      if (req.query.segments) {
        try {
          const parsedSegs = JSON.parse(String(req.query.segments));
          if (Array.isArray(parsedSegs)) {
            segments = parsedSegs
              .filter((s: any) => Number.isFinite(Number(s?.start)) && Number.isFinite(Number(s?.end)) && Number(s.end) > Number(s.start))
              .map((s: any) => ({ start: Math.max(0, Number(s.start)), end: Math.min(1, Number(s.end)) }));
          }
        } catch {}
      }

      // Probe input video streams and duration
      let videoCodec = '';
      let hasVideoAudio = false;
      let rawDuration = 0;
      try {
        const { stdout } = await execFileAsync('ffprobe', [
          '-v', 'quiet',
          '-print_format', 'json',
          '-show_format',
          '-show_streams',
          inVideoPath,
        ]);
        const probe = JSON.parse(stdout || '{}');
        const streams = Array.isArray(probe.streams) ? probe.streams : [];
        const vStream = streams.find((s: any) => s.codec_type === 'video');
        const aStream = streams.find((s: any) => s.codec_type === 'audio');
        videoCodec = String(vStream?.codec_name || '').toLowerCase();
        hasVideoAudio = Boolean(aStream);
        const durCandidates = [
          Number(probe?.format?.duration),
          Number(vStream?.duration),
          Number(aStream?.duration),
        ].filter((n) => Number.isFinite(n) && n > 0.1);
        if (durCandidates.length > 0) {
          rawDuration = durCandidates[0];
        }
      } catch (probeErr) {
        console.warn('[Mux] ffprobe failed:', probeErr);
      }

      let hasAudioFile = false;
      if (audioUrl && /^https?:\/\//i.test(audioUrl)) {
        try {
          const audRes = await fetch(audioUrl, { headers: { 'User-Agent': 'Mozilla/5.0' } });
          if (audRes.ok) {
            const audArr = Buffer.from(await audRes.arrayBuffer());
            if (audArr.length > 100) {
              fs.writeFileSync(inAudioPath, audArr);
              hasAudioFile = true;
            }
          }
        } catch (audErr) {
          console.warn('[Mux] Could not download song audioUrl:', audErr);
        }
      }

      const hasSegmentEdits =
        rawDuration > 0.2 &&
        segments.length > 0 &&
        (segments.length > 1 || segments[0].start > 0.005 || segments[0].end < 0.995);

      const ffArgs: string[] = ['-y', '-i', inVideoPath];
      if (hasAudioFile) {
        if (trimStart > 0) {
          ffArgs.push('-ss', trimStart.toFixed(3));
        }
        ffArgs.push('-i', inAudioPath);
      }

      const filters: string[] = [];
      let vMap = '0:v:0';
      let aMap = '';

      if (hasSegmentEdits) {
        const vParts: string[] = [];
        const aParts: string[] = [];
        segments.forEach((seg, idx) => {
          const st = (seg.start * rawDuration).toFixed(3);
          const en = (seg.end * rawDuration).toFixed(3);
          filters.push(`[0:v]trim=start=${st}:end=${en},setpts=PTS-STARTPTS[vseg${idx}]`);
          vParts.push(`[vseg${idx}]`);
          if (hasVideoAudio && originalSoundVol > 0.01) {
            filters.push(`[0:a]atrim=start=${st}:end=${en},asetpts=PTS-STARTPTS[aseg${idx}]`);
            aParts.push(`[aseg${idx}]`);
          }
        });
        filters.push(`${vParts.join('')}concat=n=${vParts.length}:v=1:a=0[vout]`);
        vMap = '[vout]';
        if (aParts.length > 0) {
          filters.push(`${aParts.join('')}concat=n=${aParts.length}:v=0:a=1[aorig_raw]`);
        }
      }

      const origAudioLabel = hasSegmentEdits ? '[aorig_raw]' : '[0:a]';

      if (hasAudioFile && musicVol > 0.01) {
        const clipFilter = trimEnd !== null
          ? `atrim=start=0:end=${(trimEnd - trimStart).toFixed(3)},asetpts=PTS-STARTPTS,aloop=loop=-1:size=2000000000,`
          : `asetpts=PTS-STARTPTS,aloop=loop=-1:size=2000000000,`;
        if (hasVideoAudio && originalSoundVol > 0.01 && (!hasSegmentEdits || filters.some((f) => f.includes('[aorig_raw]')))) {
          filters.push(`${origAudioLabel}volume=${originalSoundVol.toFixed(3)}[aorig_vol]`);
          filters.push(`[1:a]${clipFilter}volume=${musicVol.toFixed(3)}[amusic_vol]`);
          filters.push(`[aorig_vol][amusic_vol]amix=inputs=2:duration=first:dropout_transition=0[aout]`);
          aMap = '[aout]';
        } else {
          filters.push(`[1:a]${clipFilter}volume=${musicVol.toFixed(3)}[aout]`);
          aMap = '[aout]';
        }
      } else if (hasVideoAudio && originalSoundVol > 0.01) {
        if (Math.abs(originalSoundVol - 1) > 0.02 || hasSegmentEdits) {
          filters.push(`${origAudioLabel}volume=${originalSoundVol.toFixed(3)}[aout]`);
          aMap = '[aout]';
        } else {
          aMap = '0:a:0';
        }
      }

      if (filters.length > 0) {
        ffArgs.push('-filter_complex', filters.join(';'));
      }

      ffArgs.push('-map', vMap);
      if (aMap) {
        ffArgs.push('-map', aMap);
        ffArgs.push('-c:a', 'aac', '-b:a', '128k', '-ac', '2', '-ar', '44100');
      } else {
        ffArgs.push('-an');
      }

      if (!hasSegmentEdits && videoCodec === 'h264') {
        ffArgs.push('-c:v', 'copy');
      } else {
        ffArgs.push('-c:v', 'libx264', '-preset', 'ultrafast', '-crf', '24', '-pix_fmt', 'yuv420p');
      }

      if (hasAudioFile && aMap) {
        ffArgs.push('-shortest');
      }
      ffArgs.push('-movflags', '+faststart', outVideoPath);

      await execFileAsync('ffmpeg', ffArgs, { maxBuffer: 20 * 1024 * 1024 });

      let outDuration = rawDuration;
      try {
        const { stdout: outProbeRaw } = await execFileAsync('ffprobe', [
          '-v', 'quiet',
          '-print_format', 'json',
          '-show_format',
          outVideoPath,
        ]);
        const outProbe = JSON.parse(outProbeRaw || '{}');
        const d = Number(outProbe?.format?.duration);
        if (Number.isFinite(d) && d > 0.1) outDuration = d;
      } catch {}

      const outBuf = fs.readFileSync(outVideoPath);
      res.setHeader('Content-Type', 'video/mp4');
      res.setHeader('X-Audio-Muxed', hasAudioFile && musicVol > 0.01 && Boolean(aMap) ? '1' : '0');
      if (outDuration > 0) {
        res.setHeader('X-Video-Duration', String(Number(outDuration.toFixed(2))));
      }
      return res.status(200).send(outBuf);
    } catch (err: any) {
      console.error('[Mux] FFmpeg mux error:', err?.stderr || err?.message || err);
      return res.status(500).json({ success: false, error: err?.message || 'Mux failed' });
    } finally {
      cleanup();
    }
  },
);

app.get('/api/videos/feed', async (req: Request, res: Response) => {
  try {
    const page = req.query.page || '1';
    const limit = req.query.limit || '20';
    const viewerId = req.query.viewerId ? `&viewerId=${encodeURIComponent(String(req.query.viewerId))}` : '';
    const upstream = await fetch(
      `https://masti-basen.lovable.app/api/videos/feed?page=${page}&limit=${limit}${viewerId}`,
      {
        headers: {
          'x-api-key': 'mb_live_rajkali_8789_7b31a6fe89d24643',
          'Content-Type': 'application/json',
        },
      }
    );
    if (!upstream.ok) {
      return res.status(upstream.status).json({ success: false, data: [] });
    }
    const data = await upstream.json();
    const listKey = Array.isArray(data?.data) ? 'data' : Array.isArray(data?.videos) ? 'videos' : null;
    if (listKey && Array.isArray(data[listKey])) {
      data[listKey] = data[listKey]
        .filter((item: any) => !isServerVideoDeleted(item))
        .map((item: any) => {
          const idStr = String(item?.id || '').trim();
          const cleanId = idStr.replace(/^db-/, '');
          const itemUrl = String(item?.videoUrl || item?.url || item?.mediaUrl || '').trim();
          const saved =
            videoSongsStore[idStr] ||
            videoSongsStore[cleanId] ||
            videoSongsStore[`db-${cleanId}`] ||
            videoSongsStore[itemUrl] ||
            videoSongsStore[itemUrl.split('?')[0]];
          if (!saved?.song) return item;
          return {
            ...item,
            song: saved.song,
            musicCover: saved.song.cover || item.musicCover,
            musicVol: saved.musicVol ?? item.musicVol,
            originalSoundVol: saved.originalSoundVol ?? item.originalSoundVol,
          };
        });
    }
    return res.status(200).json(data);
  } catch (err: any) {
    console.error('[Server Proxy] Feed error:', err);
    return res.status(200).json({ success: true, count: 0, data: [] });
  }
});

app.get('/api/videos/download', async (req: Request, res: Response) => {
  const rawUrl = String(req.query.url || '').trim();
  const rawName = String(req.query.filename || `masti_video_${Date.now()}.mp4`).trim();
  const cleanName = rawName.replace(/[^a-zA-Z0-9._-]/g, '_') || `masti_video_${Date.now()}.mp4`;
  const safeFilename = cleanName.toLowerCase().endsWith('.mp4') ? cleanName : `${cleanName}.mp4`;

  if (!rawUrl || !/^https?:\/\//i.test(rawUrl)) {
    return res.status(400).json({ success: false, error: 'Valid http(s) video URL is required.' });
  }

  try {
    const upstream = await fetch(rawUrl, {
      headers: {
        'User-Agent': 'Mozilla/5.0',
      },
      redirect: 'follow',
    });

    if (!upstream.ok) {
      return res.status(upstream.status).json({ success: false, error: `Upstream returned ${upstream.status}` });
    }

    const upstreamType = String(upstream.headers.get('content-type') || '').toLowerCase();
    const contentType = upstreamType.startsWith('video/') ? upstreamType : 'video/mp4';
    const contentLength = upstream.headers.get('content-length');

    res.setHeader('Content-Type', contentType);
    res.setHeader('Content-Disposition', `attachment; filename="${safeFilename}"`);
    res.setHeader('Cache-Control', 'no-store');
    if (contentLength) {
      res.setHeader('Content-Length', contentLength);
    }

    if (upstream.body && typeof (Readable as any).fromWeb === 'function') {
      const nodeStream = (Readable as any).fromWeb(upstream.body as any);
      nodeStream.on('error', (streamErr: any) => {
        console.warn('[Server] Video download stream error:', streamErr);
        if (!res.headersSent) {
          res.status(500).end();
        } else {
          res.end();
        }
      });
      nodeStream.pipe(res);
      return;
    }

    const buffer = Buffer.from(await upstream.arrayBuffer());
    return res.status(200).send(buffer);
  } catch (err: any) {
    console.error('[Server] Video download proxy error:', err);
    if (!res.headersSent) {
      return res.status(500).json({ success: false, error: err?.message || 'Video download failed' });
    }
    res.end();
  }
});

function decryptSaavnUrl(enc: string | undefined): string | null {
  try {
    if (!enc) return null;
    const key = CryptoJS.enc.Utf8.parse("38346591");
    const decrypted = CryptoJS.DES.decrypt(
      { ciphertext: CryptoJS.enc.Base64.parse(enc) },
      key,
      { mode: CryptoJS.mode.ECB, padding: CryptoJS.pad.Pkcs7 }
    );
    const url = decrypted.toString(CryptoJS.enc.Utf8);
    if (!url || !url.startsWith('http')) return null;
    return url.replace('_96.mp4', '_160.mp4');
  } catch (e) {
    return null;
  }
}

app.get('/api/music/search', async (req: Request, res: Response) => {
  const q = String(req.query.q || '').trim();
  if (!q) return res.json({ success: true, results: [] });

  try {
    // 1. Search JioSaavn for 100% full songs
    const saavnUrl = `https://www.jiosaavn.com/api.php?__call=search.getResults&q=${encodeURIComponent(q)}&_format=json&_marker=0&api_version=4&ctx=web6dot0&n=25&p=1`;
    const saavnPromise = fetch(saavnUrl, { headers: { 'User-Agent': 'Mozilla/5.0' } })
      .then(r => r.json())
      .then(data => {
        const results = (data.results || []).map((r: any) => {
          const enc = r.more_info?.encrypted_media_url;
          const audioUrl = decryptSaavnUrl(enc);
          const durSec = parseInt(r.more_info?.duration || '0', 10);
          return {
            id: 'saavn_' + r.id,
            title: (r.title || '').replace(/&quot;/g, '"').replace(/&#039;/g, "'").replace(/&amp;/g, '&'),
            artist: (r.more_info?.singers || r.subtitle || '').replace(/&quot;/g, '"').replace(/&#039;/g, "'").replace(/&amp;/g, '&'),
            duration: durSec > 0 ? `${Math.floor(durSec / 60)}:${String(durSec % 60).padStart(2, '0')}` : '3:00',
            trackTimeMillis: durSec * 1000,
            cover: (r.image || '').replace('150x150', '350x350').replace('50x50', '350x350'),
            genre: r.language || 'Music',
            audioUrl: audioUrl,
            isFullSong: true,
          };
        }).filter((s: any) => Boolean(s.audioUrl));
        return results;
      })
      .catch((err) => {
        console.warn('[Server] Saavn search error:', err);
        return [];
      });

    // 2. Search iTunes as parallel fallback
    const itunesUrl = `https://itunes.apple.com/search?term=${encodeURIComponent(q)}&media=music&entity=song&limit=15&country=in`;
    const itunesPromise = fetch(itunesUrl)
      .then(r => r.json())
      .then(data => {
        return (data.results || [])
          .filter((r: any) => r.previewUrl)
          .map((r: any) => ({
            id: 'itunes_' + r.trackId,
            title: r.trackName,
            artist: r.artistName,
            trackTimeMillis: r.trackTimeMillis,
            duration: r.trackTimeMillis
              ? Math.floor(r.trackTimeMillis / 60000) + ':' + String(Math.floor((r.trackTimeMillis % 60000) / 1000)).padStart(2, '0')
              : '—',
            cover: (r.artworkUrl100 || r.artworkUrl60 || '').replace('100x100', '300x300'),
            genre: r.primaryGenreName || 'Music',
            audioUrl: r.previewUrl,
            isItunes: true,
          }));
      })
      .catch((err) => {
        console.warn('[Server] iTunes search error:', err);
        return [];
      });

    const [saavnResults, itunesResults] = await Promise.all([saavnPromise, itunesPromise]);

    // Full songs from JioSaavn come first, followed by non-duplicate iTunes songs
    const combined = [...saavnResults];
    for (const ir of itunesResults) {
      const exists = combined.some(
        s => s.title.toLowerCase() === ir.title.toLowerCase()
      );
      if (!exists) {
        combined.push(ir);
      }
    }

    return res.json({ success: true, results: combined });
  } catch (err: any) {
    console.error('[Music Search] Server Error:', err);
    return res.json({ success: false, results: [] });
  }
});

app.get('/manifest.json', (_req: Request, res: Response) => {
  const manifestPath = path.resolve('public/manifest.json');
  if (fs.existsSync(manifestPath)) {
    res.setHeader('Content-Type', 'application/json');
    res.sendFile(manifestPath);
  } else {
    res.status(404).json({ error: 'Manifest not found' });
  }
});

// ── Vite Dev Middleware & Prod Static Handling ───────────────────────────
async function startServer() {
  const isProd = process.env.NODE_ENV === 'production' && fs.existsSync(path.resolve('dist'));

  if (!isProd) {
    const vite = await createViteServer({
      server: { middlewareMode: true, host: '0.0.0.0' },
      appType: 'spa',
    });
    app.use(vite.middlewares);
  } else {
    app.use(express.static(path.resolve('dist')));
    app.get('*', (_req: Request, res: Response) => {
      res.sendFile(path.resolve('dist/index.html'));
    });
  }

  app.listen(PORT, '0.0.0.0', () => {
    console.log(`[Masti] Server running on http://0.0.0.0:${PORT}`);
  });
}

void startServer();
