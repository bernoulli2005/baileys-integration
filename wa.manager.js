// wa.manager.js
"use strict";

const EventEmitter = require("events");
const pino = require("pino");
const NodeCache = require("node-cache");
const QRCode = require("qrcode");
const axios = require("axios");
const { Boom } = require("@hapi/boom");

/**
 * ✅ Baileys loader (CJS + ESM)
 * - Baileys v6.x suele ser CommonJS => require() funciona
 * - Baileys v7.x puede ser ESM-only => require() tira ERR_REQUIRE_ESM => usamos import()
 *
 * Importante: si usás import() contra un módulo CJS, Node lo envuelve en { default: module.exports }
 * y eso fue lo que te rompía makeWASocket (te quedaba un objeto, no la función).
 */
let _baileys = null;

async function getBaileys() {
  if (_baileys) return _baileys;

  const moduleNames = ["@whiskeysockets/baileys", "baileys"];

  for (const name of moduleNames) {
    try {
      // ✅ preferimos require para v6 (CommonJS)
      // eslint-disable-next-line global-require
      _baileys = require(name);
      return _baileys;
    } catch (err) {
      const isEsmRequireError =
        err?.code === "ERR_REQUIRE_ESM" ||
        String(err?.message || "").includes("ERR_REQUIRE_ESM");
      const isMissing =
        err?.code === "MODULE_NOT_FOUND" ||
        String(err?.message || "").includes("Cannot find module");

      if (!isEsmRequireError && !isMissing) throw err;

      if (isEsmRequireError) {
        // ✅ fallback para v7 (ESM)
        try {
          _baileys = await import(name);
          return _baileys;
        } catch (importErr) {
          const isMissingImport =
            importErr?.code === "MODULE_NOT_FOUND" ||
            String(importErr?.message || "").includes("Cannot find module");
          if (!isMissingImport) throw importErr;
        }
      }
    }
  }

  throw new Error("Baileys package not found. Install @whiskeysockets/baileys or baileys.");
}

/**
 * Helpers para leer exports tanto del root como de .default (por compatibilidad)
 */
function pickExport(mod, key) {
  return (
    mod?.[key] ??
    mod?.default?.[key] ??
    // por si algún bundler te mete doble default
    mod?.default?.default?.[key]
  );
}

function resolveMakeWASocket(mod) {
  const fn =
    (typeof mod?.default === "function" && mod.default) ||
    (typeof mod?.makeWASocket === "function" && mod.makeWASocket) ||
    (typeof mod?.default?.default === "function" && mod.default.default);

  return fn || null;
}

/**
 * ✅ FIX: en algunas versiones makeInMemoryStore no viene exportado en el root.
 * Intentamos resolverlo desde paths internos, y si no existe, usamos un "mini-store" propio.
 */
async function safeImport(modulePath) {
  try {
    return await import(modulePath);
  } catch {
    return null;
  }
}

async function resolveMakeInMemoryStore(baileysMod) {
  // soporta root, default (CJS wrapped) y default.default
  const root =
    (baileysMod?.default && typeof baileysMod.default === "object"
      ? baileysMod.default
      : baileysMod) || {};

  if (typeof root?.makeInMemoryStore === "function") return root.makeInMemoryStore;
  if (typeof baileysMod?.makeInMemoryStore === "function") return baileysMod.makeInMemoryStore;

  // paths típicos (según build)
  const candidates = [
    "@whiskeysockets/baileys/lib/Store/make-in-memory-store.js",
    "@whiskeysockets/baileys/lib/Store/make-in-memory-store",
    "@whiskeysockets/baileys/lib/Store/index.js",
    "@whiskeysockets/baileys/lib/Store",
  ];

  for (const p of candidates) {
    const mod = await safeImport(p);
    const fn = mod?.default || mod?.makeInMemoryStore;
    if (typeof fn === "function") return fn;
  }

  return null;
}

/**
 * ✅ Fallback store (si makeInMemoryStore no está disponible)
 * - Mantiene chats/mensajes/contacts mínimo para que tus endpoints funcionen
 * - NO es tan completo como el store oficial, pero evita crash y permite avanzar
 */
function createMiniStore() {
  const chatsMap = new Map(); // jid -> chat
  const messagesMap = new Map(); // jid -> array messages
  const contacts = {}; // id -> contact

  const store = {
    chats: {
      all: () => Array.from(chatsMap.values()),
      clear: () => chatsMap.clear(),
    },
    messages: {}, // jid -> { array: [...] }
    contacts,
    bind: (ev) => {
      // chats
      ev.on("chats.set", ({ chats }) => {
        (chats || []).forEach((c) => {
          const id = c?.id || c?.jid;
          if (!id) return;
          chatsMap.set(id, c);
        });
      });

      ev.on("chats.upsert", (chats) => {
        (chats || []).forEach((c) => {
          const id = c?.id || c?.jid;
          if (!id) return;
          const prev = chatsMap.get(id) || {};
          chatsMap.set(id, { ...prev, ...c, id });
        });
      });

      // contacts
      ev.on("contacts.set", ({ contacts: cs }) => {
        (cs || []).forEach((c) => {
          const id = c?.id;
          if (!id) return;
          contacts[id] = c;
        });
      });

      ev.on("contacts.upsert", (cs) => {
        (cs || []).forEach((c) => {
          const id = c?.id;
          if (!id) return;
          contacts[id] = { ...(contacts[id] || {}), ...c };
        });
      });

      // messages
      ev.on("messages.upsert", ({ messages }) => {
        (messages || []).forEach((m) => {
          const jid = m?.key?.remoteJid;
          if (!jid) return;

          if (!messagesMap.has(jid)) messagesMap.set(jid, []);
          const arr = messagesMap.get(jid);

          arr.push(m);
          // keep last N messages per chat to limit memory
          const MAX = 300;
          if (arr.length > MAX) arr.splice(0, arr.length - MAX);

          store.messages[jid] = { array: arr };
        });
      });
    },
    loadMessage: async (jid, id) => {
      const arr = messagesMap.get(jid);
      if (!arr) return undefined;
      for (let i = arr.length - 1; i >= 0; i--) {
        if (arr[i]?.key?.id === id) return arr[i];
      }
      return undefined;
    },
  };

  return store;
}

const {
  S3Client,
  GetObjectCommand,
  PutObjectCommand,
  ListObjectsV2Command,
  DeleteObjectsCommand,
} = (() => {
  try {
    return require("@aws-sdk/client-s3");
  } catch {
    return {};
  }
})();

const WEBHOOK_URL =
  process.env.WEBHOOK_URL || "https://andeshire.com/v1/api/whatsapp_webhook/";

const PUPPETEER_RETRY_OPTIONS = {
  maxRetries: 3,
  retryDelay: 5000,
  exponentialBackoff: true,
};

async function retryOperation(operation, options = PUPPETEER_RETRY_OPTIONS) {
  let lastError;
  let delay = options.retryDelay;

  for (let attempt = 0; attempt <= options.maxRetries; attempt++) {
    try {
      return await operation();
    } catch (error) {
      lastError = error;
      if (attempt >= options.maxRetries) throw error;
      await new Promise((r) => setTimeout(r, delay));
      if (options.exponentialBackoff) delay *= 2;
    }
  }
  throw lastError;
}

const isValidFirmId = (firmId) => /^[a-zA-Z0-9-_]+$/.test(firmId);
const isValidMessage = (message) =>
  message && typeof message === "string" && message.length <= 4096;

function normalizeChatId(input) {
  if (!input) return input;
  if (
    input.endsWith("@s.whatsapp.net") ||
    input.endsWith("@g.us") ||
    input.includes("@")
  )
    return input;
  const digits = String(input).replace(/[^\d]/g, "");
  if (!digits) return input;
  return `${digits}@s.whatsapp.net`;
}

function buildChatIdCandidates(input) {
  if (!input) return [];
  const raw = String(input).trim();
  if (!raw) return [];

  if (raw.endsWith("@g.us")) return [raw];

  if (raw.endsWith("@c.us")) {
    const digits = raw.split("@")[0];
    return [raw, `${digits}@s.whatsapp.net`];
  }

  if (raw.endsWith("@s.whatsapp.net")) {
    const digits = raw.split("@")[0];
    return [raw, `${digits}@c.us`];
  }

  if (raw.includes("@")) return [raw];

  const digits = raw.replace(/[^\d]/g, "");
  if (!digits) return [raw];
  return [`${digits}@c.us`, `${digits}@s.whatsapp.net`];
}

function toWebhookChatId(jid) {
  if (!jid) return jid;
  const raw = String(jid);
  if (raw.endsWith("@g.us") || raw.endsWith("@c.us")) return raw;
  if (raw.endsWith("@s.whatsapp.net")) {
    const digits = raw.split("@")[0];
    return `${digits}@c.us`;
  }
  return raw;
}

function extractBodyAndMeta(webMsg) {
  const msg = webMsg?.message || {};
  const key = webMsg?.key || {};
  const fromMe = !!key.fromMe;

  const text =
    msg.conversation ||
    msg.extendedTextMessage?.text ||
    msg.imageMessage?.caption ||
    msg.videoMessage?.caption ||
    msg.documentMessage?.caption ||
    "";

  let type = "text";
  let hasMedia = false;
  let caption = "";

  if (msg.stickerMessage) type = "sticker";
  else if (msg.imageMessage) {
    type = "image";
    hasMedia = true;
    caption = msg.imageMessage.caption || "";
  } else if (msg.videoMessage) {
    type = "video";
    hasMedia = true;
    caption = msg.videoMessage.caption || "";
  } else if (msg.documentMessage) {
    type = "document";
    hasMedia = true;
    caption = msg.documentMessage.caption || "";
  } else if (msg.audioMessage) {
    type = msg.audioMessage.ptt ? "ptt" : "audio";
    hasMedia = true;
  } else if (msg.locationMessage) type = "location";
  else if (msg.contactMessage) type = "contact";

  const ts = Number(webMsg.messageTimestamp || 0);

  return {
    body: text || "",
    type,
    caption,
    hasMedia,
    fromMe,
    timestamp: ts,
  };
}

async function downloadMediaAsDataUrl(webMsg) {
  const baileys = await getBaileys();
  const downloadContentFromMessage = pickExport(baileys, "downloadContentFromMessage");
  if (typeof downloadContentFromMessage !== "function") {
    return { mediaUrl: null, mimetype: null, fileName: "", fileSize: "" };
  }

  const msg = webMsg?.message || {};
  let node, kind;

  if (msg.imageMessage) {
    node = msg.imageMessage;
    kind = "image";
  } else if (msg.videoMessage) {
    node = msg.videoMessage;
    kind = "video";
  } else if (msg.audioMessage) {
    node = msg.audioMessage;
    kind = "audio";
  } else if (msg.documentMessage) {
    node = msg.documentMessage;
    kind = "document";
  } else return { mediaUrl: null, mimetype: null, fileName: "", fileSize: "" };

  const mimetype = node.mimetype || null;
  const fileName = node.fileName || "";
  const fileSize = node.fileLength ? String(node.fileLength) : "";

  if (node.fileLength && Number(node.fileLength) > 8 * 1024 * 1024) {
    return { mediaUrl: null, mimetype, fileName, fileSize };
  }

  const stream = await downloadContentFromMessage(node, kind);
  let buffer = Buffer.from([]);
  for await (const chunk of stream) buffer = Buffer.concat([buffer, chunk]);

  const b64 = buffer.toString("base64");
  const mediaUrl = mimetype ? `data:${mimetype};base64,${b64}` : null;

  return { mediaUrl, mimetype, fileName, fileSize };
}

/**
 * =========================
 * Auth state: Local o S3
 * =========================
 */
function s3Enabled() {
  return !!(
    S3Client &&
    process.env.S3_BUCKET &&
    process.env.AWS_REGION &&
    process.env.AWS_ACCESS_KEY_ID &&
    process.env.AWS_SECRET_ACCESS_KEY
  );
}

function makeS3Client() {
  return new S3Client({
    region: process.env.AWS_REGION,
    credentials: {
      accessKeyId: process.env.AWS_ACCESS_KEY_ID,
      secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
    },
  });
}

function streamToString(stream) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    stream.on("data", (c) => chunks.push(Buffer.from(c)));
    stream.on("error", reject);
    stream.on("end", () => resolve(Buffer.concat(chunks).toString("utf-8")));
  });
}

async function useS3AuthState(firmId) {
  const baileys = await getBaileys();
  const initAuthCreds = pickExport(baileys, "initAuthCreds");
  const BufferJSON = pickExport(baileys, "BufferJSON");

  if (typeof initAuthCreds !== "function" || !BufferJSON) {
    throw new Error("Baileys exports missing: initAuthCreds/BufferJSON");
  }

  const bucket = process.env.S3_BUCKET;
  const prefix = (process.env.S3_DATA_PATH || "wwebjs_sessions/")
    .replace(/^\//, "")
    .replace(/\/?$/, "/");
  const keyName = `${prefix}baileys_auth/${firmId}.json`;
  const s3 = makeS3Client();

  let data = null;
  try {
    const res = await s3.send(
      new GetObjectCommand({ Bucket: bucket, Key: keyName })
    );
    const json = await streamToString(res.Body);
    data = JSON.parse(json, BufferJSON.reviver);
  } catch {
    data = null;
  }

  const creds = data?.creds || initAuthCreds();
  const keysStore = data?.keys || {};

  let saveTimer = null;
  const scheduleSave = () => {
    if (saveTimer) return;
    saveTimer = setTimeout(async () => {
      saveTimer = null;
      const payload = JSON.stringify(
        { creds, keys: keysStore },
        BufferJSON.replacer
      );
      await s3.send(
        new PutObjectCommand({
          Bucket: bucket,
          Key: keyName,
          Body: payload,
          ContentType: "application/json",
        })
      );
    }, 500);
  };

  const state = {
    creds,
    keys: {
      get: async (type, ids) => {
        const bucketObj = keysStore[type] || {};
        const res = {};
        for (const id of ids) res[id] = bucketObj[id];
        return res;
      },
      set: async (dataToSet) => {
        for (const type of Object.keys(dataToSet)) {
          keysStore[type] = keysStore[type] || {};
          Object.assign(keysStore[type], dataToSet[type]);
        }
        scheduleSave();
      },
    },
  };

  const saveCreds = async () => scheduleSave();

  const deleteAuth = async () => {
    await s3
      .send(
        new DeleteObjectsCommand({
          Bucket: bucket,
          Delete: { Objects: [{ Key: keyName }], Quiet: true },
        })
      )
      .catch(() => {});
  };

  return { state, saveCreds, deleteAuth };
}

async function deleteS3Prefix(prefixToDelete) {
  if (!s3Enabled()) return;
  const bucket = process.env.S3_BUCKET;
  const s3 = makeS3Client();

  const listed = await s3.send(
    new ListObjectsV2Command({ Bucket: bucket, Prefix: prefixToDelete })
  );
  const objects = (listed.Contents || []).map((o) => ({ Key: o.Key }));
  if (!objects.length) return;

  await s3.send(
    new DeleteObjectsCommand({
      Bucket: bucket,
      Delete: { Objects: objects, Quiet: true },
    })
  );
}

/**
 * =========================
 * Firm sockets manager
 * =========================
 */
const firms = new Map(); // firmId -> { sock, store, emitter, state, ... }
const lastMessages = {}; // dedupe

function getFirm(firmId) {
  return firms.get(firmId);
}

async function cleanupFirm(firmId, opts = { deleteAuth: true }) {
  const firm = firms.get(firmId);
  if (!firm) return;

  const { sock, auth, store } = firm;

  firms.delete(firmId);

  try {
    await Promise.race([
      (async () => {
        try {
          await sock.logout();
        } catch {}
        try {
          sock.end();
        } catch {}
      })(),
      new Promise((r) => setTimeout(r, 1000)),
    ]);
  } catch {}

  if (opts.deleteAuth && auth?.deleteAuth) {
    await auth.deleteAuth().catch(() => {});
  }

  try {
    store?.chats?.clear?.();
  } catch {}
}

function shouldReconnectFromDisconnect(statusCode, DisconnectReason) {
  return statusCode !== DisconnectReason.loggedOut;
}

async function initFirm(firmId, { forceNewSession = false } = {}) {
  const baileys = await getBaileys();

  const makeWASocket = resolveMakeWASocket(baileys);
  const DisconnectReason = pickExport(baileys, "DisconnectReason");
  const fetchLatestBaileysVersion = pickExport(baileys, "fetchLatestBaileysVersion");
  const makeCacheableSignalKeyStore = pickExport(baileys, "makeCacheableSignalKeyStore");
  const jidNormalizedUser = pickExport(baileys, "jidNormalizedUser");
  const useMultiFileAuthState = pickExport(baileys, "useMultiFileAuthState");

  if (typeof makeWASocket !== "function") {
    throw new Error(
      "Baileys export mismatch: makeWASocket no es function. " +
        "Revisá que el paquete instalado coincida (v6 CJS o v7 ESM) y que no estés importando el equivocado."
    );
  }
  if (!DisconnectReason) throw new Error("Baileys export missing: DisconnectReason");
  if (typeof makeCacheableSignalKeyStore !== "function")
    throw new Error("Baileys export missing: makeCacheableSignalKeyStore");
  if (typeof jidNormalizedUser !== "function")
    throw new Error("Baileys export missing: jidNormalizedUser");
  if (typeof useMultiFileAuthState !== "function")
    throw new Error("Baileys export missing: useMultiFileAuthState");

  if (!isValidFirmId(firmId)) throw new Error("Invalid firm ID format");

  if (firms.has(firmId)) {
    await cleanupFirm(firmId, { deleteAuth: forceNewSession });
  }

  const emitter = new EventEmitter();

  // auth local o s3
  let auth;
  if (s3Enabled()) {
    auth = await useS3AuthState(firmId);
    if (forceNewSession) await auth.deleteAuth().catch(() => {});
  } else {
    auth = await useMultiFileAuthState(`./.baileys_auth/${firmId}`);

    if (forceNewSession) {
      const fs = require("fs");
      const path = require("path");
      const dir = path.join(process.cwd(), ".baileys_auth", firmId);
      try {
        fs.rmSync(dir, { recursive: true, force: true });
      } catch {}
      auth = await useMultiFileAuthState(`./.baileys_auth/${firmId}`);
    }

    auth.deleteAuth = async () => {
      const fs = require("fs");
      const path = require("path");
      const dir = path.join(process.cwd(), ".baileys_auth", firmId);
      try {
        fs.rmSync(dir, { recursive: true, force: true });
      } catch {}
    };
  }

  const logger = pino({
    level: process.env.NODE_ENV === "production" ? "info" : "silent",
  });

  // ✅ store resolver (o fallback)
  const makeInMemoryStore = await resolveMakeInMemoryStore(baileys);
  const store =
    typeof makeInMemoryStore === "function"
      ? makeInMemoryStore({
          logger: logger.child({ level: "silent", stream: "store" }),
        })
      : (() => {
          console.warn(
            "[wa.manager] makeInMemoryStore no disponible. Usando mini-store fallback (recomendado: Baileys v6.x)."
          );
          return createMiniStore();
        })();

  const msgRetryCounterCache = new NodeCache();

  let version;
  if (typeof fetchLatestBaileysVersion === "function") {
    ({ version } = await fetchLatestBaileysVersion().catch(() => ({
      version: undefined,
    })));
  }

  const sock = makeWASocket({
    ...(version ? { version } : {}),
    logger,
    printQRInTerminal: false,
    markOnlineOnConnect: false,
    msgRetryCounterCache,
    defaultQueryTimeoutMs: undefined,
    auth: {
      creds: auth.state.creds,
      keys: makeCacheableSignalKeyStore(auth.state.keys, logger),
    },
    getMessage: async (key) => {
      const jid = jidNormalizedUser(key.remoteJid);
      const msg = await store.loadMessage(jid, key.id);
      return msg?.message || undefined;
    },
  });

  store.bind(sock.ev);

  const firmState = {
    lastActivity: Date.now(),
    messageCount: 0,
    isReady: false,
    initTime: Date.now(),
    reconnectAttempts: 0,
    errorLog: [],
    connection: "connecting",
    lastQrDataUrl: null,
  };

  const firm = { firmId, sock, store, emitter, state: firmState, auth };
  firms.set(firmId, firm);

  sock.ev.on("creds.update", async () => {
    try {
      await auth.saveCreds?.();
    } catch {}
  });

  sock.ev.on("connection.update", async (update) => {
    const { connection, lastDisconnect, qr } = update;
    if (connection) firmState.connection = connection;

    if (qr) {
      try {
        firmState.lastQrDataUrl = await QRCode.toDataURL(qr);
      } catch {
        firmState.lastQrDataUrl = null;
      }
      emitter.emit("qr", firmState.lastQrDataUrl);
    }

    if (connection === "open") {
      firmState.isReady = true;
      firmState.reconnectAttempts = 0;
      emitter.emit("open");
    }

    if (connection === "close") {
      firmState.isReady = false;

      const statusCode =
        lastDisconnect?.error instanceof Boom
          ? lastDisconnect.error.output.statusCode
          : lastDisconnect?.error?.output?.statusCode;

      firmState.errorLog.push({
        time: Date.now(),
        error: "disconnected",
        reason: statusCode,
      });

      if (statusCode === DisconnectReason.loggedOut) {
        await cleanupFirm(firmId, { deleteAuth: true });
        return;
      }

      if (shouldReconnectFromDisconnect(statusCode, DisconnectReason)) {
        const attempts = firmState.reconnectAttempts || 0;
        if (attempts < 5) {
          firmState.reconnectAttempts = attempts + 1;
          const backoffTime = Math.min(30000, 1000 * Math.pow(2, attempts));
          setTimeout(
            () => initFirm(firmId, { forceNewSession: false }).catch(() => {}),
            backoffTime
          );
        } else {
          await cleanupFirm(firmId, { deleteAuth: false }).catch(() => {});
        }
      }
    }
  });

  sock.ev.on("messages.upsert", async ({ messages, type }) => {
    if (type !== "notify") return;
    const webMsg = messages?.[0];
    if (!webMsg?.key?.remoteJid) return;

    const tsMs = Number(webMsg.messageTimestamp || 0) * 1000;
    if (tsMs <= firmState.initTime) return;

    await sendWebhook(firmId, webMsg).catch(() => {});
  });

  return firm;
}

async function sendWebhook(firmId, webMsg) {
  const firm = firms.get(firmId);
  if (!firm) return;

  const { store, state } = firm;

  state.lastActivity = Date.now();
  state.messageCount++;

  const chatId = webMsg.key.remoteJid;
  const webhookChatId = toWebhookChatId(chatId);
  const meta = extractBodyAndMeta(webMsg);

  const msgId = webMsg.key.id;
  const sender = webMsg.key.participant || webMsg.key.remoteJid;

  const arr = store.messages?.[chatId]?.array || [];
  const latest = arr.slice(-10).map((m) => {
    const mm = extractBodyAndMeta(m);
    return {
      messageId: m.key?.id || "",
      body: mm.body,
      from: m.key?.remoteJid || "",
      timestamp: Number(m.messageTimestamp || 0),
      type: mm.type,
      caption: mm.caption || "",
      isPtt: mm.type === "ptt",
      isVoice: mm.type === "audio",
      fileName: m.message?.documentMessage?.fileName || "",
      fileSize: m.message?.documentMessage?.fileLength
        ? String(m.message.documentMessage.fileLength)
        : "",
      latitude: m.message?.locationMessage?.degreesLatitude ?? null,
      longitude: m.message?.locationMessage?.degreesLongitude ?? null,
      contactName: "",
      contactPhone: "",
      sender: m.key?.participant || m.key?.remoteJid || "",
      hasMedia: !!mm.hasMedia,
    };
  });

  const webhookData = {
    firmId,
    messageId: msgId,
    chatId: webhookChatId,
    body: meta.body,
    timestamp: meta.timestamp,
    type: meta.type,
    caption: meta.caption || "",
    isPtt: meta.type === "ptt",
    isVoice: meta.type === "audio",
    fileName: webMsg.message?.documentMessage?.fileName || "",
    fileSize: webMsg.message?.documentMessage?.fileLength
      ? String(webMsg.message.documentMessage.fileLength)
      : "",
    latitude: webMsg.message?.locationMessage?.degreesLatitude ?? null,
    longitude: webMsg.message?.locationMessage?.degreesLongitude ?? null,
    contactName: "",
    contactPhone: "",
    sender,
    hasMedia: !!meta.hasMedia,
    latestMessages: latest,
  };

  console.log("[webhook] sending", {
    firmId,
    chatId: webhookChatId,
    messageId: msgId,
    type: meta.type,
  });

  try {
    const res = await axios.post(WEBHOOK_URL, webhookData);
    console.log("[webhook] sent", {
      status: res?.status,
      statusText: res?.statusText,
      chatId: webhookChatId,
      messageId: msgId,
    });
  } catch (error) {
    console.log("[webhook] error", {
      message: error?.message || String(error),
      status: error?.response?.status,
      data: error?.response?.data,
      chatId: webhookChatId,
      messageId: msgId,
    });
    throw error;
  }
}

/**
 * ✅ FIX: evita race condition (QR puede emitirse antes de escuchar)
 */
async function waitForQrOrReady(firm, timeoutMs = 30000) {
  if (firm.state.isReady) return { alreadyConnected: true };
  if (firm.state.lastQrDataUrl) return { qrCode: firm.state.lastQrDataUrl };

  return await Promise.race([
    new Promise((resolve) =>
      firm.emitter.once("qr", (dataUrl) => resolve({ qrCode: dataUrl }))
    ),
    new Promise((resolve) =>
      firm.emitter.once("open", () => resolve({ alreadyConnected: true }))
    ),
    new Promise((_, reject) =>
      setTimeout(() => reject(new Error("QR code generation timeout")), timeoutMs)
    ),
  ]);
}

async function ensureGroupSessions(sock, jid) {
  if (!sock || typeof sock.groupMetadata !== "function") return null;

  const meta = await sock.groupMetadata(jid);
  const participants = (meta?.participants || [])
    .map((p) => p?.id)
    .filter(Boolean);

  if (participants.length && typeof sock.assertSessions === "function") {
    await sock.assertSessions(participants);
  }

  return meta;
}

async function sendText(firmId, chatId, text) {
  const firm = firms.get(firmId);
  if (!firm || !firm.state.isReady)
    throw new Error("Client not found or not ready");

  const rawChatId = chatId;
  const jid = normalizeChatId(chatId);
  const isGroup = String(jid || "").endsWith("@g.us");
  const messageKey = `${firmId}_${jid}`;

  console.log("[sendText] start", {
    firmId,
    rawChatId,
    normalizedJid: jid,
    isGroup,
  });

  const currentMessage = {
    content: text.trim(),
    chatId: jid,
    timestamp: Date.now(),
  };
  const last = lastMessages[messageKey];
  if (last) {
    const timeDiff = currentMessage.timestamp - last.timestamp;
    if (
      last.content === currentMessage.content &&
      last.chatId === currentMessage.chatId &&
      timeDiff < 5000
    ) {
      const err = new Error("Duplicate message detected");
      err.code = "DUPLICATE";
      throw err;
    }
  }
  lastMessages[messageKey] = currentMessage;

  try {
    if (isGroup) {
      try {
        console.log("[sendText] group-prep: fetching metadata", { firmId, jid });
        const meta = await ensureGroupSessions(firm.sock, jid);
        console.log("[sendText] group-prep: metadata", {
          firmId,
          jid,
          subject: meta?.subject || "",
          participants: meta?.participants?.length || 0,
        });
      } catch (prepError) {
        console.log("[sendText] group-prep: error", {
          firmId,
          jid,
          message: prepError?.message || String(prepError),
          name: prepError?.name,
        });
      }
    }

    console.log("[sendText] pre-send", {
      firmId,
      jid,
      hasStoreMessages: !!firm.store?.messages?.[jid],
      storeMessageCount: firm.store?.messages?.[jid]?.array?.length || 0,
      textLength: text?.length || 0,
    });

    const res = await retryOperation(() => firm.sock.sendMessage(jid, { text }));

    console.log("[sendText] sent", {
      firmId,
      jid,
      messageId: res?.key?.id || "",
    });

    firm.state.lastActivity = Date.now();
    firm.state.messageCount++;

    return {
      messageId: res?.key?.id || "",
      timestamp: Date.now(),
    };
  } catch (error) {
    console.log("[sendText] error", {
      firmId,
      rawChatId,
      normalizedJid: jid,
      isGroup,
      message: error?.message || String(error),
      name: error?.name,
      stack: error?.stack,
    });
    if (isGroup && error?.name === "SessionError") {
      try {
        console.log("[sendText] group-recover: fetching metadata", { firmId, jid });
        const meta = await firm.sock.groupMetadata(jid);
        console.log("[sendText] group-recover: metadata", {
          firmId,
          jid,
          subject: meta?.subject || "",
          participants: meta?.participants?.length || 0,
        });

        const retryRes = await retryOperation(() =>
          firm.sock.sendMessage(jid, { text })
        );
        console.log("[sendText] group-recover: sent", {
          firmId,
          jid,
          messageId: retryRes?.key?.id || "",
        });

        firm.state.lastActivity = Date.now();
        firm.state.messageCount++;

        return {
          messageId: retryRes?.key?.id || "",
          timestamp: Date.now(),
        };
      } catch (retryError) {
        console.log("[sendText] group-recover: error", {
          firmId,
          jid,
          message: retryError?.message || String(retryError),
          name: retryError?.name,
          stack: retryError?.stack,
        });
      }
    }
    throw error;
  }
}

async function getChats(firmId, { limit = 8, offset = 0 } = {}) {
  const firm = firms.get(firmId);
  if (!firm || !firm.state.isReady)
    throw new Error("Client not found or not ready");

  const { sock, store, state } = firm;

  const chats = store.chats?.all?.() || [];
  const total = chats.length;

  const slice = chats.slice(offset, offset + limit);

  const summaries = await Promise.all(
    slice.map(async (chat) => {
      const id = chat.id || chat.jid;
      const isGroup = String(id || "").endsWith("@g.us");

      let profilePicUrl = null;
      try {
        profilePicUrl = await sock.profilePictureUrl(id, "image");
      } catch {
        profilePicUrl = null;
      }

      const arr = store.messages?.[id]?.array || [];
      const lastMsg = arr.length ? arr[arr.length - 1] : null;

      let lastMessage = null;
      if (lastMsg) {
        const meta = extractBodyAndMeta(lastMsg);

        let senderInfo = null;
        if (isGroup && lastMsg.key?.participant) {
          const pid = lastMsg.key.participant;
          const contact = store.contacts?.[pid];
          senderInfo = {
            id: pid,
            name: contact?.name || contact?.notify || pid.split("@")[0],
          };
        }

        lastMessage = {
          body: meta.body,
          timestamp: meta.timestamp,
          type: meta.type,
          sender: senderInfo,
        };
      }

      return {
        name: chat.name || chat.subject || id,
        id,
        unreadCount: chat.unreadCount || 0,
        timestamp: Number(chat.conversationTimestamp || chat.t || 0),
        profilePicUrl,
        isGroup,
        lastMessage,
        isMuted: !!chat.muteEndTime,
      };
    })
  );

  state.lastActivity = Date.now();

  return {
    chats: summaries,
    pagination: {
      total,
      offset,
      limit,
      hasMore: offset + limit < total,
    },
  };
}

async function getChatMessages(firmId, chatId) {
  const firm = firms.get(firmId);
  if (!firm || !firm.state.isReady)
    throw new Error("Client not found or not ready");

  const { sock, store } = firm;
  const candidates = buildChatIdCandidates(chatId);
  const scored = candidates.map((jid, idx) => ({
    jid,
    idx,
    len: (store.messages?.[jid]?.array || []).length,
  }));
  scored.sort((a, b) => {
    if (b.len !== a.len) return b.len - a.len;
    return a.idx - b.idx;
  });
  const jid =
    scored[0]?.jid || normalizeChatId(chatId);

  const selfId = sock.user?.id || "";

  const arr = store.messages?.[jid]?.array || [];
  const last10 = arr.slice(-10);

  const simplified = await Promise.all(
    last10.map(async (m) => {
      const meta = extractBodyAndMeta(m);

      let mediaUrl = null;
      let mimetype = null;
      let fileName = "";
      let fileSize = "";

      if (meta.hasMedia) {
        try {
          const d = await downloadMediaAsDataUrl(m);
          mediaUrl = d.mediaUrl;
          mimetype = d.mimetype;
          fileName = d.fileName || "";
          fileSize = d.fileSize || "";
        } catch {}
      }

      const latitude = m.message?.locationMessage?.degreesLatitude ?? null;
      const longitude = m.message?.locationMessage?.degreesLongitude ?? null;

      return {
        body: meta.body,
        from: m.key?.remoteJid || jid,
        fromMe: meta.fromMe,
        sender: meta.fromMe
          ? selfId
          : m.key?.participant || m.key?.remoteJid || jid,
        timestamp: meta.timestamp,
        type: meta.type,
        mediaUrl,
        mimetype,
        caption: meta.caption || "",
        isPtt: meta.type === "ptt",
        isVoice: meta.type === "audio",
        fileName,
        fileSize,
        latitude,
        longitude,
        contactName: "",
        contactPhone: "",
      };
    })
  );

  return {
    messages: simplified,
    selfId,
    chatId,
    resolvedChatId: jid,
    messageCount: arr.length,
  };
}

module.exports = {
  isValidFirmId,
  isValidMessage,
  initFirm,
  getFirm,
  cleanupFirm,
  waitForQrOrReady,
  sendText,
  getChats,
  getChatMessages,
};
