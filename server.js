// server.js
require("dotenv").config();

const express = require("express");
const helmet = require("helmet");
const cors = require("cors");
const rateLimit = require("express-rate-limit");
const winston = require("winston");

const {
  isValidFirmId,
  isValidMessage,
  initFirm,
  getFirm,
  cleanupFirm,
  waitForQrOrReady,
  sendText,
  getChats,
  getChatMessages,
} = require("./wa.manager");

const app = express();
const port = process.env.PORT || 3000;

// Winston logger
const logger = winston.createLogger({
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.json(),
  ),
  transports: [
    new winston.transports.File({ filename: "error.log", level: "error" }),
    new winston.transports.File({ filename: "combined.log" }),
  ],
});

if (process.env.NODE_ENV !== "production") {
  logger.add(
    new winston.transports.Console({ format: winston.format.simple() }),
  );
}

app.use(helmet());

// ✅ CORS (DEV) — permite requests desde Live Server (127.0.0.1:5500) y localhost
const allowedOrigins = (
  process.env.CORS_ORIGINS ||
  "http://127.0.0.1:5500,http://localhost:5500,http://localhost:3000"
)
  .split(",")
  .map((s) => s.trim());

app.use(
  cors({
    origin: (origin, cb) => {
      // permite curl/postman (sin origin)
      if (!origin) return cb(null, true);

      if (allowedOrigins.includes(origin)) return cb(null, true);
      return cb(new Error(`CORS blocked for origin: ${origin}`));
    },
    methods: ["GET", "POST", "OPTIONS"],
    allowedHeaders: ["Content-Type", "Authorization"],
    maxAge: 86400,
  }),
);

// ✅ Responder preflight en todos los endpoints
app.options(/.*/, cors());

app.use(express.json({ limit: "10mb" }));

app.use(
  rateLimit({
    windowMs: 15 * 60 * 1000,
    max: 100000,
  }),
);

/**
 * LOGOUT (no bloqueante)
 */
app.get("/logout/:firmId", async (req, res) => {
  const { firmId } = req.params;

  try {
    if (!isValidFirmId(firmId))
      return res.status(400).json({ error: "Invalid firm ID format" });

    const firm = getFirm(firmId);
    if (!firm) {
      return res.json({ success: true, message: "No active client found" });
    }

    cleanupFirm(firmId, { deleteAuth: true }).catch(() => {});
    return res.json({ success: true, message: "Logout initiated" });
  } catch (error) {
    res
      .status(500)
      .json({ error: "Failed to logout client", message: error.message });
  }
});

/**
 * GENERATE QR (igual comportamiento: si ya está conectado => alreadyConnected)
 * En Baileys, el QR viene por connection.update. :contentReference[oaicite:3]{index=3}
 */
app.get("/generate-qr/:firmId", async (req, res) => {
  const { firmId } = req.params;

  try {
    if (!isValidFirmId(firmId))
      return res.status(400).json({ error: "Invalid firm ID format" });

    const firm = await initFirm(firmId, { forceNewSession: true });
    const result = await waitForQrOrReady(firm, 30000);

    if (result.alreadyConnected) return res.json({ alreadyConnected: true });
    return res.json({ qrCode: result.qrCode });
  } catch (error) {
    logger.error("QR generation error:", {
      message: error.message,
      stack: error.stack,
    });

    return res.status(500).json({
      error: "Failed to generate QR code",
      message: process.env.NODE_ENV === "development" ? error.message : undefined,
    });
  }
});


/**
 * CHECK CONNECTION STATUS
 */
app.get("/check-connection-status/:firmId", async (req, res) => {
  const { firmId } = req.params;

  try {
    if (!isValidFirmId(firmId)) {
      return res
        .status(400)
        .json({ status: "error", message: "Invalid firm ID format" });
    }

    const firm = getFirm(firmId);
    if (!firm) {
      return res.json({
        status: "disconnected",
        message: "No client instance found",
      });
    }

    const c = firm.state.connection;
    if (c === "open")
      return res.json({
        status: "connected",
        message: "Client is connected and ready",
      });
    if (c === "connecting")
      return res.json({
        status: "connecting",
        message: "Client is in the process of connecting",
      });
    if (c === "close")
      return res.json({
        status: "disconnected",
        message: "Client is disconnected",
      });

    return res.json({ status: "unknown", message: `Client is in ${c} state` });
  } catch (error) {
    logger.error("Status check error:", error);
    return res
      .status(500)
      .json({
        status: "error",
        message: "Internal server error while checking status",
      });
  }
});

/**
 * SEND MESSAGE (+ dedupe 5s)
 */
app.post("/send-message/:firmId", async (req, res) => {
  const { firmId } = req.params;
  const { chatId, message } = req.body || {};

  try {
    if (!isValidFirmId(firmId))
      return res.status(400).json({ error: "Invalid firm ID" });
    if (!isValidMessage(message))
      return res
        .status(400)
        .json({ error: "Invalid message format or length" });

    const firm = getFirm(firmId);
    if (!firm || !firm.state.isReady)
      return res.status(404).json({ error: "Client not found or not ready" });

    const result = await sendText(firmId, chatId, message);

    return res.json({
      success: true,
      messageId: result.messageId,
      timestamp: result.timestamp,
    });
  } catch (error) {
    if (error.code === "DUPLICATE") {
      return res
        .status(400)
        .json({
          error: "Duplicate message detected",
          message: "Duplicate message skipping",
        });
    }
    logger.error(`Error sending message for firm ${firmId}:`, error);
    return res.status(500).json({ error: "Failed to send message" });
  }
});

/**
 * REINITIALIZE CLIENT
 */
app.post("/reinitialize-client/:firmId", async (req, res) => {
  const { firmId } = req.params;

  try {
    if (!isValidFirmId(firmId))
      return res.status(400).json({ error: "Invalid firm ID format" });

    await cleanupFirm(firmId, { deleteAuth: false }).catch(() => {});
    await initFirm(firmId, { forceNewSession: false });

    return res.json({
      success: true,
      message: `Client ${firmId} is being reinitialized. Check status in a moment.`,
    });
  } catch (error) {
    logger.error(`Error during manual reinitialization for ${firmId}:`, error);
    return res.status(500).json({ error: "Failed to reinitialize client" });
  }
});

/**
 * GET CHATS (limit/offset)
 * En Baileys necesitás store (in-memory o DB) para poder listar chats/mensajes. :contentReference[oaicite:4]{index=4}
 */
app.get("/get-chats/:firmId", async (req, res) => {
  const { firmId } = req.params;
  const limit = parseInt(req.query.limit, 10) || 8;
  const offset = parseInt(req.query.offset, 10) || 0;

  try {
    if (!isValidFirmId(firmId))
      return res.status(400).json({ error: "Invalid firm ID" });

    const firm = getFirm(firmId);
    if (!firm || !firm.state.isReady)
      return res.status(404).json({ error: "Client not found or not ready" });

    const payload = await getChats(firmId, { limit, offset });
    return res.json(payload);
  } catch (error) {
    logger.error(`Error getting chats for firm ${firmId}:`, error);
    return res.status(500).json({ error: "Failed to get chats" });
  }
});

/**
 * GET CHAT MESSAGES (últimos 10, con media base64 si se puede)
 */
app.get("/get-chat-messages/:firmId/:chatId", async (req, res) => {
  const { firmId, chatId } = req.params;

  try {
    if (!isValidFirmId(firmId))
      return res.status(400).json({ error: "Invalid firm ID" });

    const firm = getFirm(firmId);
    if (!firm || !firm.state.isReady)
      return res
        .status(503)
        .json({ error: "Client is not ready. Please reconnect." });

    const payload = await getChatMessages(firmId, chatId);
    return res.json(payload);
  } catch (error) {
    logger.error("Error getting chat messages:", error);
    return res
      .status(500)
      .json({ error: "Error getting chat messages. Please try again later." });
  }
});

/**
 * STATUS (igual shape)
 */
app.get("/status/:firmId", async (req, res) => {
  const { firmId } = req.params;

  try {
    if (!isValidFirmId(firmId))
      return res.status(400).json({ error: "Invalid firm ID" });

    const firm = getFirm(firmId);
    if (!firm) return res.json({ status: "disconnected" });

    return res.json({
      status: firm.state.connection,
      lastActivity: firm.state.lastActivity,
      messageCount: firm.state.messageCount,
      isReady: firm.state.isReady,
    });
  } catch (error) {
    logger.error(`Error getting status for firm ${firmId}:`, error);
    return res.status(500).json({ error: "Failed to get status" });
  }
});

const errorHandler = (error, req, res, next) => {
  logger.error("Application error:", {
    error: error.message,
    stack: error.stack,
    path: req.path,
    method: req.method,
    body: req.body,
    params: req.params,
  });

  res.status(500).json({
    error: "Internal server error",
    message: process.env.NODE_ENV === "development" ? error.message : undefined,
  });
};

app.use(errorHandler);

// Graceful shutdown
process.on("SIGTERM", async () => {
  logger.info("SIGTERM received. Cleaning up...");
  process.exit(0);
});
process.on("uncaughtException", (error) => {
  logger.error("Uncaught Exception:", error);
  process.exit(1);
});
process.on("unhandledRejection", (reason, promise) => {
  logger.error("Unhandled Rejection at:", promise, "reason:", reason);
});

app.listen(port, "0.0.0.0", () => {
  console.log(`WhatsApp (Baileys) server running at http://0.0.0.0:${port}`);
});
