const express = require("express");
const http = require("http");
const path = require("path");
const crypto = require("crypto");
const { Pool } = require("pg");
const { Server } = require("socket.io");

const app = express();
const server = http.createServer(app);
const io = new Server(server);

// ---------- База данных ----------
// DATABASE_URL задаётся в Render автоматически при подключении Postgres-сервиса.
// Если переменной нет — работаем без БД (in-memory fallback), как раньше.
let db = null;

async function initDb() {
  const url = process.env.DATABASE_URL;
  if (!url) {
    console.warn("[db] DATABASE_URL не задан — работаем без БД (бан-лист сбросится при рестарте)");
    return;
  }

  db = new Pool({
    connectionString: url,
    ssl: { rejectUnauthorized: false } // Render требует SSL для внешних подключений
  });

  // Создаём таблицы если их нет — idempotent, безопасно запускать при каждом старте
  await db.query(`
    CREATE TABLE IF NOT EXISTS users (
      telegram_id   BIGINT PRIMARY KEY,
      username      TEXT,
      first_name    TEXT,
      language_code TEXT,
      is_premium    BOOLEAN NOT NULL DEFAULT FALSE,
      premium_until TIMESTAMPTZ,
      gender        TEXT,           -- 'male' | 'female' | null (заполнится позже)
      created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      last_seen_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS bans (
      id            SERIAL PRIMARY KEY,
      telegram_id   BIGINT NOT NULL UNIQUE,
      reason        TEXT,
      banned_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS reports (
      id              SERIAL PRIMARY KEY,
      reporter_tg_id  BIGINT,
      offender_tg_id  BIGINT,
      verdict         TEXT,   -- 'violation' | 'clean' | 'error'
      nudity_score    REAL,
      created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS blocks (
      blocker_tg_id   BIGINT NOT NULL,
      blocked_tg_id   BIGINT NOT NULL,
      created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      PRIMARY KEY (blocker_tg_id, blocked_tg_id)
    );
  `);

  // Индекс ускоряет проверку бана при каждом подключении
  await db.query(`
    CREATE INDEX IF NOT EXISTS bans_telegram_id_idx ON bans(telegram_id);
    CREATE INDEX IF NOT EXISTS blocks_blocker_idx ON blocks(blocker_tg_id);
  `);

  console.log("[db] подключено и схема готова");
}

// ---------- Хелперы для работы с БД (с in-memory fallback) ----------

// In-memory fallback — используется ТОЛЬКО если DATABASE_URL не задан
const bannedUsersMemory = new Set();

async function isBanned(telegramId) {
  if (!telegramId) return false;
  if (!db) return bannedUsersMemory.has(telegramId);
  const r = await db.query("SELECT 1 FROM bans WHERE telegram_id = $1 LIMIT 1", [telegramId]);
  return r.rowCount > 0;
}

async function banUser(telegramId, reason) {
  if (!telegramId) return;
  if (!db) {
    bannedUsersMemory.add(telegramId);
    console.log("[ban] (in-memory) добавлен:", telegramId);
    return;
  }
  // INSERT OR IGNORE — не падаем если пользователь уже забанен
  await db.query(
    "INSERT INTO bans (telegram_id, reason) VALUES ($1, $2) ON CONFLICT (telegram_id) DO NOTHING",
    [telegramId, reason || null]
  );
  console.log("[ban] (db) добавлен:", telegramId);
}

async function upsertUser(user) {
  if (!db || !user) return;
  await db.query(`
    INSERT INTO users (telegram_id, username, first_name, language_code, last_seen_at)
    VALUES ($1, $2, $3, $4, NOW())
    ON CONFLICT (telegram_id) DO UPDATE SET
      username      = EXCLUDED.username,
      first_name    = EXCLUDED.first_name,
      language_code = EXCLUDED.language_code,
      last_seen_at  = NOW()
  `, [user.id, user.username || null, user.first_name || null, user.language_code || null]);
}

async function isPremium(telegramId) {
  if (!db || !telegramId) return false;
  const r = await db.query(
    "SELECT is_premium, premium_until FROM users WHERE telegram_id = $1 LIMIT 1",
    [telegramId]
  );
  if (!r.rowCount) return false;
  const { is_premium, premium_until } = r.rows[0];
  // Если есть дата истечения — проверяем актуальность
  if (premium_until && new Date(premium_until) < new Date()) {
    await db.query("UPDATE users SET is_premium = FALSE WHERE telegram_id = $1", [telegramId]);
    return false;
  }
  return is_premium;
}

async function grantPremium(telegramId, days = 30) {
  if (!db || !telegramId) return;
  await db.query(`
    INSERT INTO users (telegram_id, is_premium, premium_until)
    VALUES ($1, TRUE, NOW() + INTERVAL '${days} days')
    ON CONFLICT (telegram_id) DO UPDATE SET
      is_premium    = TRUE,
      premium_until = NOW() + INTERVAL '${days} days'
  `, [telegramId]);
  console.log("[premium] выдан на", days, "дней пользователю", telegramId);
}

async function setGender(telegramId, gender) {
  if (!db || !telegramId) return;
  await db.query("UPDATE users SET gender = $1 WHERE telegram_id = $2", [gender, telegramId]);
}

async function getGender(telegramId) {
  if (!db || !telegramId) return null;
  const r = await db.query("SELECT gender FROM users WHERE telegram_id = $1 LIMIT 1", [telegramId]);
  return r.rows[0]?.gender ?? null;
}

async function saveReport({ reporterTgId, offenderTgId, verdict, nudityScore }) {
  if (!db) return;
  await db.query(
    "INSERT INTO reports (reporter_tg_id, offender_tg_id, verdict, nudity_score) VALUES ($1,$2,$3,$4)",
    [reporterTgId || null, offenderTgId || null, verdict, nudityScore ?? null]
  );
}

async function blockUser(blockerTgId, blockedTgId) {
  if (!blockerTgId || !blockedTgId || !db) return;
  await db.query(
    "INSERT INTO blocks (blocker_tg_id, blocked_tg_id) VALUES ($1, $2) ON CONFLICT DO NOTHING",
    [blockerTgId, blockedTgId]
  );
  console.log("[block] (db)", blockerTgId, "заблокировал", blockedTgId);
}

// Возвращает true если хотя бы один из двух заблокировал другого
async function isBlockedBetween(tgIdA, tgIdB) {
  if (!tgIdA || !tgIdB || !db) return false;
  const r = await db.query(
    `SELECT 1 FROM blocks
     WHERE (blocker_tg_id = $1 AND blocked_tg_id = $2)
        OR (blocker_tg_id = $2 AND blocked_tg_id = $1)
     LIMIT 1`,
    [tgIdA, tgIdB]
  );
  return r.rowCount > 0;
}

// ---------- Telegram initData верификация ----------
function verifyTelegramInitData(initData, botToken) {
  if (!initData || !botToken) return null;
  try {
    const params = new URLSearchParams(initData);
    const hash = params.get("hash");
    if (!hash) return null;
    params.delete("hash");

    const dataCheckString = [...params.entries()]
      .sort((a, b) => a[0].localeCompare(b[0]))
      .map(([k, v]) => `${k}=${v}`)
      .join("\n");

    const secretKey = crypto.createHmac("sha256", "WebAppData").update(botToken).digest();
    const computedHash = crypto.createHmac("sha256", secretKey).update(dataCheckString).digest("hex");

    if (computedHash !== hash) return null;

    const userJson = params.get("user");
    if (!userJson) return null;
    return JSON.parse(userJson);
  } catch (e) {
    console.warn("[auth] ошибка разбора initData:", e.message);
    return null;
  }
}

// ---------- HTTP роуты ----------
app.use(express.static(path.join(__dirname, "public")));
app.use(express.json());

app.get("/", (req, res) => {
  res.sendFile(path.join(__dirname, "public", "test.html"));
});

// ---------- Stars: создать инвойс ----------
// Клиент вызывает этот эндпоинт, сервер шлёт инвойс боту в личку пользователю,
// Telegram открывает нативный экран оплаты.
app.post("/create-invoice", async (req, res) => {
  const { telegramId, product } = req.body;
  const botToken = process.env.TELEGRAM_BOT_TOKEN;

  if (!botToken) return res.status(500).json({ error: "BOT_TOKEN не задан" });
  if (!telegramId) return res.status(400).json({ error: "telegramId обязателен" });

  const PRODUCTS = {
    premium_30: {
      title: "Spinny Premium — 30 дней",
      description: "Фильтр по полу, приоритетный матчинг и значок премиума на 30 дней",
      payload: `premium_30:${telegramId}`,
      amount: 75,  // 75 Stars ≈ ~$1
      days: 30,
    },
    premium_90: {
      title: "Spinny Premium — 3 месяца",
      description: "Фильтр по полу, приоритетный матчинг и значок премиума на 3 месяца",
      payload: `premium_90:${telegramId}`,
      amount: 200, // 200 Stars ≈ ~$2.6
      days: 90,
    },
  };

  const p = PRODUCTS[product] || PRODUCTS.premium_30;

  try {
    const r = await fetch(`https://api.telegram.org/bot${botToken}/sendInvoice`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        chat_id: telegramId,
        title: p.title,
        description: p.description,
        payload: p.payload,
        provider_token: "",  // пусто для Stars (XTR)
        currency: "XTR",
        prices: [{ label: p.title, amount: p.amount }],
      }),
    });
    const data = await r.json();
    if (!data.ok) throw new Error(data.description);
    console.log("[invoice] отправлен пользователю", telegramId, "| продукт:", product);
    res.json({ ok: true });
  } catch (e) {
    console.error("[invoice] ошибка:", e.message);
    res.status(500).json({ error: e.message });
  }
});

// ---------- Telegram Webhook ----------
// Telegram шлёт сюда все апдейты бота (successful_payment, pre_checkout_query и т.д.)
// Нужно указать этот URL в настройках бота: POST /tg-webhook
app.post("/tg-webhook", async (req, res) => {
  res.sendStatus(200); // Отвечаем Telegram сразу, чтобы не таймаутить

  const update = req.body;
  const botToken = process.env.TELEGRAM_BOT_TOKEN;
  if (!botToken) return;

  // Подтверждаем pre_checkout_query (обязательно в течение 10 сек)
  if (update.pre_checkout_query) {
    const pcq = update.pre_checkout_query;
    await fetch(`https://api.telegram.org/bot${botToken}/answerPreCheckoutQuery`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ pre_checkout_query_id: pcq.id, ok: true }),
    });
    console.log("[pre_checkout] подтверждён для", pcq.from.id);
    return;
  }

  // Успешная оплата
  const payment = update.message?.successful_payment;
  if (payment && payment.currency === "XTR") {
    const telegramId = update.message.from.id;
    const payload = payment.invoice_payload;
    const chargeId = payment.telegram_payment_charge_id;

    console.log("[payment] Stars от", telegramId, "| payload:", payload, "| charge:", chargeId);

    // Парсим payload: "premium_30:123456" или "premium_90:123456"
    const [product] = payload.split(":");
    const days = product === "premium_90" ? 90 : 30;

    await grantPremium(telegramId, days);

    // Уведомляем клиента если он сейчас онлайн
    for (const [sockId, user] of telegramUserOf.entries()) {
      if (user.id === telegramId) {
        io.to(sockId).emit("premium-granted", { days });
        break;
      }
    }

    // Благодарственное сообщение в бот
    await fetch(`https://api.telegram.org/bot${botToken}/sendMessage`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        chat_id: telegramId,
        text: `⭐ Спасибо! Премиум активирован на ${days} дней. Открой Spinny и наслаждайся!`,
      }),
    });
  }
});

app.get("/ice-servers", async (req, res) => {
  const appName = process.env.METERED_APP_NAME;
  const apiKey = process.env.METERED_API_KEY;
  const fallback = [{ urls: "stun:stun.l.google.com:19302" }];

  if (!appName || !apiKey) {
    console.warn("[ice-servers] METERED_APP_NAME / METERED_API_KEY не заданы, отдаю только STUN");
    return res.json(fallback);
  }

  try {
    const r = await fetch(`https://${appName}.metered.live/api/v1/turn/credentials?apiKey=${apiKey}`);
    if (!r.ok) throw new Error(`Metered ответил ${r.status}`);
    const iceServers = await r.json();
    console.log("[ice-servers] получены TURN-креды от Metered");
    res.json(iceServers);
  } catch (e) {
    console.error("[ice-servers] не удалось получить TURN-креды:", e.message);
    res.json(fallback);
  }
});

// ---------- Матчинг ----------
let matchLock = false; // ГЛОБАЛЬНЫЙ — защищает от параллельных tryMatch
const partners = new Map();
const roomOf = new Map();
const telegramUserOf = new Map();

// Очереди ожидания: язык -> socketId (один ожидающий на язык)
// Если не нашли своего языка за LANG_TIMEOUT мс — fallback на любой язык
const waitingByLang = new Map(); // language_code -> socketId
let waitingAny = null;           // fallback-очередь (любой язык)
const LANG_TIMEOUT = 10000;      // 10 сек ждём своего языка, потом расширяем
const langFallbackTimers = new Map(); // socketId -> timer

// Фильтры и статусы пользователей на время сессии
// { gender, wantGender, premium }
const userFilters = new Map();

function getLang(socketId) {
  return telegramUserOf.get(socketId)?.language_code ?? null;
}

function removeFromQueues(socketId) {
  const lang = getLang(socketId);
  if (lang && waitingByLang.get(lang) === socketId) waitingByLang.delete(lang);
  if (waitingAny === socketId) waitingAny = null;
  const t = langFallbackTimers.get(socketId);
  if (t) { clearTimeout(t); langFallbackTimers.delete(socketId); }
}

function clearMatch(socketId, reason = "unknown") {
  const partnerId = partners.get(socketId);
  partners.delete(socketId);
  roomOf.delete(socketId);

  if (partnerId) {
    partners.delete(partnerId);
    roomOf.delete(partnerId);
    console.log("[partner-left] причина:", reason, "| уведомляем:", partnerId);
    io.to(partnerId).emit("partner-left");
  }
}

// ---------- Socket.IO ----------
io.on("connection", (socket) => {
  console.log("[connect]", socket.id);

  socket.on("auth", async (data) => {
    const user = verifyTelegramInitData(data?.initData, process.env.TELEGRAM_BOT_TOKEN);

    if (!user) {
      console.log("[auth] анонимный/тестовый режим для", socket.id);
      return;
    }

    telegramUserOf.set(socket.id, user);
    console.log("[auth] верифицирован Telegram user", user.id, "для сокета", socket.id);

    try {
      await upsertUser(user);

      if (await isBanned(user.id)) {
        console.log("[auth] ЗАБАНЕН:", user.id);
        socket.emit("banned", { reason: "Вы заблокированы за нарушение правил." });
        socket.disconnect(true);
        return;
      }

      // Отправляем клиенту его статус (премиум, пол) чтобы показать правильный UI
      const userPremium = await isPremium(user.id);
      const userGender = await getGender(user.id);
      socket.emit("user-status", { premium: userPremium, gender: userGender });
    } catch (e) {
      console.error("[auth] ошибка БД:", e.message);
    }
  });

  async function doMatch(a, b) {
    const tgA = telegramUserOf.get(a)?.id ?? null;
    const tgB = telegramUserOf.get(b)?.id ?? null;

    if (await isBlockedBetween(tgA, tgB)) {
      console.log("[match] пропускаем пару", a, "<->", b, "— в блок-листе");
      return false;
    }

    // Проверяем гендерный фильтр (только у премиум-пользователей)
    const fA = userFilters.get(a) || {};
    const fB = userFilters.get(b) || {};

    if (fA.premium && fA.wantGender && fB.gender && fA.wantGender !== fB.gender) {
      console.log("[match] пропускаем пару", a, "<->", b, "— гендерный фильтр A");
      return false;
    }
    if (fB.premium && fB.wantGender && fA.gender && fB.wantGender !== fA.gender) {
      console.log("[match] пропускаем пару", a, "<->", b, "— гендерный фильтр B");
      return false;
    }

    const socketA = io.sockets.sockets.get(a);
    const socketB = io.sockets.sockets.get(b);
    if (!socketA || !socketB) return false;

    const room = `${a}#${b}`;
    socketA.join(room);
    socketB.join(room);

    partners.set(a, b);
    partners.set(b, a);
    roomOf.set(a, room);
    roomOf.set(b, room);

    const langA = getLang(a) || "any";
    const langB = getLang(b) || "any";
    console.log("[match]", a, `(${langA})`, "<->", b, `(${langB})`);

    io.to(a).emit("matched", { room, initiator: true });
    io.to(b).emit("matched", { room, initiator: false });
    return true;
  }

  async function tryMatch() {
    if (matchLock) return;
    matchLock = true;

    try {
      const myId = socket.id;
      const myLang = getLang(myId);

      // 1) Ищем собеседника того же языка
      if (myLang && waitingByLang.has(myLang)) {
        const candidate = waitingByLang.get(myLang);
        if (candidate !== myId && io.sockets.sockets.has(candidate)) {
          waitingByLang.delete(myLang);
          removeFromQueues(candidate);
          const matched = await doMatch(candidate, myId);
          if (matched) return;
        } else {
          waitingByLang.delete(myLang);
        }
      }

      // 2) Ищем в общей очереди (любой язык)
      if (waitingAny && waitingAny !== myId && io.sockets.sockets.has(waitingAny)) {
        const candidate = waitingAny;
        waitingAny = null;
        removeFromQueues(candidate);
        const matched = await doMatch(candidate, myId);
        if (matched) return;
      }

      // 3) Никого не нашли — встаём в очередь своего языка
      if (myLang) {
        waitingByLang.set(myLang, myId);
        console.log("[waiting]", myId, "| lang:", myLang);

        // Через LANG_TIMEOUT переходим в общую очередь (fallback)
        const t = setTimeout(() => {
          if (waitingByLang.get(myLang) === myId) {
            waitingByLang.delete(myLang);
            if (!waitingAny) {
              waitingAny = myId;
              console.log("[waiting→any]", myId, "| lang timeout, переходим в общую очередь");
            }
          }
          langFallbackTimers.delete(myId);
        }, LANG_TIMEOUT);
        langFallbackTimers.set(myId, t);
      } else {
        // Язык неизвестен — сразу в общую очередь
        waitingAny = myId;
        console.log("[waiting]", myId, "| lang: unknown → общая очередь");
      }
    } finally {
      matchLock = false;
    }
  }

  // Пол и фильтр по полу (только премиум)
  socket.on("set-gender", async ({ gender, wantGender }) => {
    const tgId = telegramUserOf.get(socket.id)?.id;
    const premium = tgId ? await isPremium(tgId) : false;

    const filters = { gender: gender || null, wantGender: premium ? (wantGender || null) : null, premium };
    userFilters.set(socket.id, filters);

    if (tgId && gender) await setGender(tgId, gender);
    console.log("[set-gender]", socket.id, "| gender:", gender, "| wantGender:", filters.wantGender, "| premium:", premium);
  });

  // Покупка премиума — сервер шлёт инвойс в личку через Telegram Bot API
  socket.on("buy-premium", async ({ product }) => {
    const tgId = telegramUserOf.get(socket.id)?.id;
    if (!tgId) {
      socket.emit("error-msg", "Откройте приложение через Telegram для покупки премиума");
      return;
    }

    const botToken = process.env.TELEGRAM_BOT_TOKEN;
    const PRODUCTS = {
      premium_30: { title: "Spinny Premium — 30 дней", amount: 75, days: 30 },
      premium_90: { title: "Spinny Premium — 3 месяца", amount: 200, days: 90 },
    };
    const p = PRODUCTS[product] || PRODUCTS.premium_30;

    try {
      const r = await fetch(`https://api.telegram.org/bot${botToken}/sendInvoice`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          chat_id: tgId,
          title: p.title,
          description: "Фильтр по полу и приоритетный матчинг в Spinny",
          payload: `${product}:${tgId}`,
          provider_token: "",
          currency: "XTR",
          prices: [{ label: p.title, amount: p.amount }],
        }),
      });
      const data = await r.json();
      if (!data.ok) throw new Error(data.description);
      console.log("[buy-premium] инвойс отправлен пользователю", tgId, "| продукт:", product);
      socket.emit("invoice-sent", { message: "Инвойс отправлен в чат с ботом" });
    } catch (e) {
      console.error("[buy-premium] ошибка:", e.message);
      socket.emit("error-msg", "Ошибка создания инвойса: " + e.message);
    }
  });

  socket.on("find", async () => {    console.log("[find]", socket.id);
    await tryMatch();
  });

  socket.on("skip", async () => {
    console.log("[skip]", socket.id);
    clearMatch(socket.id, "skip");
    removeFromQueues(socket.id);
    await tryMatch();
  });

  // Пользователь блокирует текущего собеседника — они больше не будут мэтчиться
  socket.on("block", async () => {
    const partnerId = partners.get(socket.id);
    const blockerTgId = telegramUserOf.get(socket.id)?.id ?? null;
    const blockedTgId = telegramUserOf.get(partnerId)?.id ?? null;

    console.log("[block]", socket.id, "блокирует", partnerId);

    if (blockerTgId && blockedTgId) {
      await blockUser(blockerTgId, blockedTgId);
    }

    // После блокировки — уходим от этого собеседника и ищем нового
    clearMatch(socket.id, "skip");
    removeFromQueues(socket.id);
    await tryMatch();
  });

  socket.on("report", async ({ imageBase64 }) => {
    const partnerId = partners.get(socket.id);
    console.log("[report] от", socket.id, "| партнёр:", partnerId);

    if (!partnerId) return;

    const apiUser = process.env.SIGHTENGINE_API_USER;
    const apiSecret = process.env.SIGHTENGINE_API_SECRET;

    const reporterTgId = telegramUserOf.get(socket.id)?.id ?? null;
    const offenderTgId = telegramUserOf.get(partnerId)?.id ?? null;

    if (!apiUser || !apiSecret) {
      console.warn("[report] Sightengine не настроен — пропускаем");
      return;
    }

    try {
      const base64Data = (imageBase64 || "").split(",")[1] || imageBase64;
      const buffer = Buffer.from(base64Data, "base64");

      const formData = new FormData();
      formData.append("media", new Blob([buffer], { type: "image/jpeg" }), "report.jpg");
      formData.append("models", "nudity-2.1");
      formData.append("api_user", apiUser);
      formData.append("api_secret", apiSecret);

      const r = await fetch("https://api.sightengine.com/1.0/check.json", { method: "POST", body: formData });
      const result = await r.json();
      console.log("[report] Sightengine:", JSON.stringify(result.nudity || result));

      const raw = result?.nudity?.sexual_activity ?? result?.nudity?.raw ?? 0;
      const partial = result?.nudity?.suggestive ?? result?.nudity?.partial ?? 0;
      const score = Math.max(raw, partial);
      const isViolation = score > 0.6;

      const verdict = isViolation ? "violation" : "clean";
      await saveReport({ reporterTgId, offenderTgId, verdict, nudityScore: score });

      if (isViolation) {
        await banUser(offenderTgId, "nudity — auto ban after report");
        io.to(partnerId).emit("banned", { reason: "Жалоба подтверждена: обнаружен недопустимый контент." });
        io.sockets.sockets.get(partnerId)?.disconnect(true);
        clearMatch(socket.id, "skip");
      }
    } catch (e) {
      console.error("[report] ошибка:", e.message);
      await saveReport({ reporterTgId, offenderTgId, verdict: "error", nudityScore: null });
    }
  });

  socket.on("signal", ({ room, data }) => {
    socket.to(room).emit("signal", data);
  });

  socket.on("reaction", ({ emoji }) => {
    const room = roomOf.get(socket.id);
    console.log("[reaction]", socket.id, "->", emoji, "| roomOf:", room, "| partners:", partners.get(socket.id));
    if (!room) {
      console.log("[reaction] IGNORED — нет активной комнаты для", socket.id);
      return;
    }
    socket.to(room).emit("reaction", { emoji });
  });

  socket.on("disconnect", (reason) => {
    console.log("[disconnect]", socket.id, reason);
    removeFromQueues(socket.id);
    telegramUserOf.delete(socket.id);
    userFilters.delete(socket.id);
    clearMatch(socket.id, "disconnect:" + reason);
  });
});

// ---------- Старт ----------
initDb()
  .then(() => {
    server.listen(process.env.PORT || 3000, () => {
      console.log("SERVER RUNNING");
    });
  })
  .catch((e) => {
    console.error("[db] критическая ошибка инициализации:", e.message);
    // Запускаем сервер даже если БД не поднялась — работаем в in-memory режиме
    server.listen(process.env.PORT || 3000, () => {
      console.log("SERVER RUNNING (без БД)");
    });
  });