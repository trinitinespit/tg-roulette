const express = require("express");
const http = require("http");
const path = require("path");
const crypto = require("crypto");
const { Pool } = require("pg");
const { Server } = require("socket.io");

console.log("[env] BOT_TOKEN:", process.env.TELEGRAM_BOT_TOKEN ? "SET" : "NOT SET");
console.log("[env] DATABASE_URL:", process.env.DATABASE_URL ? "SET" : "NOT SET");
console.log("[env] METERED_APP_NAME:", process.env.METERED_APP_NAME ? "SET" : "NOT SET");
console.log("[env] METERED_API_KEY:", process.env.METERED_API_KEY ? "SET" : "NOT SET");
console.log("[env] TURN_HOST:", process.env.TURN_HOST ? `SET (${process.env.TURN_HOST})` : "NOT SET");
console.log("[env] TURN_SECRET:", process.env.TURN_SECRET ? "SET" : "NOT SET");
console.log("[env] SIGHTENGINE_API_USER:", process.env.SIGHTENGINE_API_USER ? "SET" : "NOT SET");
console.log("[env] SIGHTENGINE_API_SECRET:", process.env.SIGHTENGINE_API_SECRET ? "SET" : "NOT SET");

// GeoIP по IP на сервере не используется — прокси Amvera подменяет реальный IP
// клиента на внутренний (10.244.x.x) ещё до нашего процесса, так что серверный
// MaxMind/geolite2-redist был бесполезен. Страну теперь определяет сам браузер
// клиента через ipapi.co и присылает нам событием "report-my-country".

const app = express();

// ---------- Self-service реклама: цены и настройки ----------
// ВАЖНО: цены — плейсхолдеры, поменяйте под свою модель монетизации.
// Реклама тарифицируется за пакеты по 1000 показов, а не за дни — гаснет
// сама, как только исчерпан оплаченный лимит показов (см. /ad-event).
const AD_MIN_IMPRESSIONS = 1000;
const AD_PRICE_PER_1000_STARS = 500; // ⭐ за 1000 показов
// Эквивалент Stars по курсу на момент настройки (Stars ≈ $0.013, TON ≈ $1.65).
// Курс TON плавающий — время от времени сверяйте и поправляйте это число,
// чтобы не расходилось слишком сильно с ценой в Stars.
const AD_PRICE_PER_1000_TON = 4;     // TON за 1000 показов
const TON_WALLET_ADDRESS = process.env.TON_WALLET_ADDRESS || ""; // ваш адрес для приёма оплаты
const ADMIN_TG_ID = process.env.ADMIN_TG_ID || ""; // куда слать уведомления о новых заявках на рекламу
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
    ssl: { rejectUnauthorized: false }, // требуется для внешних подключений к managed Postgres
    connectionTimeoutMillis: 5000 // не ждём вечно, если БД недоступна
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

    CREATE TABLE IF NOT EXISTS unban_requests (
      id              SERIAL PRIMARY KEY,
      telegram_id     BIGINT NOT NULL,
      charge_id       TEXT NOT NULL UNIQUE,
      stars_paid      INTEGER NOT NULL,
      created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS ads (
      id            SERIAL PRIMARY KEY,
      type          TEXT NOT NULL DEFAULT 'image',  -- 'image' | 'video'
      media_url     TEXT NOT NULL,
      link_url      TEXT,                            -- куда ведёт клик (бот/сайт/канал)
      title         TEXT,
      active        BOOLEAN NOT NULL DEFAULT TRUE,
      impressions   INTEGER NOT NULL DEFAULT 0,
      clicks        INTEGER NOT NULL DEFAULT 0,
      created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS transactions (
      id            SERIAL PRIMARY KEY,
      category      TEXT NOT NULL,  -- 'premium' | 'donate' | 'unban' | 'ad_slot'
      telegram_id   BIGINT,
      amount        NUMERIC NOT NULL,
      currency      TEXT NOT NULL DEFAULT 'XTR', -- 'XTR' (Stars) | 'TON'
      description   TEXT,
      charge_id     TEXT,
      created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);

  // Миграции — добавляем колонки если их нет (безопасно для существующих таблиц)
  await db.query(`
    ALTER TABLE users ADD COLUMN IF NOT EXISTS premium_until TIMESTAMPTZ;
    ALTER TABLE users ADD COLUMN IF NOT EXISTS gender TEXT;
    ALTER TABLE reports ADD COLUMN IF NOT EXISTS image_base64 TEXT;
    ALTER TABLE users ADD COLUMN IF NOT EXISTS reminder_stage INTEGER NOT NULL DEFAULT 0;
    ALTER TABLE users ADD COLUMN IF NOT EXISTS last_reminder_at TIMESTAMPTZ;
    ALTER TABLE users ADD COLUMN IF NOT EXISTS reminders_blocked BOOLEAN NOT NULL DEFAULT FALSE;
    ALTER TABLE users ADD COLUMN IF NOT EXISTS source_param TEXT;
    ALTER TABLE users ADD COLUMN IF NOT EXISTS referred_by BIGINT;
    ALTER TABLE users ADD COLUMN IF NOT EXISTS referral_balance INTEGER NOT NULL DEFAULT 0;
    ALTER TABLE bans ADD COLUMN IF NOT EXISTS evidence_image TEXT;
    ALTER TABLE ads ADD COLUMN IF NOT EXISTS advertiser_tg_id BIGINT;
    ALTER TABLE ads ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'approved'; -- pending_payment | pending_review | approved | rejected
    ALTER TABLE ads ADD COLUMN IF NOT EXISTS payment_method TEXT; -- stars | ton | null (добавлено вручную из админки)
    ALTER TABLE ads ADD COLUMN IF NOT EXISTS payment_amount NUMERIC;
    ALTER TABLE ads ADD COLUMN IF NOT EXISTS ton_comment TEXT;   -- уникальный комментарий для сверки TON-платежа
    ALTER TABLE ads ADD COLUMN IF NOT EXISTS expires_at TIMESTAMPTZ;
    ALTER TABLE ads ADD COLUMN IF NOT EXISTS impression_limit INTEGER;
    ALTER TABLE ads ADD COLUMN IF NOT EXISTS rejection_reason TEXT;
  `).catch(e => console.warn("[db] миграция users:", e.message));

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

async function getBanInfo(telegramId) {
  if (!db || !telegramId) return null;
  const r = await db.query(
    "SELECT reason, evidence_image FROM bans WHERE telegram_id = $1 LIMIT 1",
    [telegramId]
  );
  return r.rows[0] || null;
}

async function banUser(telegramId, reason, evidenceImage = null) {
  if (!telegramId) return;
  if (!db) {
    bannedUsersMemory.add(telegramId);
    console.log("[ban] (in-memory) добавлен:", telegramId);
    return;
  }
  // INSERT OR IGNORE — не падаем если пользователь уже забанен
  await db.query(
    "INSERT INTO bans (telegram_id, reason, evidence_image) VALUES ($1, $2, $3) ON CONFLICT (telegram_id) DO NOTHING",
    [telegramId, reason || null, evidenceImage || null]
  );
  console.log("[ban] (db) добавлен:", telegramId);
}

const REFERRAL_BONUS = 10; // бонусов рефереру за каждого приведённого друга

async function upsertUser(user) {
  if (!db || !user) return;

  // Реферер, если пришли по ссылке вида ?startapp=ref_123456
  let referrerId = null;
  const sp = user._startParam || "";
  const m = sp.match(/^ref_(\d+)$/);
  if (m && String(m[1]) !== String(user.id)) referrerId = m[1]; // нельзя привести самого себя

  const { rows } = await db.query(`
    INSERT INTO users (telegram_id, username, first_name, language_code, source_param, referred_by, last_seen_at)
    VALUES ($1, $2, $3, $4, $5, $6, NOW())
    ON CONFLICT (telegram_id) DO UPDATE SET
      username        = EXCLUDED.username,
      first_name      = EXCLUDED.first_name,
      language_code   = EXCLUDED.language_code,
      source_param    = COALESCE(users.source_param, EXCLUDED.source_param),
      last_seen_at    = NOW(),
      reminder_stage  = 0,
      last_reminder_at = NULL
    RETURNING (xmax = 0) AS inserted
  `, [user.id, user.username || null, user.first_name || null, user.language_code || null, user._startParam || null, referrerId]);

  const isNewUser = rows[0]?.inserted;
  if (isNewUser && referrerId) {
    // Начисляем бонус рефереру, только если такой реферер реально существует
    const res = await db.query(
      `UPDATE users SET referral_balance = referral_balance + $1 WHERE telegram_id = $2`,
      [REFERRAL_BONUS, referrerId]
    );
    if (res.rowCount > 0) {
      console.log("[referral]", user.id, "пришёл от", referrerId, "| начислено:", REFERRAL_BONUS);
    }
  }
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

async function revokePremium(telegramId) {
  if (!db || !telegramId) return;
  await db.query(
    "UPDATE users SET is_premium = FALSE, premium_until = NULL WHERE telegram_id = $1",
    [telegramId]
  );
  console.log("[premium] отозван у пользователя", telegramId);
}

async function logTransaction({ category, telegramId, amount, currency = "XTR", description, chargeId }) {
  if (!db) return;
  try {
    await db.query(
      "INSERT INTO transactions (category, telegram_id, amount, currency, description, charge_id) VALUES ($1,$2,$3,$4,$5,$6)",
      [category, telegramId || null, amount, currency, description || null, chargeId || null]
    );
  } catch (e) {
    console.error("[transactions] ошибка записи:", e.message);
  }
}

// Уведомляет админа в личку боту о новой оплаченной заявке на рекламу,
// ждущей ручного одобрения. Требует ADMIN_TG_ID в переменных окружения —
// без него просто логирует и ничего не отправляет (не критично для работы).
async function notifyAdminNewAdRequest(ad) {
  const botToken = process.env.TELEGRAM_BOT_TOKEN;
  if (!botToken || !ADMIN_TG_ID) {
    console.warn("[ads] ADMIN_TG_ID не задан — уведомление о заявке #" + ad.id + " не отправлено");
    return;
  }
  const paymentInfo = ad.payment_method === "ton"
    ? `TON, комментарий для сверки: ${ad.ton_comment}`
    : `Stars (${ad.payment_amount})`;
  try {
    await fetch(`https://api.telegram.org/bot${botToken}/sendMessage`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        chat_id: ADMIN_TG_ID,
        text: `📢 Новая заявка на рекламу #${ad.id}\n\nТип: ${ad.type}\nОплата: ${paymentInfo}\nСсылка при клике: ${ad.link_url || "—"}\n\nПроверьте и одобрите в /admin → вкладка «Реклама»`,
      }),
    });
  } catch (e) {
    console.error("[ads] не удалось уведомить админа:", e.message);
  }
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

async function saveReport({ reporterTgId, offenderTgId, verdict, nudityScore, imageBase64 }) {
  if (!db) return;
  await db.query(
    "INSERT INTO reports (reporter_tg_id, offender_tg_id, verdict, nudity_score, image_base64) VALUES ($1,$2,$3,$4,$5)",
    [reporterTgId || null, offenderTgId || null, verdict, nudityScore ?? null, imageBase64 || null]
  );
}

async function unbanUser(telegramId, chargeId, starsPaid) {
  if (!db || !telegramId) return;
  await db.query("DELETE FROM bans WHERE telegram_id = $1", [telegramId]);
  await db.query(
    "INSERT INTO unban_requests (telegram_id, charge_id, stars_paid) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING",
    [telegramId, chargeId, starsPaid]
  );
  console.log("[unban] разбанен:", telegramId, "| Stars:", starsPaid, "| charge:", chargeId);
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
    const startParam = params.get("start_param") || null;
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
    const user = JSON.parse(userJson);
    user._startParam = startParam; // куда пришёл пользователь — источник трафика
    return user;
  } catch (e) {
    console.warn("[auth] ошибка разбора initData:", e.message);
    return null;
  }
}

// ---------- Сессия для прямых заходов на домен (не через Mini App) ----------
// Используется когда человек открывает spinnyapp.ru напрямую в браузере —
// там нет Telegram initData, поэтому просим войти через Telegram Login Widget
// и держим личность в подписанной cookie.
const SESSION_SECRET = process.env.SESSION_SECRET || process.env.TELEGRAM_BOT_TOKEN || "spinny-fallback-secret";
const SESSION_MAX_AGE_MS = 30 * 24 * 60 * 60 * 1000; // 30 дней

function signSession(payload) {
  const json = Buffer.from(JSON.stringify(payload)).toString("base64url");
  const sig = crypto.createHmac("sha256", SESSION_SECRET).update(json).digest("hex");
  return `${json}.${sig}`;
}

function verifySession(token) {
  if (!token) return null;
  const [json, sig] = token.split(".");
  if (!json || !sig) return null;
  const expected = crypto.createHmac("sha256", SESSION_SECRET).update(json).digest("hex");
  if (sig !== expected) return null;
  try {
    const payload = JSON.parse(Buffer.from(json, "base64url").toString());
    if (!payload.exp || payload.exp < Date.now()) return null;
    return payload;
  } catch {
    return null;
  }
}

function parseCookies(header) {
  const out = {};
  (header || "").split(";").forEach(pair => {
    const idx = pair.indexOf("=");
    if (idx === -1) return;
    const k = pair.slice(0, idx).trim();
    const v = pair.slice(idx + 1).trim();
    if (k) out[k] = decodeURIComponent(v);
  });
  return out;
}

// Проверка данных от Telegram Login Widget (используется на голом домене,
// в отличие от verifyTelegramInitData который проверяет Mini App).
// Алгоритм отличается от Mini App: secret = SHA256(bot_token), а не HMAC("WebAppData", ...).
function verifyTelegramWidgetAuth(data, botToken) {
  if (!data || !botToken) return null;
  const { hash, ...rest } = data;
  if (!hash) return null;

  const checkString = Object.keys(rest)
    .sort()
    .map(k => `${k}=${rest[k]}`)
    .join("\n");

  const secretKey = crypto.createHash("sha256").update(botToken).digest();
  const computedHash = crypto.createHmac("sha256", secretKey).update(checkString).digest("hex");
  if (computedHash !== hash) return null;

  // Защита от replay — данные виджета не должны быть старше суток
  const authDate = parseInt(rest.auth_date, 10);
  if (!authDate || (Date.now() / 1000 - authDate) > 86400) return null;

  return rest; // { id, first_name, last_name, username, photo_url, auth_date }
}

// Имя бота нужно фронтенду для рендера Login Widget — получаем один раз при старте.
let BOT_USERNAME = null;

// ---------- HTTP роуты ----------
app.use(express.static(path.join(__dirname, "public")));
app.use(express.json());

app.get("/", (req, res) => {
  res.sendFile(path.join(__dirname, "public", "test.html"));
});

// Отдаём фронтенду username бота, чтобы отрендерить Telegram Login Widget
app.get("/bot-info", (req, res) => {
  res.json({ username: BOT_USERNAME });
});

// Приём результата Telegram Login Widget — проверяем подпись и выдаём сессию
app.post("/auth/telegram-widget", (req, res) => {
  const botToken = process.env.TELEGRAM_BOT_TOKEN;
  const user = verifyTelegramWidgetAuth(req.body, botToken);
  if (!user) return res.status(401).json({ ok: false, error: "Неверная подпись Telegram" });

  const sessionPayload = {
    id: Number(user.id),
    first_name: user.first_name || null,
    username: user.username || null,
    exp: Date.now() + SESSION_MAX_AGE_MS,
  };
  const token = signSession(sessionPayload);
  res.setHeader(
    "Set-Cookie",
    `spinny_session=${token}; Path=/; Max-Age=${Math.floor(SESSION_MAX_AGE_MS / 1000)}; HttpOnly; SameSite=Lax${req.secure ? "; Secure" : ""}`
  );
  res.json({ ok: true });
});

// Проверка текущей сессии (используется фронтендом при прямом заходе)
app.get("/auth/me", (req, res) => {
  const cookies = parseCookies(req.headers.cookie);
  const session = verifySession(cookies.spinny_session);
  if (!session) return res.status(401).json({ ok: false });
  res.json({ ok: true, user: session });
});

// ---------- Stars: создать инвойс ----------
// Клиент вызывает этот эндпоинт, сервер шлёт инвойс боту в личку пользователю,
// Telegram открывает нативный экран оплаты.
app.post("/create-invoice", async (req, res) => {
  const { telegramId, product, amount: customAmount } = req.body;
  const botToken = process.env.TELEGRAM_BOT_TOKEN;

  if (!botToken) return res.status(500).json({ error: "BOT_TOKEN не задан" });
  if (!telegramId) return res.status(400).json({ error: "telegramId обязателен" });
  if (isRateLimited("http:" + telegramId, "create-invoice", 8, 60000)) {
    return res.status(429).json({ error: "Слишком много попыток, подождите немного" });
  }

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
    unban: {
      title: "Разблокировка аккаунта Spinny",
      description: "Однократная разблокировка. Пожалуйста, соблюдайте правила после разблокировки.",
      payload: `unban:${telegramId}`,
      amount: 100,
      days: 0,
    },
  };

  let p;

  if (product === "donate") {
    // Донат с произвольной суммой — задаётся пользователем на клиенте
    const amt = Math.floor(Number(customAmount));
    if (!Number.isFinite(amt) || amt < 1 || amt > 100000) {
      return res.status(400).json({ error: "Некорректная сумма доната (1–100000 Stars)" });
    }
    p = {
      title: "Поддержать Spinny",
      description: `Донат на развитие проекта — ${amt} Stars. Спасибо за поддержку! ❤️`,
      payload: `donate:${telegramId}:${amt}`,
      amount: amt,
    };
  } else {
    p = PRODUCTS[product] || PRODUCTS.premium_30;
  }

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

  // Команда /start — приветственное сообщение с кнопкой открытия Mini App
  const message = update.message;
  if (message?.text === "/start" || message?.text?.startsWith("/start ")) {
    const userId = message.from.id;
    const firstName = message.from.first_name || "друг";
    const langCode = (message.from.language_code || "ru").split("-")[0];
    console.log("[start] user", userId, "| raw language_code от Telegram:", message.from.language_code || "(пусто)", "| используем:", langCode);

    const GREETINGS = {
      ru: {
        text: (name) => `Привет, ${name}!\n\n*Spinny* — случайный видеочат прямо в Telegram.\n\nНажми кнопку ниже, разреши доступ к камере — и через секунды окажешься на связи с новым собеседником из любой точки мира.\n\n• Мэтчинг по языку\n• Реакции во время звонка\n• Premium с фильтром по полу\n• Система модерации`,
        button: "Открыть Spinny",
      },
      en: {
        text: (name) => `Hi, ${name}!\n\n*Spinny* is a random video chat right inside Telegram.\n\nTap the button below, allow camera access — and in seconds you'll be talking to someone new from anywhere in the world.\n\n• Language-based matching\n• Reactions during the call\n• Premium with gender filter\n• Moderation system`,
        button: "Open Spinny",
      },
      de: {
        text: (name) => `Hallo, ${name}!\n\n*Spinny* ist ein zufälliger Videochat direkt in Telegram.\n\nTippe unten auf den Button, erlaube den Kamerazugriff — und in Sekunden bist du mit jemand Neuem aus der ganzen Welt verbunden.\n\n• Sprachbasiertes Matching\n• Reaktionen während des Anrufs\n• Premium mit Geschlechterfilter\n• Moderationssystem`,
        button: "Spinny öffnen",
      },
      tr: {
        text: (name) => `Merhaba, ${name}!\n\n*Spinny* Telegram içinde rastgele bir görüntülü sohbet.\n\nAşağıdaki butona dokun, kamera erişimine izin ver — saniyeler içinde dünyanın herhangi bir yerinden biriyle bağlanacaksın.\n\n• Dile göre eşleştirme\n• Görüşme sırasında tepkiler\n• Cinsiyet filtreli Premium\n• Moderasyon sistemi`,
        button: "Spinny'yi Aç",
      },
      es: {
        text: (name) => `¡Hola, ${name}!\n\n*Spinny* es un videochat aleatorio directo en Telegram.\n\nToca el botón de abajo, permite el acceso a la cámara — y en segundos estarás conectado con alguien nuevo de cualquier parte del mundo.\n\n• Emparejamiento por idioma\n• Reacciones durante la llamada\n• Premium con filtro de género\n• Sistema de moderación`,
        button: "Abrir Spinny",
      },
      it: {
        text: (name) => `Ciao, ${name}!\n\n*Spinny* è una videochat casuale direttamente in Telegram.\n\nTocca il pulsante qui sotto, consenti l'accesso alla fotocamera — e in pochi secondi sarai connesso con qualcuno di nuovo da qualsiasi parte del mondo.\n\n• Abbinamento per lingua\n• Reazioni durante la chiamata\n• Premium con filtro di genere\n• Sistema di moderazione`,
        button: "Apri Spinny",
      },
      zh: {
        text: (name) => `你好，${name}！\n\n*Spinny* 是直接在 Telegram 内的随机视频聊天。\n\n点击下方按钮，允许摄像头访问——几秒钟内即可与来自世界各地的新朋友建立联系。\n\n• 按语言匹配\n• 通话中的表情反应\n• 带性别筛选的 Premium\n• 审核系统`,
        button: "打开 Spinny",
      },
      ar: {
        text: (name) => `مرحباً ${name}!\n\n*Spinny* هو دردشة فيديو عشوائية مباشرة داخل تيليجرام.\n\nاضغط على الزر أدناه، اسمح بالوصول إلى الكاميرا — وخلال ثوانٍ ستكون على تواصل مع شخص جديد من أي مكان في العالم.\n\n• مطابقة حسب اللغة\n• تفاعلات أثناء المكالمة\n• Premium مع فلتر الجنس\n• نظام الإشراف`,
        button: "افتح Spinny",
      },
      hi: {
        text: (name) => `नमस्ते, ${name}!\n\n*Spinny* सीधे Telegram में एक रैंडम वीडियो चैट है।\n\nनीचे दिए बटन को दबाएं, कैमरा एक्सेस की अनुमति दें — और कुछ ही सेकंड में आप दुनिया के किसी भी कोने से किसी नए व्यक्ति से जुड़ जाएंगे।\n\n• भाषा के अनुसार मैचिंग\n• कॉल के दौरान रिएक्शन\n• जेंडर फ़िल्टर के साथ Premium\n• मॉडरेशन सिस्टम`,
        button: "Spinny खोलें",
      },
      ja: {
        text: (name) => `こんにちは、${name}さん！\n\n*Spinny* はTelegram内で使える、ランダムなビデオチャットです。\n\n下のボタンをタップして、カメラへのアクセスを許可すると、数秒で世界中の新しい相手とつながります。\n\n• 言語によるマッチング\n• 通話中のリアクション\n• 性別フィルター付きPremium\n• モデレーションシステム`,
        button: "Spinnyを開く",
      },
      fr: {
        text: (name) => `Salut, ${name} !\n\n*Spinny* est un videochat aléatoire directement dans Telegram.\n\nAppuie sur le bouton ci-dessous, autorise l'accès à la caméra — et en quelques secondes tu seras en contact avec quelqu'un de nouveau, n'importe où dans le monde.\n\n• Appariement par langue\n• Réactions pendant l'appel\n• Premium avec filtre de genre\n• Système de modération`,
        button: "Ouvrir Spinny",
      },
      pt: {
        text: (name) => `Olá, ${name}!\n\n*Spinny* é um videochat aleatório direto no Telegram.\n\nToque no botão abaixo, permita o acesso à câmera — e em segundos você estará conectado com alguém novo de qualquer parte do mundo.\n\n• Correspondência por idioma\n• Reações durante a chamada\n• Premium com filtro de gênero\n• Sistema de moderação`,
        button: "Abrir Spinny",
      },
      ko: {
        text: (name) => `안녕하세요, ${name}님!\n\n*Spinny*는 텔레그램 안에서 바로 이용하는 랜덤 영상 채팅입니다.\n\n아래 버튼을 누르고 카메라 접근을 허용하면 몇 초 안에 전 세계 새로운 사람과 연결됩니다.\n\n• 언어별 매칭\n• 통화 중 리액션\n• 성별 필터가 있는 프리미엄\n• 모더레이션 시스템`,
        button: "Spinny 열기",
      },
      id: {
        text: (name) => `Hai, ${name}!\n\n*Spinny* adalah video chat acak langsung di Telegram.\n\nTekan tombol di bawah, izinkan akses kamera — dan dalam hitungan detik kamu akan terhubung dengan orang baru dari seluruh dunia.\n\n• Pencocokan berdasarkan bahasa\n• Reaksi selama panggilan\n• Premium dengan filter gender\n• Sistem moderasi`,
        button: "Buka Spinny",
      },
      uk: {
        text: (name) => `Привіт, ${name}!\n\n*Spinny* — випадковий відеочат прямо в Telegram.\n\nНатисни кнопку нижче, дозволь доступ до камери — і за кілька секунд опинишся на зв'язку з новим співрозмовником з будь-якої точки світу.\n\n• Підбір за мовою\n• Реакції під час дзвінка\n• Premium з фільтром за статтю\n• Система модерації`,
        button: "Відкрити Spinny",
      },
      pl: {
        text: (name) => `Cześć, ${name}!\n\n*Spinny* to losowy wideoczat prosto w Telegramie.\n\nNaciśnij przycisk poniżej, zezwól na dostęp do kamery — a w kilka sekund połączysz się z nową osobą z dowolnego miejsca na świecie.\n\n• Dopasowanie według języka\n• Reakcje podczas rozmowy\n• Premium z filtrem płci\n• System moderacji`,
        button: "Otwórz Spinny",
      },
      nl: {
        text: (name) => `Hoi, ${name}!\n\n*Spinny* is een willekeurige videochat rechtstreeks in Telegram.\n\nTik op de knop hieronder, geef cameratoegang — en binnen enkele seconden ben je verbonden met iemand nieuws van waar dan ook ter wereld.\n\n• Matching op taal\n• Reacties tijdens het gesprek\n• Premium met genderfilter\n• Moderatiesysteem`,
        button: "Spinny openen",
      },
      fa: {
        text: (name) => `سلام ${name}!\n\n*Spinny* یک ویدیوچت تصادفی مستقیماً در تلگرام است.\n\nروی دکمه زیر بزن، دسترسی به دوربین را مجاز کن — و در چند ثانیه با فردی جدید از هر نقطه جهان در ارتباط خواهی بود.\n\n• تطبیق بر اساس زبان\n• واکنش‌ها در حین تماس\n• پرمیوم با فیلتر جنسیت\n• سیستم نظارت`,
        button: "باز کردن Spinny",
      },
      th: {
        text: (name) => `สวัสดี ${name}!\n\n*Spinny* คือวิดีโอแชทแบบสุ่มโดยตรงใน Telegram\n\nแตะปุ่มด้านล่าง อนุญาตให้เข้าถึงกล้อง — และในไม่กี่วินาทีคุณจะได้เชื่อมต่อกับคนใหม่จากทั่วโลก\n\n• จับคู่ตามภาษา\n• รีแอคชันระหว่างการโทร\n• Premium พร้อมตัวกรองเพศ\n• ระบบตรวจสอบ`,
        button: "เปิด Spinny",
      },
      vi: {
        text: (name) => `Chào ${name}!\n\n*Spinny* là video chat ngẫu nhiên ngay trong Telegram.\n\nNhấn nút bên dưới, cho phép truy cập camera — và chỉ trong vài giây bạn sẽ kết nối với người mới từ khắp nơi trên thế giới.\n\n• Ghép đôi theo ngôn ngữ\n• Biểu cảm trong cuộc gọi\n• Premium với bộ lọc giới tính\n• Hệ thống kiểm duyệt`,
        button: "Mở Spinny",
      },
      ms: {
        text: (name) => `Hai, ${name}!\n\n*Spinny* ialah video chat rawak terus dalam Telegram.\n\nTekan butang di bawah, benarkan akses kamera — dan dalam beberapa saat anda akan berhubung dengan seseorang baharu dari mana-mana pelosok dunia.\n\n• Padanan mengikut bahasa\n• Reaksi semasa panggilan\n• Premium dengan penapis jantina\n• Sistem moderasi`,
        button: "Buka Spinny",
      },
      bn: {
        text: (name) => `হ্যালো, ${name}!\n\n*Spinny* হলো টেলিগ্রামের মধ্যেই একটি র‌্যান্ডম ভিডিও চ্যাট।\n\nনিচের বোতামে চাপুন, ক্যামেরা অ্যাক্সেসের অনুমতি দিন — আর কয়েক সেকেন্ডের মধ্যে বিশ্বের যেকোনো প্রান্তের নতুন কারো সাথে যুক্ত হয়ে যাবেন।\n\n• ভাষা অনুযায়ী মিলকরণ\n• কলের সময় রিয়্যাকশন\n• জেন্ডার ফিল্টার সহ প্রিমিয়াম\n• মডারেশন সিস্টেম`,
        button: "Spinny খুলুন",
      },
      ro: {
        text: (name) => `Salut, ${name}!\n\n*Spinny* este un videochat aleatoriu direct în Telegram.\n\nApasă butonul de mai jos, permite accesul la cameră — și în câteva secunde vei fi conectat cu cineva nou din orice colț al lumii.\n\n• Potrivire după limbă\n• Reacții în timpul apelului\n• Premium cu filtru de gen\n• Sistem de moderare`,
        button: "Deschide Spinny",
      },
      el: {
        text: (name) => `Γεια σου, ${name}!\n\nΤο *Spinny* είναι ένα τυχαίο βιντεοτσάτ απευθείας μέσα στο Telegram.\n\nΠάτησε το κουμπί παρακάτω, επίτρεψε την πρόσβαση στην κάμερα — και σε λίγα δευτερόλεπτα θα συνδεθείς με κάποιον νέο από οποιοδήποτε σημείο του κόσμου.\n\n• Αντιστοίχιση βάσει γλώσσας\n• Αντιδράσεις κατά τη διάρκεια της κλήσης\n• Premium με φίλτρο φύλου\n• Σύστημα συντονισμού`,
        button: "Άνοιγμα Spinny",
      },
      cs: {
        text: (name) => `Ahoj, ${name}!\n\n*Spinny* je náhodný videochat přímo v Telegramu.\n\nKlepni na tlačítko níže, povol přístup ke kameře — a během pár vteřin se spojíš s novým člověkem odkudkoli na světě.\n\n• Párování podle jazyka\n• Reakce během hovoru\n• Premium s filtrem pohlaví\n• Systém moderace`,
        button: "Otevřít Spinny",
      },
      sv: {
        text: (name) => `Hej, ${name}!\n\n*Spinny* är en slumpmässig videochatt direkt i Telegram.\n\nTryck på knappen nedan, tillåt kameraåtkomst — och på några sekunder är du kopplad till någon ny från vilken plats som helst i världen.\n\n• Matchning efter språk\n• Reaktioner under samtalet\n• Premium med könsfilter\n• Modereringssystem`,
        button: "Öppna Spinny",
      },
      fi: {
        text: (name) => `Hei, ${name}!\n\n*Spinny* on satunnainen videochat suoraan Telegramissa.\n\nPaina alla olevaa painiketta, salli kameran käyttö — ja muutamassa sekunnissa olet yhteydessä uuteen ihmiseen mistä tahansa päin maailmaa.\n\n• Kielen mukainen yhdistäminen\n• Reaktiot puhelun aikana\n• Premium sukupuolisuodattimella\n• Moderointijärjestelmä`,
        button: "Avaa Spinny",
      },
      da: {
        text: (name) => `Hej, ${name}!\n\n*Spinny* er en tilfældig videochat direkte i Telegram.\n\nTryk på knappen nedenfor, giv adgang til kameraet — og om få sekunder er du forbundet med en ny person fra hele verden.\n\n• Matchning efter sprog\n• Reaktioner under opkaldet\n• Premium med kønsfilter\n• Moderationssystem`,
        button: "Åbn Spinny",
      },
      no: {
        text: (name) => `Hei, ${name}!\n\n*Spinny* er en tilfeldig videochat rett i Telegram.\n\nTrykk på knappen nedenfor, gi tilgang til kameraet — og om noen sekunder er du koblet til en ny person fra hvor som helst i verden.\n\n• Matching etter språk\n• Reaksjoner under samtalen\n• Premium med kjønnsfilter\n• Modereringssystem`,
        button: "Åpne Spinny",
      },
      hu: {
        text: (name) => `Szia, ${name}!\n\nA *Spinny* egy véletlenszerű videocsevegés közvetlenül a Telegramban.\n\nÉrintsd meg az alábbi gombot, engedélyezd a kamera hozzáférést — és pár másodperc múlva már beszélgethetsz valakivel a világ bármely pontjáról.\n\n• Párosítás nyelv szerint\n• Reakciók hívás közben\n• Premium nemi szűrővel\n• Moderációs rendszer`,
        button: "Spinny megnyitása",
      },
      he: {
        text: (name) => `היי, ${name}!\n\n*Spinny* הוא צ'אט וידאו אקראי ישירות בטלגרם.\n\nלחץ על הכפתור למטה, אשר גישה למצלמה — ותוך שניות תתחבר לאדם חדש מכל מקום בעולם.\n\n• התאמה לפי שפה\n• ריאקציות במהלך השיחה\n• פרימיום עם סינון לפי מגדר\n• מערכת מודרציה`,
        button: "פתח את Spinny",
      },
      az: {
        text: (name) => `Salam, ${name}!\n\n*Spinny* birbaşa Telegramda təsadüfi video çatdır.\n\nAşağıdakı düyməyə bas, kameraya girişə icazə ver — bir neçə saniyə ərzində dünyanın istənilən nöqtəsindən yeni biri ilə əlaqədə olacaqsan.\n\n• Dilə görə uyğunlaşdırma\n• Zəng zamanı reaksiyalar\n• Cins filtrli Premium\n• Moderasiya sistemi`,
        button: "Spinny-ni aç",
      },
      kk: {
        text: (name) => `Сәлем, ${name}!\n\n*Spinny* — Telegram ішінде тікелей кездейсоқ бейне чат.\n\nТөмендегі түймені бас, камераға қатынасуға рұқсат бер — бірнеше секундтан кейін әлемнің кез келген нүктесінен жаңа адаммен байланыста боласың.\n\n• Тіл бойынша сәйкестендіру\n• Қоңырау кезіндегі реакциялар\n• Жыныс сүзгісі бар Premium\n• Модерация жүйесі`,
        button: "Spinny ашу",
      },
      uz: {
        text: (name) => `Salom, ${name}!\n\n*Spinny* — bu Telegram ichida to'g'ridan-to'g'ri tasodifiy video chat.\n\nPastdagi tugmani bosing, kameraga ruxsat bering — va bir necha soniyada dunyoning istalgan nuqtasidan yangi odam bilan bog'lanasiz.\n\n• Til bo'yicha moslashtirish\n• Qo'ng'iroq davomidagi reaksiyalar\n• Jins filtri bilan Premium\n• Moderatsiya tizimi`,
        button: "Spinny-ni ochish",
      },
    };

    const g = GREETINGS[langCode] || GREETINGS.ru;

    try {
      const resp = await fetch(`https://api.telegram.org/bot${botToken}/sendPhoto`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          chat_id: userId,
          photo: "https://spinnyapp.ru/spinny_logo.png?v=4",
          caption: g.text(firstName),
          parse_mode: "Markdown",
          reply_markup: {
            inline_keyboard: [[
              {
                text: g.button,
                web_app: { url: "https://spinnyapp.ru" }
              }
            ]]
          }
        }),
      });
      const respJson = await resp.json();
      if (!respJson.ok) {
        console.error("[start] Telegram отклонил sendPhoto:", JSON.stringify(respJson));
      }
    } catch (e) {
      console.error("[start] ошибка при отправке приветствия:", e.message);
    }
    return;
  }

  // Успешная оплата
  const payment = update.message?.successful_payment;
  if (payment && payment.currency === "XTR") {
    const telegramId = update.message.from.id;
    const payload = payment.invoice_payload;
    const chargeId = payment.telegram_payment_charge_id;

    console.log("[payment] Stars от", telegramId, "| payload:", payload, "| charge:", chargeId);

    // Парсим payload: "premium_30:123456", "premium_90:123456", "unban:123456"
    const [product] = payload.split(":");

    if (product === "unban") {
      await unbanUser(telegramId, chargeId, payment.total_amount);
      await logTransaction({ category: "unban", telegramId, amount: payment.total_amount, currency: "XTR", description: "Разблокировка аккаунта", chargeId });

      // Уведомляем клиента если онлайн
      for (const [sockId, user] of telegramUserOf.entries()) {
        if (user.id === telegramId) {
          bannedSockets.delete(sockId);
          io.to(sockId).emit("unbanned");
          break;
        }
      }

      await fetch(`https://api.telegram.org/bot${botToken}/sendMessage`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          chat_id: telegramId,
          text: "✅ Ваш аккаунт разблокирован! Открывайте Spinny и продолжайте общение.",
        }),
      });
      return;
    }

    if (product === "donate") {
      console.log("[donate] получен донат от", telegramId, "| Stars:", payment.total_amount, "| charge:", chargeId);
      await logTransaction({ category: "donate", telegramId, amount: payment.total_amount, currency: "XTR", description: "Донат", chargeId });

      await fetch(`https://api.telegram.org/bot${botToken}/sendMessage`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          chat_id: telegramId,
          text: `❤️ Спасибо огромное за поддержку — ${payment.total_amount} Stars! Это очень помогает развитию Spinny.`,
        }),
      });
      return;
    }

    if (product === "ad_slot") {
      const adId = payload.split(":")[1];
      console.log("[ad_slot] оплата получена, заявка #", adId, "| Stars:", payment.total_amount);
      await logTransaction({ category: "ad_slot", telegramId, amount: payment.total_amount, currency: "XTR", description: "Реклама, заявка #" + adId, chargeId });

      if (db) {
        try {
          const r = await db.query(
            "UPDATE ads SET status = 'pending_review' WHERE id = $1 RETURNING *",
            [adId]
          );
          if (r.rowCount) await notifyAdminNewAdRequest(r.rows[0]);
        } catch (e) {
          console.error("[ad_slot] ошибка обновления заявки:", e.message);
        }
      }

      await fetch(`https://api.telegram.org/bot${botToken}/sendMessage`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          chat_id: telegramId,
          text: "✅ Оплата получена! Заявка на размещение рекламы отправлена на модерацию — как только одобрим, она появится в приложении.",
        }),
      });
      return;
    }

    const days = product === "premium_90" ? 90 : 30;

    await grantPremium(telegramId, days);
    await logTransaction({ category: "premium", telegramId, amount: payment.total_amount, currency: "XTR", description: `Premium на ${days} дней`, chargeId });

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

// ---------- Админка ----------
// Защита через ADMIN_SECRET из env — добавь в Render переменную ADMIN_SECRET=любой_пароль
function adminAuth(req, res, next) {
  const secret = process.env.ADMIN_SECRET;
  if (!secret) return res.status(503).send("ADMIN_SECRET не настроен");
  const token = req.headers["x-admin-secret"] || req.query.secret;
  if (token !== secret) return res.status(401).send("Неверный ключ");
  next();
}

app.get("/admin", adminAuth, async (req, res) => {
  if (!db) return res.status(503).send("БД недоступна");

  try {
    const [bans, reports, users, unban, blocks, ads, transactions, sources, totalUsers, totalPremium] = await Promise.all([
    db.query(`SELECT b.telegram_id, b.reason, b.banned_at,
              u.username, u.first_name
              FROM bans b LEFT JOIN users u ON u.telegram_id = b.telegram_id
              ORDER BY b.banned_at DESC LIMIT 100`),
    db.query(`SELECT r.*, 
              ru.username as reporter_name,
              ou.username as offender_name
              FROM reports r
              LEFT JOIN users ru ON ru.telegram_id = r.reporter_tg_id
              LEFT JOIN users ou ON ou.telegram_id = r.offender_tg_id
              ORDER BY r.created_at DESC LIMIT 100`),
    db.query(`SELECT telegram_id, username, first_name, is_premium, premium_until, gender, last_seen_at, source_param
              FROM users ORDER BY last_seen_at DESC LIMIT 100`),
    db.query(`SELECT ur.*, u.username FROM unban_requests ur
              LEFT JOIN users u ON u.telegram_id = ur.telegram_id
              ORDER BY ur.created_at DESC LIMIT 50`),
    db.query(`SELECT bl.blocker_tg_id, bl.blocked_tg_id, bl.created_at,
              bu.username as blocker_name, ou.username as blocked_name
              FROM blocks bl
              LEFT JOIN users bu ON bu.telegram_id = bl.blocker_tg_id
              LEFT JOIN users ou ON ou.telegram_id = bl.blocked_tg_id
              ORDER BY bl.created_at DESC LIMIT 200`),
    db.query(`SELECT * FROM ads ORDER BY created_at DESC LIMIT 100`),
    db.query(`SELECT t.*, u.username FROM transactions t
              LEFT JOIN users u ON u.telegram_id = t.telegram_id
              ORDER BY t.created_at DESC LIMIT 300`),
    db.query(`SELECT COALESCE(source_param, 'органика') AS source, COUNT(*) AS cnt
              FROM users GROUP BY source ORDER BY cnt DESC LIMIT 50`),
    db.query(`SELECT COUNT(*) AS cnt FROM users`),
    db.query(`SELECT COUNT(*) AS cnt FROM users WHERE is_premium = TRUE`),
  ]);

  // Группируем доходы по категориям и валюте для круговых диаграмм
  const CATEGORY_LABELS = { premium: 'Premium', donate: 'Донаты', unban: 'Разблокировки', ad_slot: 'Реклама', external_ads: 'Telegram Ads' };
  const CATEGORY_COLORS = { premium: '#fbbf24', donate: '#f472b6', unban: '#22c55e', ad_slot: '#38bdf8', external_ads: '#a78bfa' };
  function sumByCategory(rows, currency) {
    const totals = {};
    rows.filter(t => t.currency === currency).forEach(t => {
      totals[t.category] = (totals[t.category] || 0) + Number(t.amount);
    });
    return totals;
  }
  const starsTotals = sumByCategory(transactions.rows, 'XTR');
  const tonTotals = sumByCategory(transactions.rows, 'TON');
  const starsGrandTotal = Object.values(starsTotals).reduce((a, b) => a + b, 0);
  const tonGrandTotal = Object.values(tonTotals).reduce((a, b) => a + b, 0);

  // Строим SVG-«пончик» без библиотек — просто круг с чередующимися stroke-dasharray сегментами
  function buildDonutSvg(totals, grandTotal) {
    if (!grandTotal) return '<div class="empty" style="padding:20px;">Пока пусто</div>';
    const r = 60, circumference = 2 * Math.PI * r;
    let offset = 0;
    const segments = Object.entries(totals).map(([cat, amt]) => {
      const frac = amt / grandTotal;
      const dash = frac * circumference;
      const seg = `<circle cx="80" cy="80" r="${r}" fill="none" stroke="${CATEGORY_COLORS[cat] || '#64748b'}"
        stroke-width="22" stroke-dasharray="${dash} ${circumference - dash}"
        stroke-dashoffset="${-offset}" transform="rotate(-90 80 80)"/>`;
      offset += dash;
      return seg;
    }).join('');
    return `<svg width="160" height="160" viewBox="0 0 160 160">${segments}</svg>`;
  }
  function buildLegend(totals, grandTotal, currencyLabel) {
    return Object.entries(totals).sort((a,b) => b[1]-a[1]).map(([cat, amt]) => `
      <div style="display:flex;align-items:center;gap:8px;font-size:12px;margin-bottom:6px;">
        <span style="width:10px;height:10px;border-radius:50%;background:${CATEGORY_COLORS[cat]||'#64748b'};display:inline-block;"></span>
        <span style="flex:1;color:#cbd5e1;">${CATEGORY_LABELS[cat] || cat}</span>
        <span style="color:#64748b;">${(amt/grandTotal*100).toFixed(0)}%</span>
        <b>${cat === 'ad_slot' && currencyLabel==='TON' ? amt.toFixed(2) : Math.round(amt)} ${currencyLabel}</b>
      </div>`).join('');
  }

  // Считаем онлайн прямо из памяти сервера
  const onlineCount = telegramUserOf.size;
  const activeCalls = getActiveCallsCount();

  res.send(`<!doctype html>
<html lang="ru">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>Spinny Admin</title>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: -apple-system, sans-serif; background: #0f172a; color: #e2e8f0; font-size: 14px; }
  .header { background: #1e293b; padding: 20px 24px; border-bottom: 1px solid #334155; display: flex; align-items: center; justify-content: space-between; }
  .header h1 { font-size: 20px; font-weight: 700; }
  .badge { background: #22c55e; color: #000; border-radius: 999px; padding: 4px 12px; font-size: 12px; font-weight: 700; }
  .tabs { display: flex; gap: 2px; background: #1e293b; padding: 0 24px; border-bottom: 1px solid #334155; }
  .tab { padding: 12px 20px; cursor: pointer; font-weight: 600; color: #94a3b8; border-bottom: 2px solid transparent; font-size: 13px; }
  .tab.active { color: #38bdf8; border-bottom-color: #38bdf8; }
  .content { padding: 24px; }
  .section { display: none; }
  .section.active { display: block; }
  .stats { display: grid; grid-template-columns: repeat(auto-fit, minmax(140px, 1fr)); gap: 12px; margin-bottom: 24px; }
  .stat { background: #1e293b; border-radius: 12px; padding: 16px; text-align: center; }
  .panel-card { background: #1e293b; border-radius: 12px; padding: 18px; flex: 1; min-width: 280px; }
  .stat-num { font-size: 28px; font-weight: 800; color: #38bdf8; }
  .stat-label { font-size: 11px; color: #64748b; margin-top: 4px; text-transform: uppercase; letter-spacing: 0.5px; }
  table { width: 100%; border-collapse: collapse; background: #1e293b; border-radius: 12px; overflow: hidden; }
  th { background: #0f172a; padding: 10px 14px; text-align: left; font-size: 11px; text-transform: uppercase; letter-spacing: 0.5px; color: #64748b; }
  td { padding: 10px 14px; border-top: 1px solid #1e293b; vertical-align: top; }
  tr:hover td { background: rgba(56,189,248,0.04); }
  .verdict-violation { color: #f87171; font-weight: 600; }
  .verdict-csam { color: #ef4444; font-weight: 800; background: rgba(239,68,68,0.15); padding: 2px 8px; border-radius: 4px; }
  .verdict-clean { color: #4ade80; }
  .verdict-error { color: #94a3b8; }
  .verdict-unreviewed { color: #fbbf24; font-weight: 600; }
  .ban-btn { background: #ef4444; border: none; color: white; padding: 4px 10px; border-radius: 6px; cursor: pointer; font-size: 12px; }
  .unban-btn { background: #22c55e; border: none; color: #000; padding: 4px 10px; border-radius: 6px; cursor: pointer; font-size: 12px; }
  .premium-badge { background: #fbbf24; color: #000; border-radius: 4px; padding: 2px 6px; font-size: 11px; font-weight: 700; }
  .empty { text-align: center; color: #475569; padding: 40px; }
  input { background: #1e293b; border: 1px solid #334155; color: #e2e8f0; padding: 8px 12px; border-radius: 8px; font-size: 13px; width: 300px; }
  .action-row { display: flex; gap: 8px; margin-bottom: 16px; align-items: center; }
  .btn { background: #334155; border: none; color: #e2e8f0; padding: 8px 16px; border-radius: 8px; cursor: pointer; font-size: 13px; font-weight: 600; }
  .btn:hover { background: #475569; }
  .btn-danger { background: #ef4444; color: white; }
</style>
</head>
<body>

<div class="header">
  <h1>⚙️ Spinny Admin</h1>
  <span class="badge">🟢 ${onlineCount} онлайн</span>
  <span class="badge" style="background:#38bdf8;">📞 ${activeCalls} звонков сейчас</span>
</div>

<div class="tabs">
  <div class="tab active" onclick="showTab('overview')">Обзор</div>
  <div class="tab" onclick="showTab('bans')">Баны (${bans.rowCount})</div>
  <div class="tab" onclick="showTab('reports')">Репорты (${reports.rowCount})</div>
  <div class="tab" onclick="showTab('users')">Пользователи (${users.rowCount})</div>
  <div class="tab" onclick="showTab('unbans')">Разблокировки (${unban.rowCount})</div>
  <div class="tab" onclick="showTab('blocks')">Блоки (${blocks.rowCount})</div>
  <div class="tab" onclick="showTab('ads')">Реклама (${ads.rowCount})</div>
  <div class="tab" onclick="showTab('revenue')">Доходы</div>
</div>

<div class="content">

  <!-- Обзор -->
  <div class="section active" id="tab-overview">
    <div class="stats">
      <div class="stat"><div class="stat-num">${onlineCount}</div><div class="stat-label">Онлайн сейчас</div></div>
      <div class="stat"><div class="stat-num">${activeCalls}</div><div class="stat-label">Звонков сейчас</div></div>
      <div class="stat"><div class="stat-num">${peakCallsToday}</div><div class="stat-label">Пик звонков сегодня</div></div>
      <div class="stat"><div class="stat-num">${totalUsers.rows[0].cnt}</div><div class="stat-label">Юзеров всего</div></div>
      <div class="stat"><div class="stat-num">${bans.rowCount}</div><div class="stat-label">Банов</div></div>
      <div class="stat"><div class="stat-num">${reports.rows.filter(r=>r.verdict==='violation'||r.verdict==='csam').length}</div><div class="stat-label">Нарушений</div></div>
      <div class="stat"><div class="stat-num">${reports.rows.filter(r=>r.verdict==='csam').length}</div><div class="stat-label">CSAM</div></div>
      <div class="stat"><div class="stat-num">${totalPremium.rows[0].cnt}</div><div class="stat-label">Премиум</div></div>
      <div class="stat"><div class="stat-num">${unban.rowCount}</div><div class="stat-label">Разблокировок</div></div>
    </div>
  </div>

  <!-- Баны -->
  <div class="section" id="tab-bans">
    <div class="action-row">
      <input type="number" id="banIdInput" placeholder="Telegram ID для бана"/>
      <button class="btn btn-danger" onclick="manualBan()">Забанить вручную</button>
    </div>
    ${bans.rowCount === 0 ? '<div class="empty">Банов нет 🎉</div>' : `
    <table>
      <tr><th>TG ID</th><th>Имя</th><th>Причина</th><th>Дата</th><th>Действие</th></tr>
      ${bans.rows.map(b => `
      <tr>
        <td>${b.telegram_id}</td>
        <td>${b.first_name || ''} @${b.username || '—'}</td>
        <td style="max-width:200px;word-break:break-word">${b.reason || '—'}</td>
        <td>${new Date(b.banned_at).toLocaleString('ru')}</td>
        <td><button class="unban-btn" onclick="manualUnban(${b.telegram_id})">Разбанить</button></td>
      </tr>`).join('')}
    </table>`}
  </div>

  <!-- Репорты -->
  <div class="section" id="tab-reports">
    ${reports.rowCount === 0 ? '<div class="empty">Репортов нет</div>' : `
    <table>
      <tr><th>Скриншот</th><th>Жалобщик</th><th>Нарушитель</th><th>Вердикт</th><th>Score</th><th>Дата</th></tr>
      ${reports.rows.map(r => `
      <tr>
        <td>${r.image_base64
          ? `<img src="${r.image_base64.startsWith('data:') ? r.image_base64 : 'data:image/jpeg;base64,' + r.image_base64}" style="width:60px;height:45px;object-fit:cover;border-radius:6px;display:block;cursor:pointer;" onclick="openLightbox(this.src)"/>`
          : '<span style="color:#475569;font-size:11px;">нет</span>'}</td>
        <td>@${r.reporter_name || r.reporter_tg_id || '—'}</td>
        <td>@${r.offender_name || r.offender_tg_id || '—'}</td>
        <td><span class="verdict-${r.verdict}">${r.verdict}</span></td>
        <td>${r.nudity_score != null ? (r.nudity_score*100).toFixed(0)+'%' : '—'}</td>
        <td>${new Date(r.created_at).toLocaleString('ru')}</td>
      </tr>`).join('')}
    </table>`}
  </div>

  <!-- Пользователи -->
  <div class="section" id="tab-users">
    <div class="panel-card" style="margin-bottom:20px;">
      <div class="panel-title" style="margin-bottom:10px;">🔗 Создать ссылку с меткой источника</div>
      <div class="action-row">
        <input type="text" id="sourceLabelInput" placeholder="Метка (например: tiktok_video1)" style="width:260px;"/>
        <button class="btn" onclick="generateSourceLink()">Сгенерировать</button>
      </div>
      <div id="generatedLinkBox" style="display:none;margin-top:10px;">
        <input type="text" id="generatedLinkOutput" readonly style="width:100%;box-sizing:border-box;background:#0f172a;border:1px solid #334155;color:#4ade80;padding:10px 12px;border-radius:8px;font-family:monospace;font-size:13px;"/>
        <button class="btn" style="margin-top:8px;" onclick="copyGeneratedLink()">📋 Скопировать</button>
      </div>
    </div>

    <div class="panel-card" style="margin-bottom:20px;">
      <div class="panel-title" style="margin-bottom:10px;">📊 Пользователи по источникам</div>
      <table>
        <tr><th>Источник</th><th>Пользователей</th></tr>
        ${sources.rows.map(s => `<tr><td>${s.source}</td><td>${s.cnt}</td></tr>`).join('')}
      </table>
    </div>

    <div class="action-row">
      <input type="number" id="premiumIdInput" placeholder="Telegram ID"/>
      <input type="number" id="premiumDaysInput" placeholder="Дней" style="width:100px;" value="30"/>
      <button class="btn" onclick="manualGrantPremium()">Выдать Premium</button>
    </div>
    <table>
      <tr><th>TG ID</th><th>Имя</th><th>Статус</th><th>Пол</th><th>Источник</th><th>Последний вход</th><th>Действие</th></tr>
      ${users.rows.map(u => `
      <tr>
        <td>${u.telegram_id}</td>
        <td>${u.first_name || ''} @${u.username || '—'}</td>
        <td>${u.is_premium ? '<span class="premium-badge">⭐ Premium</span>' : 'Free'}</td>
        <td>${u.gender || '—'}</td>
        <td style="font-size:12px;color:#64748b;">${u.source_param || 'органика'}</td>
        <td>${new Date(u.last_seen_at).toLocaleString('ru')}</td>
        <td>
          ${u.is_premium
            ? `<button class="ban-btn" onclick="manualRevokePremium(${u.telegram_id})">Забрать Premium</button>`
            : `<button class="unban-btn" onclick="manualGrantPremium(${u.telegram_id})">Выдать 30 дней</button>`}
          <button class="ban-btn" onclick="manualBanFromUsers(${u.telegram_id})">Забанить</button>
        </td>
      </tr>`).join('')}
    </table>
  </div>

  <!-- Разблокировки -->
  <div class="section" id="tab-unbans">
    ${unban.rowCount === 0 ? '<div class="empty">Разблокировок нет</div>' : `
    <table>
      <tr><th>TG ID</th><th>Имя</th><th>Stars</th><th>Charge ID</th><th>Дата</th></tr>
      ${unban.rows.map(u => `
      <tr>
        <td>${u.telegram_id}</td>
        <td>@${u.username || '—'}</td>
        <td>${u.stars_paid} ⭐</td>
        <td style="font-size:11px;color:#64748b">${u.charge_id}</td>
        <td>${new Date(u.created_at).toLocaleString('ru')}</td>
      </tr>`).join('')}
    </table>`}
  </div>

  <!-- Блоки -->
  <div class="section" id="tab-blocks">
    <div class="action-row">
      <input type="number" id="unblockIdInput" placeholder="Telegram ID — снять все блоки этого юзера"/>
      <button class="btn" onclick="manualUnblockAll()">Снять все блоки для ID</button>
    </div>
    ${blocks.rowCount === 0 ? '<div class="empty">Блокировок нет 🎉</div>' : `
    <table>
      <tr><th>Кто заблокировал</th><th>Кого заблокировал</th><th>Дата</th><th>Действие</th></tr>
      ${blocks.rows.map(b => `
      <tr>
        <td>${b.blocker_tg_id} ${b.blocker_name ? '@'+b.blocker_name : ''}</td>
        <td>${b.blocked_tg_id} ${b.blocked_name ? '@'+b.blocked_name : ''}</td>
        <td>${new Date(b.created_at).toLocaleString('ru')}</td>
        <td><button class="unban-btn" onclick="manualUnblockPair(${b.blocker_tg_id}, ${b.blocked_tg_id})">Снять эту пару</button></td>
      </tr>`).join('')}
    </table>`}
  </div>

  <!-- Реклама -->
  <div class="section" id="tab-ads">
    ${(() => {
      const pending = ads.rows.filter(a => a.status === 'pending_review');
      if (!pending.length) return '';
      return `
      <h3 style="margin-bottom:10px;color:#fbbf24;">⏳ Заявки на модерации (${pending.length})</h3>
      <table style="margin-bottom:24px;">
        <tr><th>Превью</th><th>Тип</th><th>Ссылка</th><th>Рекламодатель</th><th>Пакет</th><th>Оплата</th><th>Действие</th></tr>
        ${pending.map(a => `
        <tr>
          <td>${a.type === 'image'
            ? `<img src="${a.media_url}" style="width:70px;height:45px;object-fit:cover;border-radius:6px;cursor:pointer;" onclick="openLightbox(this.src)"/>`
            : `<video src="${a.media_url}" style="width:70px;height:45px;object-fit:cover;border-radius:6px;" muted controls></video>`}</td>
          <td>${a.type === 'image' ? '🖼️' : '🎬'}</td>
          <td style="max-width:200px;word-break:break-word;font-size:11px;color:#64748b">${a.link_url || '—'}</td>
          <td>${a.advertiser_tg_id || '—'}</td>
          <td>${(a.impression_limit || 0).toLocaleString('ru')} показов</td>
          <td>${a.payment_method === 'ton' ? `TON ${a.payment_amount || ''}<br><span style="font-size:10px;color:#64748b">${a.ton_comment || ''}</span>` : `⭐ ${a.payment_amount || ''}`}</td>
          <td>
            <button class="unban-btn" onclick="approveAd(${a.id})">Одобрить</button>
            <button class="ban-btn" onclick="rejectAd(${a.id})">Отклонить</button>
          </td>
        </tr>`).join('')}
      </table>`;
    })()}

    <h3 style="margin-bottom:10px;">Добавить рекламу вручную</h3>
    <div class="action-row" style="flex-wrap:wrap;">
      <select id="adType" style="background:#1e293b;border:1px solid #334155;color:#e2e8f0;padding:8px 12px;border-radius:8px;">
        <option value="image">Картинка (интерстишл между звонками)</option>
        <option value="video">Видео (во время поиска собеседника)</option>
      </select>
      <input type="text" id="adMediaUrl" placeholder="URL картинки/видео" style="width:280px;"/>
      <input type="text" id="adLinkUrl" placeholder="Ссылка при клике (t.me/... или https://...)" style="width:280px;"/>
      <input type="text" id="adTitle" placeholder="Название (для себя, необязательно)" style="width:200px;"/>
      <button class="btn" onclick="addAd()">Добавить рекламу</button>
    </div>
    ${ads.rowCount === 0 ? '<div class="empty">Реклама не добавлена</div>' : `
    <table>
      <tr><th>Превью</th><th>Тип</th><th>Название</th><th>Ссылка</th><th>Показы</th><th>Клики</th><th>Статус</th><th>Действие</th></tr>
      ${ads.rows.filter(a => a.status !== 'pending_review').map(a => `
      <tr>
        <td>${a.type === 'image'
          ? `<img src="${a.media_url}" style="width:70px;height:45px;object-fit:cover;border-radius:6px;cursor:pointer;" onclick="openLightbox(this.src)"/>`
          : `<video src="${a.media_url}" style="width:70px;height:45px;object-fit:cover;border-radius:6px;" muted></video>`}</td>
        <td>${a.type === 'image' ? '🖼️ Картинка' : '🎬 Видео'}</td>
        <td>${a.title || '—'}</td>
        <td style="max-width:200px;word-break:break-word;font-size:11px;color:#64748b">${a.link_url || '—'}</td>
        <td>${a.impressions}${a.impression_limit ? ' / ' + a.impression_limit : ''}</td>
        <td>${a.clicks}</td>
        <td>${a.active && a.status === 'approved' ? '<span style="color:#4ade80">Активна</span>'
              : a.status === 'rejected' ? '<span style="color:#f87171">Отклонена</span>'
              : a.status === 'pending_payment' ? '<span style="color:#64748b">Ждёт оплаты</span>'
              : a.status === 'approved' ? '<span style="color:#64748b">Лимит исчерпан</span>'
              : '<span style="color:#64748b">Выключена</span>'}</td>
        <td>
          ${a.status === 'approved' ? `<button class="btn" onclick="toggleAd(${a.id})">${a.active ? 'Выключить' : 'Включить'}</button>` : ''}
          <button class="ban-btn" onclick="deleteAd(${a.id})">Удалить</button>
        </td>
      </tr>`).join('')}
    </table>`}
  </div>

  <!-- Доходы -->
  <div class="section" id="tab-revenue">
    <div style="display:flex;gap:24px;flex-wrap:wrap;margin-bottom:24px;">
      <div class="panel-card">
        <div class="panel-title" style="text-align:center;margin-bottom:10px;color:#64748b;font-size:11px;text-transform:uppercase;">Доходы в Stars</div>
        <div style="display:flex;align-items:center;gap:16px;">
          ${buildDonutSvg(starsTotals, starsGrandTotal)}
          <div>${buildLegend(starsTotals, starsGrandTotal, '⭐')}</div>
        </div>
        <div style="text-align:center;margin-top:10px;font-size:20px;font-weight:800;color:#38bdf8;">${Math.round(starsGrandTotal)} ⭐</div>
      </div>
      <div class="panel-card">
        <div class="panel-title" style="text-align:center;margin-bottom:10px;color:#64748b;font-size:11px;text-transform:uppercase;">Доходы в TON</div>
        <div style="display:flex;align-items:center;gap:16px;">
          ${buildDonutSvg(tonTotals, tonGrandTotal)}
          <div>${buildLegend(tonTotals, tonGrandTotal, 'TON')}</div>
        </div>
        <div style="text-align:center;margin-top:10px;font-size:20px;font-weight:800;color:#38bdf8;">${tonGrandTotal.toFixed(2)} TON</div>
      </div>
    </div>

    <div class="action-row" style="flex-wrap:wrap;">
      <select id="txCategory" style="background:#1e293b;border:1px solid #334155;color:#e2e8f0;padding:8px 12px;border-radius:8px;">
        <option value="external_ads" style="color:#000;">Telegram Ads / Adsgram (вручную)</option>
        <option value="premium" style="color:#000;">Premium</option>
        <option value="donate" style="color:#000;">Донат</option>
        <option value="ad_slot" style="color:#000;">Наша реклама</option>
      </select>
      <input type="number" id="txAmount" placeholder="Сумма" style="width:120px;"/>
      <select id="txCurrency" style="background:#1e293b;border:1px solid #334155;color:#e2e8f0;padding:8px 12px;border-radius:8px;">
        <option value="XTR" style="color:#000;">⭐ Stars</option>
        <option value="TON" style="color:#000;">TON</option>
      </select>
      <input type="text" id="txDescription" placeholder="Описание (необязательно)" style="width:220px;"/>
      <button class="btn" onclick="addManualTransaction()">Добавить</button>
    </div>

    ${transactions.rowCount === 0 ? '<div class="empty">Транзакций пока нет</div>' : `
    <table>
      <tr><th>Дата и время</th><th>Категория</th><th>Пользователь</th><th>Описание</th><th>Сумма</th></tr>
      ${transactions.rows.map(t => `
      <tr>
        <td style="font-size:12px;color:#64748b;">${new Date(t.created_at).toLocaleString('ru')}</td>
        <td><span style="color:${CATEGORY_COLORS[t.category]||'#64748b'};">● ${CATEGORY_LABELS[t.category] || t.category}</span></td>
        <td>${t.username ? '@'+t.username : (t.telegram_id || '—')}</td>
        <td style="font-size:12px;color:#94a3b8;">${t.description || '—'}</td>
        <td style="font-weight:700;color:#4ade80;">+${t.currency === 'TON' ? Number(t.amount).toFixed(2) : Math.round(t.amount)} ${t.currency === 'TON' ? 'TON' : '⭐'}</td>
      </tr>`).join('')}
    </table>`}
  </div>

</div>

<!-- Лайтбокс для просмотра скриншотов в полный размер -->
<div id="lightbox" onclick="closeLightbox()" style="display:none;position:fixed;inset:0;background:rgba(0,0,0,0.9);z-index:99999;align-items:center;justify-content:center;cursor:zoom-out;">
  <img id="lightboxImg" style="max-width:90vw;max-height:90vh;border-radius:8px;" />
</div>

<script>
const SECRET = new URLSearchParams(location.search).get('secret') || '';

function openLightbox(src) {
  document.getElementById('lightboxImg').src = src;
  document.getElementById('lightbox').style.display = 'flex';
}
function closeLightbox() {
  document.getElementById('lightbox').style.display = 'none';
}

function showTab(name) {
  document.querySelectorAll('.section').forEach(s => s.classList.remove('active'));
  document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
  document.getElementById('tab-' + name).classList.add('active');
  event.target.classList.add('active');
}

async function manualBan() {
  const id = document.getElementById('banIdInput').value;
  if (!id) return alert('Введите Telegram ID');
  if (!confirm('Забанить ' + id + '?')) return;
  const r = await fetch('/admin/ban', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'x-admin-secret': SECRET },
    body: JSON.stringify({ telegramId: parseInt(id), reason: 'manual ban' })
  });
  if (r.ok) { alert('Забанен'); location.reload(); }
  else alert('Ошибка: ' + await r.text());
}

async function manualBanFromUsers(id) {
  if (!confirm('Забанить ' + id + '?')) return;
  const r = await fetch('/admin/ban', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'x-admin-secret': SECRET },
    body: JSON.stringify({ telegramId: id, reason: 'manual ban' })
  });
  if (r.ok) { alert('Забанен'); location.reload(); }
  else alert('Ошибка: ' + await r.text());
}

function generateSourceLink() {
  const label = document.getElementById('sourceLabelInput').value.trim();
  if (!label) return alert('Введите метку');
  if (!/^[a-zA-Z0-9_-]{1,64}$/.test(label)) return alert('Метка: только латинские буквы, цифры, _ и -, до 64 символов, без пробелов');
  const link = \`https://t.me/SpinnyChat_bot/app?startapp=\${label}\`;
  document.getElementById('generatedLinkOutput').value = link;
  document.getElementById('generatedLinkBox').style.display = 'block';
}
function copyGeneratedLink() {
  const input = document.getElementById('generatedLinkOutput');
  input.select();
  navigator.clipboard.writeText(input.value).then(() => alert('Скопировано!'));
}

async function manualGrantPremium(idFromRow) {
  const id = idFromRow || document.getElementById('premiumIdInput').value;
  if (!id) return alert('Введите Telegram ID');
  const daysInput = document.getElementById('premiumDaysInput');
  const days = idFromRow ? 30 : (parseInt(daysInput.value) || 30);
  if (!confirm('Выдать Premium на ' + days + ' дней пользователю ' + id + '?')) return;
  const r = await fetch('/admin/grant-premium', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'x-admin-secret': SECRET },
    body: JSON.stringify({ telegramId: parseInt(id), days })
  });
  if (r.ok) { alert('Premium выдан'); location.reload(); }
  else alert('Ошибка: ' + await r.text());
}

async function manualRevokePremium(id) {
  if (!confirm('Забрать Premium у ' + id + '?')) return;
  const r = await fetch('/admin/revoke-premium', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'x-admin-secret': SECRET },
    body: JSON.stringify({ telegramId: id })
  });
  if (r.ok) { alert('Premium отозван'); location.reload(); }
  else alert('Ошибка: ' + await r.text());
}

async function addAd() {
  const type = document.getElementById('adType').value;
  const mediaUrl = document.getElementById('adMediaUrl').value.trim();
  const linkUrl = document.getElementById('adLinkUrl').value.trim();
  const title = document.getElementById('adTitle').value.trim();
  if (!mediaUrl) return alert('Введите URL картинки/видео');
  const r = await fetch('/admin/ads', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'x-admin-secret': SECRET },
    body: JSON.stringify({ type, mediaUrl, linkUrl, title })
  });
  if (r.ok) { alert('Реклама добавлена'); location.reload(); }
  else alert('Ошибка: ' + await r.text());
}

async function toggleAd(id) {
  const r = await fetch('/admin/ads/' + id + '/toggle', {
    method: 'POST',
    headers: { 'x-admin-secret': SECRET }
  });
  if (r.ok) location.reload();
  else alert('Ошибка: ' + await r.text());
}

async function approveAd(id) {
  if (!confirm('Одобрить и запустить эту рекламу?')) return;
  const r = await fetch('/admin/ads/' + id + '/approve', {
    method: 'POST',
    headers: { 'x-admin-secret': SECRET }
  });
  if (r.ok) { alert('Реклама одобрена и запущена'); location.reload(); }
  else alert('Ошибка: ' + await r.text());
}

async function rejectAd(id) {
  const reason = prompt('Причина отклонения (необязательно):') || '';
  const r = await fetch('/admin/ads/' + id + '/reject', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'x-admin-secret': SECRET },
    body: JSON.stringify({ reason })
  });
  if (r.ok) { alert('Заявка отклонена'); location.reload(); }
  else alert('Ошибка: ' + await r.text());
}

async function deleteAd(id) {
  if (!confirm('Удалить это объявление насовсем?')) return;
  const r = await fetch('/admin/ads/' + id + '/delete', {
    method: 'POST',
    headers: { 'x-admin-secret': SECRET }
  });
  if (r.ok) location.reload();
  else alert('Ошибка: ' + await r.text());
}

async function addManualTransaction() {
  const category = document.getElementById('txCategory').value;
  const amount = document.getElementById('txAmount').value;
  const currency = document.getElementById('txCurrency').value;
  const description = document.getElementById('txDescription').value;
  if (!amount || Number(amount) <= 0) return alert('Введите сумму');
  const r = await fetch('/admin/transactions', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'x-admin-secret': SECRET },
    body: JSON.stringify({ category, amount, currency, description })
  });
  if (r.ok) location.reload();
  else alert('Ошибка: ' + await r.text());
}

async function manualUnban(id) {
  if (!confirm('Разбанить ' + id + '?')) return;
  const r = await fetch('/admin/unban', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'x-admin-secret': SECRET },
    body: JSON.stringify({ telegramId: id })
  });
  if (r.ok) { alert('Разбанен'); location.reload(); }
  else alert('Ошибка: ' + await r.text());
}

async function manualUnblockAll() {
  const id = document.getElementById('unblockIdInput').value;
  if (!id) return alert('Введите Telegram ID');
  if (!confirm('Снять все блоки для ' + id + ' (в обе стороны)?')) return;
  const r = await fetch('/admin/unblock', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'x-admin-secret': SECRET },
    body: JSON.stringify({ telegramId: parseInt(id) })
  });
  if (r.ok) { alert('Блоки сняты'); location.reload(); }
  else alert('Ошибка: ' + await r.text());
}

async function manualUnblockPair(blockerId, blockedId) {
  if (!confirm('Снять блок ' + blockerId + ' → ' + blockedId + '?')) return;
  const r = await fetch('/admin/unblock', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'x-admin-secret': SECRET },
    body: JSON.stringify({ blockerTgId: blockerId, blockedTgId: blockedId })
  });
  if (r.ok) { alert('Блок снят'); location.reload(); }
  else alert('Ошибка: ' + await r.text());
}
</script>
</body>
</html>`);
  } catch (e) {
    console.error("[admin] ошибка:", e.message, e.stack);
    res.status(500).send("Ошибка: " + e.message);
  }
});

// Ручной бан через админку
app.post("/admin/ban", adminAuth, async (req, res) => {
  const { telegramId, reason } = req.body;
  if (!telegramId) return res.status(400).send("telegramId обязателен");
  await banUser(telegramId, reason || "manual ban");
  // Кикаем если онлайн
  for (const [sockId, user] of telegramUserOf.entries()) {
    if (user.id === telegramId) {
      io.to(sockId).emit("banned", { reason: "Ваш аккаунт заблокирован администратором." });
      setTimeout(() => io.sockets.sockets.get(sockId)?.disconnect(true), 500);
      break;
    }
  }
  res.json({ ok: true });
});

// Ручная выдача премиума через админку
app.post("/admin/grant-premium", adminAuth, async (req, res) => {
  const { telegramId, days } = req.body;
  if (!telegramId) return res.status(400).send("telegramId обязателен");
  const d = Number.isFinite(Number(days)) && Number(days) > 0 ? Math.floor(Number(days)) : 30;
  await grantPremium(telegramId, d);
  // Уведомляем клиента если он сейчас онлайн
  for (const [sockId, user] of telegramUserOf.entries()) {
    if (user.id === telegramId) {
      io.to(sockId).emit("premium-granted", { days: d });
      break;
    }
  }
  console.log("[admin] ручная выдача премиума:", telegramId, "| дней:", d);
  res.json({ ok: true });
});

// Ручной отзыв премиума через админку
app.post("/admin/revoke-premium", adminAuth, async (req, res) => {
  const { telegramId } = req.body;
  if (!telegramId) return res.status(400).send("telegramId обязателен");
  await revokePremium(telegramId);
  console.log("[admin] ручной отзыв премиума:", telegramId);
  res.json({ ok: true });
});

// ---------- Реклама: управление из админки ----------
// Ручное добавление транзакции — для доходов, которые не проходят через
// наш платёжный поток (например, выплаты от Adsgram/Telegram Ads)
app.post("/admin/transactions", adminAuth, async (req, res) => {
  const { category, amount, currency, description } = req.body;
  if (!category || !amount) return res.status(400).send("category и amount обязательны");
  await logTransaction({ category, amount: Number(amount), currency: currency || "XTR", description });
  console.log("[admin] добавлена ручная транзакция:", category, amount, currency);
  res.json({ ok: true });
});

app.post("/admin/ads", adminAuth, async (req, res) => {
  if (!db) return res.status(503).send("БД недоступна");
  const { type, mediaUrl, linkUrl, title } = req.body;
  if (!mediaUrl) return res.status(400).send("mediaUrl обязателен");
  if (!["image", "video"].includes(type)) return res.status(400).send("type должен быть image или video");
  try {
    await db.query(
      "INSERT INTO ads (type, media_url, link_url, title) VALUES ($1,$2,$3,$4)",
      [type, mediaUrl, linkUrl || null, title || null]
    );
    console.log("[admin] новая реклама добавлена:", type, mediaUrl);
    res.json({ ok: true });
  } catch (e) {
    res.status(500).send("Ошибка: " + e.message);
  }
});

app.post("/admin/ads/:id/toggle", adminAuth, async (req, res) => {
  if (!db) return res.status(503).send("БД недоступна");
  try {
    await db.query("UPDATE ads SET active = NOT active WHERE id = $1", [req.params.id]);
    res.json({ ok: true });
  } catch (e) {
    res.status(500).send("Ошибка: " + e.message);
  }
});

app.post("/admin/ads/:id/delete", adminAuth, async (req, res) => {
  if (!db) return res.status(503).send("БД недоступна");
  try {
    await db.query("DELETE FROM ads WHERE id = $1", [req.params.id]);
    res.json({ ok: true });
  } catch (e) {
    res.status(500).send("Ошибка: " + e.message);
  }
});

// Одобрить заявку на рекламу — активирует до исчерпания оплаченного лимита показов
app.post("/admin/ads/:id/approve", adminAuth, async (req, res) => {
  if (!db) return res.status(503).send("БД недоступна");
  try {
    const r = await db.query(
      `UPDATE ads SET status = 'approved', active = TRUE
       WHERE id = $1 RETURNING advertiser_tg_id, impression_limit, payment_method, payment_amount, ton_comment`,
      [req.params.id]
    );
    console.log("[admin] реклама #" + req.params.id + " одобрена");

    const adRow = r.rows[0];
    if (adRow?.payment_method === "ton") {
      await logTransaction({
        category: "ad_slot", telegramId: adRow.advertiser_tg_id, amount: adRow.payment_amount,
        currency: "TON", description: "Реклама (TON), заявка #" + req.params.id, chargeId: adRow.ton_comment,
      });
    }

    // Уведомляем рекламодателя, если это была self-service заявка
    const advertiserTgId = r.rows[0]?.advertiser_tg_id;
    const impressionLimit = r.rows[0]?.impression_limit;
    const botToken = process.env.TELEGRAM_BOT_TOKEN;
    if (advertiserTgId && botToken) {
      await fetch(`https://api.telegram.org/bot${botToken}/sendMessage`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          chat_id: advertiserTgId,
          text: `✅ Ваша реклама одобрена и запущена! Пакет: ${(impressionLimit || 0).toLocaleString('ru')} показов.`,
        }),
      }).catch(() => {});
    }
    res.json({ ok: true });
  } catch (e) {
    res.status(500).send("Ошибка: " + e.message);
  }
});

// Отклонить заявку на рекламу
app.post("/admin/ads/:id/reject", adminAuth, async (req, res) => {
  if (!db) return res.status(503).send("БД недоступна");
  const { reason } = req.body;
  try {
    const r = await db.query(
      "UPDATE ads SET status = 'rejected', rejection_reason = $2 WHERE id = $1 RETURNING advertiser_tg_id",
      [req.params.id, reason || null]
    );
    console.log("[admin] реклама #" + req.params.id + " отклонена");

    const advertiserTgId = r.rows[0]?.advertiser_tg_id;
    const botToken = process.env.TELEGRAM_BOT_TOKEN;
    if (advertiserTgId && botToken) {
      await fetch(`https://api.telegram.org/bot${botToken}/sendMessage`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          chat_id: advertiserTgId,
          text: `❌ Ваша заявка на рекламу отклонена.${reason ? "\n\nПричина: " + reason : ""}`,
        }),
      }).catch(() => {});
    }
    res.json({ ok: true });
  } catch (e) {
    res.status(500).send("Ошибка: " + e.message);
  }
});

// Ручной разбан через админку
app.post("/admin/unban", adminAuth, async (req, res) => {
  const { telegramId } = req.body;
  if (!telegramId) return res.status(400).send("telegramId обязателен");
  if (db) await db.query("DELETE FROM bans WHERE telegram_id = $1", [telegramId]);
  // Снимаем флаг и уведомляем клиента если он сейчас онлайн
  for (const [sockId, user] of telegramUserOf.entries()) {
    if (user.id === telegramId) {
      bannedSockets.delete(sockId);
      io.to(sockId).emit("unbanned");
      break;
    }
  }
  console.log("[admin] ручной разбан:", telegramId);
  res.json({ ok: true });
});

// Снятие блокировки через админку.
// Либо { telegramId } — снимает ВСЕ блоки этого юзера (в обе стороны, как блокировщик и как заблокированный),
// либо { blockerTgId, blockedTgId } — снимает конкретную пару.
app.post("/admin/unblock", adminAuth, async (req, res) => {
  if (!db) return res.status(503).send("БД недоступна");
  const { telegramId, blockerTgId, blockedTgId } = req.body;

  try {
    if (blockerTgId && blockedTgId) {
      await db.query(
        "DELETE FROM blocks WHERE blocker_tg_id = $1 AND blocked_tg_id = $2",
        [blockerTgId, blockedTgId]
      );
      console.log("[admin] снят блок:", blockerTgId, "->", blockedTgId);
    } else if (telegramId) {
      await db.query(
        "DELETE FROM blocks WHERE blocker_tg_id = $1 OR blocked_tg_id = $1",
        [telegramId]
      );
      console.log("[admin] сняты все блоки для:", telegramId);
    } else {
      return res.status(400).send("Нужен telegramId, либо blockerTgId + blockedTgId");
    }
    res.json({ ok: true });
  } catch (e) {
    console.error("[admin] ошибка unblock:", e.message);
    res.status(500).send("Ошибка: " + e.message);
  }
});

// Генерация временных TURN-кредов по стандарту TURN REST API (RFC-подобный,
// тот же механизм, что use-auth-secret в coturn). Секрет общий с turnserver.conf
// (static-auth-secret), поэтому coturn проверяет creds без похода в БД.
function getTurnCredentials(ttlSeconds = 86400) {
  const secret = process.env.TURN_SECRET;
  const username = String(Math.floor(Date.now() / 1000) + ttlSeconds);
  const credential = crypto.createHmac("sha1", secret).update(username).digest("base64");
  return { username, credential };
}

// ---------- Реклама (управляется вручную из админки) ----------
app.get("/custom-ads", async (req, res) => {
  if (!db) return res.json([]);
  try {
    const r = await db.query(
      `SELECT id, type, media_url, link_url, title FROM ads
       WHERE active = TRUE AND status = 'approved'
         AND (expires_at IS NULL OR expires_at > NOW())
       ORDER BY created_at DESC LIMIT 20`
    );
    res.json(r.rows);
  } catch (e) {
    console.error("[custom-ads] ошибка:", e.message);
    res.json([]);
  }
});

app.post("/ad-event", async (req, res) => {
  if (!db) return res.json({ ok: true });
  const { adId, type } = req.body;
  if (!adId || !["impression", "click"].includes(type)) return res.status(400).json({ error: "bad request" });
  try {
    const col = type === "click" ? "clicks" : "impressions";
    const r = await db.query(
      `UPDATE ads SET ${col} = ${col} + 1 WHERE id = $1 RETURNING impressions, impression_limit, active`,
      [adId]
    );
    const row = r.rows[0];
    // Показ достиг оплаченного лимита — гасим рекламу сами, дальше её показывать нельзя
    if (row && row.active && row.impression_limit != null && row.impressions >= row.impression_limit) {
      await db.query("UPDATE ads SET active = FALSE WHERE id = $1", [adId]);
      console.log("[ads] реклама #" + adId + " исчерпала лимит показов (" + row.impression_limit + ") — отключена");
    }
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ---------- Self-service заявка на размещение рекламы ----------
// Рекламодатель отправляет заявку прямо из Mini App (экран «Разместить рекламу»),
// выбирает Stars или TON, платит, и заявка уходит на ручную модерацию (см. /admin).
app.post("/submit-ad", async (req, res) => {
  if (!db) return res.status(503).json({ error: "БД недоступна" });
  const { telegramId, type, mediaUrl, linkUrl, title, paymentMethod, impressions } = req.body;

  if (!telegramId) return res.status(400).json({ error: "telegramId обязателен" });
  if (isRateLimited("http:" + telegramId, "submit-ad", 5, 60000)) {
    return res.status(429).json({ error: "Слишком много заявок подряд, подождите немного" });
  }
  if (!mediaUrl) return res.status(400).json({ error: "Укажите ссылку на картинку/видео" });
  if (!["image", "video"].includes(type)) return res.status(400).json({ error: "Неверный тип" });
  if (!["stars", "ton"].includes(paymentMethod)) return res.status(400).json({ error: "Выберите способ оплаты" });

  const impressionCount = parseInt(impressions, 10);
  if (!Number.isFinite(impressionCount) || impressionCount < AD_MIN_IMPRESSIONS || impressionCount % 1000 !== 0) {
    return res.status(400).json({ error: `Количество показов должно быть кратно 1000, минимум ${AD_MIN_IMPRESSIONS}` });
  }
  const packsOf1000 = impressionCount / 1000;

  try {
    if (paymentMethod === "stars") {
      const botToken = process.env.TELEGRAM_BOT_TOKEN;
      if (!botToken) return res.status(500).json({ error: "BOT_TOKEN не задан" });

      const priceStars = Math.round(packsOf1000 * AD_PRICE_PER_1000_STARS);
      const insert = await db.query(
        `INSERT INTO ads (type, media_url, link_url, title, active, status, advertiser_tg_id, payment_method, payment_amount, impression_limit)
         VALUES ($1,$2,$3,$4,FALSE,'pending_payment',$5,'stars',$6,$7) RETURNING id`,
        [type, mediaUrl, linkUrl || null, title || null, telegramId, priceStars, impressionCount]
      );
      const adId = insert.rows[0].id;

      const r = await fetch(`https://api.telegram.org/bot${botToken}/sendInvoice`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          chat_id: telegramId,
          title: "Размещение рекламы в Spinny",
          description: `${impressionCount.toLocaleString('ru')} показов после одобрения модератором`,
          payload: `ad_slot:${adId}`,
          provider_token: "",
          currency: "XTR",
          prices: [{ label: "Реклама в Spinny", amount: priceStars }],
        }),
      });
      const data = await r.json();
      if (!data.ok) throw new Error(data.description);

      console.log("[submit-ad] заявка #", adId, "|", impressionCount, "показов | Stars-инвойс отправлен", telegramId);
      res.json({ ok: true, method: "stars" });
    } else {
      // TON — своя оплата вне Bot API, генерируем уникальный комментарий для сверки
      if (!TON_WALLET_ADDRESS) return res.status(503).json({ error: "TON-оплата пока не настроена" });

      const priceTon = Math.round(packsOf1000 * AD_PRICE_PER_1000_TON * 100) / 100;
      const tonComment = `AD${Date.now().toString(36).toUpperCase()}`;
      const insert = await db.query(
        `INSERT INTO ads (type, media_url, link_url, title, active, status, advertiser_tg_id, payment_method, payment_amount, ton_comment, impression_limit)
         VALUES ($1,$2,$3,$4,FALSE,'pending_payment',$5,'ton',$6,$7,$8) RETURNING id`,
        [type, mediaUrl, linkUrl || null, title || null, telegramId, priceTon, tonComment, impressionCount]
      );
      const adId = insert.rows[0].id;

      console.log("[submit-ad] заявка #", adId, "|", impressionCount, "показов | ожидает TON-оплаты, комментарий:", tonComment);
      res.json({
        ok: true,
        method: "ton",
        adId,
        tonAddress: TON_WALLET_ADDRESS,
        tonAmount: priceTon,
        tonComment,
        tonDeepLink: `ton://transfer/${TON_WALLET_ADDRESS}?amount=${Math.round(priceTon * 1e9)}&text=${encodeURIComponent(tonComment)}`,
      });
    }
  } catch (e) {
    console.error("[submit-ad] ошибка:", e.message);
    res.status(500).json({ error: e.message });
  }
});

// Рекламодатель нажал «Я оплатил» после перевода TON — уходит на ручную сверку.
app.post("/confirm-ton-payment", async (req, res) => {
  if (!db) return res.status(503).json({ error: "БД недоступна" });
  const { adId } = req.body;
  if (!adId) return res.status(400).json({ error: "adId обязателен" });

  try {
    const r = await db.query(
      "UPDATE ads SET status = 'pending_review' WHERE id = $1 AND payment_method = 'ton' RETURNING *",
      [adId]
    );
    if (!r.rowCount) return res.status(404).json({ error: "Заявка не найдена" });

    const ad = r.rows[0];
    console.log("[confirm-ton-payment] заявка #", adId, "заявлена как оплаченная, комментарий:", ad.ton_comment, "— нужна ручная сверка в TON-эксплорере");

    await notifyAdminNewAdRequest(ad);
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.get("/ice-servers", async (req, res) => {
  const host = process.env.TURN_HOST;     // свой coturn на Aeza (когда будет готов)
  const secret = process.env.TURN_SECRET;
  const appName = process.env.METERED_APP_NAME; // временный fallback, пока свой TURN не готов
  const apiKey = process.env.METERED_API_KEY;
  const fallback = [{ urls: "stun:stun.l.google.com:19302" }];

  // 1) Приоритет — свой TURN-сервер, если настроен
  if (host && secret) {
    try {
      const { username, credential } = getTurnCredentials();
      const iceServers = [
        { urls: `stun:${host}:3478` },
        { urls: `turn:${host}:3478?transport=udp`, username, credential },
        { urls: `turn:${host}:3478?transport=tcp`, username, credential },
      ];
      console.log("[ice-servers] используем свой TURN:", host);
      return res.json(iceServers);
    } catch (e) {
      console.error("[ice-servers] свой TURN не удалось сконфигурировать:", e.message);
      // падаем ниже на Metered/STUN
    }
  } else {
    console.warn(
      "[ice-servers] свой TURN пропущен: TURN_HOST",
      host ? "задан" : "НЕ ЗАДАН",
      "| TURN_SECRET",
      secret ? "задан" : "НЕ ЗАДАН"
    );
  }

  // 2) Временный fallback — Metered (пока свой TURN не готов)
  if (appName && apiKey) {
    try {
      const r = await fetch(`https://${appName}.metered.live/api/v1/turn/credentials?apiKey=${apiKey}`);
      if (!r.ok) throw new Error(`Metered ответил ${r.status}`);
      const iceServers = await r.json();
      console.log("[ice-servers] используем Metered (временно)");
      return res.json(iceServers);
    } catch (e) {
      console.error("[ice-servers] Metered недоступен:", e.message);
    }
  }

  // 3) Совсем без TURN — только STUN (часть звонков за NAT не соединится)
  console.warn("[ice-servers] ни свой TURN, ни Metered не настроены, отдаю только STUN");
  res.json(fallback);
});

// ---------- Матчинг ----------
let matchLock = false; // ГЛОБАЛЬНЫЙ — защищает от параллельных tryMatch
const partners = new Map();
const roomOf = new Map();

// Трекер пиковой нагрузки — сколько одновременных звонков было максимум за сегодня.
// Сбрасывается сам при смене календарной даты. Хранится только в памяти,
// поэтому обнуляется при рестарте сервера — этого достаточно для мониторинга нагрузки.
let peakCallsToday = 0;
let peakCallsDate = new Date().toISOString().slice(0, 10);

function getActiveCallsCount() {
  return Math.floor(partners.size / 2);
}

function trackCallsPeak() {
  const today = new Date().toISOString().slice(0, 10);
  if (today !== peakCallsDate) {
    peakCallsDate = today;
    peakCallsToday = 0;
  }
  const current = getActiveCallsCount();
  if (current > peakCallsToday) {
    peakCallsToday = current;
    console.log("[load] новый пик одновременных звонков сегодня:", peakCallsToday);
  }
}
const bannedSockets = new Set(); // сокеты забаненных юзеров — оставляем на связи, чтобы могли оплатить разбан

// ---------- Rate limiting ----------
// Простой rate limiter с фиксированным окном, ключ — "socketId:событие".
// Не защищает от смены socket.id (переподключения), но закрывает основной
// случай — спам одним и тем же клиентом в рамках одной сессии.
const rateLimitState = new Map(); // "socketId:event" -> { count, windowStart }

function isRateLimited(socketId, eventName, maxCount, windowMs) {
  const key = `${socketId}:${eventName}`;
  const now = Date.now();
  const entry = rateLimitState.get(key);
  if (!entry || now - entry.windowStart > windowMs) {
    rateLimitState.set(key, { count: 1, windowStart: now });
    return false;
  }
  entry.count++;
  return entry.count > maxCount;
}

// Чистим все лимиты этого сокета при отключении, чтобы Map не росла бесконечно
function clearRateLimits(socketId) {
  for (const key of rateLimitState.keys()) {
    if (key.startsWith(socketId + ":")) rateLimitState.delete(key);
  }
}

const telegramUserOf = new Map();

// Очереди ожидания: язык -> socketId (один ожидающий на язык)
// Если не нашли своего языка за LANG_TIMEOUT мс — fallback на любой язык
const waitingByLang = new Map(); // language_code -> socketId
let waitingAny = null;           // fallback-очередь (любой язык)
const LANG_TIMEOUT = 2000;       // 2 сек ждём своего языка, потом расширяем
const langFallbackTimers = new Map(); // socketId -> timer

// Страна пользователя, определённая по IP через GeoIP (не зависит от языка
// Telegram — так можно отличить, например, США от Британии, чего язык не может).
const countryOf = new Map(); // socketId -> ISO-код страны ('US','GB','RU',...) | null

// Если пользователь явно выбрал фильтр «искать только эту страну» — храним тут.
// Если фильтр не выбран (null/весь мир) — матчинг идёт как раньше, по языку.
const preferredCountryOf = new Map(); // socketId -> ISO-код страны | null

// Очередь ожидания по фактической стране (не по предпочтению!) — сюда попадает
// КАЖДЫЙ ожидающий, у кого определилась страна, независимо от того, искал ли он
// сам по стране. Так его может найти кто-то другой, кто ищет именно его страну.
const waitingByCountry = new Map(); // ISO-код страны -> socketId
const COUNTRY_TIMEOUT = 2000;       // 2 сек ждём свою страну, потом — рандом
const countryFallbackTimers = new Map(); // socketId -> timer

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

  const country = countryOf.get(socketId);
  if (country && waitingByCountry.get(country) === socketId) waitingByCountry.delete(country);
  const ct = countryFallbackTimers.get(socketId);
  if (ct) { clearTimeout(ct); countryFallbackTimers.delete(socketId); }
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

  // Страну теперь определяет сам браузер клиента через публичный сервис
  // (ipapi.co) — серверный GeoIP по IP не сработал из-за внутреннего прокси
  // Amvera, который подменяет x-forwarded-for/x-real-ip на приватный адрес
  // (10.244.x.x) ещё до того, как запрос доходит до нашего процесса.
  countryOf.set(socket.id, null); // пока клиент не сообщит — считаем неизвестной

  socket.on("report-my-country", ({ country }) => {
    if (isRateLimited(socket.id, "report-my-country", 5, 10000)) return;
    const clean = typeof country === "string" && /^[A-Z]{2}$/.test(country) ? country : null;
    countryOf.set(socket.id, clean);
    console.log("[geoip]", socket.id, "| страна от клиента:", clean || "не определена");
  });

  socket.on("set-country-filter", ({ country: preferred }) => {
    if (isRateLimited(socket.id, "set-country-filter", 10, 10000)) return;
    preferredCountryOf.set(socket.id, preferred || null);
    console.log("[country-filter]", socket.id, "| фильтр:", preferred || "весь мир");
  });

  socket.on("auth", async (data) => {
    let user = verifyTelegramInitData(data?.initData, process.env.TELEGRAM_BOT_TOKEN);

    // Нет Mini App initData — вероятно, зашли напрямую по ссылке на домен.
    // Проверяем сессионную cookie, выданную после входа через Telegram Login Widget.
    if (!user) {
      const cookies = parseCookies(socket.handshake.headers.cookie);
      const session = verifySession(cookies.spinny_session);
      if (session) {
        user = { id: session.id, first_name: session.first_name, username: session.username };
      }
    }

    if (!user) {
      console.log("[auth] не идентифицирован (нет initData и нет сессии):", socket.id);
      socket.emit("auth-result", { authenticated: false });
      return;
    }

    telegramUserOf.set(socket.id, user);
    socket.emit("auth-result", { authenticated: true });
    console.log("[auth] верифицирован Telegram user", user.id, "для сокета", socket.id);

    try {
      await upsertUser(user);

      if (await isBanned(user.id)) {
        console.log("[auth] ЗАБАНЕН:", user.id);
        bannedSockets.add(socket.id);
        const banInfo = await getBanInfo(user.id);
        // reason в БД — техническая строка для админки (со score и т.п.), пользователю
        // показываем общую формулировку, но прикладываем реальный скриншот-доказательство
        socket.emit("banned", {
          reason: "Вы заблокированы за нарушение правил.",
          evidenceImage: banInfo?.evidence_image || null,
        });
        // НЕ отключаем сокет — иначе клиент уйдёт в цикл reconnect→auth→disconnect
        // и не успеет отправить "buy-unban" / получить "invoice-sent" в ответ.
        // Просто не даём забаненному искать собеседников (см. обработчик "find").
        return;
      }

      // Отправляем клиенту его статус (премиум, пол) чтобы показать правильный UI
      const userPremium = await isPremium(user.id);
      const userGender = await getGender(user.id);
      const refRes = await db.query(`SELECT referral_balance FROM users WHERE telegram_id = $1`, [user.id]);
      const referralBalance = refRes.rows[0]?.referral_balance || 0;
      socket.emit("user-status", { premium: userPremium, gender: userGender, referralBalance });
    } catch (e) {
      console.error("[auth] ошибка БД:", e.message);
    }
  });

  const REDEEM_COST_BONUSES = 50; // сколько бонусов стоит обмен
  const REDEEM_PREMIUM_DAYS = 7;  // сколько дней Premium дают за обмен

  socket.on("redeem-referral-balance", async () => {
    if (isRateLimited(socket.id, "redeem-referral-balance", 3, 10000)) return;
    const user = telegramUserOf.get(socket.id);
    if (!db || !user) return;
    try {
      const res = await db.query(
        `UPDATE users SET referral_balance = referral_balance - $1
         WHERE telegram_id = $2 AND referral_balance >= $1
         RETURNING referral_balance`,
        [REDEEM_COST_BONUSES, user.id]
      );
      if (res.rowCount === 0) {
        socket.emit("error-msg", `Недостаточно бонусов — нужно ${REDEEM_COST_BONUSES}, приведите ещё друзей`);
        return;
      }
      await grantPremium(user.id, REDEEM_PREMIUM_DAYS);
      const userPremium = await isPremium(user.id);
      socket.emit("user-status", { premium: userPremium, referralBalance: res.rows[0].referral_balance });
      socket.emit("error-msg", `🎉 Обменяли бонусы на ${REDEEM_PREMIUM_DAYS} дней Premium!`);
      console.log("[referral]", user.id, "обменял бонусы на Premium");
    } catch (e) {
      console.error("[redeem-referral-balance] ошибка:", e.message);
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
    trackCallsPeak();

    const langA = getLang(a) || "any";
    const langB = getLang(b) || "any";
    const countryA = countryOf.get(a) || null;
    const countryB = countryOf.get(b) || null;
    console.log("[match]", a, `(${langA}, ${countryA || "??"})`, "<->", b, `(${langB}, ${countryB || "??"})`);

    io.to(a).emit("matched", { room, initiator: true, partnerCountry: countryB });
    io.to(b).emit("matched", { room, initiator: false, partnerCountry: countryA });
    return true;
  }

  async function tryMatch() {
    if (matchLock) return;
    matchLock = true;

    try {
      const myId = socket.id;
      const myLang = getLang(myId);
      const myActualCountry = countryOf.get(myId) || null;
      const myPreferredCountry = preferredCountryOf.get(myId) || null;

      // 0) Явно выбран фильтр по стране — ищем СТРОГО среди тех, кто реально
      // из этой страны (не по их предпочтениям, по факту GeoIP). Если никого
      // нет — ждём ровно COUNTRY_TIMEOUT, потом падаем в общую очередь (как
      // договаривались), не проверяя waitingAny раньше времени.
      if (myPreferredCountry) {
        if (waitingByCountry.has(myPreferredCountry)) {
          const candidate = waitingByCountry.get(myPreferredCountry);
          if (candidate !== myId && io.sockets.sockets.has(candidate)) {
            waitingByCountry.delete(myPreferredCountry);
            removeFromQueues(candidate);
            const matched = await doMatch(candidate, myId);
            if (matched) return;
          } else {
            waitingByCountry.delete(myPreferredCountry);
          }
        }

        // Не нашли — встаём в очередь СВОЕЙ фактической страны (чтобы нас
        // нашли те, кто ищет именно её), и запускаем таймер фоллбэка.
        if (myActualCountry && !waitingByCountry.has(myActualCountry)) {
          waitingByCountry.set(myActualCountry, myId);
        }
        console.log("[waiting]", myId, "| country filter:", myPreferredCountry, "| моя страна:", myActualCountry || "??");

        const ct = setTimeout(async () => {
          if (myActualCountry && waitingByCountry.get(myActualCountry) === myId) {
            waitingByCountry.delete(myActualCountry);
          }
          countryFallbackTimers.delete(myId);
          if (!io.sockets.sockets.has(myId)) return; // уже отключился

          // Если в общей очереди уже кто-то ждёт — матчимся с ним сразу,
          // а не просто занимаем очередь молча (иначе оба зависают навсегда)
          if (waitingAny && waitingAny !== myId && io.sockets.sockets.has(waitingAny)) {
            const candidate = waitingAny;
            waitingAny = null;
            removeFromQueues(candidate);
            await doMatch(candidate, myId);
          } else {
            waitingAny = myId;
            console.log("[waiting→any]", myId, "| country timeout, переходим в общую очередь");
          }
        }, COUNTRY_TIMEOUT);
        countryFallbackTimers.set(myId, ct);
        return;
      }

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

      // 3) Никого не нашли — встаём в очередь своего языка (и заодно своей
      // фактической страны — чтобы нас мог найти кто-то, кто выбрал фильтр
      // именно по нашей стране, даже если сами мы фильтр не выбирали)
      if (myLang) {
        waitingByLang.set(myLang, myId);
        console.log("[waiting]", myId, "| lang:", myLang);

        // Через LANG_TIMEOUT переходим в общую очередь (fallback)
        const t = setTimeout(async () => {
          if (waitingByLang.get(myLang) === myId) {
            waitingByLang.delete(myLang);
          }
          langFallbackTimers.delete(myId);
          if (!io.sockets.sockets.has(myId)) return; // уже отключился

          // Если в общей очереди уже кто-то ждёт — матчимся с ним сразу,
          // а не просто занимаем очередь молча (иначе оба зависают навсегда)
          if (waitingAny && waitingAny !== myId && io.sockets.sockets.has(waitingAny)) {
            const candidate = waitingAny;
            waitingAny = null;
            removeFromQueues(candidate);
            await doMatch(candidate, myId);
          } else {
            waitingAny = myId;
            console.log("[waiting→any]", myId, "| lang timeout, переходим в общую очередь");
          }
        }, LANG_TIMEOUT);
        langFallbackTimers.set(myId, t);
      } else {
        // Язык неизвестен — сразу в общую очередь
        waitingAny = myId;
        console.log("[waiting]", myId, "| lang: unknown → общая очередь");
      }

      if (myActualCountry && !waitingByCountry.has(myActualCountry)) {
        waitingByCountry.set(myActualCountry, myId);
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
    if (isRateLimited(socket.id, "buy-premium", 5, 60000)) {
      socket.emit("error-msg", "Слишком много попыток, подождите немного");
      return;
    }
    const tgId = telegramUserOf.get(socket.id)?.id;
    if (!tgId) {
      socket.emit("error-msg", "Откройте приложение через Telegram для покупки премиума");
      return;
    }

    const botToken = process.env.TELEGRAM_BOT_TOKEN;
    const PRODUCTS = {
      premium_30: { title: "Spinny Premium — 30 дней", amount: 75, days: 30 },
      premium_90: { title: "Spinny Premium — 3 месяца", amount: 200, days: 90 },
      unban:      { title: "Разблокировка аккаунта", amount: 100, days: 0 },
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

  // Разблокировка за Stars
  socket.on("buy-unban", async () => {
    if (isRateLimited(socket.id, "buy-unban", 5, 60000)) {
      socket.emit("error-msg", "Слишком много попыток, подождите немного");
      return;
    }
    const tgId = telegramUserOf.get(socket.id)?.id;
    const botToken = process.env.TELEGRAM_BOT_TOKEN;

    if (!tgId || !botToken) {
      socket.emit("error-msg", "Откройте приложение через Telegram");
      return;
    }

    try {
      const r = await fetch(`https://api.telegram.org/bot${botToken}/sendInvoice`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          chat_id: tgId,
          title: "Разблокировка аккаунта Spinny",
          description: "Однократная разблокировка. Пожалуйста, соблюдайте правила после разблокировки.",
          payload: `unban:${tgId}`,
          provider_token: "",
          currency: "XTR",
          prices: [{ label: "Разблокировка", amount: 100 }],
        }),
      });
      const data = await r.json();
      if (!data.ok) throw new Error(data.description);
      socket.emit("invoice-sent", { message: "Инвойс на разблокировку отправлен в чат с ботом" });
    } catch (e) {
      socket.emit("error-msg", "Ошибка: " + e.message);
    }
  });

  socket.on("find", async () => {    console.log("[find]", socket.id);
    if (isRateLimited(socket.id, "find", 8, 3000)) return; // не больше 8 раз за 3с
    if (!telegramUserOf.has(socket.id)) {
      socket.emit("need-telegram-auth");
      return;
    }
    if (bannedSockets.has(socket.id)) {
      socket.emit("banned", { reason: "Вы заблокированы за нарушение правил." });
      return;
    }
    await tryMatch();
  });

  socket.on("skip", async () => {
    if (isRateLimited(socket.id, "skip", 8, 3000)) return;
    console.log("[skip]", socket.id);
    clearMatch(socket.id, "skip");
    removeFromQueues(socket.id);
    await tryMatch();
  });

  // Пользователь нажал "Стоп" — завершает звонок насовсем, БЕЗ повторной постановки в очередь
  socket.on("leave", () => {
    console.log("[leave]", socket.id);
    clearMatch(socket.id, "leave");
    removeFromQueues(socket.id);
  });

  // Пользователь блокирует текущего собеседника — они больше не будут мэтчиться
  socket.on("block", async () => {
    if (isRateLimited(socket.id, "block", 5, 60000)) return; // не больше 5 в минуту
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
    if (isRateLimited(socket.id, "report", 4, 60000)) { // не больше 4 жалоб в минуту — Sightengine платный
      socket.emit("error-msg", "Слишком много жалоб подряд, подождите немного");
      return;
    }
    const partnerId = partners.get(socket.id);
    console.log("[report] от", socket.id, "| партнёр:", partnerId);

    if (!partnerId) return;

    const apiUser = process.env.SIGHTENGINE_API_USER;
    const apiSecret = process.env.SIGHTENGINE_API_SECRET;

    const reporterTgId = telegramUserOf.get(socket.id)?.id ?? null;
    const offenderTgId = telegramUserOf.get(partnerId)?.id ?? null;

    // Скриншот сохраняем в БД в любом случае — для ручной проверки в админке,
    // даже если автоматический анализ (Sightengine) недоступен или не настроен.
    const imageForStorage = imageBase64 || null;

    if (!apiUser || !apiSecret) {
      console.warn("[report] Sightengine не настроен — сохраняем без автоанализа, нужна ручная проверка");
      await saveReport({ reporterTgId, offenderTgId, verdict: "unreviewed", nudityScore: null, imageBase64: imageForStorage });
      return;
    }

    try {
      const base64Data = (imageBase64 || "").split(",")[1] || imageBase64;
      const buffer = Buffer.from(base64Data, "base64");

      const formData = new FormData();
      formData.append("media", new Blob([buffer], { type: "image/jpeg" }), "report.jpg");
      // nudity-2.1 — обнажёнка/сексуальный контент
      // face-age — определение несовершеннолетних по лицу (faces[].attributes.age.minor, 0-1).
      // ВАЖНО: модели "faces" и "minors" не существуют в API Sightengine — из-за них раньше
      // весь запрос падал целиком (status: failure), и любая жалоба тихо считалась "clean".
      formData.append("models", "nudity-2.1,face-age");
      formData.append("api_user", apiUser);
      formData.append("api_secret", apiSecret);

      const r = await fetch("https://api.sightengine.com/1.0/check.json", { method: "POST", body: formData });
      const result = await r.json();
      console.log("[report] Sightengine полный ответ:", JSON.stringify(result));

      if (result?.status !== "success") {
        // Запрос к Sightengine упал (неверные модели, квота, сбой API и т.п.) —
        // не считаем это "чисто", а явно помечаем как требующее ручной проверки.
        console.error("[report] Sightengine вернул ошибку:", result?.error?.message || "неизвестная ошибка");
        await saveReport({ reporterTgId, offenderTgId, verdict: "unreviewed", nudityScore: null, imageBase64: imageForStorage });
        return;
      }

      // --- Nudity check ---
      const raw = result?.nudity?.sexual_activity ?? 0;
      const partial = result?.nudity?.suggestive ?? 0;
      const nudityScore = Math.max(raw, partial);
      const isNudity = nudityScore > 0.6;

      // --- Minors check (face-age: faces[].attributes.age.minor, 0-1) ---
      const faces = result?.faces ?? [];
      const maxMinorScore = faces.reduce((max, f) => Math.max(max, f?.attributes?.age?.minor ?? 0), 0);
      const isMinorViolation = maxMinorScore > 0.5;

      const isViolation = isNudity || isMinorViolation;

      // Определяем причину бана для логов
      let banReason = null;
      if (isMinorViolation) banReason = `CSAM/minors (score: ${maxMinorScore.toFixed(2)}) — auto ban after report`;
      else if (isNudity) banReason = `nudity (score: ${nudityScore.toFixed(2)}) — auto ban after report`;

      const verdict = isViolation
        ? (isMinorViolation ? "csam" : "violation")
        : "clean";

      console.log("[report] вердикт:", verdict,
        "| nudity:", nudityScore.toFixed(2),
        "| minorScore:", maxMinorScore.toFixed(2),
        "| ban:", banReason);

      await saveReport({ reporterTgId, offenderTgId, verdict, nudityScore, imageBase64: imageForStorage });

      if (isViolation) {
        const banMsg = isMinorViolation
          ? "Обнаружен контент с участием несовершеннолетних. Аккаунт заблокирован."
          : "Жалоба подтверждена: обнаружен недопустимый контент.";

        await banUser(offenderTgId, banReason, imageForStorage);
        io.to(partnerId).emit("banned", { reason: banMsg, evidenceImage: imageForStorage });
        setTimeout(() => io.sockets.sockets.get(partnerId)?.disconnect(true), 500);
        clearMatch(socket.id, "report");
      }
    } catch (e) {
      console.error("[report] ошибка:", e.message);
      await saveReport({ reporterTgId, offenderTgId, verdict: "error", nudityScore: null, imageBase64: imageForStorage });
    }
  });

  socket.on("signal", ({ room, data }) => {
    // Кандидатов на звонок штук 8-10 — логируем только offer/answer (значимые вехи),
    // иначе лог захламляется десятками строк на каждый звонок.
    if (data?.offer) console.log("[signal]", socket.id, "->", room, "| offer");
    else if (data?.answer) console.log("[signal]", socket.id, "->", room, "| answer");
    socket.to(room).emit("signal", data);
  });

  // Диагностика WebRTC с клиента — чтобы видеть состояние соединения (ICE/connection state,
  // получен ли трек, есть ли ошибки) прямо в серверных логах, без доступа к консоли устройства.
  socket.on("webrtc-debug", ({ event, detail }) => {
    console.log("[webrtc]", socket.id, "|", event, "|", detail);
  });

  socket.on("reaction", ({ emoji }) => {
    if (isRateLimited(socket.id, "reaction", 20, 10000)) return; // не больше 20 за 10с
    const room = roomOf.get(socket.id);
    console.log("[reaction]", socket.id, "->", emoji, "| roomOf:", room, "| partners:", partners.get(socket.id));
    if (!room) {
      console.log("[reaction] IGNORED — нет активной комнаты для", socket.id);
      return;
    }
    socket.to(room).emit("reaction", { emoji });
  });

  socket.on("chat-message", ({ text }) => {
    if (isRateLimited(socket.id, "chat-message", 15, 10000)) return; // не больше 15 сообщений за 10с
    if (bannedSockets.has(socket.id)) return;

    const room = roomOf.get(socket.id);
    if (!room) return;

    const clean = String(text ?? "").trim().slice(0, 500);
    if (!clean) return;

    socket.to(room).emit("chat-message", { text: clean });
  });

  socket.on("disconnect", (reason) => {
    console.log("[disconnect]", socket.id, reason);
    removeFromQueues(socket.id);
    telegramUserOf.delete(socket.id);
    userFilters.delete(socket.id);
    bannedSockets.delete(socket.id);
    clearRateLimits(socket.id);
    countryOf.delete(socket.id);
    preferredCountryOf.delete(socket.id);
    clearMatch(socket.id, "disconnect:" + reason);
  });
});

// ---------- Напоминания неактивным пользователям ----------
// Трёхступенчатая схема: 48ч → 7 дней → 30 дней, разные тексты на каждом этапе,
// дальше не напоминаем вообще (чтобы не превратиться в спам и не словить блок бота).
// Цикл сбрасывается сам, как только пользователь снова заходит (см. upsertUser).
const REMINDER_STAGES = [
  { stage: 1, afterHours: 48 },
  { stage: 2, afterHours: 24 * 7 },
  { stage: 3, afterHours: 24 * 30 },
];

const REMINDER_MESSAGES = {
  1: {
    ru: { text: (name) => `👋 ${name}, привет! Соскучились — загляни в Spinny, там уже ждут новые собеседники со всего мира 🎲`, button: "🎲 Открыть Spinny" },
    en: { text: (name) => `👋 Hey ${name}! We miss you — come back to Spinny, new people from all over the world are waiting 🎲`, button: "🎲 Open Spinny" },
  },
  2: {
    ru: { text: (name) => `🎲 ${name}, пока тебя не было, в Spinny прошло много новых знакомств! Самое время вернуться и пообщаться 👋`, button: "🎲 Вернуться в Spinny" },
    en: { text: (name) => `🎲 ${name}, lots of new people have joined Spinny since you left! Perfect time to come back and chat 👋`, button: "🎲 Back to Spinny" },
  },
  3: {
    ru: { text: (name) => `👋 ${name}, давно не виделись. Если Spinny всё ещё интересен — будем рады видеть тебя снова. Больше напоминать не будем 🙂`, button: "🎲 Открыть Spinny" },
    en: { text: (name) => `👋 ${name}, it's been a while. If you're still interested in Spinny, we'd love to see you back. This is the last reminder 🙂`, button: "🎲 Open Spinny" },
  },
};

async function sendReminders() {
  if (!db) return;
  const botToken = process.env.TELEGRAM_BOT_TOKEN;
  if (!botToken) return;

  for (const { stage, afterHours } of REMINDER_STAGES) {
    let candidates;
    try {
      candidates = await db.query(
        `SELECT u.telegram_id, u.first_name, u.language_code
         FROM users u
         WHERE u.reminder_stage = $1
           AND u.reminders_blocked = FALSE
           AND u.last_seen_at < NOW() - INTERVAL '1 hour' * $2
           AND NOT EXISTS (SELECT 1 FROM bans b WHERE b.telegram_id = u.telegram_id)
         LIMIT 200`,
        [stage - 1, afterHours]
      );
    } catch (e) {
      console.error("[reminders] ошибка запроса к БД:", e.message);
      continue;
    }

    if (!candidates.rowCount) continue;
    console.log(`[reminders] этап ${stage}: кандидатов —`, candidates.rowCount);

    for (const u of candidates.rows) {
      const lang = (u.language_code || "ru").split("-")[0];
      const msg = REMINDER_MESSAGES[stage][lang] || REMINDER_MESSAGES[stage].ru;
      const name = u.first_name || "друг";

      try {
        const r = await fetch(`https://api.telegram.org/bot${botToken}/sendMessage`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            chat_id: u.telegram_id,
            text: msg.text(name),
            reply_markup: {
              inline_keyboard: [[{ text: msg.button, web_app: { url: "https://spinnyapp.ru" } }]]
            }
          }),
        });
        const data = await r.json();

        if (!data.ok && (data.error_code === 403 || /blocked/i.test(data.description || ""))) {
          // Пользователь заблокировал бота — больше не пытаемся, чтобы не тратить лимиты API впустую
          await db.query("UPDATE users SET reminders_blocked = TRUE WHERE telegram_id = $1", [u.telegram_id]);
        } else if (data.ok) {
          await db.query(
            "UPDATE users SET reminder_stage = $1, last_reminder_at = NOW() WHERE telegram_id = $2",
            [stage, u.telegram_id]
          );
        }
      } catch (e) {
        console.error("[reminders] ошибка отправки", u.telegram_id, ":", e.message);
      }

      // Небольшая пауза между сообщениями, чтобы не упереться в rate limit Telegram
      await new Promise((resolve) => setTimeout(resolve, 50));
    }
  }
}

// Проверяем раз в час — этого достаточно с учётом того, что пороги измеряются днями
const REMINDER_INTERVAL_MS = 60 * 60 * 1000;
setInterval(() => {
  sendReminders().catch((e) => console.error("[reminders] необработанная ошибка:", e.message));
}, REMINDER_INTERVAL_MS);

// Автоматически гасим рекламу, у которой истёк оплаченный срок размещения (AD_SLOT_DAYS)
async function expireOldAds() {
  if (!db) return;
  try {
    const r = await db.query(
      "UPDATE ads SET active = FALSE WHERE active = TRUE AND expires_at IS NOT NULL AND expires_at < NOW() RETURNING id"
    );
    if (r.rowCount) console.log("[ads] истёк срок размещения, отключено:", r.rows.map(x => x.id).join(", "));
  } catch (e) {
    console.error("[ads] ошибка проверки истёкшей рекламы:", e.message);
  }
}
const AD_EXPIRY_CHECK_MS = 60 * 60 * 1000; // раз в час
setInterval(() => {
  expireOldAds().catch((e) => console.error("[ads] необработанная ошибка:", e.message));
}, AD_EXPIRY_CHECK_MS);

// ---------- Старт ----------
// Сервер стартует НЕМЕДЛЕННО — Amvera/Render требуют чтобы порт слушался быстро.
// БД инициализируется в фоне параллельно.
const PORT = process.env.PORT || 8080;

server.listen(PORT, () => {
  console.log("SERVER RUNNING on port", PORT);
});

// Инициализируем БД в фоне — не блокируем старт сервера
initDb().catch(e => {
  console.error("[db] ошибка инициализации:", e.message);
});

// Держим кнопку меню бота актуальной автоматически при каждом старте —
// чтобы при смене домена не приходилось руками лезть в BotFather.
(async () => {
  const botToken = process.env.TELEGRAM_BOT_TOKEN;
  if (!botToken) return;
  try {
    const r = await fetch(`https://api.telegram.org/bot${botToken}/getMe`);
    const data = await r.json();
    if (data.ok) {
      BOT_USERNAME = data.result.username;
      console.log("[bot] username:", BOT_USERNAME);
    }
  } catch (e) {
    console.warn("[bot] не удалось получить username:", e.message);
  }

  try {
    await fetch(`https://api.telegram.org/bot${botToken}/setChatMenuButton`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        menu_button: {
          type: "web_app",
          text: "Открыть Spinny",
          web_app: { url: "https://spinnyapp.ru" }
        }
      })
    });
    console.log("[menu_button] обновлена: https://spinnyapp.ru");
  } catch (e) {
    console.warn("[menu_button] не удалось обновить:", e.message);
  }
})();Mon Jul 13 11:18:19 MSK 2026
