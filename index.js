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
  `);

  // Миграции — добавляем колонки если их нет (безопасно для существующих таблиц)
  await db.query(`
    ALTER TABLE users ADD COLUMN IF NOT EXISTS premium_until TIMESTAMPTZ;
    ALTER TABLE users ADD COLUMN IF NOT EXISTS gender TEXT;
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
    unban: {
      title: "Разблокировка аккаунта Spinny",
      description: "Однократная разблокировка. Пожалуйста, соблюдайте правила после разблокировки.",
      payload: `unban:${telegramId}`,
      amount: 100,
      days: 0,
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

  // Команда /start — приветственное сообщение с кнопкой открытия Mini App
  const message = update.message;
  if (message?.text === "/start" || message?.text?.startsWith("/start ")) {
    const userId = message.from.id;
    const firstName = message.from.first_name || "друг";

    await fetch(`https://api.telegram.org/bot${botToken}/sendPhoto`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        chat_id: userId,
        photo: "https://spinnyapp.ru/spinny_logo.png",
        caption: `👋 Привет, ${firstName}!\n\n🎲 *Spinny* — случайный видеочат прямо в Telegram.\n\nНажми кнопку ниже, разреши доступ к камере — и через секунды окажешься на связи с новым собеседником из любой точки мира.\n\n• 🌍 Мэтчинг по языку\n• ❤️ Реакции во время звонка\n• ⭐ Premium с фильтром по полу\n• 🚩 Система модерации`,
        parse_mode: "Markdown",
        reply_markup: {
          inline_keyboard: [[
            {
              text: "🎲 Открыть Spinny",
              web_app: { url: "https://spinnyapp.ru" }
            }
          ]]
        }
      }),
    });
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
    const [bans, reports, users, unban, blocks] = await Promise.all([
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
    db.query(`SELECT telegram_id, username, first_name, is_premium, premium_until, gender, last_seen_at
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
  ]);

  // Считаем онлайн прямо из памяти сервера
  const onlineCount = telegramUserOf.size;

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
</div>

<div class="tabs">
  <div class="tab active" onclick="showTab('overview')">Обзор</div>
  <div class="tab" onclick="showTab('bans')">Баны (${bans.rowCount})</div>
  <div class="tab" onclick="showTab('reports')">Репорты (${reports.rowCount})</div>
  <div class="tab" onclick="showTab('users')">Пользователи (${users.rowCount})</div>
  <div class="tab" onclick="showTab('unbans')">Разблокировки (${unban.rowCount})</div>
  <div class="tab" onclick="showTab('blocks')">Блоки (${blocks.rowCount})</div>
</div>

<div class="content">

  <!-- Обзор -->
  <div class="section active" id="tab-overview">
    <div class="stats">
      <div class="stat"><div class="stat-num">${onlineCount}</div><div class="stat-label">Онлайн сейчас</div></div>
      <div class="stat"><div class="stat-num">${users.rowCount}</div><div class="stat-label">Юзеров (100)</div></div>
      <div class="stat"><div class="stat-num">${bans.rowCount}</div><div class="stat-label">Банов</div></div>
      <div class="stat"><div class="stat-num">${reports.rows.filter(r=>r.verdict==='violation'||r.verdict==='csam').length}</div><div class="stat-label">Нарушений</div></div>
      <div class="stat"><div class="stat-num">${reports.rows.filter(r=>r.verdict==='csam').length}</div><div class="stat-label">CSAM</div></div>
      <div class="stat"><div class="stat-num">${users.rows.filter(u=>u.is_premium).length}</div><div class="stat-label">Премиум</div></div>
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
      <tr><th>Жалобщик</th><th>Нарушитель</th><th>Вердикт</th><th>Score</th><th>Дата</th></tr>
      ${reports.rows.map(r => `
      <tr>
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
    <table>
      <tr><th>TG ID</th><th>Имя</th><th>Статус</th><th>Пол</th><th>Последний вход</th></tr>
      ${users.rows.map(u => `
      <tr>
        <td>${u.telegram_id}</td>
        <td>${u.first_name || ''} @${u.username || '—'}</td>
        <td>${u.is_premium ? '<span class="premium-badge">⭐ Premium</span>' : 'Free'}</td>
        <td>${u.gender || '—'}</td>
        <td>${new Date(u.last_seen_at).toLocaleString('ru')}</td>
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

</div>

<script>
const SECRET = new URLSearchParams(location.search).get('secret') || '';

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
const bannedSockets = new Set(); // сокеты забаненных юзеров — оставляем на связи, чтобы могли оплатить разбан
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
        bannedSockets.add(socket.id);
        socket.emit("banned", { reason: "Вы заблокированы за нарушение правил." });
        // НЕ отключаем сокет — иначе клиент уйдёт в цикл reconnect→auth→disconnect
        // и не успеет отправить "buy-unban" / получить "invoice-sent" в ответ.
        // Просто не даём забаненному искать собеседников (см. обработчик "find").
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
    if (bannedSockets.has(socket.id)) {
      socket.emit("banned", { reason: "Вы заблокированы за нарушение правил." });
      return;
    }
    await tryMatch();
  });

  socket.on("skip", async () => {
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
      // nudity-2.1 — обнаженка/сексуальный контент
      // faces — определяет возраст лиц на кадре
      // minors — специализированная модель защиты несовершеннолетних
      formData.append("models", "nudity-2.1,faces,minors");
      formData.append("api_user", apiUser);
      formData.append("api_secret", apiSecret);

      const r = await fetch("https://api.sightengine.com/1.0/check.json", { method: "POST", body: formData });
      const result = await r.json();
      console.log("[report] Sightengine полный ответ:", JSON.stringify(result));

      // --- Nudity check ---
      const raw = result?.nudity?.sexual_activity ?? result?.nudity?.raw ?? 0;
      const partial = result?.nudity?.suggestive ?? result?.nudity?.partial ?? 0;
      const nudityScore = Math.max(raw, partial);
      const isNudity = nudityScore > 0.6;

      // --- Minors check ---
      // Sightengine возвращает minor: { found: true/false, score: 0-1 }
      const minorFound = result?.minor?.found === true || (result?.minor?.score ?? 0) > 0.5;

      // --- Faces age check (дополнительная защита) ---
      // Если среди определённых лиц есть кто-то моложе 18 — флаг
      const faces = result?.faces ?? [];
      const hasMinorFace = faces.some(f => f.age?.min < 18);

      const isMinorViolation = minorFound || hasMinorFace;
      const isViolation = isNudity || isMinorViolation;

      // Определяем причину бана для логов
      let banReason = null;
      if (isMinorViolation) banReason = "CSAM/minors — auto ban after report";
      else if (isNudity) banReason = `nudity (score: ${nudityScore.toFixed(2)}) — auto ban after report`;

      const verdict = isViolation
        ? (isMinorViolation ? "csam" : "violation")
        : "clean";

      console.log("[report] вердикт:", verdict,
        "| nudity:", nudityScore.toFixed(2),
        "| minor:", isMinorViolation,
        "| ban:", banReason);

      await saveReport({ reporterTgId, offenderTgId, verdict, nudityScore });

      if (isViolation) {
        const banMsg = isMinorViolation
          ? "Обнаружен контент с участием несовершеннолетних. Аккаунт заблокирован."
          : "Жалоба подтверждена: обнаружен недопустимый контент.";

        await banUser(offenderTgId, banReason);
        io.to(partnerId).emit("banned", { reason: banMsg });
        setTimeout(() => io.sockets.sockets.get(partnerId)?.disconnect(true), 500);
        clearMatch(socket.id, "report");
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
    bannedSockets.delete(socket.id);
    clearMatch(socket.id, "disconnect:" + reason);
  });
});

// ---------- Старт ----------
// Гонка initDb() с таймаутом: даже если БД зависла (не отвечает и не падает с ошибкой),
// сервер всё равно поднимется и не будет вечно возвращать 503.
function withTimeout(promise, ms) {
  return Promise.race([
    promise,
    new Promise((_, reject) =>
      setTimeout(() => reject(new Error(`initDb timeout после ${ms}мс`)), ms)
    ),
  ]);
}

withTimeout(initDb(), 8000)
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

// Держим кнопку меню бота актуальной автоматически при каждом старте —
// чтобы при смене домена не приходилось руками лезть в BotFather.
(async () => {
  const botToken = process.env.TELEGRAM_BOT_TOKEN;
  if (!botToken) return;
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
})();