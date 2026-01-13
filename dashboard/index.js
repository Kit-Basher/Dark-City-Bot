const crypto = require('crypto');
const express = require('express');
const session = require('express-session');
const { MongoClient } = require('mongodb');
const MongoStore = require('connect-mongo');

function requireEnv(name) {
  const value = process.env[name];
  if (!value) throw new Error(`Missing required environment variable: ${name}`);
  return value;
}

function nav(req) {
  if (!req.session?.user?.allowed) return '';
  return `<div style="display:flex; gap:12px; margin-bottom:14px; align-items:center;">
    <a href="/dashboard">Dashboard</a>
    <a href="/quiz">Quiz</a>
    <a href="/settings">Settings</a>
    <a href="/xp">XP</a>
    <a href="/logs">Logs</a>
    <span style="flex:1"></span>
    <a href="/logout">Log out</a>
  </div>`;
}

const PORT = parseInt(process.env.PORT || '3002', 10);

const DISCORD_CLIENT_ID = requireEnv('DISCORD_CLIENT_ID');
const DISCORD_CLIENT_SECRET = requireEnv('DISCORD_CLIENT_SECRET');
const DISCORD_REDIRECT_URI = requireEnv('DISCORD_REDIRECT_URI');
const DISCORD_GUILD_ID = requireEnv('DISCORD_GUILD_ID');
const DASHBOARD_ALLOWED_ROLE_ID = process.env.DASHBOARD_ALLOWED_ROLE_ID;
const ADMIN_ROLE_ID = String(process.env.ADMIN_ROLE_ID || DASHBOARD_ALLOWED_ROLE_ID || '').trim();
const DISCORD_CALLBACK_URL = String(process.env.DISCORD_CALLBACK_URL || DISCORD_REDIRECT_URI || '').trim();
const SESSION_SECRET = requireEnv('SESSION_SECRET');

const DARK_CITY_API_BASE_URL = String(process.env.DARK_CITY_API_BASE_URL || '').trim().replace(/\/$/, '');
let DARK_CITY_MODERATOR_PASSWORD = String(process.env.DARK_CITY_MODERATOR_PASSWORD || '').trim();

const TELEMETRY_INGEST_TOKEN = String(process.env.TELEMETRY_INGEST_TOKEN || '').trim();

const SERVICE_HEALTHCHECKS_ENABLED = String(process.env.SERVICE_HEALTHCHECKS_ENABLED || 'true').trim().toLowerCase() !== 'false';
const SERVICE_HEALTHCHECK_INTERVAL_SECONDS = parseInt(process.env.SERVICE_HEALTHCHECK_INTERVAL_SECONDS || '60', 10);
const SERVICE_HEALTHCHECK_TIMEOUT_MS = parseInt(process.env.SERVICE_HEALTHCHECK_TIMEOUT_MS || '6000', 10);
const SERVICE_HEALTHCHECK_DISCORD_WEBHOOK_URL = String(process.env.SERVICE_HEALTHCHECK_DISCORD_WEBHOOK_URL || '').trim();
const SERVICE_HEALTHCHECK_ERROR_WINDOW = parseInt(process.env.SERVICE_HEALTHCHECK_ERROR_WINDOW || '20', 10);
const SERVICE_HEALTHCHECK_ERROR_RATE_THRESHOLD = parseFloat(process.env.SERVICE_HEALTHCHECK_ERROR_RATE_THRESHOLD || '0.5');
const SERVICE_HEALTHCHECK_ERROR_ALERT_COOLDOWN_MINUTES = parseInt(process.env.SERVICE_HEALTHCHECK_ERROR_ALERT_COOLDOWN_MINUTES || '30', 10);

const BOT_HEARTBEAT_ENABLED = String(process.env.BOT_HEARTBEAT_ENABLED || 'true').trim().toLowerCase() !== 'false';
const BOT_HEARTBEAT_STALE_SECONDS = parseInt(process.env.BOT_HEARTBEAT_STALE_SECONDS || '120', 10);

const MONGODB_URI = process.env.MONGODB_URI;
const BOT_DB_NAME = process.env.BOT_DB_NAME || 'dark_city_bot';

const app = express();

app.set('trust proxy', 1);

app.use(express.json({ limit: '64kb' }));
app.use(express.urlencoded({ extended: true }));
app.use(
  session({
    secret: SESSION_SECRET,
    resave: false,
    saveUninitialized: false,
    store: MONGODB_URI
      ? MongoStore.create({
          mongoUrl: MONGODB_URI,
          collectionName: 'dashboard_sessions',
          ttl: 60 * 60 * 24 * 30,
        })
      : undefined,
    cookie: {
      httpOnly: true,
      sameSite: 'lax',
      secure: process.env.NODE_ENV === 'production',
    },
  })
);

app.get('/favicon.ico', (req, res) => {
  res.status(204).end();
});

app.post('/api/telemetry/event', async (req, res) => {
  try {
    if (!requireTelemetryToken(req, res)) return;
    if (!botDb) {
      res.status(503).json({ error: 'Mongo not configured' });
      return;
    }

    const body = req.body && typeof req.body === 'object' ? req.body : {};
    const service = sanitizeTelemetryString(body.service, 32);
    const level = sanitizeTelemetryString(body.level, 12);
    const category = sanitizeTelemetryString(body.category, 32);
    const event = sanitizeTelemetryString(body.event, 64);
    const message = sanitizeTelemetryString(body.message, 500);
    const actorUserId = sanitizeTelemetryString(body.actorUserId || body.userId, 64);

    if (!service || !level || !event) {
      res.status(400).json({ error: 'service, level, and event are required' });
      return;
    }

    const allowedLevels = new Set(['info', 'warn', 'error', 'security']);
    if (!allowedLevels.has(level)) {
      res.status(400).json({ error: 'Invalid level' });
      return;
    }

    const meta = body.meta && typeof body.meta === 'object' ? body.meta : null;
    const safeMeta = meta
      ? {
          requestId: sanitizeTelemetryString(meta.requestId, 64),
          targetId: sanitizeTelemetryString(meta.targetId, 64),
          resourceId: sanitizeTelemetryString(meta.resourceId, 64),
        }
      : null;

    await insertTelemetryEvent({
      guildId: DISCORD_GUILD_ID,
      service,
      level,
      category: category || null,
      event,
      message: message || null,
      actorUserId: actorUserId || null,
      meta: safeMeta,
      createdAt: new Date(),
    });

    res.json({ ok: true });
  } catch (e) {
    console.error('Telemetry ingest error:', e);
    res.status(500).json({ error: e?.message || 'Server error' });
  }
});

let mongoClient;
let botDb;

async function initMongo() {
  if (!MONGODB_URI) {
    console.log('ℹ️ Mongo: MONGODB_URI not set; settings/logs pages will be unavailable');
    return;
  }

  mongoClient = new MongoClient(MONGODB_URI);
  await mongoClient.connect();
  botDb = mongoClient.db(BOT_DB_NAME);
  console.log('✅ Mongo: Connected');

  try {
    // Auto-expire telemetry after 7 days
    await botDb.collection('telemetry_events').createIndex({ createdAt: 1 }, { expireAfterSeconds: 60 * 60 * 24 * 7 });
  } catch (e) {
    console.error('Mongo telemetry TTL index failed:', e);
  }

  try {
    await botDb.collection('service_health_checks').createIndex({ checkedAt: 1 }, { expireAfterSeconds: 60 * 60 * 24 * 7 });
    await botDb.collection('service_health_checks').createIndex({ guildId: 1, service: 1, checkedAt: -1 });
  } catch (e) {
    console.error('Mongo service health TTL/index failed:', e);
  }

  try {
    await botDb.collection('bot_heartbeats').createIndex({ guildId: 1, service: 1 }, { unique: true });
  } catch (e) {
    console.error('Mongo bot_heartbeats index failed:', e);
  }
}

async function getBotHeartbeat() {
  if (!botDb) return null;
  return botDb.collection('bot_heartbeats').findOne({ guildId: DISCORD_GUILD_ID, service: 'bot' });
}

function stripTrailingApi(baseUrl) {
  return String(baseUrl || '')
    .trim()
    .replace(/\/$/, '')
    .replace(/\/api$/, '');
}

function parseHealthcheckTargets() {
  const raw = String(process.env.SERVICE_HEALTHCHECK_TARGETS || '').trim();
  if (raw) {
    try {
      const parsed = JSON.parse(raw);
      if (Array.isArray(parsed)) {
        return parsed
          .map((t) => ({
            service: String(t?.service || '').trim(),
            url: String(t?.url || '').trim(),
            expectStatus: Number.isFinite(t?.expectStatus) ? t.expectStatus : undefined,
          }))
          .filter((t) => t.service && t.url);
      }
    } catch {
      const items = raw.split(',').map((s) => s.trim()).filter(Boolean);
      return items
        .map((entry) => {
          const [service, url] = entry.split('|').map((s) => String(s || '').trim());
          return { service, url };
        })
        .filter((t) => t.service && t.url);
    }
  }

  const targets = [];
  if (DARK_CITY_API_BASE_URL) {
    const root = stripTrailingApi(DARK_CITY_API_BASE_URL);
    targets.push({ service: 'game', url: `${root}/status-ping`, expectStatus: 200 });
  }
  const mapBase = String(process.env.DARK_CITY_MAP_BASE_URL || '').trim().replace(/\/$/, '');
  if (mapBase) targets.push({ service: 'map', url: `${mapBase}/status-ping`, expectStatus: 200 });
  const moderatorBase = String(process.env.DARK_CITY_MODERATOR_BASE_URL || '').trim().replace(/\/$/, '');
  if (moderatorBase) targets.push({ service: 'moderator', url: `${moderatorBase}/health`, expectStatus: 200 });
  const dashboardBase = String(process.env.DARK_CITY_DASHBOARD_BASE_URL || '').trim().replace(/\/$/, '');
  if (dashboardBase) targets.push({ service: 'dashboard', url: `${dashboardBase}/health`, expectStatus: 200 });
  return targets;
}

async function postDiscordWebhook(content) {
  if (!SERVICE_HEALTHCHECK_DISCORD_WEBHOOK_URL) return;
  try {
    await fetch(SERVICE_HEALTHCHECK_DISCORD_WEBHOOK_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ content: String(content || '').slice(0, 1900) }),
    });
  } catch (e) {
    console.error('Healthcheck webhook post failed:', e?.message || e);
  }
}

async function insertServiceHealthCheck(doc) {
  if (!botDb) return;
  await botDb.collection('service_health_checks').insertOne(doc);
}

async function computeRecentErrorRate(service, windowSize) {
  if (!botDb) return null;
  const n = Math.max(1, Math.min(200, Number.isFinite(windowSize) ? windowSize : 20));
  const rows = await botDb
    .collection('service_health_checks')
    .find({ guildId: DISCORD_GUILD_ID, service: String(service) })
    .sort({ checkedAt: -1 })
    .limit(n)
    .toArray();
  if (!rows.length) return null;
  const failed = rows.filter((r) => r && r.ok === false).length;
  return failed / rows.length;
}

async function pingServiceTarget(target) {
  const startedAt = Date.now();
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), Math.max(1000, SERVICE_HEALTHCHECK_TIMEOUT_MS));
  try {
    const res = await fetch(target.url, {
      method: 'GET',
      redirect: 'follow',
      signal: ctrl.signal,
    });
    const ms = Date.now() - startedAt;
    const expected = Number.isFinite(target.expectStatus) ? target.expectStatus : 200;
    const ok = res.status === expected;
    return { ok, status: res.status, responseTimeMs: ms };
  } catch (e) {
    const ms = Date.now() - startedAt;
    return { ok: false, status: 0, responseTimeMs: ms, error: e?.name || e?.message || 'fetch_failed' };
  } finally {
    clearTimeout(t);
  }
}

function startServiceHealthMonitor() {
  if (!SERVICE_HEALTHCHECKS_ENABLED) return;
  if (!SERVICE_HEALTHCHECK_DISCORD_WEBHOOK_URL) {
    console.log('ℹ️ Healthchecks: SERVICE_HEALTHCHECK_DISCORD_WEBHOOK_URL not set; alerts disabled');
  }

  const targets = parseHealthcheckTargets();
  if (!targets.length) {
    console.log('ℹ️ Healthchecks: no targets configured');
    return;
  }

  const lastOkByService = new Map();
  const lastErrorRateAlertAtByService = new Map();
  let botHeartbeatWasOk = null;
  const intervalMs = Math.max(15, Number.isFinite(SERVICE_HEALTHCHECK_INTERVAL_SECONDS) ? SERVICE_HEALTHCHECK_INTERVAL_SECONDS : 60) * 1000;

  async function tick() {
    for (const target of targets) {
      const result = await pingServiceTarget(target);
      const checkedAt = new Date();

      try {
        await insertServiceHealthCheck({
          guildId: DISCORD_GUILD_ID,
          service: target.service,
          url: target.url,
          ok: result.ok,
          status: result.status,
          responseTimeMs: result.responseTimeMs,
          error: result.ok ? null : (result.error || null),
          checkedAt,
        });
      } catch (e) {
        console.error('Healthcheck insert failed:', e?.message || e);
      }

      const prev = lastOkByService.get(target.service);
      if (prev === undefined) {
        lastOkByService.set(target.service, result.ok);
      } else if (prev !== result.ok) {
        lastOkByService.set(target.service, result.ok);
        const msg = result.ok
          ? `✅ Service recovered: **${target.service}** (${target.url})`
          : `🚨 Service DOWN: **${target.service}** (${target.url})`;
        await postDiscordWebhook(msg);
      }

      if (!result.ok) {
        const rate = await computeRecentErrorRate(target.service, SERVICE_HEALTHCHECK_ERROR_WINDOW);
        if (rate !== null && rate >= SERVICE_HEALTHCHECK_ERROR_RATE_THRESHOLD) {
          const now = Date.now();
          const last = lastErrorRateAlertAtByService.get(target.service) || 0;
          const cooldownMs = Math.max(1, SERVICE_HEALTHCHECK_ERROR_ALERT_COOLDOWN_MINUTES) * 60 * 1000;
          if (now - last >= cooldownMs) {
            lastErrorRateAlertAtByService.set(target.service, now);
            await postDiscordWebhook(
              `⚠️ Elevated error rate: **${target.service}** (${Math.round(rate * 100)}% failures over last ${Math.max(1, SERVICE_HEALTHCHECK_ERROR_WINDOW)} checks)` +
                ` | last=${result.status || 0}${result.error ? ` (${result.error})` : ''}`
            );
          }
        }
      }
    }

    if (BOT_HEARTBEAT_ENABLED) {
      try {
        const hb = await getBotHeartbeat();
        const lastSeenAt = hb && hb.lastSeenAt ? new Date(hb.lastSeenAt) : null;
        const ageMs = lastSeenAt ? (Date.now() - lastSeenAt.getTime()) : Number.POSITIVE_INFINITY;
        const staleMs = Math.max(10, Number.isFinite(BOT_HEARTBEAT_STALE_SECONDS) ? BOT_HEARTBEAT_STALE_SECONDS : 120) * 1000;
        const ok = ageMs <= staleMs;

        if (botHeartbeatWasOk === null) {
          botHeartbeatWasOk = ok;
        } else if (botHeartbeatWasOk !== ok) {
          botHeartbeatWasOk = ok;
          const msg = ok
            ? '✅ Background bot recovered (heartbeat resumed)'
            : `🚨 Background bot DOWN (no heartbeat for ${Math.round(ageMs / 1000)}s)`;
          await postDiscordWebhook(msg);
        }
      } catch (e) {
        console.error('Heartbeat monitor failed:', e?.message || e);
      }
    }
  }

  void tick();
  setInterval(() => {
    void tick();
  }, intervalMs);
}

async function getSettings() {
  if (!botDb) return null;
  return botDb.collection('bot_settings').findOne({ guildId: DISCORD_GUILD_ID });
}

async function upsertSettings(values) {
  if (!botDb) return;
  await botDb.collection('bot_settings').updateOne(
    { guildId: DISCORD_GUILD_ID },
    {
      $set: {
        ...values,
        updatedAt: new Date(),
      },
      $setOnInsert: {
        guildId: DISCORD_GUILD_ID,
        createdAt: new Date(),
      },
    },
    { upsert: true }
  );
}

async function getRecentLogs(limit) {
  if (!botDb) return [];
  const n = Number.isFinite(limit) ? limit : 100;
  return botDb
    .collection('bot_logs')
    .find({ guildId: DISCORD_GUILD_ID })
    .sort({ createdAt: -1 })
    .limit(Math.max(1, Math.min(500, n)))
    .toArray();
}

async function getRecentTelemetryEvents(filters, limit) {
  if (!botDb) return [];
  const n = Number.isFinite(limit) ? limit : 100;

  const q = { guildId: DISCORD_GUILD_ID };
  if (filters && filters.service) q.service = String(filters.service);
  if (filters && filters.level) q.level = String(filters.level);
  if (filters && filters.category) q.category = String(filters.category);

  return botDb
    .collection('telemetry_events')
    .find(q)
    .sort({ createdAt: -1 })
    .limit(Math.max(1, Math.min(500, n)))
    .toArray();
}

function requireTelemetryToken(req, res) {
  if (!TELEMETRY_INGEST_TOKEN) {
    res.status(503).json({ error: 'Telemetry ingestion is not configured' });
    return false;
  }

  const provided = String(req.headers['x-telemetry-token'] || '').trim();
  if (!provided || provided !== TELEMETRY_INGEST_TOKEN) {
    res.status(401).json({ error: 'Unauthorized' });
    return false;
  }
  return true;
}

function sanitizeTelemetryString(v, maxLen) {
  if (!v) return '';
  const s = String(v);
  return s.length > maxLen ? s.slice(0, maxLen) : s;
}

async function insertTelemetryEvent(doc) {
  if (!botDb) return;
  await botDb.collection('telemetry_events').insertOne(doc);
}

function escapeHtml(s) {
  return String(s || '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function htmlPage(title, body) {
  return `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>${title}</title>
  <style>
    body { font-family: system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif; margin: 0; background: #0b0f19; color: #e6e9f2; }
    .wrap { max-width: 980px; margin: 0 auto; padding: 32px 20px; }
    .card { background: rgba(255,255,255,0.06); border: 1px solid rgba(255,255,255,0.12); border-radius: 12px; padding: 18px; }
    .card + .card { margin-top: 16px; }
    h1, h2 { margin: 0 0 8px 0; }
    h2 { font-size: 18px; }
    a { color: #8ab4ff; text-decoration: none; }
    a:hover { text-decoration: underline; }
    .btn { display: inline-block; background: #3b82f6; color: white; padding: 10px 14px; border-radius: 10px; font-weight: 700; border: 0; cursor: pointer; }
    .btn:disabled { opacity: 0.5; cursor: not-allowed; }
    .btn.secondary { background: rgba(255,255,255,0.08); border: 1px solid rgba(255,255,255,0.12); }
    .muted { color: rgba(230,233,242,0.72); }
    .pill { display:inline-flex; align-items:center; gap:8px; padding: 4px 10px; border-radius: 999px; border: 1px solid rgba(255,255,255,0.14); background: rgba(0,0,0,0.18); font-weight: 650; }
    .pill.ok { border-color: rgba(34,197,94,0.4); }
    .pill.bad { border-color: rgba(239,68,68,0.4); }
    .notice { padding: 10px 12px; border-radius: 10px; border: 1px solid rgba(255,255,255,0.12); background: rgba(0,0,0,0.20); }
    .notice.error { border-color: rgba(239,68,68,0.35); }
    .notice.ok { border-color: rgba(34,197,94,0.35); }
    code { background: rgba(0,0,0,0.35); padding: 2px 6px; border-radius: 6px; }
    .grid { display: grid; gap: 12px; }
    .grid.cols-2 { grid-template-columns: repeat(2, minmax(0, 1fr)); }
    @media (max-width: 800px) { .grid.cols-2 { grid-template-columns: 1fr; } }
    input, textarea { width: 100%; padding: 10px; border-radius: 10px; border: 1px solid rgba(255,255,255,0.12); background: rgba(0,0,0,0.25); color: #e6e9f2; }
    textarea { font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace; }
    label { display:block; margin-bottom: 6px; }
    details > summary { cursor: pointer; user-select: none; list-style: none; }
    details > summary::-webkit-details-marker { display:none; }
    .summaryRow { display:flex; align-items:center; justify-content:space-between; gap:12px; }
    .actionsRow { display:flex; align-items:center; gap:10px; flex-wrap: wrap; }
    .stickyActions { position: sticky; bottom: 0; padding: 12px; border-radius: 12px; border: 1px solid rgba(255,255,255,0.12); background: rgba(11,15,25,0.92); backdrop-filter: blur(8px); }
  </style>
</head>
<body>
  <div class="wrap">
    ${body}
  </div>
</body>
</html>`;
}

function requireLogin(req, res, next) {
  if (req.session?.user?.allowed) return next();
  res.status(401).send(
    htmlPage(
      'Dark City Bot Dashboard',
      `<div class="card">
        <h1>Dark City Bot Dashboard</h1>
        <p class="muted">You must sign in with Discord as an Admin.</p>
        <p><a class="btn" href="/auth/discord">Sign in with Discord</a></p>
      </div>`
    )
  );
}

app.get('/', (req, res) => {
  if (req.session?.user?.allowed) {
    return res.redirect('/dashboard');
  }
  res.send(
    htmlPage(
      'Dark City Bot Dashboard',
      `<div class="card">
        <h1>Dark City Bot Dashboard</h1>
        <p class="muted">Sign in to continue.</p>
        <p><a class="btn" href="/auth/discord">Sign in with Discord</a></p>
      </div>`
    )
  );
});

app.get('/xp', requireLogin, (req, res) => {
  const ok = Boolean(DARK_CITY_API_BASE_URL && DARK_CITY_MODERATOR_PASSWORD);
  res.send(
    htmlPage(
      'XP',
      `<div class="card">
        ${nav(req)}
        <h1>XP</h1>
        <p class="muted">Game API: <strong>${ok ? 'configured' : 'missing env vars'}</strong></p>
        <form method="POST" action="/xp/reset" style="display:grid; gap:12px; max-width:420px;">
          <div>
            <label class="muted" for="discordUserId">Discord user ID</label><br/>
            <input id="discordUserId" name="discordUserId" inputmode="numeric" placeholder="123456789012345678" style="width:100%; padding:10px; border-radius:10px; border:1px solid rgba(255,255,255,0.12); background: rgba(0,0,0,0.25); color: #e6e9f2;" />
          </div>
          <button class="btn" type="submit">Reset XP to 0</button>
          <p class="muted" style="margin:0;">This updates the character’s XP immediately. If the level changes, the character sheet is regenerated.</p>
        </form>
      </div>`
    )
  );
});

app.post('/xp/reset', requireLogin, async (req, res) => {
  try {
    const discordUserId = String(req.body?.discordUserId || '').trim();
    if (!discordUserId) {
      return res.status(400).send('discordUserId is required');
    }

    await darkCityApiRequest('/api/characters/discord/set-xp', {
      method: 'POST',
      body: JSON.stringify({ discordUserId, xp: 0 }),
    }, req);

    res.redirect('/xp');
  } catch (error) {
    console.error('XP reset error:', error);
    res.status(500).send(`XP reset error: ${error?.message || String(error)}`);
  }
});

app.get('/logout', (req, res) => {
  req.session.destroy(() => res.redirect('/'));
});

app.get('/auth/discord', (req, res) => {
  const state = crypto.randomBytes(16).toString('hex');
  req.session.oauthState = state;

  const params = new URLSearchParams({
    client_id: DISCORD_CLIENT_ID,
    redirect_uri: DISCORD_REDIRECT_URI,
    response_type: 'code',
    scope: 'identify guilds.members.read',
    state,
    prompt: 'none',
  });

  res.redirect(`https://discord.com/api/oauth2/authorize?${params.toString()}`);
});

async function discordFetchJson(url, options) {
  const res = await fetch(url, options);
  const text = await res.text();
  let json;
  try {
    json = text ? JSON.parse(text) : null;
  } catch {
    json = null;
  }

  if (!res.ok) {
    const msg = json ? JSON.stringify(json) : text;
    const err = new Error(`Discord API error ${res.status}: ${msg}`);
    err.status = res.status;
    throw err;
  }

  return json;
}

function assertGameApiConfigured() {
  if (!DARK_CITY_API_BASE_URL) {
    throw new Error('DARK_CITY_API_BASE_URL is not set');
  }
  if (!DARK_CITY_MODERATOR_PASSWORD) {
    throw new Error('DARK_CITY_MODERATOR_PASSWORD is not set');
  }
}

function joinGameApiUrl(path) {
  const base = String(DARK_CITY_API_BASE_URL || '').trim().replace(/\/$/, '');
  let p = String(path || '').trim();
  if (!p.startsWith('/')) p = `/${p}`;

  // Allow either base URL form:
  // - https://service.onrender.com
  // - https://service.onrender.com/api
  // while callers usually pass paths like /api/quiz/config
  if (base.endsWith('/api') && p.startsWith('/api/')) {
    p = p.slice('/api'.length);
  }

  return `${base}${p}`;
}

async function darkCityApiRequest(path, opts, req) {
  assertGameApiConfigured();
  const url = joinGameApiUrl(path);
  const headers = {
    'Content-Type': 'application/json',
    // Use Discord OAuth token instead of moderator password
    'Authorization': `Bearer ${req.session?.user?.accessToken}`,
    ...(opts?.headers || {}),
  };
  
  console.log(`[API] Making request to: ${url}`);
  console.log(`[API] Method: ${opts?.method || 'GET'}`);
  console.log(`[API] User authenticated: ${Boolean(req.session?.user?.accessToken)}`);
  console.log(`[API] User ID: ${req.session?.user?.id}`);
  console.log(`[API] Token preview: ${req.session?.user?.accessToken?.slice(0, 20)}...`);
  
  const res = await fetch(url, { ...opts, headers });
  const text = await res.text();
  
  console.log(`[API] Response status: ${res.status}`);
  console.log(`[API] Response text length: ${text.length}`);
  console.log(`[API] Response text preview: ${text.slice(0, 200)}`);
  
  let json;
  try {
    json = text ? JSON.parse(text) : null;
  } catch {
    json = null;
  }
  
  if (!res.ok) {
    const preview = (text || '').slice(0, 300);
    const msg = json?.error || json?.message || preview || `HTTP ${res.status}`;
    const err = new Error(msg);
    err.status = res.status;
    err.url = url;
    throw err;
  }
  
  console.log(`[API] Parsed JSON: ${json ? 'success' : 'null'}`);
  return json;
}

app.get('/auth/discord/callback', async (req, res) => {
  try {
    const { code, state } = req.query;

    if (!code || typeof code !== 'string') {
      return res.status(400).send('Missing code');
    }

    if (!state || typeof state !== 'string' || state !== req.session.oauthState) {
      return res.status(400).send('Invalid state');
    }

    const tokenRes = await fetch('https://discord.com/api/oauth2/token', {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body: new URLSearchParams({
        client_id: DISCORD_CLIENT_ID,
        client_secret: DISCORD_CLIENT_SECRET,
        grant_type: 'authorization_code',
        code,
        redirect_uri: DISCORD_REDIRECT_URI,
      }),
    });

    const tokenText = await tokenRes.text();
    const tokenJson = tokenText ? JSON.parse(tokenText) : null;

    if (!tokenRes.ok) {
      return res.status(401).send(`OAuth failed: ${tokenRes.status} ${tokenText}`);
    }

    const accessToken = tokenJson.access_token;

    const user = await discordFetchJson('https://discord.com/api/users/@me', {
      headers: { Authorization: `Bearer ${accessToken}` },
    });

    // This returns membership (including roles) for the authorized user in the target guild.
    const member = await discordFetchJson(`https://discord.com/api/users/@me/guilds/${DISCORD_GUILD_ID}/member`, {
      headers: { Authorization: `Bearer ${accessToken}` },
    });

    const roles = Array.isArray(member?.roles) ? member.roles : [];
    const allowed = Boolean(ADMIN_ROLE_ID && roles.includes(ADMIN_ROLE_ID));

    req.session.user = {
      id: user?.id,
      username: user?.username,
      global_name: user?.global_name,
      allowed,
      roles,
      accessToken, // Store the access token for API calls
    };

    if (!allowed) {
      return res.status(403).send(
        htmlPage(
          'Access denied',
          `<div class="card">
            <h1>Access denied</h1>
            <p class="muted">Your Discord account is not allowed to access this dashboard.</p>
            <p class="muted">Required role id: <code>${ADMIN_ROLE_ID}</code></p>
            <p><a href="/logout">Log out</a></p>
          </div>`
        )
      );
    }

    res.redirect('/dashboard');
  } catch (error) {
    console.error('OAuth callback error:', error);
    res.status(500).send('OAuth error');
  }
});

app.get('/dashboard', requireLogin, (req, res) => {
  const displayName = req.session?.user?.global_name || req.session?.user?.username || 'Moderator';
  const error = typeof req.query?.error === 'string' ? req.query.error : '';
  const ok = Boolean(DARK_CITY_API_BASE_URL && DARK_CITY_MODERATOR_PASSWORD);

  res.send(
    htmlPage(
      'Dashboard',
      `<div class="card">
        ${nav(req)}
        <div class="summaryRow">
          <div>
            <h1>Dashboard</h1>
            <p class="muted" style="margin:6px 0 0 0;">Signed in as <strong>${displayName}</strong></p>
          </div>
          <div class="pill ${ok ? 'ok' : 'bad'}" title="Dashboard ↔ Game API configuration">
            Game API: ${ok ? 'configured' : 'missing env'}
          </div>
        </div>

        <div class="actionsRow" style="margin-top:14px;">
          <a class="btn" href="/quiz">Quiz Editor</a>
          <a class="btn" href="https://dark-city-map.onrender.com/?edit=1" target="_blank" rel="noopener">Open Map Editor</a>
          <a class="btn secondary" href="/settings">Settings</a>
          <a class="btn secondary" href="/xp">XP</a>
          <a class="btn secondary" href="/logs">Logs</a>
        </div>
      </div>

      ${(error ? `<div class="card notice error"><strong>Error:</strong> ${error}</div>` : '')}`
    )
  );
});

app.get('/quiz', requireLogin, (req, res) => {
  const saved = req.query?.saved === '1';
  const error = typeof req.query?.error === 'string' ? req.query.error : '';
  const ok = Boolean(DARK_CITY_API_BASE_URL);

  (async () => {
    let quizConfigJson = '';
    let quizLoadError = '';

    console.log('[QUIZ] Starting quiz config load...');
    console.log('[QUIZ] API configured:', Boolean(DARK_CITY_API_BASE_URL));
    console.log('[QUIZ] User session exists:', Boolean(req.session?.user));
    console.log('[QUIZ] Access token exists:', Boolean(req.session?.user?.accessToken));

    // Test the Discord token directly
    if (req.session?.user?.accessToken) {
      try {
        console.log('[QUIZ] Testing Discord token...');
        const discordResponse = await fetch('https://discord.com/api/v10/users/@me', {
          headers: {
            'Authorization': `Bearer ${req.session.user.accessToken}`
          }
        });
        console.log('[QUIZ] Discord token test status:', discordResponse.status);
        if (discordResponse.ok) {
          const discordUser = await discordResponse.json();
          console.log('[QUIZ] Discord token valid for user:', discordUser.username);
        } else {
          console.log('[QUIZ] Discord token invalid - status:', discordResponse.status);
        }
      } catch (error) {
        console.log('[QUIZ] Discord token test error:', error.message);
      }
    }

    if (ok) {
      try {
        console.log('[QUIZ] Making API call...');
        const cfg = await darkCityApiRequest('/api/quiz/config', { method: 'GET' }, req);
        console.log('[QUIZ] API response:', cfg ? 'success' : 'null');
        if (cfg === null) {
          quizLoadError = 'API returned null response - check Discord OAuth authentication';
        } else {
          quizConfigJson = JSON.stringify(cfg, null, 2);
          console.log('[QUIZ] Config loaded successfully, length:', quizConfigJson.length);
        }
      } catch (e) {
        console.error('[QUIZ] API call failed:', e);
        quizLoadError = e?.message || String(e);
      }
    } else {
      quizLoadError = 'Game API not configured - set DARK_CITY_API_BASE_URL';
    }

    res.send(
      htmlPage(
        'Quiz',
        `<div class="card">
          ${nav(req)}
          <div class="summaryRow">
            <div>
              <h1>Quiz Editor</h1>
              <p class="muted" style="margin:6px 0 0 0;">Edit and save the quiz configuration used by the game.</p>
            </div>
            <div class="pill ${ok ? 'ok' : 'bad'}" title="Dashboard ↔ Game API configuration">
              Game API: ${ok ? 'configured' : 'missing env'}
            </div>
          </div>
        </div>

        ${(saved ? '<div class="card notice ok"><strong>Quiz config saved.</strong></div>' : '')}
        ${(error ? `<div class="card notice error"><strong>Error:</strong> ${error}</div>` : '')}

        <div class="card">
          <div class="summaryRow">
            <h2 style="margin:0;">Quiz Questions</h2>
            ${quizLoadError ? '<span class="pill bad">Load failed</span>' : '<span class="pill ok">Loaded</span>'}
          </div>
          ${quizLoadError ? `<div class="notice error" style="margin-top:12px;"><strong>Load error:</strong> ${quizLoadError}</div>` : ''}
          <form method="POST" action="/dashboard/quiz-config" class="grid" style="margin-top:12px;">
            <div>
              <label class="muted" for="quizConfigJson">Quiz config JSON</label>
              <textarea id="quizConfigJson" name="quizConfigJson" rows="20">${quizConfigJson}</textarea>
              <p class="muted" style="margin:8px 0 0 0;">Edit <code>questions</code> and each question’s <code>rule</code>. Supported rule types: <code>any</code>, <code>all_groups</code>, <code>and</code>, <code>or</code>.</p>
            </div>
            <div class="stickyActions">
              <div class="actionsRow" style="justify-content:space-between;">
                <div class="muted">Be careful: invalid JSON will be rejected.</div>
                <button class="btn" type="submit" ${ok ? '' : 'disabled'}>Save Quiz Config</button>
              </div>
            </div>
          </form>
        </div>`
      )
    );
  })().catch((e) => {
    console.error('Quiz page error:', e);
    res.status(500).send('Quiz page error');
  });
});

app.post('/dashboard/moderator-password', requireLogin, async (req, res) => {
  try {
    assertGameApiConfigured();

    const newPassword = String(req.body?.newPassword || '').trim();
    const confirmPassword = String(req.body?.confirmPassword || '').trim();
    if (!newPassword) {
      return res.redirect('/dashboard?error=' + encodeURIComponent('newPassword is required'));
    }
    if (newPassword !== confirmPassword) {
      return res.redirect('/dashboard?error=' + encodeURIComponent('Passwords do not match'));
    }

    const url = `${DARK_CITY_API_BASE_URL}/api/characters/moderator/password`;
    const response = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ currentPassword: DARK_CITY_MODERATOR_PASSWORD, newPassword }),
    });

    const text = await response.text();
    let json;
    try {
      json = text ? JSON.parse(text) : null;
    } catch {
      json = null;
    }

    if (!response.ok) {
      const msg = json?.error || json?.message || text || `HTTP ${response.status}`;
      return res.redirect('/dashboard?error=' + encodeURIComponent(msg));
    }

    DARK_CITY_MODERATOR_PASSWORD = newPassword;
    res.redirect('/quiz?saved=1');
  } catch (error) {
    console.error('Moderator password update error:', error);
    res.redirect('/quiz?error=' + encodeURIComponent(error?.message || 'Password update failed'));
  }
});

app.post('/dashboard/quiz-config', requireLogin, async (req, res) => {
  try {
    assertGameApiConfigured();

    const raw = String(req.body?.quizConfigJson || '').trim();
    if (!raw) {
      return res.redirect('/dashboard?error=' + encodeURIComponent('quizConfigJson is required'));
    }

    let parsed;
    try {
      parsed = JSON.parse(raw);
    } catch {
      return res.redirect('/dashboard?error=' + encodeURIComponent('Invalid JSON'));
    }

    if (!parsed || typeof parsed !== 'object') {
      return res.redirect('/dashboard?error=' + encodeURIComponent('Invalid config payload'));
    }

    await darkCityApiRequest('/api/quiz/config', {
      method: 'PUT',
      body: JSON.stringify(parsed),
    }, req);

    res.redirect('/quiz?saved=1');
  } catch (error) {
    console.error('Quiz config save error:', error);
    res.redirect('/quiz?error=' + encodeURIComponent(error?.message || 'Save failed'));
  }
});

app.get('/settings', requireLogin, async (req, res) => {
  try {
    const settings = await getSettings();
    const rUser = settings?.rCooldownUserMs ?? '';
    const rChan = settings?.rCooldownChannelMs ?? '';
    const inviteAutoDeleteEnabled = typeof settings?.inviteAutoDeleteEnabled === 'boolean' ? settings.inviteAutoDeleteEnabled : true;
    const inviteWarnEnabled = typeof settings?.inviteWarnEnabled === 'boolean' ? settings.inviteWarnEnabled : true;
    const inviteWarnDeleteSeconds = Number.isFinite(settings?.inviteWarnDeleteSeconds) ? settings.inviteWarnDeleteSeconds : 12;

    const lowTrustLinkFilterEnabled = typeof settings?.lowTrustLinkFilterEnabled === 'boolean' ? settings.lowTrustLinkFilterEnabled : true;
    const lowTrustMinAccountAgeDays = Number.isFinite(settings?.lowTrustMinAccountAgeDays) ? settings.lowTrustMinAccountAgeDays : 7;
    const lowTrustWarnDmEnabled = typeof settings?.lowTrustWarnDmEnabled === 'boolean' ? settings.lowTrustWarnDmEnabled : true;

    const spamAutoModEnabled = typeof settings?.spamAutoModEnabled === 'boolean' ? settings.spamAutoModEnabled : true;
    const spamFloodWindowSeconds = Number.isFinite(settings?.spamFloodWindowSeconds) ? settings.spamFloodWindowSeconds : 8;
    const spamFloodMaxMessages = Number.isFinite(settings?.spamFloodMaxMessages) ? settings.spamFloodMaxMessages : 5;
    const spamRepeatWindowSeconds = Number.isFinite(settings?.spamRepeatWindowSeconds) ? settings.spamRepeatWindowSeconds : 30;
    const spamRepeatMaxRepeats = Number.isFinite(settings?.spamRepeatMaxRepeats) ? settings.spamRepeatMaxRepeats : 3;
    const spamWarnEnabled = typeof settings?.spamWarnEnabled === 'boolean' ? settings.spamWarnEnabled : true;
    const spamWarnDeleteSeconds = Number.isFinite(settings?.spamWarnDeleteSeconds) ? settings.spamWarnDeleteSeconds : 12;
    const spamTimeoutEnabled = typeof settings?.spamTimeoutEnabled === 'boolean' ? settings.spamTimeoutEnabled : true;
    const spamTimeoutMinutes = Number.isFinite(settings?.spamTimeoutMinutes) ? settings.spamTimeoutMinutes : 10;
    const spamStrikeDecayMinutes = Number.isFinite(settings?.spamStrikeDecayMinutes) ? settings.spamStrikeDecayMinutes : 30;
    const spamIgnoredChannelIds = Array.isArray(settings?.spamIgnoredChannelIds)
      ? settings.spamIgnoredChannelIds.join(',')
      : (settings?.spamIgnoredChannelIds ?? '');
    const spamBypassRoleIds = Array.isArray(settings?.spamBypassRoleIds)
      ? settings.spamBypassRoleIds.join(',')
      : (settings?.spamBypassRoleIds ?? '');

    const xpEnabled = typeof settings?.xpEnabled === 'boolean' ? settings.xpEnabled : false;
    const xpPerMessage = Number.isFinite(settings?.xpPerMessage) ? settings.xpPerMessage : 1;
    const xpCooldownSeconds = Number.isFinite(settings?.xpCooldownSeconds) ? settings.xpCooldownSeconds : 60;
    const xpMinMessageChars = Number.isFinite(settings?.xpMinMessageChars) ? settings.xpMinMessageChars : 20;
    const xpAllowedChannelIds = Array.isArray(settings?.xpAllowedChannelIds)
      ? settings.xpAllowedChannelIds.join(',')
      : (settings?.xpAllowedChannelIds ?? '');
    const mongoOk = Boolean(botDb);

    res.send(
      htmlPage(
        'Settings',
        `<div class="card">
          ${nav(req)}
          <h1>Settings</h1>
          <p class="muted">Guild: <code>${DISCORD_GUILD_ID}</code></p>
          <p class="muted">Mongo: <strong>${mongoOk ? 'connected' : 'not configured'}</strong></p>
          <form method="POST" action="/settings" style="display:grid; gap:12px; max-width:420px;">
            <div>
              <label class="muted" for="rCooldownUserMs">/r cooldown (per user, ms)</label><br/>
              <input id="rCooldownUserMs" name="rCooldownUserMs" inputmode="numeric" value="${rUser}" style="width:100%; padding:10px; border-radius:10px; border:1px solid rgba(255,255,255,0.12); background: rgba(0,0,0,0.25); color: #e6e9f2;" />
            </div>
            <div>
              <label class="muted" for="rCooldownChannelMs">/r cooldown (per channel, ms)</label><br/>
              <input id="rCooldownChannelMs" name="rCooldownChannelMs" inputmode="numeric" value="${rChan}" style="width:100%; padding:10px; border-radius:10px; border:1px solid rgba(255,255,255,0.12); background: rgba(0,0,0,0.25); color: #e6e9f2;" />
            </div>

            <div style="margin-top:8px; padding-top:8px; border-top:1px solid rgba(255,255,255,0.12);">
              <div class="muted" style="font-weight:700; margin-bottom:8px;">Auto-mod: Invite links</div>
              <label style="display:flex; gap:8px; align-items:center;">
                <input type="checkbox" name="inviteAutoDeleteEnabled" ${inviteAutoDeleteEnabled ? 'checked' : ''} />
                <span>Delete Discord invite links</span>
              </label>
              <label style="display:flex; gap:8px; align-items:center; margin-top:6px;">
                <input type="checkbox" name="inviteWarnEnabled" ${inviteWarnEnabled ? 'checked' : ''} />
                <span>Post channel warning (temporary)</span>
              </label>
              <div style="margin-top:8px;">
                <label class="muted" for="inviteWarnDeleteSeconds">Warning auto-delete (seconds)</label><br/>
                <input id="inviteWarnDeleteSeconds" name="inviteWarnDeleteSeconds" inputmode="numeric" value="${inviteWarnDeleteSeconds}" style="width:100%; padding:10px; border-radius:10px; border:1px solid rgba(255,255,255,0.12); background: rgba(0,0,0,0.25); color: #e6e9f2;" />
              </div>
            </div>

            <div style="margin-top:8px; padding-top:8px; border-top:1px solid rgba(255,255,255,0.12);">
              <div class="muted" style="font-weight:700; margin-bottom:8px;">Auto-mod: Low-trust link filter</div>
              <label style="display:flex; gap:8px; align-items:center;">
                <input type="checkbox" name="lowTrustLinkFilterEnabled" ${lowTrustLinkFilterEnabled ? 'checked' : ''} />
                <span>Delete links from new accounts</span>
              </label>
              <div style="margin-top:8px;">
                <label class="muted" for="lowTrustMinAccountAgeDays">Minimum account age to post links (days)</label><br/>
                <input id="lowTrustMinAccountAgeDays" name="lowTrustMinAccountAgeDays" inputmode="numeric" value="${lowTrustMinAccountAgeDays}" style="width:100%; padding:10px; border-radius:10px; border:1px solid rgba(255,255,255,0.12); background: rgba(0,0,0,0.25); color: #e6e9f2;" />
              </div>
              <label style="display:flex; gap:8px; align-items:center; margin-top:8px;">
                <input type="checkbox" name="lowTrustWarnDmEnabled" ${lowTrustWarnDmEnabled ? 'checked' : ''} />
                <span>DM user when a link is removed</span>
              </label>
            </div>

            <div style="margin-top:8px; padding-top:8px; border-top:1px solid rgba(255,255,255,0.12);">
              <div class="muted" style="font-weight:700; margin-bottom:8px;">Auto-mod: Spam (flood / repeat)</div>
              <label style="display:flex; gap:8px; align-items:center;">
                <input type="checkbox" name="spamAutoModEnabled" ${spamAutoModEnabled ? 'checked' : ''} />
                <span>Enable spam auto-mod</span>
              </label>

              <div style="margin-top:10px; display:grid; gap:10px;">
                <div>
                  <label class="muted" for="spamFloodWindowSeconds">Flood window (seconds)</label><br/>
                  <input id="spamFloodWindowSeconds" name="spamFloodWindowSeconds" inputmode="numeric" value="${spamFloodWindowSeconds}" style="width:100%; padding:10px; border-radius:10px; border:1px solid rgba(255,255,255,0.12); background: rgba(0,0,0,0.25); color: #e6e9f2;" />
                </div>
                <div>
                  <label class="muted" for="spamFloodMaxMessages">Max messages in window</label><br/>
                  <input id="spamFloodMaxMessages" name="spamFloodMaxMessages" inputmode="numeric" value="${spamFloodMaxMessages}" style="width:100%; padding:10px; border-radius:10px; border:1px solid rgba(255,255,255,0.12); background: rgba(0,0,0,0.25); color: #e6e9f2;" />
                </div>

                <div>
                  <label class="muted" for="spamRepeatWindowSeconds">Repeat window (seconds)</label><br/>
                  <input id="spamRepeatWindowSeconds" name="spamRepeatWindowSeconds" inputmode="numeric" value="${spamRepeatWindowSeconds}" style="width:100%; padding:10px; border-radius:10px; border:1px solid rgba(255,255,255,0.12); background: rgba(0,0,0,0.25); color: #e6e9f2;" />
                </div>
                <div>
                  <label class="muted" for="spamRepeatMaxRepeats">Max repeats (same message)</label><br/>
                  <input id="spamRepeatMaxRepeats" name="spamRepeatMaxRepeats" inputmode="numeric" value="${spamRepeatMaxRepeats}" style="width:100%; padding:10px; border-radius:10px; border:1px solid rgba(255,255,255,0.12); background: rgba(0,0,0,0.25); color: #e6e9f2;" />
                </div>

                <label style="display:flex; gap:8px; align-items:center; margin-top:6px;">
                  <input type="checkbox" name="spamWarnEnabled" ${spamWarnEnabled ? 'checked' : ''} />
                  <span>Post channel warning (temporary)</span>
                </label>
                <div>
                  <label class="muted" for="spamWarnDeleteSeconds">Warning auto-delete (seconds)</label><br/>
                  <input id="spamWarnDeleteSeconds" name="spamWarnDeleteSeconds" inputmode="numeric" value="${spamWarnDeleteSeconds}" style="width:100%; padding:10px; border-radius:10px; border:1px solid rgba(255,255,255,0.12); background: rgba(0,0,0,0.25); color: #e6e9f2;" />
                </div>

                <label style="display:flex; gap:8px; align-items:center; margin-top:6px;">
                  <input type="checkbox" name="spamTimeoutEnabled" ${spamTimeoutEnabled ? 'checked' : ''} />
                  <span>Timeout user on repeated spam (2+ strikes)</span>
                </label>
                <div>
                  <label class="muted" for="spamTimeoutMinutes">Timeout length (minutes)</label><br/>
                  <input id="spamTimeoutMinutes" name="spamTimeoutMinutes" inputmode="numeric" value="${spamTimeoutMinutes}" style="width:100%; padding:10px; border-radius:10px; border:1px solid rgba(255,255,255,0.12); background: rgba(0,0,0,0.25); color: #e6e9f2;" />
                </div>

                <div>
                  <label class="muted" for="spamStrikeDecayMinutes">Strike decay (minutes)</label><br/>
                  <input id="spamStrikeDecayMinutes" name="spamStrikeDecayMinutes" inputmode="numeric" value="${spamStrikeDecayMinutes}" style="width:100%; padding:10px; border-radius:10px; border:1px solid rgba(255,255,255,0.12); background: rgba(0,0,0,0.25); color: #e6e9f2;" />
                </div>

                <div>
                  <label class="muted" for="spamIgnoredChannelIds">Ignored channel IDs (comma or newline separated)</label><br/>
                  <textarea id="spamIgnoredChannelIds" name="spamIgnoredChannelIds" rows="3" style="width:100%; padding:10px; border-radius:10px; border:1px solid rgba(255,255,255,0.12); background: rgba(0,0,0,0.25); color: #e6e9f2;">${spamIgnoredChannelIds}</textarea>
                </div>

                <div>
                  <label class="muted" for="spamBypassRoleIds">Bypass role IDs (comma or newline separated)</label><br/>
                  <textarea id="spamBypassRoleIds" name="spamBypassRoleIds" rows="3" style="width:100%; padding:10px; border-radius:10px; border:1px solid rgba(255,255,255,0.12); background: rgba(0,0,0,0.25); color: #e6e9f2;">${spamBypassRoleIds}</textarea>
                </div>
              </div>
            </div>

            <div style="margin-top:8px; padding-top:8px; border-top:1px solid rgba(255,255,255,0.12);">
              <div class="muted" style="font-weight:700; margin-bottom:8px;">XP: Activity rewards</div>
              <label style="display:flex; gap:8px; align-items:center;">
                <input type="checkbox" name="xpEnabled" ${xpEnabled ? 'checked' : ''} />
                <span>Enable XP awards for messages</span>
              </label>
              <div style="margin-top:10px; display:grid; gap:10px;">
                <div>
                  <label class="muted" for="xpPerMessage">XP per message</label><br/>
                  <input id="xpPerMessage" name="xpPerMessage" inputmode="numeric" value="${xpPerMessage}" style="width:100%; padding:10px; border-radius:10px; border:1px solid rgba(255,255,255,0.12); background: rgba(0,0,0,0.25); color: #e6e9f2;" />
                </div>
                <div>
                  <label class="muted" for="xpCooldownSeconds">Cooldown per user (seconds)</label><br/>
                  <input id="xpCooldownSeconds" name="xpCooldownSeconds" inputmode="numeric" value="${xpCooldownSeconds}" style="width:100%; padding:10px; border-radius:10px; border:1px solid rgba(255,255,255,0.12); background: rgba(0,0,0,0.25); color: #e6e9f2;" />
                </div>
                <div>
                  <label class="muted" for="xpMinMessageChars">Minimum message length (chars)</label><br/>
                  <input id="xpMinMessageChars" name="xpMinMessageChars" inputmode="numeric" value="${xpMinMessageChars}" style="width:100%; padding:10px; border-radius:10px; border:1px solid rgba(255,255,255,0.12); background: rgba(0,0,0,0.25); color: #e6e9f2;" />
                </div>
                <div>
                  <label class="muted" for="xpAllowedChannelIds">Allowed channel IDs (Districts) (comma or newline separated)</label><br/>
                  <textarea id="xpAllowedChannelIds" name="xpAllowedChannelIds" rows="3" style="width:100%; padding:10px; border-radius:10px; border:1px solid rgba(255,255,255,0.12); background: rgba(0,0,0,0.25); color: #e6e9f2;">${xpAllowedChannelIds}</textarea>
                </div>
              </div>
            </div>

            <button class="btn" type="submit">Save</button>
          </form>
          <p class="muted" style="margin-top:12px;">Bot reloads settings periodically, so changes can take up to ~30 seconds.</p>
        </div>`
      )
    );
  } catch (error) {
    console.error('Settings page error:', error);
    res.status(500).send('Settings error');
  }
});

app.post('/settings', requireLogin, async (req, res) => {
  try {
    if (!botDb) {
      return res.status(400).send('Mongo not configured');
    }

    const rCooldownUserMs = parseInt(req.body?.rCooldownUserMs, 10);
    const rCooldownChannelMs = parseInt(req.body?.rCooldownChannelMs, 10);
    const inviteWarnDeleteSeconds = parseInt(req.body?.inviteWarnDeleteSeconds, 10);
    const lowTrustMinAccountAgeDays = parseInt(req.body?.lowTrustMinAccountAgeDays, 10);

    const spamFloodWindowSeconds = parseInt(req.body?.spamFloodWindowSeconds, 10);
    const spamFloodMaxMessages = parseInt(req.body?.spamFloodMaxMessages, 10);
    const spamRepeatWindowSeconds = parseInt(req.body?.spamRepeatWindowSeconds, 10);
    const spamRepeatMaxRepeats = parseInt(req.body?.spamRepeatMaxRepeats, 10);
    const spamWarnDeleteSeconds = parseInt(req.body?.spamWarnDeleteSeconds, 10);
    const spamTimeoutMinutes = parseInt(req.body?.spamTimeoutMinutes, 10);
    const spamStrikeDecayMinutes = parseInt(req.body?.spamStrikeDecayMinutes, 10);
    const spamIgnoredChannelIds = String(req.body?.spamIgnoredChannelIds || '')
      .split(/[\n,]/g)
      .map((x) => String(x).trim())
      .filter(Boolean);
    const spamBypassRoleIds = String(req.body?.spamBypassRoleIds || '')
      .split(/[\n,]/g)
      .map((x) => String(x).trim())
      .filter(Boolean);

    const xpPerMessage = parseInt(req.body?.xpPerMessage, 10);
    const xpCooldownSeconds = parseInt(req.body?.xpCooldownSeconds, 10);
    const xpMinMessageChars = parseInt(req.body?.xpMinMessageChars, 10);
    const xpAllowedChannelIds = String(req.body?.xpAllowedChannelIds || '')
      .split(/[\n,]/g)
      .map((x) => String(x).trim())
      .filter(Boolean);

    const inviteAutoDeleteEnabled = req.body?.inviteAutoDeleteEnabled === 'on';
    const inviteWarnEnabled = req.body?.inviteWarnEnabled === 'on';
    const lowTrustLinkFilterEnabled = req.body?.lowTrustLinkFilterEnabled === 'on';
    const lowTrustWarnDmEnabled = req.body?.lowTrustWarnDmEnabled === 'on';

    const spamAutoModEnabled = req.body?.spamAutoModEnabled === 'on';
    const spamWarnEnabled = req.body?.spamWarnEnabled === 'on';
    const spamTimeoutEnabled = req.body?.spamTimeoutEnabled === 'on';

    const xpEnabled = req.body?.xpEnabled === 'on';

    const update = {};
    if (Number.isFinite(rCooldownUserMs) && rCooldownUserMs >= 0 && rCooldownUserMs <= 600000) {
      update.rCooldownUserMs = rCooldownUserMs;
    }
    if (Number.isFinite(rCooldownChannelMs) && rCooldownChannelMs >= 0 && rCooldownChannelMs <= 600000) {
      update.rCooldownChannelMs = rCooldownChannelMs;
    }

    update.inviteAutoDeleteEnabled = inviteAutoDeleteEnabled;
    update.inviteWarnEnabled = inviteWarnEnabled;
    if (Number.isFinite(inviteWarnDeleteSeconds) && inviteWarnDeleteSeconds >= 0 && inviteWarnDeleteSeconds <= 120) {
      update.inviteWarnDeleteSeconds = inviteWarnDeleteSeconds;
    }

    update.lowTrustLinkFilterEnabled = lowTrustLinkFilterEnabled;
    if (Number.isFinite(lowTrustMinAccountAgeDays) && lowTrustMinAccountAgeDays >= 0 && lowTrustMinAccountAgeDays <= 365) {
      update.lowTrustMinAccountAgeDays = lowTrustMinAccountAgeDays;
    }
    update.lowTrustWarnDmEnabled = lowTrustWarnDmEnabled;

    update.spamAutoModEnabled = spamAutoModEnabled;
    if (Number.isFinite(spamFloodWindowSeconds) && spamFloodWindowSeconds >= 1 && spamFloodWindowSeconds <= 120) {
      update.spamFloodWindowSeconds = spamFloodWindowSeconds;
    }
    if (Number.isFinite(spamFloodMaxMessages) && spamFloodMaxMessages >= 2 && spamFloodMaxMessages <= 50) {
      update.spamFloodMaxMessages = spamFloodMaxMessages;
    }
    if (Number.isFinite(spamRepeatWindowSeconds) && spamRepeatWindowSeconds >= 5 && spamRepeatWindowSeconds <= 600) {
      update.spamRepeatWindowSeconds = spamRepeatWindowSeconds;
    }
    if (Number.isFinite(spamRepeatMaxRepeats) && spamRepeatMaxRepeats >= 1 && spamRepeatMaxRepeats <= 20) {
      update.spamRepeatMaxRepeats = spamRepeatMaxRepeats;
    }
    update.spamWarnEnabled = spamWarnEnabled;
    if (Number.isFinite(spamWarnDeleteSeconds) && spamWarnDeleteSeconds >= 0 && spamWarnDeleteSeconds <= 120) {
      update.spamWarnDeleteSeconds = spamWarnDeleteSeconds;
    }
    update.spamTimeoutEnabled = spamTimeoutEnabled;
    if (Number.isFinite(spamTimeoutMinutes) && spamTimeoutMinutes >= 1 && spamTimeoutMinutes <= 43200) {
      update.spamTimeoutMinutes = spamTimeoutMinutes;
    }
    if (Number.isFinite(spamStrikeDecayMinutes) && spamStrikeDecayMinutes >= 0 && spamStrikeDecayMinutes <= 1440) {
      update.spamStrikeDecayMinutes = spamStrikeDecayMinutes;
    }
    update.spamIgnoredChannelIds = spamIgnoredChannelIds;
    update.spamBypassRoleIds = spamBypassRoleIds;

    update.xpEnabled = xpEnabled;
    if (Number.isFinite(xpPerMessage) && xpPerMessage >= 0 && xpPerMessage <= 100) {
      update.xpPerMessage = xpPerMessage;
    }
    if (Number.isFinite(xpCooldownSeconds) && xpCooldownSeconds >= 0 && xpCooldownSeconds <= 86400) {
      update.xpCooldownSeconds = xpCooldownSeconds;
    }
    if (Number.isFinite(xpMinMessageChars) && xpMinMessageChars >= 0 && xpMinMessageChars <= 5000) {
      update.xpMinMessageChars = xpMinMessageChars;
    }
    update.xpAllowedChannelIds = xpAllowedChannelIds;

    await upsertSettings(update);
    res.redirect('/settings');
  } catch (error) {
    console.error('Save settings error:', error);
    res.status(500).send('Save settings error');
  }
});

app.get('/logs', requireLogin, async (req, res) => {
  try {
    const limit = parseInt(req.query?.limit, 10);
    const n = Number.isFinite(limit) ? limit : 150;
    const source = String(req.query?.source || 'all');
    const service = String(req.query?.service || '');
    const level = String(req.query?.level || '');
    const category = String(req.query?.category || '');

    const botLogs = source === 'telemetry' ? [] : await getRecentLogs(n);
    const telemetryLogs = source === 'bot' ? [] : await getRecentTelemetryEvents(
      {
        service: service || null,
        level: level || null,
        category: category || null,
      },
      n
    );

    const logs = [];
    for (const l of botLogs) {
      logs.push({
        createdAt: l.createdAt,
        service: 'bot',
        level: l.level,
        category: null,
        event: l.event,
        message: l.message,
        actorUserId: l.meta?.actorUserId || null,
        meta: l.meta || null,
      });
    }
    for (const l of telemetryLogs) {
      logs.push({
        createdAt: l.createdAt,
        service: l.service || 'unknown',
        level: l.level,
        category: l.category || null,
        event: l.event,
        message: l.message,
        actorUserId: l.actorUserId || null,
        meta: l.meta || null,
      });
    }

    logs.sort((a, b) => {
      const ta = a.createdAt ? new Date(a.createdAt).getTime() : 0;
      const tb = b.createdAt ? new Date(b.createdAt).getTime() : 0;
      return tb - ta;
    });

    const merged = logs.slice(0, n);
    const mongoOk = Boolean(botDb);

    const rows = merged
      .map((l) => {
        const ts = l.createdAt ? new Date(l.createdAt).toISOString() : '';
        const meta = l.meta ? JSON.stringify(l.meta) : '';
        return `<tr>
          <td style="white-space:nowrap; padding:8px; border-bottom:1px solid rgba(255,255,255,0.08);">${escapeHtml(ts)}</td>
          <td style="padding:8px; border-bottom:1px solid rgba(255,255,255,0.08);">${escapeHtml(l.service || '')}</td>
          <td style="padding:8px; border-bottom:1px solid rgba(255,255,255,0.08);">${escapeHtml(l.level || '')}</td>
          <td style="padding:8px; border-bottom:1px solid rgba(255,255,255,0.08);">${escapeHtml(l.category || '')}</td>
          <td style="padding:8px; border-bottom:1px solid rgba(255,255,255,0.08);">${escapeHtml(l.event || '')}</td>
          <td style="padding:8px; border-bottom:1px solid rgba(255,255,255,0.08);">${escapeHtml(l.message || '')}</td>
          <td style="padding:8px; border-bottom:1px solid rgba(255,255,255,0.08); max-width:320px; overflow:hidden; text-overflow:ellipsis;">${escapeHtml(meta)}</td>
        </tr>`;
      })
      .join('');

    res.send(
      htmlPage(
        'Logs',
        `<div class="card">
          ${nav(req)}
          <h1>Logs</h1>
          <p class="muted">Mongo: <strong>${mongoOk ? 'connected' : 'not configured'}</strong></p>
          <form method="GET" action="/logs" style="display:flex; gap:10px; flex-wrap:wrap; align-items:end; margin: 12px 0 16px 0;">
            <div>
              <label class="muted" for="source">Source</label><br/>
              <select id="source" name="source" style="padding:10px; border-radius:10px; border:1px solid rgba(255,255,255,0.12); background: rgba(0,0,0,0.25); color: #e6e9f2;">
                <option value="all" ${source === 'all' ? 'selected' : ''}>All</option>
                <option value="telemetry" ${source === 'telemetry' ? 'selected' : ''}>Telemetry</option>
                <option value="bot" ${source === 'bot' ? 'selected' : ''}>Bot</option>
              </select>
            </div>
            <div>
              <label class="muted" for="service">Service</label><br/>
              <select id="service" name="service" style="padding:10px; border-radius:10px; border:1px solid rgba(255,255,255,0.12); background: rgba(0,0,0,0.25); color: #e6e9f2;">
                <option value="" ${!service ? 'selected' : ''}>All</option>
                <option value="game" ${service === 'game' ? 'selected' : ''}>game</option>
                <option value="map" ${service === 'map' ? 'selected' : ''}>map</option>
                <option value="bot" ${service === 'bot' ? 'selected' : ''}>bot</option>
              </select>
            </div>
            <div>
              <label class="muted" for="level">Level</label><br/>
              <select id="level" name="level" style="padding:10px; border-radius:10px; border:1px solid rgba(255,255,255,0.12); background: rgba(0,0,0,0.25); color: #e6e9f2;">
                <option value="" ${!level ? 'selected' : ''}>All</option>
                <option value="info" ${level === 'info' ? 'selected' : ''}>info</option>
                <option value="warn" ${level === 'warn' ? 'selected' : ''}>warn</option>
                <option value="error" ${level === 'error' ? 'selected' : ''}>error</option>
                <option value="security" ${level === 'security' ? 'selected' : ''}>security</option>
              </select>
            </div>
            <div>
              <label class="muted" for="category">Category</label><br/>
              <input id="category" name="category" value="${escapeHtml(category)}" placeholder="e.g. pins" style="padding:10px; border-radius:10px; border:1px solid rgba(255,255,255,0.12); background: rgba(0,0,0,0.25); color: #e6e9f2;" />
            </div>
            <div>
              <label class="muted" for="limit">Limit</label><br/>
              <input id="limit" name="limit" inputmode="numeric" value="${String(n)}" style="width:120px; padding:10px; border-radius:10px; border:1px solid rgba(255,255,255,0.12); background: rgba(0,0,0,0.25); color: #e6e9f2;" />
            </div>
            <button class="btn" type="submit">Apply</button>
          </form>
          <p class="muted">Showing latest ${merged.length} events.</p>
          <div style="overflow:auto;">
            <table style="width:100%; border-collapse:collapse; font-size: 13px;">
              <thead>
                <tr>
                  <th style="text-align:left; padding:8px; border-bottom:1px solid rgba(255,255,255,0.12);">Time</th>
                  <th style="text-align:left; padding:8px; border-bottom:1px solid rgba(255,255,255,0.12);">Service</th>
                  <th style="text-align:left; padding:8px; border-bottom:1px solid rgba(255,255,255,0.12);">Level</th>
                  <th style="text-align:left; padding:8px; border-bottom:1px solid rgba(255,255,255,0.12);">Category</th>
                  <th style="text-align:left; padding:8px; border-bottom:1px solid rgba(255,255,255,0.12);">Event</th>
                  <th style="text-align:left; padding:8px; border-bottom:1px solid rgba(255,255,255,0.12);">Message</th>
                  <th style="text-align:left; padding:8px; border-bottom:1px solid rgba(255,255,255,0.12);">Meta</th>
                </tr>
              </thead>
              <tbody>
                ${rows || '<tr><td colspan="7" class="muted" style="padding:8px;">No logs yet.</td></tr>'}
              </tbody>
            </table>
          </div>
        </div>`
      )
    );
  } catch (error) {
    console.error('Logs page error:', error);
    res.status(500).send('Logs error');
  }
});

app.get('/health', (req, res) => {
  res.json({ ok: true });
});

app.listen(PORT, () => {
  console.log(`🛠️ Bot dashboard listening on port ${PORT}`);
});

initMongo()
  .then(() => {
    startServiceHealthMonitor();
  })
  .catch((e) => {
    console.error('Mongo init failed:', e);
    startServiceHealthMonitor();
  });
