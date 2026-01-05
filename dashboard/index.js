const crypto = require('crypto');
const express = require('express');
const session = require('express-session');
const { MongoClient } = require('mongodb');

function requireEnv(name) {
  const value = process.env[name];
  if (!value) throw new Error(`Missing required environment variable: ${name}`);
  return value;
}

function nav(req) {
  if (!req.session?.user?.allowed) return '';
  return `<div style="display:flex; gap:12px; margin-bottom:14px; align-items:center;">
    <a href="/dashboard">Dashboard</a>
    <a href="/settings">Settings</a>
    <a href="/logs">Logs</a>
    <span style="flex:1"></span>
    <a href="/logout">Log out</a>
  </div>`;
}

const PORT = parseInt(process.env.PORT || '3000', 10);

const DISCORD_CLIENT_ID = requireEnv('DISCORD_CLIENT_ID');
const DISCORD_CLIENT_SECRET = requireEnv('DISCORD_CLIENT_SECRET');
const DISCORD_REDIRECT_URI = requireEnv('DISCORD_REDIRECT_URI');
const DISCORD_GUILD_ID = requireEnv('DISCORD_GUILD_ID');
const DASHBOARD_ALLOWED_ROLE_ID = requireEnv('DASHBOARD_ALLOWED_ROLE_ID');
const SESSION_SECRET = requireEnv('SESSION_SECRET');

const MONGODB_URI = process.env.MONGODB_URI;
const BOT_DB_NAME = process.env.BOT_DB_NAME || 'dark_city_bot';

const app = express();

app.set('trust proxy', 1);

app.use(express.urlencoded({ extended: false }));

app.use(
  session({
    secret: SESSION_SECRET,
    resave: false,
    saveUninitialized: false,
    cookie: {
      httpOnly: true,
      sameSite: 'lax',
      secure: process.env.NODE_ENV === 'production',
    },
  })
);

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

function htmlPage(title, body) {
  return `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>${title}</title>
  <style>
    body { font-family: system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif; margin: 0; background: #0b0f19; color: #e6e9f2; }
    .wrap { max-width: 900px; margin: 0 auto; padding: 32px 20px; }
    .card { background: rgba(255,255,255,0.06); border: 1px solid rgba(255,255,255,0.12); border-radius: 12px; padding: 18px; }
    a { color: #8ab4ff; text-decoration: none; }
    a:hover { text-decoration: underline; }
    .btn { display: inline-block; background: #3b82f6; color: white; padding: 10px 14px; border-radius: 10px; font-weight: 600; }
    .muted { color: rgba(230,233,242,0.7); }
    code { background: rgba(0,0,0,0.35); padding: 2px 6px; border-radius: 6px; }
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
        <p class="muted">You must sign in with Discord as a Moderator.</p>
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
    const allowed = roles.includes(DASHBOARD_ALLOWED_ROLE_ID);

    req.session.user = {
      id: user?.id,
      username: user?.username,
      global_name: user?.global_name,
      allowed,
      roles,
    };

    if (!allowed) {
      return res.status(403).send(
        htmlPage(
          'Access denied',
          `<div class="card">
            <h1>Access denied</h1>
            <p class="muted">Your Discord account is not allowed to access this dashboard.</p>
            <p class="muted">Required role id: <code>${DASHBOARD_ALLOWED_ROLE_ID}</code></p>
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
  res.send(
    htmlPage(
      'Dashboard',
      `<div class="card">
        ${nav(req)}
        <h1>Dashboard</h1>
        <p class="muted">Signed in as <strong>${displayName}</strong></p>
        <p class="muted">Use the links above to manage settings and view bot logs.</p>
      </div>`
    )
  );
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

    const inviteAutoDeleteEnabled = req.body?.inviteAutoDeleteEnabled === 'on';
    const inviteWarnEnabled = req.body?.inviteWarnEnabled === 'on';
    const lowTrustLinkFilterEnabled = req.body?.lowTrustLinkFilterEnabled === 'on';
    const lowTrustWarnDmEnabled = req.body?.lowTrustWarnDmEnabled === 'on';

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
    const logs = await getRecentLogs(Number.isFinite(limit) ? limit : 100);
    const mongoOk = Boolean(botDb);

    const rows = logs
      .map((l) => {
        const ts = l.createdAt ? new Date(l.createdAt).toISOString() : '';
        const meta = l.meta ? JSON.stringify(l.meta) : '';
        return `<tr>
          <td style="white-space:nowrap; padding:8px; border-bottom:1px solid rgba(255,255,255,0.08);">${ts}</td>
          <td style="padding:8px; border-bottom:1px solid rgba(255,255,255,0.08);">${l.level || ''}</td>
          <td style="padding:8px; border-bottom:1px solid rgba(255,255,255,0.08);">${l.event || ''}</td>
          <td style="padding:8px; border-bottom:1px solid rgba(255,255,255,0.08);">${l.message || ''}</td>
          <td style="padding:8px; border-bottom:1px solid rgba(255,255,255,0.08); max-width:320px; overflow:hidden; text-overflow:ellipsis;">${meta}</td>
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
          <p class="muted">Showing latest ${logs.length} events.</p>
          <div style="overflow:auto;">
            <table style="width:100%; border-collapse:collapse; font-size: 13px;">
              <thead>
                <tr>
                  <th style="text-align:left; padding:8px; border-bottom:1px solid rgba(255,255,255,0.12);">Time</th>
                  <th style="text-align:left; padding:8px; border-bottom:1px solid rgba(255,255,255,0.12);">Level</th>
                  <th style="text-align:left; padding:8px; border-bottom:1px solid rgba(255,255,255,0.12);">Event</th>
                  <th style="text-align:left; padding:8px; border-bottom:1px solid rgba(255,255,255,0.12);">Message</th>
                  <th style="text-align:left; padding:8px; border-bottom:1px solid rgba(255,255,255,0.12);">Meta</th>
                </tr>
              </thead>
              <tbody>
                ${rows || '<tr><td colspan="5" class="muted" style="padding:8px;">No logs yet.</td></tr>'}
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

initMongo().catch((e) => {
  console.error('Mongo init failed:', e);
});
