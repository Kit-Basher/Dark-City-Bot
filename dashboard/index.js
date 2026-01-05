const crypto = require('crypto');
const express = require('express');
const session = require('express-session');

function requireEnv(name) {
  const value = process.env[name];
  if (!value) throw new Error(`Missing required environment variable: ${name}`);
  return value;
}

const PORT = parseInt(process.env.PORT || '3000', 10);

const DISCORD_CLIENT_ID = requireEnv('DISCORD_CLIENT_ID');
const DISCORD_CLIENT_SECRET = requireEnv('DISCORD_CLIENT_SECRET');
const DISCORD_REDIRECT_URI = requireEnv('DISCORD_REDIRECT_URI');
const DISCORD_GUILD_ID = requireEnv('DISCORD_GUILD_ID');
const DASHBOARD_ALLOWED_ROLE_ID = requireEnv('DASHBOARD_ALLOWED_ROLE_ID');
const SESSION_SECRET = requireEnv('SESSION_SECRET');

const app = express();

app.set('trust proxy', 1);

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
        <h1>Dashboard</h1>
        <p class="muted">Signed in as <strong>${displayName}</strong></p>
        <p class="muted">This is the starter dashboard. Next we’ll add settings + logs here.</p>
        <p><a href="/logout">Log out</a></p>
      </div>`
    )
  );
});

app.get('/health', (req, res) => {
  res.json({ ok: true });
});

app.listen(PORT, () => {
  console.log(`🛠️ Bot dashboard listening on port ${PORT}`);
});
