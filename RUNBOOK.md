# Runbook (Render) — dark-city-bot

This repo backs two Render services:

- **Dark-City-Bot**: the Discord bot process (`index.js`)
- **Dark-City-Bot dashboard**: a small web dashboard (`dashboard/index.js`)

## Deploy model

- **Provider**: Render
- **Deploy trigger**: Render auto-deploy from GitHub on push to `main` (typical)

## Service 1: Bot (`index.js`)

### Start command

- `npm start`

### Required environment variables

- `DISCORD_BOT_TOKEN`
- `DISCORD_APPLICATION_ID`
- `DISCORD_GUILD_ID`

Optional:

- `MODERATOR_ROLE_ID`
- `ASPECTS_CHANNEL_ID`
- `MONGODB_URI` (enables settings/log persistence)
- `BOT_DB_NAME` (default: `dark_city_bot`)

Optional integration with game server:

- `DARK_CITY_API_BASE_URL`
- `DARK_CITY_MODERATOR_PASSWORD`

### What to check when broken

- In Render logs: look for “Logged in as …”
- If commands don’t appear in Discord:
  - verify `DISCORD_APPLICATION_ID` and `DISCORD_GUILD_ID`
  - confirm the bot has correct permissions in the server

## Service 2: Dashboard (`dashboard/index.js`)

### Start command

- From the `dashboard/` folder: `npm start`

### Required environment variables

- `DISCORD_CLIENT_ID`
- `DISCORD_CLIENT_SECRET`
- `DISCORD_REDIRECT_URI`
- `DISCORD_GUILD_ID`
- `DASHBOARD_ALLOWED_ROLE_ID`
- `SESSION_SECRET`

Optional:

- `MONGODB_URI` / `BOT_DB_NAME` (enables Settings/Logs pages)

Optional integration with game server:

- `DARK_CITY_API_BASE_URL`
- `DARK_CITY_MODERATOR_PASSWORD`

### What to check when broken

- OAuth errors:
  - verify `DISCORD_REDIRECT_URI` matches the Discord app config exactly.
- 403 after login:
  - user is missing `DASHBOARD_ALLOWED_ROLE_ID` in the guild.

## MongoDB notes

- Both bot + dashboard can use MongoDB for persisted settings/logs.
- If Mongo is not configured, the bot still runs but settings/log pages may be unavailable.

## Change checklist

- If you change slash command registration behavior, expect it to take a moment to propagate.
- After aspects changes, you may need to repost menus in Discord.
