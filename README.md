# dark-city-bot

Discord bot for the Dark City game community.

## What it does

- **Slash commands** (example: `/r` for 2d6)
- **Moderation utilities** (purge, timeout, lock/unlock, slowmode)
- **Aspects system**: posts role menus in a dedicated channel and keeps roles in sync with `dark_city_aspects.md`
- **Optional persistence**: if `MONGODB_URI` is set, settings and bot logs are stored in MongoDB

## Repo boundary

This is a **separate Git repo** from the rest of the Dark City workspace.

- If you changed files under `dark-city-bot/…`, commit/push from this repo.

## Ops

- Render operations / troubleshooting: `RUNBOOK.md`

## Requirements

- Node `22.x` (see `package.json`)

If you use `nvm`, this repo includes:

- `.nvmrc`
- `.node-version`

## Run locally

```bash
nvm use 22 || true
npm install
cp .env.example .env
export DISCORD_BOT_TOKEN="..."
export DISCORD_APPLICATION_ID="..."
export DISCORD_GUILD_ID="..."
npm start
```

## Environment variables

Required:

- `DISCORD_BOT_TOKEN`
- `DISCORD_APPLICATION_ID`
- `DISCORD_GUILD_ID`

Optional:

- `MODERATOR_ROLE_ID` (or `DASHBOARD_ALLOWED_ROLE_ID`)
- `ASPECTS_CHANNEL_ID`
- `MONGODB_URI` (enables persisted settings/logs; dashboard sessions are also stored in MongoDB when set)
- `BOT_DB_NAME` (default: `dark_city_bot`)

Optional integration with the game server:

- `DARK_CITY_API_BASE_URL`
- `DARK_CITY_MODERATOR_PASSWORD`

## Aspects content

The canonical list of aspects is stored in:

- `dark_city_aspects.md`

If you update aspect names/categories, you typically need to re-run the posting command in Discord.

## Dashboard

There is a small dashboard app under `dashboard/` with its own `package.json` and `npm start`.