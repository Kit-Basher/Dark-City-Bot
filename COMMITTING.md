# Committing / pushing (dark-city-bot)

This repo contains the **Discord bot** and a small **dashboard** under `dashboard/`.

## Quick decision map (what did you change?)

Commit in **this repo** if you changed:

- `index.js` (bot behavior)
- `dark_city_aspects.md` (aspects source of truth)
- `dashboard/*` (dashboard web app)

Do **not** commit here if you changed:

- `../dark-city-game/*` (game hub / server)
- `../dark-city-map-web/*` (map viewer service)

## Common workflows

### Updating aspects

- Edit: `dark_city_aspects.md`
- Commit: here
- Then in Discord: re-run the bot command that posts/updates the aspects menus.

### Updating moderation commands

- Edit: `index.js`
- Commit: here

## Push checklist

- Run `./dc-preflight.sh` to confirm repo/branch/remote.
- Make sure you’re inside the `dark-city-bot/` repo (look for `dark-city-bot/.git/`).
- Confirm required env vars are set in your deployment:
  - `DISCORD_BOT_TOKEN`
  - `DISCORD_APPLICATION_ID`
  - `DISCORD_GUILD_ID`
