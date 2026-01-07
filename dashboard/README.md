# dark-city-bot dashboard

This folder contains a small Express dashboard for the Discord bot.

## Run locally

From this folder:

```bash
npm install
cp .env.example .env
npm start
```

Then open:

- http://localhost:3002

## Notes

- This is its own Node project (separate `package.json`) nested inside the `dark-city-bot` repo.
- If the dashboard needs environment variables, check `dashboard/index.js` and the parent bot README.
