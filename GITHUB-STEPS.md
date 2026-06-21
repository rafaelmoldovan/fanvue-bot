# Jázmin Fanvue Bot — GitHub → Railway deploy (step by step)

Everything you need is in this folder: **C:\Users\rafae\jazmin-deploy**

| File | What it is |
|------|-----------|
| `app.py` | The Fanvue bot (updated: AI-deny + Telegram cross-link + PPV) |
| `dashboard.html` | Your control dashboard UI |
| `requirements.txt` | Python deps (flask, requests, anthropic, gunicorn) |
| `Procfile` | Tells Railway how to run it |

---

## STEP 1 — Put these 4 files in your GitHub repo
Easiest way (GitHub website):
1. Go to your bot repo on github.com
2. For each of the 4 files: **Add file → Upload files** → drag it in → **Commit changes**
3. Make sure the old `app.py` gets **replaced** by this new one (same filename)

Or with git in a terminal (if your repo is cloned locally):
```
copy C:\Users\rafae\jazmin-deploy\*  <your-repo-folder>
cd <your-repo-folder>
git add app.py dashboard.html requirements.txt Procfile
git commit -m "v12: AI-deny + Telegram cross-link"
git push
```

## STEP 2 — Railway auto-deploys
If your Railway project is connected to the repo, the push **auto-deploys**. Watch the
Deploy logs for `BOT v11 BOOTED` and `[OK] polling started`.

## STEP 3 — Environment variables (Railway → your service → Variables)
Make sure these are set (most already are from before):
```
ANTHROPIC_API_KEY      = sk-ant-...
FANVUE_CLIENT_ID       = ...
FANVUE_CLIENT_SECRET   = ...
FANVUE_REFRESH_TOKEN   = ...
MY_UUID                = 38a392fc-a751-49b3-9d74-01ac6447c490
DASHBOARD_PASSWORD     = (your choice)
DB_PATH                = /data/bot_data.db
TELEGRAM_BOT_TOKEN     = (optional, for error pings)
TELEGRAM_CHAT_ID       = (optional)
```
And attach a **Volume** mounted at `/data` (so the database survives restarts).

## STEP 4 — Open the dashboard
`https://<your-railway-url>/dashboard?pw=<DASHBOARD_PASSWORD>`

---

## What's new in this version
- **AI / robot deny:** if a fan calls her AI/bot/robot → 1st time she gets mad, 2nd time she
  drops the line *"Persze, gyere be Pestre és fogod látni ahogy a körúton sétálok mint egy robot..."*
- **Telegram cross-link:** if a fan says *"I came from Telegram"* → she asks their Telegram
  username → then she **remembers the whole Telegram conversation** and references it.
- **PPV upsell:** when you send locked content, flag it (see below) → she teases the unlock.

---

## ⚠️ Telegram cross-link — important
For "she remembers the Telegram chat" to work, **both bots must use the SAME database.**
- The Telegram bot (`jazmin_tg.py`) currently runs on your PC with its own `bot_data.db`.
- `app.py` on Railway uses `/data/bot_data.db` — a DIFFERENT database.

So the cross-link only has data if they share one DB. Two ways:
1. **Simplest (local test):** run `app.py` on your PC too, pointed at the same `bot_data.db`
   the Telegram bot uses. Then the cross-link works immediately.
2. **Full cloud:** also deploy the Telegram bot to Railway on the same `/data` volume
   (needs the `jazmin_session.session` file uploaded). Ask me and I'll set this up.

---

## Testing the PPV / purchased-content upsell
The bot does **not** send locked content itself (you do that on Fanvue). It **upsells** it.
To test:
1. On Fanvue, send a fan some locked/PPV content manually.
2. Tell the bot it's pending — open this URL (or the dashboard PPV button):
   `https://<your-railway-url>/ppv_sent/<CHAT_ID>?key=<DASHBOARD_PASSWORD>`
3. On the fan's next message, she'll tease them to unlock it — once, naturally.

**Note:** auto-detection of PPV (without the button) depends on Fanvue's exact payload — send
me one real locked-message example from the API and I'll lock the auto-detect to it.
