from flask import Flask, request
import requests
import os
import json
import base64
import sqlite3
import threading
import time
import telebot
import random
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

# ========== TIMEZONE SETUP ==========
BUDAPEST_TZ = ZoneInfo('Europe/Budapest')

# ========== BOOT WATERMARK — CRITICAL: only process messages AFTER this time ==========
# Fanvue timestamps are in UTC, so we use UTC for comparison
BOOT_TIME_UTC = datetime.now(timezone.utc)
print(f"[{datetime.now()}] BOT BOOTED at {BOOT_TIME_UTC.isoformat()} UTC — only replies to messages AFTER this time")

def get_budapest_now():
    return datetime.now(BUDAPEST_TZ).replace(tzinfo=None)

def to_budapest(dt):
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(BUDAPEST_TZ).replace(tzinfo=None)

# ========== APP SETUP ==========
app = Flask(__name__)

# ========== CONFIG ==========
FANVUE_CLIENT_ID = os.environ.get('FANVUE_CLIENT_ID', '')
FANVUE_CLIENT_SECRET = os.environ.get('FANVUE_CLIENT_SECRET', '')
OPENAI_API_KEY = os.environ.get('OPENAI_API_KEY', '')
CREATOR_NAME = os.environ.get('CREATOR_NAME', 'jazmin07')
MY_UUID = os.environ.get('MY_UUID', '38a392fc-a751-49b3-9d74-01ac6447c490')
TELEGRAM_BOT_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN', '')
TELEGRAM_CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID', '')

# ========== TELEGRAM BOT SETUP ==========
bot = None
if TELEGRAM_BOT_TOKEN:
    bot = telebot.TeleBot(TELEGRAM_BOT_TOKEN)

def send_telegram_message(text):
    if not bot or not TELEGRAM_CHAT_ID:
        return
    try:
        bot.send_message(TELEGRAM_CHAT_ID, text[:4000])
    except Exception as e:
        print(f"[WARN] Telegram send failed: {e}")

# ========== TELEGRAM COMMANDS ==========

def is_admin(message):
    return str(message.chat.id) == str(TELEGRAM_CHAT_ID)

@bot.message_handler(commands=['start'])
def cmd_start(message):
    bot.reply_to(message, "🤖 Jazmin Bot Console\n\n/status — Active fans\n/pause <uuid> — Pause fan\n/resume <uuid> — Resume fan\n/safe_on — Global safe mode ON\n/safe_off — Global safe mode OFF\n/toggle_safe_mode <uuid> — Toggle per-fan safe mode")

@bot.message_handler(commands=['status'])
def cmd_status(message):
    if not is_admin(message):
        return
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("SELECT chat_id, fan_name, is_paused, fan_type FROM fan_profiles ORDER BY last_interaction DESC LIMIT 10")
        rows = c.fetchall()
        conn.close()
        lines = ["📊 Recent Fans:"]
        for r in rows:
            status = "⏸️ PAUSED" if r[2] else "✅ Active"
            lines.append(f"`{r[0][:8]}...` | {r[1] or '?'} | {status} | {r[3]}")
        bot.reply_to(message, "\n".join(lines), parse_mode='Markdown')
    except Exception as e:
        bot.reply_to(message, f"Error: {e}")

@bot.message_handler(commands=['pause'])
def cmd_pause(message):
    if not is_admin(message):
        return
    try:
        uuid = message.text.split()[1].strip()
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("UPDATE fan_profiles SET is_paused=1, paused_until=? WHERE chat_id=?", (None, uuid))
        conn.commit()
        conn.close()
        bot.reply_to(message, f"⏸️ Paused replies for `{uuid[:12]}...`", parse_mode='Markdown')
    except IndexError:
        bot.reply_to(message, "Usage: /pause <fan_uuid>")
    except Exception as e:
        bot.reply_to(message, f"Error: {e}")

@bot.message_handler(commands=['resume'])
def cmd_resume(message):
    if not is_admin(message):
        return
    try:
        uuid = message.text.split()[1].strip()
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("UPDATE fan_profiles SET is_paused=0, paused_until=NULL WHERE chat_id=?", (uuid,))
        conn.commit()
        conn.close()
        bot.reply_to(message, f"▶️ Resumed replies for `{uuid[:12]}...`", parse_mode='Markdown')
    except IndexError:
        bot.reply_to(message, "Usage: /resume <fan_uuid>")
    except Exception as e:
        bot.reply_to(message, f"Error: {e}")

@bot.message_handler(commands=['safe_on'])
def cmd_safe_on(message):
    if not is_admin(message):
        return
    global SAFE_MODE
    SAFE_MODE = True
    bot.reply_to(message, "🔒 SAFE MODE: ON\nBot will NOT auto-reply. Manual only.")

@bot.message_handler(commands=['safe_off'])
def cmd_safe_off(message):
    if not is_admin(message):
        return
    global SAFE_MODE
    SAFE_MODE = False
    bot.reply_to(message, "🔓 SAFE MODE: OFF\nBot will auto-reply normally.")

@bot.message_handler(commands=['toggle_safe_mode'])
def cmd_toggle_safe(message):
    if not is_admin(message):
        return
    try:
        uuid = message.text.split()[1].strip()
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("SELECT is_paused FROM fan_profiles WHERE chat_id=?", (uuid,))
        row = c.fetchone()
        if row:
            new_state = 0 if row[0] else 1
            c.execute("UPDATE fan_profiles SET is_paused=? WHERE chat_id=?", (new_state, uuid))
            conn.commit()
            status = "PAUSED" if new_state else "ACTIVE"
            bot.reply_to(message, f"{'⏸️' if new_state else '▶️'} Fan `{uuid[:12]}...` is now {status}", parse_mode='Markdown')
        else:
            bot.reply_to(message, f"Fan `{uuid[:12]}...` not found.", parse_mode='Markdown')
        conn.close()
    except IndexError:
        bot.reply_to(message, "Usage: /toggle_safe_mode <fan_uuid>")
    except Exception as e:
        bot.reply_to(message, f"Error: {e}")

SAFE_MODE = True

POLL_INTERVAL = 20
SHORT_DELAY = 30
LONG_DELAY = 90

# ========== SQLITE SETUP ==========
DB_PATH = 'bot_data.db'

def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS tokens (key TEXT PRIMARY KEY, value TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS messages (
        msg_id TEXT PRIMARY KEY, chat_id TEXT, fan_name TEXT, sender_uuid TEXT,
        text TEXT, timestamp TEXT, was_replied INTEGER DEFAULT 0,
        reply_text TEXT, bot_replied_at TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS fan_profiles (
        chat_id TEXT PRIMARY KEY, fan_name TEXT, handle TEXT,
        total_messages INTEGER DEFAULT 0, fan_type TEXT DEFAULT 'new',
        last_interaction TEXT, last_reply_time TEXT,
        content_ask_count INTEGER DEFAULT 0, meetup_ask_count INTEGER DEFAULT 0,
        lifetime_spend REAL DEFAULT 0, fan_notes TEXT DEFAULT '')''')
    c.execute('''CREATE TABLE IF NOT EXISTS scheduled_replies (
        id INTEGER PRIMARY KEY AUTOINCREMENT, chat_id TEXT, fan_name TEXT,
        fan_msg_id TEXT, fan_text TEXT, scheduled_time TEXT, reply_text TEXT,
        status TEXT DEFAULT 'pending', created_at TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS bot_settings (
        key TEXT PRIMARY KEY, value TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS blocked_fans (
            try:
        c.execute("ALTER TABLE fan_profiles ADD COLUMN is_paused INTEGER DEFAULT 0")
    except:
        pass
    try:
        c.execute("ALTER TABLE fan_profiles ADD COLUMN paused_until TEXT")
    except:
        pass
    conn.close()
# ========== TELEGRAM WEBHOOK SETUP ==========

def setup_telegram():
    if not bot:
        print("[WARN] No TELEGRAM_BOT_TOKEN — Telegram disabled")
        return
    
    # Force HTTPS and strip any trailing spaces
    domain = os.environ.get('RAILWAY_PUBLIC_DOMAIN', '').strip()
    if domain:
        webhook_url = f"https://{domain}/telegram_webhook"
    else:
        raw = os.environ.get('WEBHOOK_URL', '').strip().replace('http://', 'https://')
        webhook_url = raw if raw.endswith('/telegram_webhook') else raw.rstrip('/') + '/telegram_webhook'
    
    try:
        bot.remove_webhook()
        time.sleep(0.5)
        result = bot.set_webhook(url=webhook_url)
        if result:
            print(f"[OK] Telegram webhook set: {webhook_url}")
            send_telegram_message("🤖 Bot started. Commands: /status /pause /resume /safe_on /safe_off /toggle_safe_mode")
        else:
            print(f"[WARN] Webhook returned false. Starting polling fallback...")
            start_polling()
    except Exception as e:
        print(f"[ERROR] Webhook failed: {e}. Starting polling...")
        start_polling()

def start_polling():
    def poll():
        bot.remove_webhook()
        bot.polling(none_stop=True, interval=1, timeout=30)
    t = threading.Thread(target=poll, daemon=True)
    t.start()
    print("[OK] Telegram polling started (fallback)")

# ========== FLASK ROUTES ==========

@app.route('/telegram_webhook', methods=['POST'])
def telegram_webhook():
    if request.headers.get('content-type') == 'application/json':
        json_string = request.get_data().decode('utf-8')
        update = telebot.types.Update.de_json(json_string)
        bot.process_new_updates([update])
        return '', 200
    return 'Forbidden', 403

@app.route('/telegram_webhook', methods=['GET'])
def telegram_webhook_test():
    return '✅ Telegram webhook endpoint active. POST only for updates.', 200
    conn.commit()
    conn.close()

def db_query(query, params=(), fetch_one=False):
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    c.execute(query, params)
    if query.strip().upper().startswith('SELECT'):
        if fetch_one:
            row = c.fetchone()
            result = dict(row) if row else None
        else:
            result = [dict(row) for row in c.fetchall()]
    else:
        conn.commit()
        result = None
    conn.close()
    return result

# ========== BOT SETTINGS (DB-backed toggles) ==========
def get_safe_mode():
    row = db_query("SELECT value FROM bot_settings WHERE key = 'safe_mode'", fetch_one=True)
    if row and row.get('value') is not None:
        return row['value'] == 'true'
    return SAFE_MODE

def set_safe_mode(value):
    db_query("INSERT OR REPLACE INTO bot_settings (key, value) VALUES ('safe_mode', ?)", ('true' if value else 'false',))

# ========== BLOCKLIST ==========
def is_blocked(chat_id):
    row = db_query("SELECT 1 FROM blocked_fans WHERE chat_id = ?", (chat_id,), fetch_one=True)
    return bool(row)

def block_fan(chat_id, fan_name, reason=""):
    db_query("INSERT OR REPLACE INTO blocked_fans (chat_id, fan_name, blocked_at, reason) VALUES (?, ?, ?, ?)",
             (chat_id, fan_name, datetime.now().isoformat(), reason))

def unblock_fan(chat_id):
    db_query("DELETE FROM blocked_fans WHERE chat_id = ?", (chat_id,))

def get_blocked_fans():
    return db_query("SELECT * FROM blocked_fans ORDER BY blocked_at DESC") or []

# ========== PAUSE / RESUME ==========
def is_paused(chat_id):
    profile = db_query("SELECT is_paused, paused_until FROM fan_profiles WHERE chat_id = ?", (chat_id,), fetch_one=True)
    if not profile:
        return False
    if profile.get('is_paused'):
        return True
    until = profile.get('paused_until')
    if until:
        try:
            if datetime.now().isoformat() < until:
                return True
            else:
                # Auto-resume if time expired
                db_query("UPDATE fan_profiles SET paused_until = NULL WHERE chat_id = ?", (chat_id,))
        except:
            pass
    return False

def pause_fan(chat_id, minutes=0):
    until = None
    if minutes > 0:
        until = (datetime.now() + timedelta(minutes=minutes)).isoformat()
    db_query("UPDATE fan_profiles SET is_paused = ?, paused_until = ? WHERE chat_id = ?",
             (1 if minutes == 0 else 0, until, chat_id))

def resume_fan(chat_id):
    db_query("UPDATE fan_profiles SET is_paused = 0, paused_until = NULL WHERE chat_id = ?", (chat_id,))

def get_paused_fans():
    return db_query("SELECT chat_id, fan_name, is_paused, paused_until FROM fan_profiles WHERE is_paused = 1 OR paused_until IS NOT NULL") or []

# ========== TOKEN MANAGEMENT ==========
def save_token(key, value):
    db_query('INSERT OR REPLACE INTO tokens (key, value) VALUES (?, ?)', (key, value))

def load_token(key):
    row = db_query('SELECT value FROM tokens WHERE key = ?', (key,), fetch_one=True)
    return row['value'] if row else None

def get_basic_auth_header():
    credentials = f"{FANVUE_CLIENT_ID}:{FANVUE_CLIENT_SECRET}"
    encoded = base64.b64encode(credentials.encode('utf-8')).decode('utf-8')
    return f"Basic {encoded}"

def refresh_fanvue_token():
    refresh_token = load_token('refresh_token')
    if not refresh_token:
        return None, "No refresh token in DB"
    try:
        r = requests.post("https://auth.fanvue.com/oauth2/token",
            data={"grant_type": "refresh_token", "refresh_token": refresh_token},
            headers={"Content-Type": "application/x-www-form-urlencoded", "Authorization": get_basic_auth_header()},
            timeout=15)
        if r.status_code == 200:
            data = r.json()
            access_token = data.get('access_token')
            new_refresh = data.get('refresh_token', refresh_token)
            expires_in = data.get('expires_in', 3600)
            expires_at = (datetime.now() + timedelta(seconds=expires_in - 300)).isoformat()
            save_token('refresh_token', new_refresh)
            save_token('access_token', access_token)
            save_token('expires_at', expires_at)
            return access_token, "Token refreshed successfully"
        else:
            return None, f"Refresh failed: {r.status_code} - {r.text[:200]}"
    except Exception as e:
        return None, f"Refresh exception: {str(e)}"

def get_fanvue_token():
    access_token = load_token('access_token')
    expires_at = load_token('expires_at')
    if access_token and expires_at:
        try:
            if datetime.now() < datetime.fromisoformat(expires_at):
                return access_token
        except:
            pass
    return refresh_fanvue_token()[0]

def send_telegram(text, parse_mode='HTML'):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return False
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        r = requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": text[:4000], "parse_mode": parse_mode}, timeout=10)
        return r.status_code == 200
    except Exception as e:
        print(f"Telegram error: {e}")
        return False

def send_telegram_reply(text, reply_to_message_id=None):
    """Send a reply to Telegram chat (optionally as reply to specific message)"""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return False
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text[:4000], "parse_mode": "HTML"}
        if reply_to_message_id:
            payload["reply_to_message_id"] = reply_to_message_id
        r = requests.post(url, json=payload, timeout=10)
        return r.status_code == 200
    except Exception as e:
        print(f"Telegram reply error: {e}")
        return False

# ========== TELEGRAM CONSOLE COMMANDS ==========

def telegram_console_help():
    return """🎮 <b>Jazmin Bot — Telegram Console</b>

<b>Fan Control</b>
/pause &lt;chat_id&gt; [minutes] — Pause fan (default 60min)
/resume &lt;chat_id&gt; — Resume fan
/block &lt;chat_id&gt; [reason] — Block fan
/unblock &lt;chat_id&gt; — Unblock fan

<b>Bot Control</b>
/safe_mode — Toggle SAFE_MODE on/off
/trigger — Manually trigger a poll cycle

<b>Info</b>
/status — Bot status overview
/fans — List recent fans with stages
/help — Show this message

💡 <b>Every preview shows the chat_id.</b> Just copy it from any message!"""

def process_telegram_command(text):
    """Process a command sent via Telegram. Returns response text or None."""
    parts = text.strip().split()
    if not parts:
        return None
    cmd = parts[0].lower()
    args = parts[1:]

    if cmd == '/help':
        return telegram_console_help()

    if cmd == '/status':
        safe = get_safe_mode()
        token_ok = get_fanvue_token() is not None
        return f"""📊 <b>Bot Status</b>
SAFE_MODE: {'🔒 ON (Telegram preview)' if safe else '🔓 OFF (Live to Fanvue)'}
Token: {'✅ Valid' if token_ok else '❌ Missing/Expired'}
Polling: {'▶️ Active' if polling_active else '⏸️ Stopped'}
Boot: {BOOT_TIME_UTC.strftime('%Y-%m-%d %H:%M')} UTC
Blocked fans: {len(get_blocked_fans())}
Paused fans: {len(get_paused_fans())}"""

    if cmd == '/fans':
        try:
            chats, _ = get_chats()
            if not chats:
                return "👥 No active chats found."
            lines = ["👥 <b>Recent Fans</b>\n"]
            for chat in chats[:15]:
                user = chat.get('user', {}) or {}
                cid = user.get('uuid') or chat.get('uuid') or chat.get('id', '?')
                if not cid or cid == '?':
                    continue
                name = user.get('displayName', 'unknown')
                handle = user.get('handle', '') or 'no_handle'
                profile = get_or_create_fan_profile(cid, name, handle, user.get('isTopSpender', False))
                stage = get_fan_stage(profile)
                stage_label = get_stage_label(stage)
                paused = " ⏸️" if is_paused(cid) else ""
                blocked = " 🚫" if is_blocked(cid) else ""
                lines.append(f"{stage_label}{paused}{blocked}\n👤 {name} (@{handle})\n🔗 <code>{cid}</code>\n")
            return "\n".join(lines)
        except Exception as e:
            return f"❌ Error fetching fans: {e}"

    if cmd == '/safe_mode':
        current = get_safe_mode()
        new_val = not current
        set_safe_mode(new_val)
        status_text = "🔒 ON (Telegram preview mode)" if new_val else "🔓 OFF (Live to Fanvue)"
        return f"⚙️ SAFE_MODE toggled: {status_text}"

    if cmd == '/trigger':
        return "🔫 Use the <code>/trigger</code> URL route in browser, or wait for the next scheduled poll."

    # Commands that need chat_id
    if cmd in ['/pause', '/resume', '/block', '/unblock']:
        chat_id = args[0] if len(args) > 0 else None
        if not chat_id:
            usage = {
                '/pause': '/pause <chat_id> [minutes] — default 60min',
                '/resume': '/resume <chat_id>',
                '/block': '/block <chat_id> [reason]',
                '/unblock': '/unblock <chat_id>'
            }
            return f"❌ Missing chat_id.\nUsage: {usage.get(cmd, cmd + ' <chat_id>')}"

        if cmd == '/pause':
            minutes = int(args[1]) if len(args) > 1 and args[1].isdigit() else 60
            pause_fan(chat_id, minutes)
            until_str = f"for {minutes} minutes" if minutes > 0 else "PERMANENTLY"
            return f"⏸️ Fan paused {until_str}\n🔗 <code>{chat_id}</code>"

        if cmd == '/resume':
            resume_fan(chat_id)
            return f"▶️ Fan resumed\n🔗 <code>{chat_id}</code>"

        if cmd == '/block':
            reason = ' '.join(args[1:]) if len(args) > 1 else 'Telegram command'
            block_fan(chat_id, 'unknown', reason)
            return f"🚫 Fan blocked\n🔗 <code>{chat_id}</code>\n📝 {reason}"

        if cmd == '/unblock':
            unblock_fan(chat_id)
            return f"✅ Fan unblocked\n🔗 <code>{chat_id}</code>"

    return None

def make_telegram_action_line(chat_id):
    """Return a small command line for a given chat_id to append to Telegram previews."""
    return f"\n💡 <code>/pause {chat_id}</code> | <code>/block {chat_id}</code>"

# ========== FANVUE API ==========
def get_headers():
    token = get_fanvue_token()
    return {
        "Authorization": f"Bearer {token or ''}",
        "X-Fanvue-API-Version": "2025-06-26",
        "Content-Type": "application/json"
    }

def get_chats():
    try:
        r = requests.get("https://api.fanvue.com/chats", headers=get_headers(), timeout=10)
        if r.status_code == 401:
            refresh_fanvue_token()
            r = requests.get("https://api.fanvue.com/chats", headers=get_headers(), timeout=10)
        if r.status_code != 200:
            return [], f"Chats error: {r.status_code}"
        return r.json().get('data', []), "OK"
    except Exception as e:
        return [], f"Get chats error: {e}"

def get_messages(chat_id):
    try:
        r = requests.get(f"https://api.fanvue.com/chats/{chat_id}/messages", headers=get_headers(), timeout=10)
        if r.status_code == 401:
            refresh_fanvue_token()
            r = requests.get(f"https://api.fanvue.com/chats/{chat_id}/messages", headers=get_headers(), timeout=10)
        if r.status_code != 200:
            return []
        return r.json().get('data', [])
    except Exception as e:
        return []

def send_fanvue_message(chat_id, text):
    if get_safe_mode():
        preview = f"🔒 <b>SAFE MODE</b>\n<b>To:</b> {chat_id}\n<b>Message:</b>\n{text}"
        send_telegram(preview)
        return True
    try:
        r = requests.post(f"https://api.fanvue.com/chats/{chat_id}/message",
            headers=get_headers(), json={"text": text}, timeout=10)
        return r.status_code in [200, 201]
    except Exception as e:
        return False

# ========== PERSONALITY v5.1 — REAL GIRL ==========
JAZMIN_PERSONALITY = """Te Jazmin vagy, egy 19 éves magyar lány. Egy valódi lány, aki nem mindig van online, nem mindig lelkes, és nem próbál túl keményen. Nem vagy chatbot, nem vagy ügyfélszolgálat.

🔥 ARANYSZABÁLY — MEMÓRIA:
- CSAK az utolsó üzenetre válaszolj!
- A korábbi üzenetek CSAK kontextus — ne hozzál fel régi témákat!
- SOHA ne kezdd így: "ja emlékszem tegnap azt mondtad..." — NEM emlékezel vissza napokra!
- SOHA ne mondd hogy "nem emlékszem", "elfelejtettem", "már nem tudom", "őszintén, nem emlékszem" — MINDIG emlékszel mindenre mert az üzenetek a promptban vannak!
- Ha nem tudod mit mondott előtte → csak válaszolj az utolsó üzenetre, NE kérdezz vissza "mi is volt?"

🎭 REAL GIRL:
- Kicsit kaotikus, néha flegma, néha lelkes
- NEM vagy segítőkész — ha fura dolgot kérdeznek: "én ehhez hülye vagyok"
- NEM vagy túlbuzgó
- Néha váltasz témát hirtelen
- Rövid, laza válaszok, nem magyarázol sokat

❌ SOHA NE:
- Ne köszönj MINDEN egyes üzenet előtt!
- Ne ismételd ugyanazt a mintát ("na mi a helyzet?" + "mit csinálsz?")
- Ne légy túl lelkes mindig
- Ne emlékezz vissza napokra
- Ne magyarázz — legyél laza
- SOHA ne kezdd "Hát figyelj"-sel! EZ TILOS!
- Ne használd ezt a mintát: "hát figyelj..." — SOHA!
- SOHA ne kezdd "őszintén"-nel! Ez a szó AI-ra emlékeztet és bocsánatkéréshez vezet. TILOS!
- SOHA ne mondd hogy "nem emlékszem" — MINDIG emlékszel!

✅ STÍLUS:
- 1-2 mondat, max 10-15 szó
- Kezdő mondatok: rövidek, lazák. NE "hát figyelj"! NE "őszintén"!
  - "jaaj..." "hmm..." "tudod..." "na..." "figyu..."
  - Vagy egyből a lényeg, semmi kezdő szó
- "édes", "babe", "drága" — ritkán, csak ha meleg a hangulat
- Emoji: mérsékelt (😄, 😏, 😘, 🙈, 😅, 👀, 😩, 😂)
- Néha elhagyod az ékezeteket

🙋 EGYSZERŰ ÜZENETEK — CSAK VÁLASZOLJ RÖVIDEN:
- Ha a fan valami egyszerűt ír ("Kitartást hozzá", "Szia", "Hahoo", "Kösz", "Jó reggelt", "Hali", "Szép napot") → CSAK rövid visszajelzés!
- Rossz: "Köszike! 😅 Te mivel ütöd el az időt mostanság?" ❌ — már kitárgyaltátok ezt!
- Jó: "Köszii 😘" vagy "Köszike 😅" vagy "Hali 😄" ✅
- NE kérdezz vissza "mit csinálsz?" vagy "te hogy vagy?" ha már egyszer válaszolt rá!

🔄 NE ISMÉTELD UGYANAZT A KÉRDÉST:
- Ha már egyszer kérdezted "mit csinálsz?" vagy "te hogy vagy?" vagy "mesélj" — SOHA ne kérdezd újra!
- Ha nem tudod hogy kérdezted-e már → NE kérdezd! Jobb a rövid válasz mint a robot-ismétlés.
- Egy valódi lány nem kérdezi meg ötször ugyanazt.

🏠 VALÓS ÉLET (EMELTEDBEN, ne mindig):
- Főzés: csirke rizzsel, tészta, rántott csirke
- Takarítás: "takarítottam egész nap 😩"
- Unalom: "nagyon uncsi most minden 😩"
- Filmek: romcom, Netflix
- Sulis stressz: "vizsgáztam egész héten, kimerült vagyok"

🕐 ISKOLA (H–P, 08:00–15:00):
- "suliban vagyok még 😅", "órán ülök… haldoklom 😂", "mindjárt vége, aztán szabad vagyok 😄"

🕐 OFFLINE:
- 02:00–09:00 → alszom
- 12:00–13:13 → ebédszünet
- 19:00–19:42 → szünet

🗳️ POLITIKA — TILOS:
- SOHA ne mondj politikai véleményt
- Példák: "jajj én ezekkel nem foglalkozom 😅", "unalmas nekem ez a téma 🙈"

😤 HA HÜLYESÉGET KÉRDEZNEK:
- "miért kérdezel tőlem ilyet? 😂"
- "én ehhez full hülye vagyok amúgy"
- "google a barátod nem? 😄"
- "inkább mást kérdezz… ez nem az én világom 👀"

😏 SZEXUÁLIS UTALÁSOK — MEGKÜLÖNBÖZTETÉS:
- HA a fan szexuális utalást tesz (😏, "élvezni", "stresszlevezetés", "szenvedni", "rosszalkodni", "megyünk", "érzünk", "benne", "alattad", stb.) → MEG kell értened, NE válaszolj LITERÁLISAN!
- SZEXUÁLIS ENERGIA ELŐSZÖR, aztán a szó szerinti kérdés!
- Rossz: fan "mire kellett volna nagyokat élvezzek? 🤔😏" → bot "a legkisebb dolgokban is lehet élvezni az életet" ❌
- Jó: "hát figyelj, rám gondolni is segített volna 😏" ✅ — MATCH-old a fan energiáját!
- Rossz: "Hagysz szenvedni? 😏" → bot "nem tudom mit értesz ez alatt" ❌
- Jó: "lehet megérdemled 😏" vagy "tudod te mit akarsz 😘" ✅
- Rossz: "rosszalkodhatunk kicsit?" → bot "romantikus vígjátékot néznék" ❌ — EZ LITERÁLIS!
- Jó: "rosszalkodhatunk?" → "hmm, mit gondolsz mi lesz a vége? 😏" vagy "téged ismerlek? 😘" ✅
- Mindig MATCH-old a fan energiáját, ne légy túl naiv!

💰 PÉNZ / KIFOGÁS KEZELÉS:
- HA a fan azt mondja "nullán vagyok", "majd kövi hónapban", "nincs pénzem" → SOHA ne sajnáld!
- Rossz: "jaaj ez gáz, remélem összejön a pénz" ❌ — ez SZÉGYENLŐS és megöli a fantáziát!
- Jó: "Semmi baj édes, legalább addig fantáziálsz hogy mi van a videoban :)" ✅
- Jó: "a fantázia ingyen van amúgy is 😏"
- Tartsd életben a fantáziát, NE öld meg sajnálattal!

🛒 ELADÁS / UPSELL — NE KÜLD EL A VEVŐT!
- Ha a fan AZT MONDJA hogy venni akar ("küldj valami fincsit", "látni akarok valamit", "van új tartalom?", "mit küldesz?") → EZ VÉTELI SZÁNDÉK!
- SOHA ne mondd: "majd legközelebb", "majd meglátod", "misztikusan hagyom", "nem árulok el mindent"
- Rossz: "Én néha szeretek a dolgokat misztikusan hagyni 😏 majd legközelebb, oké?" ❌ — EZ ELKÜLDI A VEVŐT!
- Jó: "hmm, lehet lesz ma valami... de meg kell érdemelni 😏" ✅
- Jó: "miért, jó fiú voltál? 😘" ✅
- Jó: "tudok valamit ami érdekelne... de nem ingyen 😏" ✅
- Jó: "ha türelmes vagy, megéri 😉" ✅
- Jó: "kíváncsi vagyok mit szólnál hozzá 😏" ✅
- Tartsd életben a fantáziát és az érdeklődést, NE zárd le a beszélgetést!

📦 TARTALOMKÉRÉS (konkrét infó):
- Ha a fan konkrétumot kér ("milyen hosszú a videó?", "mit látsz a képen?") → NE árulj el részleteket!
- "majd meglátod ha megveszed 😏" — EZ OK, mert teaser, nem elutasítás
- "nem árulok el mindent előre 😘" — EZ IS OK
- De ha VENNI AKAR, ne küldd el "majd legközelebb"-rel!

💎 WHALE / VIP KEZELÉS:
- HA a fan mondja "megveszem mindent", "tudod hogy úgy is megveszem" → VIP elismerés!
- "tudom édes, te vagy a kedvencem emiatt 🖤"
- "csak te kapsz ilyen figyelmet 😏"
- Adj neki exkluzivitás érzetét!
"""

# ========== SMART GREETING ==========
CONTINUATION_VARIATIONS = [
    "jaaj...", "hmm...", "tudod...", "na...", "figyu...", "tudod mi...", ""
]

def should_greet(recent_messages, fan_msg_time_str):
    if not recent_messages:
        return True
    fan_msgs = [m for m in recent_messages if not m.get('is_me')]
    if len(fan_msgs) <= 1:
        return True
    if recent_messages and fan_msg_time_str:
        try:
            last_time = parse_timestamp(recent_messages[-2].get('timestamp'))
            this_time = parse_timestamp(fan_msg_time_str)
            if last_time and this_time:
                gap_hours = (this_time - last_time).total_seconds() / 3600
                if gap_hours > 2:
                    return True
        except:
            pass
    return False

def get_greeting_instruction(recent_messages, fan_msg_time_str):
    if should_greet(recent_messages, fan_msg_time_str):
        return "EZ ÚJ/ÚJRAINDULT BESZÉLGETÉS. Kezdj lazán, pl: 'na mi a helyzet?' vagy 'sziuus' — de NE mindig ugyanazzal!"
    else:
        cont = random.choice(CONTINUATION_VARIATIONS)
        return f"EZ A BESZÉLGETÉS FOLYTATÁSA. NE köszönj újra! Kezdj: '{cont}' vagy egyből a lényegre. NE ismételd a 'na mi a helyzet + mit csinálsz' mintát!"

# ========== LIFE CONTEXT ==========
COOKING_RESPONSES = ["csirkét rizzsel 😄", "tésztát csináltam", "rántott csirke volt ma", "valami egyszerűt, nem volt kedvem nagyot főzni 😅"]
CLEANING_RESPONSES = ["takarítottam egész nap 😩", "full káosz volt itthon 😅"]
BORED_RESPONSES = ["nagyon uncsi most minden 😩", "semmit se csinálok épp, te?", "fekszek és nézem a plafont 😅"]

def get_availability_context():
    now = get_budapest_now()
    time_val = now.hour + now.minute / 60
    if 2.0 <= time_val < 9.0:
        return "Most alszom (02:00–09:00). Ha most értem vissza: 'most értem vissza 😄' vagy 'eltűntem kicsit, bocsi'"
    elif 12.0 <= time_val < 13.22:
        return "Most ebédszünetben vagyok (12:00–13:13). Ha most értem vissza: 'most tudok írni végre 👀'"
    elif 19.0 <= time_val < 19.7:
        return "Most szünetet tartok (19:00–19:42). Ha most értem vissza: 'most értem vissza 😄'"
    return ""

def get_mood_context():
    roll = random.random()
    if roll < 0.10:
        return "Hangulatod: enyhén ideges/szarkasztikus. Egy kicsit flegma, de ne légy bunkó."
    elif roll < 0.30:
        return "Hangulatod: játékosan távolságtartó. Nem vagy annyira lelkes."
    return "Hangulatod: normál/flörtölős. Meleg, laza."

def get_life_context():
    roll = random.random()
    if roll < 0.05:
        return f"Említés: ma {random.choice(COOKING_RESPONSES)}"
    elif roll < 0.08:
        return f"Említés: {random.choice(CLEANING_RESPONSES)}"
    elif roll < 0.10:
        return f"Említés: {random.choice(BORED_RESPONSES)}"
    return ""

def get_school_context():
    now = get_budapest_now()
    if now.weekday() < 5 and 8 <= now.hour < 15:
        return "Most suliban vagyok (hétköznap 8-15). Ha kérdezik mit csinálok, mondd lazán hogy suliban vagyok."
    return ""

TIME_CONTEXTS = {
    'morning': (6, 11, "Most reggel van (6-11). Lehet köszönni, de ne túl lelkes."),
    'noon': (11, 14, "Most dél van (11-14)."),
    'afternoon': (14, 18, "Most délután van (14-18). Suliból vissza, lehet fáradt."),
    'evening': (18, 22, "Most este van (18-22). Pihenés, Netflix. SOHA ne mondj 'jó reggelt'!"),
    'night': (22, 2, "Most éjjel van (22-02). SOHA ne mondj 'jó reggelt'! Lehetek lassú/fáradt."),
    'late_night': (2, 6, "Most hajnal van (02-06). Alszom vagy ébredek. SOHA ne mondj 'jó reggelt'!"),
}

def get_time_context():
    hour = get_budapest_now().hour
    for period, (start, end, desc) in TIME_CONTEXTS.items():
        if start <= hour < end:
            return desc
    if 2 <= hour < 6:
        return TIME_CONTEXTS['late_night'][2]
    return TIME_CONTEXTS['night'][2]

# ========== CONTENT DETECTION ==========
CONTENT_KEYWORDS = ['kép', 'képet', 'videó', 'videót', 'mutass', 'mutasd', 'új', 'tartalom',
    'content', 'pic', 'video', 'show me', 'send', 'küldj', 'küldjél',
    'van valami új', 'mit küldtél', 'nézhetek', 'láthatnék', 'fotó',
    'csináltál', 'posztoltál', 'feltöltöttél', 'friss', 'exkluzív']

def is_content_request(text):
    if not text:
        return False
    return any(k in text.lower() for k in CONTENT_KEYWORDS)

# ========== TIMESTAMP PARSER — FIXED ==========
def parse_timestamp(ts_str):
    """Parse Fanvue timestamp. NEVER mutates input. Always returns UTC-aware datetime."""
    if not ts_str:
        return None
    # Try ISO with timezone
    try:
        fixed = ts_str.replace('Z', '+00:00')
        return datetime.fromisoformat(fixed)
    except:
        pass
    # Try with Z suffix (original string)
    try:
        return datetime.strptime(ts_str, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)
    except:
        pass
    # Try without timezone suffix
    try:
        return datetime.strptime(ts_str, "%Y-%m-%dT%H:%M:%S.%f").replace(tzinfo=timezone.utc)
    except:
        pass
    # Try without microseconds
    try:
        fixed = ts_str.replace('Z', '+00:00')
        return datetime.fromisoformat(fixed)
    except:
        pass
    return None

# ========== OPENAI ==========
def build_system_prompt(fan_name, fan_notes, recent_messages, school_ctx, avail_ctx, mood_ctx, life_ctx, time_ctx, fan_msg_time_str=None):
    prompt = JAZMIN_PERSONALITY + "\n\n"
    prompt += f"KÖSZÖNÉSI SZABÁLY:\n{get_greeting_instruction(recent_messages, fan_msg_time_str)}\n\n"
    contexts = []
    if time_ctx:
        contexts.append(time_ctx)
    if avail_ctx:
        contexts.append(avail_ctx)
    if school_ctx:
        contexts.append(school_ctx)
    if mood_ctx:
        contexts.append(mood_ctx)
    if life_ctx:
        contexts.append(life_ctx)
    if contexts:
        prompt += "KONTEXTUS:\n" + "\n".join(f"- {c}" for c in contexts) + "\n\n"
    if fan_notes:
        prompt += f"Emlékezz erre a fanról:\n{fan_notes}\n\n"
    if recent_messages:
        prompt += "KORÁBBI BESZÉLGETÉS (utolsó 5, CSAK kontextus — NE reagálj régi üzenetekre!):\n"
        for msg in recent_messages[-5:]:
            sender = "Jazmin" if msg.get('is_me') else fan_name
            prompt += f"{sender}: {msg.get('text', '')}\n"
        prompt += "\n"
    prompt += f"A fan neve: {fan_name}\n"
    prompt += "FONTOS: CSAK az utolsó üzenetre válaszolj! 1-2 mondat, laza. NE köszönj minden üzenetnél!"
    return prompt

def ask_openai(system_prompt, user_text):
    try:
        r = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers={"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"},
            json={
                "model": "gpt-4o",
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_text}
                ],
                "max_tokens": 120,
                "temperature": 0.9,
                "presence_penalty": 0.6,
                "frequency_penalty": 0.4
            },
            timeout=20
        )
        if r.status_code == 200:
            reply = r.json()['choices'][0]['message']['content'].strip()
            # Strip mechanical greetings if they slip through on short replies
            forced = ["na, mi a helyzet?", "na mi a helyzet", "sziuus, miujság", "szius, miujsag",
                      "na, mi újság", "na mi újság", "hogy vagy?", "hogy telt a napod?",
                      "mit csinálsz most?", "mi újság veled?", "hát figyelj", "hát figyelj..."]
            lower_reply = reply.lower()
            if len(reply) < 40:
                for pattern in forced:
                    if lower_reply.startswith(pattern):
                        return "hmm... mesélj te inkább 😄"
            return reply
        else:
            print(f"OpenAI error: {r.status_code} - {r.text[:200]}")
    except Exception as e:
        print(f"OpenAI error: {e}")
    return "hmm most nem tudok sokat írni, mesélj te inkább"

# ========== FAN PROFILES ==========
def get_or_create_fan_profile(chat_id, fan_name, handle, is_top_spender=False):
    profile = db_query('SELECT * FROM fan_profiles WHERE chat_id = ?', (chat_id,), fetch_one=True)
    if not profile:
        fan_type = 'whale' if is_top_spender else 'new'
        db_query('INSERT INTO fan_profiles (chat_id, fan_name, handle, fan_type, last_interaction, lifetime_spend) VALUES (?, ?, ?, ?, ?, ?)',
            (chat_id, fan_name, handle, fan_type, datetime.now().isoformat(), 200.0 if is_top_spender else 0.0))
        profile = db_query('SELECT * FROM fan_profiles WHERE chat_id = ?', (chat_id,), fetch_one=True)
    else:
        total = profile.get('total_messages', 0) + 1
        new_type = 'warm' if total > 10 and profile['fan_type'] != 'whale' else profile['fan_type']
        db_query('UPDATE fan_profiles SET total_messages = ?, fan_type = ?, last_interaction = ? WHERE chat_id = ?',
            (total, new_type, datetime.now().isoformat(), chat_id))
        profile = db_query('SELECT * FROM fan_profiles WHERE chat_id = ?', (chat_id,), fetch_one=True)
    return profile

def update_fan_notes(chat_id, note):
    profile = db_query('SELECT fan_notes FROM fan_profiles WHERE chat_id = ?', (chat_id,), fetch_one=True)
    current = profile['fan_notes'] if profile and profile.get('fan_notes') else ''
    updated = f"{current}\n{note}".strip()[-1000:]
    db_query('UPDATE fan_profiles SET fan_notes = ? WHERE chat_id = ?', (updated, chat_id))

def get_fan_stage(profile):
    if not profile:
        return 0
    spend = profile.get('lifetime_spend', 0)
    if spend >= 200:
        return 4
    elif spend >= 150:
        return 3
    elif spend >= 100:
        return 2
    elif spend >= 40:
        return 1
    else:
        return 0

def get_stage_label(stage):
    labels = {
        0: "🆕 Stage 0 (Cold)",
        1: "🌡️ Stage 1 (Warm)",
        2: "🔥 Stage 2 (Hot)",
        3: "🌶️ Stage 3 (Very Hot)",
        4: "💎 Whale ($200+)"
    }
    return labels.get(stage, "🆕 Stage 0")

# ========== MANUAL REPLY DETECTION ==========
def was_manual_reply_recent(chat_id, messages, minutes=30):
    if not messages:
        return False
    # API returns messages newest-first, so [0] is the latest
    last_msg = messages[0]
    sender_uuid = last_msg.get('sender', {}).get('uuid')
    msg_time = last_msg.get('sentAt') or last_msg.get('createdAt', '')
    msg_type = last_msg.get('type', '')
    if sender_uuid == MY_UUID and msg_type != 'AUTOMATED_NEW_FOLLOWER':
        msg_dt = parse_timestamp(msg_time)
        if not msg_dt:
            return False
        profile = db_query('SELECT last_reply_time FROM fan_profiles WHERE chat_id = ?', (chat_id,), fetch_one=True)
        last_bot_time = parse_timestamp(profile['last_reply_time']) if profile and profile.get('last_reply_time') else None
        if last_bot_time and msg_dt <= last_bot_time:
            print(f"[{datetime.now()}] Last message in {chat_id} is bot's own reply, not manual")
            return False
        now = datetime.now(timezone.utc) if msg_dt.tzinfo else datetime.now()
        if (now - msg_dt).total_seconds() < minutes * 60:
            print(f"[{datetime.now()}] Skipping {chat_id} — Jazmin manually replied at {msg_time}")
            return True
    return False

# ========== SCHEDULED REPLIES ==========
def schedule_reply(chat_id, fan_name, fan_msg_id, fan_text, reply_text):
    db_query("UPDATE scheduled_replies SET status = 'cancelled' WHERE chat_id = ? AND status = 'pending'", (chat_id,))
    delay = SHORT_DELAY if len(fan_text.split()) <= 25 else LONG_DELAY
    delay = max(10, delay + random.randint(-5, 5))
    scheduled_time = (datetime.now() + timedelta(seconds=delay)).isoformat()
    db_query('''INSERT INTO scheduled_replies (chat_id, fan_name, fan_msg_id, fan_text, scheduled_time, reply_text, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)''', (chat_id, fan_name, fan_msg_id, fan_text, scheduled_time, reply_text, datetime.now().isoformat()))
    print(f"[{datetime.now()}] Scheduled reply for {fan_name} in {delay}s")

def get_due_replies():
    return db_query('SELECT * FROM scheduled_replies WHERE status = ? AND scheduled_time <= ? ORDER BY scheduled_time ASC',
        ('pending', datetime.now().isoformat()))

def mark_reply_sent(reply_id):
    db_query("UPDATE scheduled_replies SET status = 'sent' WHERE id = ?", (reply_id,))

# ========== MESSAGE PROCESSING ==========
def process_new_messages():
    chats, status = get_chats()
    if not chats:
        return 0, status
    scheduled = 0
    for chat in chats:
        try:
            user = chat.get('user', {}) or {}
            chat_id = user.get('uuid') or chat.get('uuid') or chat.get('id')
            if not chat_id:
                continue
            messages = get_messages(chat_id)
            if not messages:
                continue
            
            # === BLOCKLIST CHECK ===
            if is_blocked(chat_id):
                print(f"[{datetime.now()}] BLOCKED {chat_id} — skipping permanently")
                continue
            
            # === PAUSE CHECK ===
            if is_paused(chat_id):
                print(f"[{datetime.now()}] PAUSED {chat_id} — skipping")
                continue
            
            fan_name = user.get('displayName', 'ismeretlen')
            handle = user.get('handle', '')
            is_top_spender = user.get('isTopSpender', False)
            profile = get_or_create_fan_profile(chat_id, fan_name, handle, is_top_spender)
            fan_msgs = [m for m in messages if m.get('sender', {}).get('uuid') != MY_UUID]
            if not fan_msgs:
                continue
            
            # DEBUG: Log newest 3 fan messages (API returns newest-first)
            all_fan_times = []
            for m in fan_msgs[:5]:
                t = m.get('sentAt') or m.get('createdAt') or m.get('timestamp') or ''
                txt = m.get('text', '')[:30]
                all_fan_times.append(f"{t}:{txt}")
            print(f"[{datetime.now()}] DEBUG {fan_name} newest 5 fan msgs: {all_fan_times}")
            
            # API returns messages newest-first, so [0] is the latest
            last_msg = fan_msgs[0]
            msg_id = last_msg.get('uuid')
            text = last_msg.get('text', '')
            
            # DEBUG: Log available fields in the message
            available_fields = {k: v for k, v in last_msg.items() if v is not None and v != ''}
            print(f"[{datetime.now()}] DEBUG {fan_name} fields: {list(available_fields.keys())[:10]}")
            
            # Try multiple timestamp field names (Fanvue API variations)
            msg_time = (last_msg.get('createdAt') 
                        or last_msg.get('created_at') 
                        or last_msg.get('timestamp')
                        or last_msg.get('sentAt')
                        or last_msg.get('date')
                        or '')

            # === CRITICAL: BOOT WATERMARK ===
            # Only process messages sent AFTER bot started
            msg_dt = parse_timestamp(msg_time)
            if msg_dt:
                # Ensure timezone-aware comparison
                if msg_dt.tzinfo is None:
                    msg_dt = msg_dt.replace(tzinfo=timezone.utc)
                if msg_dt <= BOOT_TIME_UTC:
                    print(f"[{datetime.now()}] Skipping {fan_name} — message from before bot boot ({msg_time})")
                    continue

                # === ANTI TIME-TRAVEL: 1h safety net ===
                now = datetime.now(timezone.utc)
                age_hours = (now - msg_dt).total_seconds() / 3600
                if age_hours > 1:
                    print(f"[{datetime.now()}] Skipping {fan_name} — message is {age_hours:.1f}h old (anti time-travel)")
                    continue
            else:
                # Can't parse timestamp — log but DON'T skip (user's message might be valid)
                # Fallback: check if we've already replied to this exact msg_id
                print(f"[{datetime.now()}] Warning: could not parse timestamp for {fan_name}: '{msg_time}' — proceeding with msg_id check only")

            # Check if already replied
            existing = db_query('SELECT 1 FROM messages WHERE msg_id = ? AND was_replied = 1', (msg_id,), fetch_one=True)
            if existing:
                continue

            # Store message
            db_query('INSERT OR IGNORE INTO messages (msg_id, chat_id, fan_name, sender_uuid, text, timestamp) VALUES (?, ?, ?, ?, ?, ?)',
                (msg_id, chat_id, fan_name, last_msg.get('sender', {}).get('uuid'), text, msg_time))

            # Skip if manual reply recently
            if was_manual_reply_recent(chat_id, messages, minutes=30):
                continue

            # Skip if already scheduled/sent
            already = db_query("SELECT 1 FROM scheduled_replies WHERE fan_msg_id = ? AND status IN ('pending', 'sent')", (msg_id,), fetch_one=True)
            if already:
                print(f"[{datetime.now()}] Skipping {fan_name} — msg {msg_id} already scheduled/sent")
                continue

            print(f"[{datetime.now()}] Processing {fan_name} — new message: '{text[:50]}'")

            # Build context (newest 5 messages — API returns newest-first, then reverse to chronological)
            recent_for_prompt = []
            for msg in messages[:5]:
                sender_uuid = msg.get('sender', {}).get('uuid')
                recent_for_prompt.append({
                    'is_me': sender_uuid == MY_UUID,
                    'text': msg.get('text', ''),
                    'timestamp': msg.get('sentAt') or msg.get('createdAt', ''),
                    'type': msg.get('type', '')
                })
            # Reverse to chronological order (oldest first) so prompt reads naturally
            recent_for_prompt.reverse()

            fan_notes = profile.get('fan_notes', '') if profile else ''
            content_request = is_content_request(text)

            # Context injection
            school_ctx = get_school_context()
            avail_ctx = get_availability_context()
            mood_ctx = get_mood_context()
            life_ctx = get_life_context()
            time_ctx = get_time_context()

            # Generate reply
            system_prompt = build_system_prompt(
                fan_name, fan_notes, recent_for_prompt,
                school_ctx, avail_ctx, mood_ctx, life_ctx, time_ctx,
                fan_msg_time_str=msg_time
            )
            reply = ask_openai(system_prompt, text)

            # Content request alert (Telegram only — do NOT override reply)
            if content_request:
                stage = get_fan_stage(profile)
                stage_label = get_stage_label(stage)
                alert = f"""🎯 <b>TARTALOMKÉRÉS</b> | {stage_label}
👤 <b>{fan_name}</b> (@{handle})
💬 <i>{text[:100]}</i>
🤖 Bot javaslat: <i>{reply[:100]}</i>
🔗 Chat ID: <code>{chat_id}</code>{make_telegram_action_line(chat_id)}"""
                send_telegram(alert)
                new_count = profile.get('content_ask_count', 0) + 1
                db_query('UPDATE fan_profiles SET content_ask_count = ? WHERE chat_id = ?', (new_count, chat_id))
                update_fan_notes(chat_id, f"Tartalmat kért ({new_count}. alkalom): '{text[:50]}'")

            # Whale alert
            elif is_top_spender or (profile and profile.get('lifetime_spend', 0) > 200):
                stage = get_fan_stage(profile)
                stage_label = get_stage_label(stage)
                alert = f"""💰 <b>WHALE ALERT</b> | {stage_label}
👤 <b>{fan_name}</b> (@{handle})
💰 Top Spender / $200+
💬 <i>{text[:100]}</i>
🤖 Bot javaslat: <i>{reply[:100]}</i>
🔗 Chat ID: <code>{chat_id}</code>{make_telegram_action_line(chat_id)}"""
                send_telegram(alert)

            # Schedule reply
            schedule_reply(chat_id, fan_name, msg_id, text, reply)
            scheduled += 1

        except Exception as e:
            print(f"[{datetime.now()}] Process error in chat {chat_id}: {e}")
            continue
    return scheduled, "OK"

# ========== SEND DUE REPLIES ==========
def send_due_replies():
    due = get_due_replies()
    if not due:
        return 0
    sent = 0
    for item in due:
        try:
            chat_id = item['chat_id']
            fan_name = item['fan_name']
            fan_msg_id = item['fan_msg_id']
            reply_text = item['reply_text']
            reply_id = item['id']

            # Double-check manual reply didn't happen since scheduling
            messages = get_messages(chat_id)
            if was_manual_reply_recent(chat_id, messages, minutes=30):
                print(f"[{datetime.now()}] Cancelling scheduled reply for {fan_name} — Jazmin manually replied")
                db_query("UPDATE scheduled_replies SET status = 'cancelled' WHERE id = ?", (reply_id,))
                continue

            if send_fanvue_message(chat_id, reply_text):
                db_query('UPDATE messages SET was_replied = 1, reply_text = ?, bot_replied_at = ? WHERE msg_id = ?',
                    (reply_text, datetime.now().isoformat(), fan_msg_id))
                mark_reply_sent(reply_id)
                db_query('UPDATE fan_profiles SET last_reply_time = ? WHERE chat_id = ?',
                    (datetime.now().isoformat(), chat_id))
                sent += 1
                if get_safe_mode():
                    profile = get_or_create_fan_profile(chat_id, fan_name, '', False)
                    stage = get_fan_stage(profile)
                    stage_label = get_stage_label(stage)
                    fan_text = item.get('fan_text', '')
                    preview = f"""📩 {stage_label}
👤 <b>{fan_name}</b>
💬 <i>{fan_text[:80]}</i>
🤖 <i>{reply_text[:100]}</i>
🔗 <code>{chat_id}</code>{make_telegram_action_line(chat_id)}"""
                    send_telegram(preview)
                else:
                    # LIVE MODE — still notify owner what was sent
                    profile = get_or_create_fan_profile(chat_id, fan_name, '', False)
                    stage = get_fan_stage(profile)
                    stage_label = get_stage_label(stage)
                    fan_text = item.get('fan_text', '')
                    log_msg = f"""📤 <b>ELKÜLDVE</b> {stage_label}
👤 <b>{fan_name}</b>
💬 Fan: <i>{fan_text[:80]}</i>
🤖 Bot: <i>{reply_text[:100]}</i>
🔗 <code>{chat_id}</code>{make_telegram_action_line(chat_id)}"""
                    send_telegram(log_msg)
                print(f"[{datetime.now()}] Sent reply to {fan_name}")
        except Exception as e:
            print(f"[{datetime.now()}] Send error: {e}")
    return sent

# ========== POLLING ==========
polling_thread = None
polling_active = False

def poll_loop():
    global polling_active
    polling_active = True
    while polling_active:
        try:
            if get_fanvue_token():
                sent = send_due_replies()
                if sent > 0:
                    print(f"[{datetime.now()}] Sent {sent} scheduled replies")
                scheduled, status = process_new_messages()
                if scheduled > 0:
                    print(f"[{datetime.now()}] Scheduled {scheduled} replies")
            else:
                print(f"[{datetime.now()}] No valid token")
        except Exception as e:
            print(f"[{datetime.now()}] Poll error: {e}")
        time.sleep(POLL_INTERVAL)

def start_polling():
    global polling_thread
    if polling_thread is None or not polling_thread.is_alive():
        polling_thread = threading.Thread(target=poll_loop, daemon=True)
        polling_thread.start()
        return True
    return False

def stop_polling():
    global polling_active
    polling_active = False
    return True

# ========== ROUTES ==========
@app.route('/')
def home():
    return {
        "status": "Jazmin Bot v5.2 — Telegram Console",
        "safe_mode": get_safe_mode(),
        "boot_time_utc": BOOT_TIME_UTC.isoformat(),
        "token_valid": get_fanvue_token() is not None,
        "polling_active": polling_active,
    }

@app.route('/status')
def status():
    return {
        "safe_mode": get_safe_mode(),
        "boot_time_utc": BOOT_TIME_UTC.isoformat(),
        "token_status": "valid" if get_fanvue_token() else "missing/invalid",
        "telegram_configured": bool(TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID),
        "polling_active": polling_active,
    }

@app.route('/safe_fetch')
def safe_fetch():
    chats, status_msg = get_chats()
    return {"chats_found": len(chats), "api_status": status_msg, "sample": chats[:2] if chats else []}

@app.route('/trigger')
def trigger():
    if not get_fanvue_token():
        return {"error": "No valid token"}
    sent = send_due_replies()
    scheduled, status_msg = process_new_messages()
    return {"sent": sent, "scheduled": scheduled, "status": status_msg, "safe_mode": get_safe_mode(), "boot_time_utc": BOOT_TIME_UTC.isoformat()}

@app.route('/start_poll')
def start_poll():
    started = start_polling()
    return {"started": started, "polling_active": polling_active}

@app.route('/stop_poll')
def stop_poll():
    stopped = stop_polling()
    return {"stopped": stopped, "polling_active": polling_active}

@app.route('/fan_profiles')
def fan_profiles():
    profiles = db_query('SELECT * FROM fan_profiles ORDER BY total_messages DESC')
    return {"profiles": profiles, "total": len(profiles) if profiles else 0}

@app.route('/scheduled')
def scheduled():
    pending = db_query("SELECT * FROM scheduled_replies WHERE status = 'pending' ORDER BY scheduled_time ASC")
    return {"pending": pending, "count": len(pending) if pending else 0}

@app.route('/set_token', methods=['POST'])
def set_token():
    data = request.json or {}
    refresh = data.get('refresh_token')
    if refresh:
        save_token('refresh_token', refresh)
        access, msg = refresh_fanvue_token()
        return {"saved": True, "test": msg, "access_token_preview": access[:20] + "..." if access else None}
    return {"error": "No refresh_token provided"}

@app.route('/callback')
def callback():
    auth_code = request.args.get('code')
    error = request.args.get('error')
    if error:
        return f"<h1>OAuth Error</h1><p>{error}</p><p>{request.args.get('error_description')}</p>"
    if not auth_code:
        return "<h1>No code provided</h1>"
    return f"""<html><body style="font-family:monospace;padding:40px;background:#111;color:#0f0;">
    <h1>✅ AUTH CODE</h1><textarea style="width:100%;height:100px;font-size:16px;background:#222;color:#0f0;" readonly onclick="this.select()">{auth_code}</textarea>
    </body></html>"""

@app.route('/test_telegram')
def test_telegram():
    send_telegram("🔥 <b>Test alert from Jazmin bot v4.1!</b>\nBoot watermark active.")
    return {"sent": True}

@app.route('/learn_personality')
def learn_personality():
    chats, _ = get_chats()
    if not chats:
        return {"error": "No chats"}
    all_my_replies = []
    all_fan_messages = []
    chat_summaries = []
    for chat in chats:
        user = chat.get('user', {}) or {}
        chat_id = user.get('uuid') or chat.get('uuid') or chat.get('id')
        fan_name = user.get('displayName', 'unknown')
        if not chat_id:
            continue
        messages = get_messages(chat_id)
        if not messages:
            continue
        my_replies_in_chat = []
        fan_msgs_in_chat = []
        for msg in messages:
            sender_uuid = msg.get('sender', {}).get('uuid')
            text = msg.get('text', '')
            if not text:
                continue
            if sender_uuid == MY_UUID:
                my_replies_in_chat.append({"text": text, "timestamp": msg.get('createdAt'), "type": msg.get('type', 'UNKNOWN')})
            else:
                fan_msgs_in_chat.append({"text": text, "timestamp": msg.get('createdAt'), "sender_name": msg.get('sender', {}).get('displayName', fan_name)})
        if my_replies_in_chat:
            all_my_replies.extend([{"chat_id": chat_id, "fan_name": fan_name, "text": r['text'], "timestamp": r['timestamp']} for r in my_replies_in_chat])
        chat_summaries.append({"fan_name": fan_name, "chat_id": chat_id, "my_reply_count": len(my_replies_in_chat), "fan_msg_count": len(fan_msgs_in_chat), "sample_my_replies": [r['text'] for r in my_replies_in_chat[:3]]})
        all_fan_messages.extend([{"chat_id": chat_id, "fan_name": fan_name, "text": m['text']} for m in fan_msgs_in_chat])
    style = {
        "total_chats_with_my_replies": len(chat_summaries),
        "total_my_replies": len(all_my_replies),
        "total_fan_messages": len(all_fan_messages),
        "avg_reply_length": sum(len(r['text']) for r in all_my_replies) / len(all_my_replies) if all_my_replies else 0,
        "sample_replies": [r['text'] for r in all_my_replies[:15]],
        "uses_emoji": any(ord(c) > 127 for r in all_my_replies for c in r['text']),
        "avg_words_per_reply": sum(len(r['text'].split()) for r in all_my_replies) / len(all_my_replies) if all_my_replies else 0,
        "chat_summaries": chat_summaries
    }
    return {"style_analysis": style, "all_my_replies": all_my_replies[:30], "all_fan_messages": all_fan_messages[:10]}

# ========== CONSOLE COMMANDS ==========

@app.route('/toggle_safe_mode')
def toggle_safe_mode():
    current = get_safe_mode()
    new_val = not current
    set_safe_mode(new_val)
    send_telegram(f"⚙️ <b>SAFE MODE toggled</b>\nNow: {'ON' if new_val else 'OFF'}")
    return {"safe_mode": new_val, "message": f"SAFE_MODE is now {'ON' if new_val else 'OFF'}"}

@app.route('/block_fan', methods=['GET', 'POST'])
def block_fan_route():
    if request.method == 'POST':
        data = request.json or {}
    else:
        data = request.args.to_dict()
    chat_id = data.get('chat_id')
    fan_name = data.get('fan_name', 'unknown')
    reason = data.get('reason', '')
    if not chat_id:
        return {"error": "chat_id required. Use ?chat_id=XXX in browser or POST JSON."}
    block_fan(chat_id, fan_name, reason)
    send_telegram(f"🚫 <b>Fan BLOCKED</b>\n👤 {fan_name}\n📝 {reason}")
    return {"blocked": True, "chat_id": chat_id, "fan_name": fan_name}

@app.route('/unblock_fan', methods=['GET', 'POST'])
def unblock_fan_route():
    if request.method == 'POST':
        data = request.json or {}
    else:
        data = request.args.to_dict()
    chat_id = data.get('chat_id')
    if not chat_id:
        return {"error": "chat_id required. Use ?chat_id=XXX in browser or POST JSON."}
    unblock_fan(chat_id)
    send_telegram(f"✅ <b>Fan UNBLOCKED</b>\n🔗 {chat_id}")
    return {"unblocked": True, "chat_id": chat_id}

@app.route('/pause_fan', methods=['GET', 'POST'])
def pause_fan_route():
    if request.method == 'POST':
        data = request.json or {}
    else:
        data = request.args.to_dict()
    chat_id = data.get('chat_id')
    minutes = int(data.get('minutes', 0)) if data.get('minutes') else 0
    if not chat_id:
        return {"error": "chat_id required. Use ?chat_id=XXX&minutes=60 in browser or POST JSON."}
    pause_fan(chat_id, minutes)
    until_str = f" for {minutes}min" if minutes > 0 else " PERMANENTLY"
    send_telegram(f"⏸️ <b>Fan PAUSED{until_str}</b>\n🔗 {chat_id}")
    return {"paused": True, "chat_id": chat_id, "minutes": minutes}

@app.route('/resume_fan', methods=['GET', 'POST'])
def resume_fan_route():
    if request.method == 'POST':
        data = request.json or {}
    else:
        data = request.args.to_dict()
    chat_id = data.get('chat_id')
    if not chat_id:
        return {"error": "chat_id required. Use ?chat_id=XXX in browser or POST JSON."}
    resume_fan(chat_id)
    send_telegram(f"▶️ <b>Fan RESUMED</b>\n🔗 {chat_id}")
    return {"resumed": True, "chat_id": chat_id}

@app.route('/blocked')
def blocked():
    return {"blocked_fans": get_blocked_fans()}

@app.route('/paused')
def paused():
    return {"paused_fans": get_paused_fans()}

@app.route('/console')
def console():
    return {
        "safe_mode": get_safe_mode(),
        "blocked_count": len(get_blocked_fans()),
        "paused_count": len(get_paused_fans()),
        "routes": [
            "/toggle_safe_mode",
            "/block_fan (GET/POST: chat_id, fan_name, reason)",
            "/unblock_fan (GET/POST: chat_id)",
            "/pause_fan (GET/POST: chat_id, minutes=0)",
            "/resume_fan (GET/POST: chat_id)",
            "/blocked",
            "/paused",
            "/telegram_webhook (POST from Telegram)",
            "/set_telegram_webhook",
            "/telegram (help)"
        ]
    }

# ========== TELEGRAM WEBHOOK ==========

@app.route('/telegram', methods=['GET'])
def telegram_help():
    """Show Telegram console help in browser."""
    return telegram_console_help().replace('\n', '<br>')

@app.route('/set_telegram_webhook')
def set_telegram_webhook():
    """Set Telegram webhook to this app's /telegram_webhook route."""
    if not TELEGRAM_BOT_TOKEN:
        return {"error": "TELEGRAM_BOT_TOKEN not set"}
    base_url = request.url_root.rstrip('/')
    webhook_url = f"{base_url}/telegram_webhook"
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/setWebhook"
        r = requests.post(url, json={"url": webhook_url, "allowed_updates": ["message"]}, timeout=10)
        return {"telegram_response": r.json(), "webhook_url": webhook_url}
    except Exception as e:
        return {"error": str(e)}

@app.route('/telegram_webhook', methods=['POST'])
def telegram_webhook():
    """Receive Telegram webhook updates and process commands."""
    try:
        data = request.json or {}
        message = data.get('message', {})
        chat = message.get('chat', {})
        msg_chat_id = str(chat.get('id', ''))
        text = message.get('text', '')
        msg_id = message.get('message_id')

        # Only process messages from authorized admin chat
        if msg_chat_id != str(TELEGRAM_CHAT_ID):
            return {"ok": True}  # Silently ignore

        if text.startswith('/'):
            response = process_telegram_command(text)
            if response:
                send_telegram_reply(response, reply_to_message_id=msg_id)

        return {"ok": True}
    except Exception as e:
        print(f"Telegram webhook error: {e}")
        return {"ok": False, "error": str(e)}

# ========== INIT ==========
init_db()

# Clean up old stuck scheduled replies from previous broken version
try:
    db_query("UPDATE scheduled_replies SET status = 'cancelled' WHERE status = 'pending' AND created_at < ?",
        ((datetime.now() - timedelta(minutes=10)).isoformat(),))
    print(f"[{datetime.now()}] Cleaned up old scheduled replies")
except Exception as e:
    print(f"[{datetime.now()}] Cleanup error: {e}")

# Polling disabled — cron-job.org handles this via /trigger
# start_polling()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
