"""
Jazmin Fanvue Bot - Complete App with Telegram Control
Rafael's automation system
"""

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

# ========== BOOT WATERMARK ==========
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
        chat_id TEXT PRIMARY KEY, fan_name TEXT, blocked_at TEXT, reason TEXT)''')
    # Add pause columns if missing
    try:
        c.execute("ALTER TABLE fan_profiles ADD COLUMN is_paused INTEGER DEFAULT 0")
    except:
        pass
    try:
        c.execute("ALTER TABLE fan_profiles ADD COLUMN paused_until TEXT")
    except:
        pass
    conn.commit()
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
            send_telegram_message("🤖 Bot started. Commands: /pause /resume /safe_on /safe_off /toggle_safe_mode")
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


@app.route('/')
def home():
    return "Jazmin Bot is running!", 200


@app.route('/callback')
def callback():
    auth_code = request.args.get('code')
    if auth_code:
        return f"Authorization code received: {auth_code[:20]}...", 200
    return "No code provided", 400


# Initialize database and Telegram
init_db()
setup_telegram()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))
