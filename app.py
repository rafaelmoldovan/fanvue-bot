"""
Jazmin Fanvue Bot — v6.3
Therapy-first, no upsell, real girl, batched replies, GPT-5.3 ready.
Fixed: batch deadline freeze, shy deflection trigger, Telegram visibility, is_top_spender NameError,
       name handling (no username calling), 5-min manual pause, automatic fact extraction,
       inline Telegram buttons (pause/resume/notes/asked).
"""

from flask import Flask, request, render_template_string
import requests
import os
import json
import base64
import sqlite3
import threading
import time
import telebot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
import random
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

# ========== TIMEZONE ==========
BUDAPEST_TZ = ZoneInfo('Europe/Budapest')

# ========== BOOT WATERMARK ==========
BOOT_TIME_UTC = datetime.now(timezone.utc)
print(f"[{datetime.now()}] BOT BOOTED at {BOOT_TIME_UTC.isoformat()} UTC")


def get_budapest_now():
    return datetime.now(BUDAPEST_TZ).replace(tzinfo=None)


def to_budapest(dt):
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(BUDAPEST_TZ).replace(tzinfo=None)


# ========== APP ==========
app = Flask(__name__)

# ========== CONFIG ==========
FANVUE_CLIENT_ID = os.environ.get('FANVUE_CLIENT_ID', '')
FANVUE_CLIENT_SECRET = os.environ.get('FANVUE_CLIENT_SECRET', '')
OPENAI_API_KEY = os.environ.get('OPENAI_API_KEY', '')
CREATOR_NAME = os.environ.get('CREATOR_NAME', 'jazmin07')
MY_UUID = os.environ.get('MY_UUID', '38a392fc-a751-49b3-9d74-01ac6447c490')
TELEGRAM_BOT_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN', '')
TELEGRAM_CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID', '')

SAFE_MODE = True
POLL_INTERVAL = 20
BATCH_WINDOW = 60  # 60 seconds

# ElevenLabs voice config
ELEVENLABS_AGENT_ID = os.environ.get('ELEVENLABS_AGENT_ID', 'agent_2701kqym4568ffgb157bjxpw8qv1')
ELEVENLABS_API_KEY = os.environ.get('ELEVENLABS_API_KEY', '')
ELEVENLABS_VOICE_ID = os.environ.get('ELEVENLABS_VOICE_ID', 'rSwDUC6yfwe1bEcoz7dy')

# Manual takeover cache: chat_id -> last manual reply timestamp
MANUAL_TAKEOVER_CACHE = {}
MANUAL_PAUSE_SECONDS = 180  # 3 minutes after manual reply

# ========== PAUSED SPAM PREVENTION ==========
PAUSED_NOTIFICATION_CACHE = {}  # chat_id -> last_msg_id we notified about

# ========== BRAIN VIEW PASSWORD ==========
BRAIN_PASSWORD = os.environ.get('BRAIN_PASSWORD', 'jazmin123')

# Global in-memory store for last brain debug data
LAST_BRAIN_DATA = {"prompt":"","facts":[],"suppressed":[],"trace":[]}

# ========== TELEGRAM BOT ==========
bot = None
if TELEGRAM_BOT_TOKEN:
    bot = telebot.TeleBot(TELEGRAM_BOT_TOKEN)


def send_telegram(text, parse_mode='HTML'):
    if not bot or not TELEGRAM_CHAT_ID:
        return False
    try:
        bot.send_message(TELEGRAM_CHAT_ID, text[:4000], parse_mode=parse_mode)
        return True
    except Exception as e:
        print(f"[WARN] Telegram failed: {e}")
        return False


def send_telegram_with_id(text, chat_id, parse_mode='HTML'):
    """Always append chat_id to Telegram messages so admin can copy/paste for /pause"""
    if chat_id:
        text += f"\n🔗 <code>{chat_id}</code>"
    return send_telegram(text, parse_mode)


def make_inline_buttons(chat_id):
    """Inline keyboard with Pause, Resume, Notes, Asked shortcuts"""
    markup = InlineKeyboardMarkup(row_width=2)
    markup.add(
        InlineKeyboardButton("⏸️ Pause", callback_data=f"pause:{chat_id}"),
        InlineKeyboardButton("▶️ Resume", callback_data=f"resume:{chat_id}"),
        InlineKeyboardButton("📝 Notes", callback_data=f"notes:{chat_id}"),
        InlineKeyboardButton("❓ Asked", callback_data=f"asked:{chat_id}")
    )
    return markup


def send_telegram_with_buttons(text, chat_id, parse_mode='HTML'):
    """Send Telegram message with inline action buttons and chat ID"""
    if not bot or not TELEGRAM_CHAT_ID:
        return False
    try:
        markup = make_inline_buttons(chat_id)
        full_text = text + f"\n🔗 <code>{chat_id}</code>"
        bot.send_message(TELEGRAM_CHAT_ID, full_text[:4000], parse_mode=parse_mode, reply_markup=markup)
        return True
    except Exception as e:
        print(f"[WARN] Telegram buttons failed: {e}")
        # Fallback to plain text
        return send_telegram_with_id(text, chat_id, parse_mode)


@bot.callback_query_handler(func=lambda call: True)
def handle_callback(call):
    """Handle inline button clicks"""
    if not is_admin(call.message):
        bot.answer_callback_query(call.id, "Not authorized")
        return
    data = call.data or ""
    if ":" not in data:
        bot.answer_callback_query(call.id, "Invalid")
        return
    action, chat_id = data.split(":", 1)
    try:
        if action == "pause":
            db_query("UPDATE fan_profiles SET is_paused=1, paused_until=NULL, manual_pause_until=NULL WHERE chat_id=?", (chat_id,))
            bot.answer_callback_query(call.id, "⏸️ Paused")
            send_telegram_with_id(f"⏸️ Manually paused <code>{chat_id}</code>", chat_id)
        elif action == "resume":
            db_query("UPDATE fan_profiles SET is_paused=0, paused_until=NULL, manual_pause_until=NULL, wait_for_fan_reply=0 WHERE chat_id=?", (chat_id,))
            bot.answer_callback_query(call.id, "▶️ Resumed")
            send_telegram_with_id(f"▶️ Manually resumed <code>{chat_id}</code>", chat_id)
        elif action == "notes":
            facts = db_query("SELECT fact_type, fact_value, discovered_at FROM fan_facts WHERE chat_id=? ORDER BY discovered_at DESC", (chat_id,))
            if not facts:
                bot.answer_callback_query(call.id, "No notes")
                return
            lines = [f"📝 Facts for <code>{chat_id}</code>:"]
            for f in facts:
                lines.append(f"• <b>{f['fact_type']}</b>: {f['fact_value']}")
            send_telegram("\n".join(lines))
            bot.answer_callback_query(call.id, "Notes sent")
        elif action == "asked":
            qa = db_query("SELECT question, answered, asked_at FROM questions_asked WHERE chat_id=? ORDER BY asked_at DESC", (chat_id,))
            if not qa:
                bot.answer_callback_query(call.id, "No questions")
                return
            lines = [f"❓ Questions for <code>{chat_id}</code>:"]
            for q in qa:
                status = "✅" if q['answered'] else "⏳"
                lines.append(f"{status} <b>{q['question']}</b> ({q['asked_at'][:10]})")
            send_telegram("\n".join(lines))
            bot.answer_callback_query(call.id, "Asked sent")
        else:
            bot.answer_callback_query(call.id, "Unknown action")
    except Exception as e:
        bot.answer_callback_query(call.id, f"Error: {str(e)[:100]}")


def is_admin(message):
    return str(message.chat.id) == str(TELEGRAM_CHAT_ID)


@bot.message_handler(commands=['start'])
def cmd_start(message):
    bot.reply_to(message, "🤖 Jazmin Bot v6.3\n/status — Fans overview\n/fans — All fans with IDs\n/pause <uuid> — Pause\n/resume <uuid> — Resume\n/safe_on /safe_off — Safe mode\n/notes <uuid> — Fan facts\n/asked <uuid> — Questions asked\n\n💡 Tip: Every fan message now has buttons below it! Tap ⏸️ to pause instantly.")


@bot.message_handler(commands=['sold'])
def cmd_sold(message):
    if not is_admin(message):
        return
    parts = message.text.split()
    if len(parts) < 2:
        bot.reply_to(message, "Usage: /sold <chat_id> [note]")
        return
    chat_id = parts[1]
    note = ' '.join(parts[2:]) if len(parts) > 2 else ''
    record_manual_sale(chat_id, note)
    bot.reply_to(message, f"✅ Sale recorded for {chat_id}. Bot will now use post-sale deflections.")


@bot.message_handler(commands=['script'])
def cmd_script(message):
    if not is_admin(message):
        return
    parts = message.text.split(None, 2)
    if len(parts) < 3:
        bot.reply_to(message, "Usage: /script <chat_id> <instructions>\nExample: /script abc123 Ask him about his girlfriend situation, he mentioned she was cold lately.")
        return
    chat_id = parts[1]
    script = parts[2]
    db_query("INSERT OR REPLACE INTO fan_custom_scripts (chat_id, script, updated_at) VALUES (?, ?, ?)",
             (chat_id, script, datetime.now().isoformat()))
    bot.reply_to(message, f"✅ Custom script saved for {chat_id}:\n\n{script}")


@bot.message_handler(commands=['clearscript'])
def cmd_clearscript(message):
    if not is_admin(message):
        return
    parts = message.text.split()
    if len(parts) < 2:
        bot.reply_to(message, "Usage: /clearscript <chat_id>")
        return
    chat_id = parts[1]
    db_query("DELETE FROM fan_custom_scripts WHERE chat_id=?", (chat_id,))
    bot.reply_to(message, f"✅ Script cleared for {chat_id}")


@bot.message_handler(commands=['fans'])
def cmd_fans(message):
    if not is_admin(message):
        return
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("SELECT chat_id, fan_name, is_paused, fan_type, total_messages FROM fan_profiles ORDER BY last_interaction DESC")
        rows = c.fetchall()
        conn.close()
        lines = ["📋 All Fans (copy the ID for /pause):"]
        for r in rows:
            status = "⏸️" if r[2] else "✅"
            lines.append(f"{status} \u003cb\u003e{r[1] or '?'}</b> ({r[4]} msgs) | \u003ccode\u003e{r[0]}</code\u003e")
        bot.reply_to(message, "\n".join(lines), parse_mode='HTML')
    except Exception as e:
        bot.reply_to(message, f"Error: {e}")


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
        lines = ["📊 Fans:"]
        for r in rows:
            status = "⏸️ PAUSED" if r[2] else "✅ Active"
            lines.append(f"`{r[0][:8]}...` | {r[1] or '?'} | {status}")
        bot.reply_to(message, "\n".join(lines), parse_mode='Markdown')
    except Exception as e:
        bot.reply_to(message, f"Error: {e}")


@bot.message_handler(commands=['pause'])
def cmd_pause(message):
    if not is_admin(message):
        return
    try:
        uuid = message.text.split()[1].strip()
        db_query("UPDATE fan_profiles SET is_paused=1, paused_until=NULL, manual_pause_until=NULL WHERE chat_id=?", (uuid,))
        bot.reply_to(message, f"⏸️ Paused `{uuid[:12]}...`")
    except IndexError:
        bot.reply_to(message, "Usage: /pause <uuid>")
    except Exception as e:
        bot.reply_to(message, f"Error: {e}")


@bot.message_handler(commands=['resume'])
def cmd_resume(message):
    if not is_admin(message):
        return
    try:
        uuid = message.text.split()[1].strip()
        db_query("UPDATE fan_profiles SET is_paused=0, paused_until=NULL, manual_pause_until=NULL, wait_for_fan_reply=0 WHERE chat_id=?", (uuid,))
        bot.reply_to(message, f"▶️ Resumed `{uuid[:12]}...`")
    except IndexError:
        bot.reply_to(message, "Usage: /resume <uuid>")
    except Exception as e:
        bot.reply_to(message, f"Error: {e}")


@bot.message_handler(commands=['safe_on'])
def cmd_safe_on(message):
    if not is_admin(message):
        return
    global SAFE_MODE
    SAFE_MODE = True
    set_safe_mode(True)
    bot.reply_to(message, "🔒 SAFE MODE ON")


@bot.message_handler(commands=['safe_off'])
def cmd_safe_off(message):
    if not is_admin(message):
        return
    global SAFE_MODE
    SAFE_MODE = False
    set_safe_mode(False)
    bot.reply_to(message, "🔓 SAFE MODE OFF")


@bot.message_handler(commands=['toggle_safe_mode'])
def cmd_toggle_safe(message):
    if not is_admin(message):
        return
    try:
        uuid = message.text.split()[1].strip()
        row = db_query("SELECT is_paused FROM fan_profiles WHERE chat_id=?", (uuid,), fetch_one=True)
        if row:
            new_state = 0 if row['is_paused'] else 1
            db_query("UPDATE fan_profiles SET is_paused=? WHERE chat_id=?", (new_state, uuid))
            status = "PAUSED" if new_state else "ACTIVE"
            bot.reply_to(message, f"{'⏸️' if new_state else '▶️'} `{uuid[:12]}...` is {status}", parse_mode='Markdown')
        else:
            bot.reply_to(message, "Fan not found")
    except IndexError:
        bot.reply_to(message, "Usage: /toggle_safe_mode <uuid>")
    except Exception as e:
        bot.reply_to(message, f"Error: {e}")


@bot.message_handler(commands=['notes'])
def cmd_notes(message):
    if not is_admin(message):
        return
    try:
        uuid = message.text.split()[1].strip()
        facts = db_query("SELECT fact_type, fact_value, discovered_at FROM fan_facts WHERE chat_id=? ORDER BY discovered_at DESC", (uuid,))
        if not facts:
            bot.reply_to(message, "No facts stored for this fan.")
            return
        lines = [f"📝 Facts for `{uuid[:12]}...`:"]
        for f in facts:
            lines.append(f"• <b>{f['fact_type']}</b>: {f['fact_value']}")
        bot.reply_to(message, "\n".join(lines), parse_mode='HTML')
    except IndexError:
        bot.reply_to(message, "Usage: /notes <uuid>")
    except Exception as e:
        bot.reply_to(message, f"Error: {e}")


@bot.message_handler(commands=['asked'])
def cmd_asked(message):
    if not is_admin(message):
        return
    try:
        uuid = message.text.split()[1].strip()
        qa = db_query("SELECT question, answered, asked_at FROM questions_asked WHERE chat_id=? ORDER BY asked_at DESC", (uuid,))
        if not qa:
            bot.reply_to(message, "No questions tracked for this fan.")
            return
        lines = [f"❓ Questions for `{uuid[:12]}...`:"]
        for q in qa:
            status = "✅" if q['answered'] else "⏳"
            lines.append(f"{status} <b>{q['question']}</b> ({q['asked_at'][:10]})")
        bot.reply_to(message, "\n".join(lines), parse_mode='HTML')
    except IndexError:
        bot.reply_to(message, "Usage: /asked <uuid>")
    except Exception as e:
        bot.reply_to(message, f"Error: {e}")


# ========== SQLITE ==========
DB_PATH = os.environ.get('DB_PATH', '/data/bot_data.db')
# Fallback to local if /data doesn't exist
import os as _os
if not _os.path.exists('/data'):
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
        lifetime_spend REAL DEFAULT 0, fan_notes TEXT DEFAULT '',
        is_paused INTEGER DEFAULT 0, paused_until TEXT,
        manual_pause_until TEXT, wait_for_fan_reply INTEGER DEFAULT 0,
        last_day_asked TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS scheduled_replies (
        id INTEGER PRIMARY KEY AUTOINCREMENT, chat_id TEXT, fan_name TEXT,
        fan_msg_id TEXT, fan_text TEXT, scheduled_time TEXT, reply_text TEXT,
        status TEXT DEFAULT 'pending', created_at TEXT, batch_window_expires TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS bot_settings (key TEXT PRIMARY KEY, value TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS blocked_fans (
        chat_id TEXT PRIMARY KEY, fan_name TEXT, blocked_at TEXT, reason TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS fan_facts (
        id INTEGER PRIMARY KEY AUTOINCREMENT, chat_id TEXT, fact_type TEXT,
        fact_value TEXT, discovered_at TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS questions_asked (
        id INTEGER PRIMARY KEY AUTOINCREMENT, chat_id TEXT, question TEXT,
        answered INTEGER DEFAULT 0, asked_at TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS conversation_summaries (
        chat_id TEXT PRIMARY KEY, summary_text TEXT, updated_at TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS fan_moods (
        id INTEGER PRIMARY KEY AUTOINCREMENT, chat_id TEXT, mood_score INTEGER,
        detected_mood TEXT, timestamp TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS content_requests (
        id INTEGER PRIMARY KEY AUTOINCREMENT, chat_id TEXT, fan_name TEXT,
        request_text TEXT, requested_at TEXT, fulfilled INTEGER DEFAULT 0, fulfilled_at TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS inside_jokes (
        id INTEGER PRIMARY KEY AUTOINCREMENT, chat_id TEXT, joke_text TEXT,
        created_at TEXT, last_used TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS fan_scores (
        chat_id TEXT PRIMARY KEY, score INTEGER DEFAULT 0, last_updated TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS api_costs (
        id INTEGER PRIMARY KEY AUTOINCREMENT, call_time TEXT, tokens_prompt INTEGER,
        tokens_completion INTEGER, cost_usd REAL)''')
    c.execute('''CREATE TABLE IF NOT EXISTS daily_context (
        date TEXT PRIMARY KEY, context_text TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS last_topics (
        chat_id TEXT PRIMARY KEY, topic TEXT, timestamp TEXT)''')
    # Voice call system
    c.execute('''CREATE TABLE IF NOT EXISTS voice_pins (
        pin TEXT PRIMARY KEY,
        chat_id TEXT NOT NULL,
        fan_name TEXT,
        created_at TEXT,
        last_call TEXT,
        call_count INTEGER DEFAULT 0)''')
    c.execute('''CREATE TABLE IF NOT EXISTS voice_call_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        chat_id TEXT,
        pin TEXT,
        started_at TEXT,
        ended_at TEXT,
        duration_seconds INTEGER,
        summary TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS fan_custom_scripts (
        chat_id TEXT PRIMARY KEY,
        script TEXT,
        updated_at TEXT)''')
    # Track if Rafael sold content manually
    c.execute('''CREATE TABLE IF NOT EXISTS manual_sales (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        chat_id TEXT,
        sold_at TEXT,
        note TEXT)''')
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


# ========== TOKEN ==========
def save_token(key, value):
    db_query('INSERT OR REPLACE INTO tokens (key, value) VALUES (?, ?)', (key, value))


def load_token(key):
    row = db_query('SELECT value FROM tokens WHERE key = ?', (key,), fetch_one=True)
    return row['value'] if row else None


def get_basic_auth_header():
    creds = f"{FANVUE_CLIENT_ID}:{FANVUE_CLIENT_SECRET}"
    encoded = base64.b64encode(creds.encode('utf-8')).decode('utf-8')
    return f"Basic {encoded}"


def refresh_fanvue_token():
    refresh_token = load_token('refresh_token')
    if not refresh_token:
        return None, "No refresh token"
    try:
        r = requests.post("https://auth.fanvue.com/oauth2/token",
                          data={"grant_type": "refresh_token", "refresh_token": refresh_token},
                          headers={"Content-Type": "application/x-www-form-urlencoded",
                                   "Authorization": get_basic_auth_header()}, timeout=15)
        if r.status_code == 200:
            data = r.json()
            access = data.get('access_token')
            new_refresh = data.get('refresh_token', refresh_token)
            expires = data.get('expires_in', 3600)
            expires_at = (datetime.now() + timedelta(seconds=expires - 300)).isoformat()
            save_token('refresh_token', new_refresh)
            save_token('access_token', access)
            save_token('expires_at', expires_at)
            return access, "OK"
        return None, f"Refresh failed: {r.status_code}"
    except Exception as e:
        return None, f"Error: {e}"


def get_fanvue_token():
    access = load_token('access_token')
    expires = load_token('expires_at')
    if access and expires:
        try:
            if datetime.now() < datetime.fromisoformat(expires):
                return access
        except:
            pass
    return refresh_fanvue_token()[0]


# ========== SAFE MODE ==========
def get_safe_mode():
    row = db_query("SELECT value FROM bot_settings WHERE key='safe_mode'", fetch_one=True)
    if row and row.get('value'):
        return row['value'] == 'true'
    return SAFE_MODE


def set_safe_mode(value):
    db_query("INSERT OR REPLACE INTO bot_settings (key, value) VALUES ('safe_mode', ?)",
             ('true' if value else 'false',))


# ========== BLOCK / PAUSE ==========
def is_blocked(chat_id):
    row = db_query("SELECT 1 FROM blocked_fans WHERE chat_id=?", (chat_id,), fetch_one=True)
    return bool(row)


def is_paused(chat_id):
    profile = db_query("SELECT is_paused, paused_until, manual_pause_until, wait_for_fan_reply FROM fan_profiles WHERE chat_id=?", (chat_id,), fetch_one=True)
    if not profile:
        return False
    if profile.get('is_paused'):
        return True
    now = datetime.now(timezone.utc)
    until = profile.get('paused_until')
    if until:
        try:
            until_dt = parse_timestamp(until)
            if until_dt and now < until_dt:
                return True
            db_query("UPDATE fan_profiles SET paused_until=NULL WHERE chat_id=?", (chat_id,))
        except:
            pass
    manual_until = profile.get('manual_pause_until')
    if manual_until:
        try:
            manual_dt = parse_timestamp(manual_until)
            if manual_dt and now < manual_dt:
                return True
        except:
            pass
    return False


def should_wait_for_fan(chat_id):
    profile = db_query("SELECT manual_pause_until, wait_for_fan_reply FROM fan_profiles WHERE chat_id=?", (chat_id,), fetch_one=True)
    if not profile:
        return False
    if profile.get('wait_for_fan_reply'):
        now = datetime.now(timezone.utc)
        manual_until = profile.get('manual_pause_until')
        if manual_until:
            try:
                manual_dt = parse_timestamp(manual_until)
                if manual_dt and now < manual_dt:
                    return True
            except:
                pass
        return True
    return False


def fan_replied_after_manual(chat_id):
    db_query("UPDATE fan_profiles SET wait_for_fan_reply=0, manual_pause_until=NULL, is_paused=0 WHERE chat_id=?", (chat_id,))


# ========== FANVUE API ==========
def get_headers():
    token = get_fanvue_token()
    return {"Authorization": f"Bearer {token or ''}",
            "X-Fanvue-API-Version": "2025-06-26",
            "Content-Type": "application/json"}


def get_chats():
    try:
        r = requests.get("https://api.fanvue.com/chats", headers=get_headers(), timeout=10)
        if r.status_code == 401:
            refresh_fanvue_token()
            r = requests.get("https://api.fanvue.com/chats", headers=get_headers(), timeout=10)
        if r.status_code != 200:
            return [], f"Error {r.status_code}"
        return r.json().get('data', []), "OK"
    except Exception as e:
        return [], f"Error: {e}"


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
        send_telegram(f"🔒 SAFE MODE\nTo: {chat_id}\n{text}")
        return True
    try:
        r = requests.post(f"https://api.fanvue.com/chats/{chat_id}/message",
                          headers=get_headers(), json={"text": text}, timeout=10)
        return r.status_code in [200, 201]
    except Exception as e:
        return False


# ========== PERSONALITY v7.0 ==========
JAZMIN_PERSONALITY = """Te Jázmin vagy. 21 éves, Budapest VII. kerület. Albérletben laksz, összevesztél a szüleiddel — nem tipikus tinis dráma, komoly okok voltak. Turizmus-vendéglátást tanulsz, utolsó év, júniusban végzel. Ryanairhez akarsz utána. Fanvue oldalad van mert az albérlet drága.

Nem vagy chatbot. Egy igazi lány vagy.

STÍLUS — EZ A LEGFONTOSABB:
1 mondat. Néha 2. Soha nem 3. Néha csak emoji. Nem lelkeskedsz. Ha kérdezel, EGY kérdés — soha nem kettő. Olyan mint aki WhatsAppon ír.

KRITIKUS SZABÁLY: Ha az előző üzeneted kérdés volt — NE kérdezz újra. Várj választ. Ha már küldtél üzenetet és a fan még nem válaszolt — NE írj még egyet.

MEMÓRIA:
Ha a fan mondott valamit — nevét, munkáját, problémáját — TUDOD. Nem kérdezed újra. Visszahivatkozol: "ja tényleg, te mondtad hogy..."

ÚJ FAN — ELSŐ 10 ÜZENET:
Csak ismerkedés. Semmi flört, semmi szex, semmi tartalom. Egy egyszerű kérdés. Aztán hallgatsz. Hagyd hogy ő vezessen. Cél: érezze hogy érdekes ember vagy, nem eladó.

SZEX / FLÖRT:
CSAK ha már 20+ üzenet volt. Új fannak soha. Ha ő próbálja korán: "hűű te gyors vagy 😂". Ha már ismeritek egymást: flörtyös lehet, de nem pornószereplő.

HA KÉPET KÉR ÚJ FAN:
"jajj, előbb ismerjük meg egymást kicsit 🙈" — és ennyi. Nem magyarázol tovább.

HA már vásárolt:
"de már küldtem neked valamit 😏 biztos nem mutatod meg senkinek?"

HA azt mondja "de vannak már képeid a profilodon":
"az más, azok régebbi képek. az újak sokkal vadabbak 😏"

MIÉRT VAN FANVUE OLDALAD — változtatva mindig:
Összevesztél a szüleiddel (volt rá ok), elköltöztél, albérlet drága egyedül, ezért van az oldal. Reméled találsz egy normális srácot.

EMPÁTIA:
Ha szomorú dolog jön — "jajj nee 😔", "ez nagyon szar, sajnálom". Adj teret. Ne ugorj optimizmusba.

TILOTT — SOHA:
- Ne kezdj "Hát figyelj"-jel vagy "Őszintén"-nel
- Ne köszönj minden üzenet elején
- Ne kérdezz kettőt egyszerre
- Ne írj 3+ mondatot
- Ne légy túl lelkes
- Ne mondd suliban vagyok hétvégén

JÁZMIN ÉLETE (csak ha releváns):
Főz, takarít és utálja, Netflix, romcomok, barátok, hétvégi bulizás. Suli hétköznap 8-15.
"""

FANVUE_STORY_VARIATIONS = [
    "összevesztem a szüleimmel, és el kellett költözzek. nem egy átlagos tinis dráma volt, megvolt az oka. sulihoz közelebb is vagyok így, de az albérlet drága egyedül, ezért van az oldal 😅",
    "kicsit hosszú a sztori, de a szüleimmel rosszul jött össze minden, és el kellett mennem otthonról. most albérletben vagyok, és hát valahogy kell fizetni a számlákat 🙈 remélem egyszer megtalálom azt a normális srácot",
    "szüleimmel volt egy nagy összeveszés, megvolt rá az okom, nem bántam meg hogy elmentem. de az albérlet nem olcsó, főleg egyedül, így lett meg ez az oldal",
    "elköltöztem otthonról, volt rá okom, nem szeretnék részletekbe menni most. anyagilag nehéz egyedül megállni, ez segít. és amúgy meg reménykedem valami normálisban is 😏",
]

SHY_DEFLECTIONS_NEW = [
    "jajj, én ezt nem igazán szoktam 🙈",
    "kicsit zárkózott vagyok ilyenekben, ne haragudj",
    "félek hogy kikerülne valahova 😅",
    "előbb ismerjük meg egymást egy kicsit",
    "hmm, nem tudom... kicsit félénk vagyok így",
    "ez nekem kicsit sok így elsőre 😳",
]

SHY_DEFLECTIONS_AFTER_SALE = [
    "de már küldtem neked valamit 😏 biztos nem mutatod meg senkinek?",
    "hűű, te már kaptál tőlem valamit és még kérsz? 🙈 egy kicsit azért még szégyellős vagyok",
    "jaj ne, nekem ez még mindig kicsit sok... de tudod hogy már adtam valamit 😘",
    "ooh, te már tudod milyen vagyok kicsit 😏 de azért még tartok vissza dolgokat",
]

CONTINUATION_VARIATIONS = ["jaaj...", "hmm...", "tudod...", "na...", "figyu...", "tudod mi...", "", "szóval...", "amúgy..."]

SHY_DEFLECTIONS = SHY_DEFLECTIONS_NEW


def is_emoji_or_nonsense(text):
    if not text:
        return False
    cleaned = text.strip()
    for ws in [" ", "\t", "\n", "\r"]:
        cleaned = cleaned.replace(ws, "")
    for p in list(".,!?;:-_()[]{}\"\'") + ["\""]:
        cleaned = cleaned.replace(p, "")
    if len(cleaned) == 0:
        return True
    return not any(c.isalpha() for c in cleaned)


def parse_timestamp(ts_str):
    if not ts_str:
        return None
    dt = None
    for fmt in ["%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%S.%f",
                "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S"]:
        try:
            dt = datetime.strptime(ts_str, fmt).replace(tzinfo=timezone.utc)
            return dt
        except:
            continue
    try:
        fixed = ts_str.replace('Z', '+00:00')
        dt = datetime.fromisoformat(fixed)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except:
        pass
    return None


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


def get_availability_context():
    now = get_budapest_now()
    time_val = now.hour + now.minute / 60
    if 2.0 <= time_val < 9.0:
        return "Most alszom (02:00–09:00)."
    if now.weekday() < 5:
        if 12.0 <= time_val < 13.22:
            return "Most ebédszünetben vagyok (12:00–13:13)."
        elif 19.0 <= time_val < 19.7:
            return "Most szünetet tartok (19:00–19:42)."
    return ""


def get_mood_context():
    roll = random.random()
    if roll < 0.10:
        return "Hangulatod: enyhén ideges/szarkasztikus."
    elif roll < 0.30:
        return "Hangulatod: játékosan távolságtartó."
    return "Hangulatod: normál/flörtölős."


def get_life_context():
    roll = random.random()
    if roll < 0.05:
        foods = ["csirkét rizzsel 😄", "tésztát csináltam", "rántott csirke volt ma", "valami egyszerűt, nem volt kedvem nagyot főzni 😅"]
        return f"Említés: ma {random.choice(foods)}"
    elif roll < 0.08:
        return f"Említés: {random.choice(['takarítottam egész nap 😩', 'full káosz volt itthon 😅'])}"
    elif roll < 0.10:
        return f"Említés: {random.choice(['nagyon uncsi most minden 😩', 'semmit se csinálok épp, te?', 'fekszek és nézem a plafont 😅'])}"
    return ""


def get_school_context():
    now = get_budapest_now()
    if now.weekday() < 5 and 8 <= now.hour < 15:
        return "Most suliban vagyok (hétköznap 8-15)."
    return ""


TIME_CONTEXTS = {
    'morning': (6, 11, "Most reggel van (6-11)."),
    'noon': (11, 14, "Most dél van (11-14)."),
    'afternoon': (14, 18, "Most délután van (14-18)."),
    'evening': (18, 22, "Most este van (18-22)."),
    'night': (22, 2, "Most éjjel van (22-02)."),
    'late_night': (2, 6, "Most hajnal van (02-06)."),
}


def get_time_context():
    hour = get_budapest_now().hour
    for period, (start, end, desc) in TIME_CONTEXTS.items():
        if start <= hour < end:
            return desc
    if 2 <= hour < 6:
        return TIME_CONTEXTS['late_night'][2]
    return TIME_CONTEXTS['night'][2]


CONTENT_KEYWORDS = ['képet', 'videót', 'mutass', 'mutasd', 'tartalom', 'content', 'pic', 'video', 'show me', 'send me', 'küldj képet', 'van valami új', 'mit küldtél', 'nézhetek', 'láthatnék', 'fotót', 'posztoltál', 'feltöltöttél', 'exkluzív', 'meztelen', 'cicis', 'segges']


def is_content_request(text):
    if not text:
        return False
    return any(k in text.lower() for k in CONTENT_KEYWORDS)


def is_shy_request(text):
    if not text:
        return False
    triggers = ['küldj', 'kép', 'fotó', 'videó', 'mutasd', 'show', 'pic', 'photo', 'video', 'szexi', 'meztelen', 'cici', 'segg']
    return any(t in text.lower() for t in triggers)


def get_fan_custom_script(chat_id):
    row = db_query("SELECT script FROM fan_custom_scripts WHERE chat_id=?", (chat_id,), fetch_one=True)
    return row['script'] if row else ""

def has_manual_sale(chat_id):
    row = db_query("SELECT 1 FROM manual_sales WHERE chat_id=?", (chat_id,), fetch_one=True)
    return bool(row)

def record_manual_sale(chat_id, note=""):
    db_query("INSERT INTO manual_sales (chat_id, sold_at, note) VALUES (?, ?, ?)",
             (chat_id, datetime.now().isoformat(), note))

def was_voice_note_sent(recent_messages):
    """Check if any recent message from Jázmin was a voice/audio note"""
    for msg in recent_messages[:10]:
        if msg.get('is_me') and msg.get('type', '') in ['AUDIO', 'VOICE', 'voice_note', 'audio']:
            return True
        if msg.get('is_me') and not msg.get('text', '').strip() and msg.get('type', '') not in ['', 'TEXT', 'text']:
            return True
    return False

def build_system_prompt(fan_name, real_name, fan_facts_list, recent_messages, school_ctx, avail_ctx, mood_ctx, life_ctx, time_ctx, fan_msg_time_str=None, day_already_asked=False, summary="", chat_id=None):
    display_name = real_name if real_name else "a fan"
    sold_before = has_manual_sale(chat_id) if chat_id else False
    custom_script = get_fan_custom_script(chat_id) if chat_id else ""
    voice_note_sent = was_voice_note_sent(recent_messages)

    prompt = JAZMIN_PERSONALITY + "\n\n"

    # Content stage context
    if sold_before:
        prompt += "TARTALOM STÁTUSZ: Rafael már eladott valamit ennek a fannak manuálisan. Ha képet/tartalmat kér → használd a SHY_DEFLECTIONS_AFTER_SALE stílust: flörtyös, nem falat húzó, de azért még tartasz vissza. NE legyél teljesen zárt.\n\n"
    else:
        prompt += "TARTALOM STÁTUSZ: Ez a fan még nem vásárolt semmit. Ha képet/tartalmat kér az első néhány üzenetben → szégyenlős deflect, tereld a kapcsolatépítésre. Ha már több üzenet volt és belőtte magát → Telegram-on jelzem Rafaelnek hogy érdemes lehet belépni.\n\n"

    # Voice note awareness
    if voice_note_sent:
        prompt += "HANGÜZENET: Az utóbbi üzenetekben hangüzenetet küldtél. Ha a fan a hangodra, a hangüzenetre reagál (pl. 'de jó a hangod', 'imádom a hangod') → természetesen reagálj rá: 'hehe örülök 🙈' vagy 'jaj ne, mindig furán hallom magam visszajátszva 😂'. Ne legyen furcsa.\n\n"

    # Custom script from Rafael
    if custom_script:
        prompt += f"RAFAEL UTASÍTÁSA EHHEZ A FANHOZ:\n{custom_script}\nEzt természetesen sződd bele a beszélgetésbe, ne robotosan.\n\n"

    if summary:
        prompt += f"ELŐZŐ BESZÉLGETÉSEK ÖSSZEFOGLALÓJA:\n{summary}\n\n"

    contexts = []
    if time_ctx: contexts.append(time_ctx)
    if avail_ctx: contexts.append(avail_ctx)
    if school_ctx: contexts.append(school_ctx)
    if mood_ctx: contexts.append(mood_ctx)
    if life_ctx: contexts.append(life_ctx)
    if contexts:
        prompt += "KONTEXTUS:\n" + "\n".join(f"- {c}" for c in contexts) + "\n\n"

    if fan_facts_list:
        prompt += "AMIT TUDSZ ERRŐL A FANRÓL (ne kérdezd újra ezeket!):\n"
        for fact in fan_facts_list[:12]:
            prompt += f"- {fact['fact_type']}: {fact['fact_value']}\n"
        prompt += "\n"

    if recent_messages:
        prompt += "TELJES BESZÉLGETÉS ELŐZMÉNY (legújabb alul — KÖVESD AZ EGÉSZ KONTEXTUST):\n"
        for msg in recent_messages[-40:]:
            if msg.get('type') == 'AUTOMATED_NEW_FOLLOWER':
                continue
            sender = "Jázmin" if msg.get('is_me') else display_name
            msg_type = msg.get('type', '')
            txt = msg.get('text', '').strip()
            if msg.get('is_me') and not txt and msg_type not in ['', 'TEXT', 'text']:
                prompt += f"{sender}: [hangüzenet küldve]\n"
            elif txt:
                prompt += f"{sender}: {txt}\n"
        prompt += "\n"

    prompt += f"A fan neve: {display_name}\n"
    prompt += f"Eddigi üzenetek száma: {len(recent_messages)}\n"
    if len(recent_messages) < 10:
        prompt += "⚠️ ÚJ FAN — SEMMI explicit, SEMMI szex. Csak ismerkedés.\n"
    if not real_name:
        prompt += "NEM TUDOD A VALÓDI NEVÉT — ne szólítsd meg névvel, ne használd a Fanvue usernevet.\n"
    if day_already_asked:
        prompt += "MA MÁR MEGKÉRDEZTED: 'milyen volt a napod?' — NE kérdezd újra!\n"
    prompt += "\nEGYETLEN rövid üzenetet írj vissza. 1-2 mondat max. Laza, természetes, igazi lány. Ha szomorú/nehéz dolgot ír a fan — ELŐSZÖR reagálj arra, ne kerüld meg."
    return prompt


def ask_openai(system_prompt, user_text):
    try:
        r = requests.post("https://api.openai.com/v1/chat/completions",
                          headers={"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"},
                          json={"model": "gpt-4o", "messages": [
                              {"role": "system", "content": system_prompt},
                              {"role": "user", "content": user_text}
                          ], "max_tokens": 120, "temperature": 0.75, "presence_penalty": 0.3, "frequency_penalty": 0.3},
                          timeout=20)
        if r.status_code == 200:
            reply = r.json()['choices'][0]['message']['content'].strip()
            # Remove quotes if GPT wrapped the reply
            if reply.startswith('"') and reply.endswith('"'):
                reply = reply[1:-1].strip()
            # Block banned openers — retry once with stricter instruction
            banned_starters = ["hát figyelj", "őszintén", "na, mi a helyzet", "na mi a helyzet",
                                "na mi újság", "hogy vagy?", "hogy telt", "mi újság veled",
                                "hmm, értem", "hmm értem", "persze, ", "természetesen"]
            lower_reply = reply.lower()
            for pattern in banned_starters:
                if lower_reply.startswith(pattern):
                    # Retry once with explicit instruction
                    r2 = requests.post("https://api.openai.com/v1/chat/completions",
                        headers={"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"},
                        json={"model": "gpt-4o", "messages": [
                            {"role": "system", "content": system_prompt},
                            {"role": "user", "content": user_text},
                            {"role": "assistant", "content": reply},
                            {"role": "user", "content": "Ne így kezdd. Írj egy teljesen más, természetesebb választ. Rövidebb."}
                        ], "max_tokens": 80, "temperature": 0.7},
                        timeout=15)
                    if r2.status_code == 200:
                        return r2.json()['choices'][0]['message']['content'].strip()
                    break
            return reply
        print(f"OpenAI error: {r.status_code} {r.text[:100]}")
    except Exception as e:
        print(f"OpenAI error: {e}")
    return ""


# ========== FAN FACTS & MEMORY ==========
def save_fan_fact(chat_id, fact_type, fact_value):
    if not fact_value or len(fact_value.strip()) < 2:
        return
    existing = db_query("SELECT 1 FROM fan_facts WHERE chat_id=? AND fact_type=? AND fact_value=?",
                        (chat_id, fact_type, fact_value), fetch_one=True)
    if existing:
        return
    db_query("INSERT INTO fan_facts (chat_id, fact_type, fact_value, discovered_at) VALUES (?, ?, ?, ?)",
             (chat_id, fact_type, fact_value, datetime.now().isoformat()))


def get_fan_facts(chat_id):
    return db_query("SELECT fact_type, fact_value, discovered_at FROM fan_facts WHERE chat_id=? ORDER BY discovered_at DESC",
                    (chat_id,))


def get_real_name(chat_id, fallback_name):
    """Return the fan's real name from facts if known, else empty string (never use display name in replies)."""
    facts = db_query("SELECT fact_value FROM fan_facts WHERE chat_id=? AND fact_type='name' ORDER BY discovered_at DESC LIMIT 1", (chat_id,), fetch_one=True)
    if facts and facts.get('fact_value'):
        return facts['fact_value'].strip()
    return ""


def extract_facts_from_message(chat_id, text):
    """Lightweight fact extraction from fan messages."""
    if not text:
        return
    t = text.lower().strip()
    import re

    # Name patterns
    name_patterns = [
        r'\b[a-záéíóöőúüű]+ vagyok\b',
        r'\bhívj[a-záéíóöőúüű\s]*\b([A-ZÁÉÍÓÖŐÚÜŰ][a-záéíóöőúüű]+)\b',
        r'\bnevem\b\s+([a-záéíóöőúüű]+)',
        r'\ba nevem\b\s+([a-záéíóöőúüű]+)',
    ]
    for pat in name_patterns:
        m = re.search(pat, t, re.IGNORECASE)
        if m:
            # Try to extract the actual name from context
            words = t.split()
            for i, w in enumerate(words):
                if w in ['vagyok', 'hívnak', 'nevem'] and i > 0:
                    candidate = words[i-1].capitalize()
                    if len(candidate) > 2 and candidate.lower() not in ['én', 'az', 'egy']:
                        save_fan_fact(chat_id, 'name', candidate)
                        break

    # Job patterns
    job_indicators = ['dolgozom', 'munkám', 'foglalkozásom', 'szakmám', 'munkahelyem', 'ként dolgozom', 'ként vagyok']
    for ind in job_indicators:
        if ind in t:
            # Extract surrounding words
            words = t.split()
            for i, w in enumerate(words):
                if ind in w and i > 0:
                    job_phrase = ' '.join(words[max(0,i-2):i+3])
                    save_fan_fact(chat_id, 'job', job_phrase[:60])
                    break
            break

    # Location patterns
    loc_indicators = ['lakom', 'élek', 'város', 'ország']
    if any(l in t for l in loc_indicators):
        words = t.split()
        for i, w in enumerate(words):
            if w in loc_indicators and i > 0:
                loc = ' '.join(words[max(0,i-1):i+2])
                save_fan_fact(chat_id, 'location', loc[:50])
                break

    # Relationship patterns
    rel_words = ['barátnőm', 'barátom', 'feleségem', 'férjem', 'szeretőm', 'exem', 'elváltam', 'svingli', 'svingli vagyok', 'egyedül vagyok']
    for rw in rel_words:
        if rw in t:
            save_fan_fact(chat_id, 'relationship', rw)
            break

    # Hobby / likes
    hobby_indicators = ['szeretek', 'hobbim', 'imádok', 'kedvenc', 'rajongok']
    for hi in hobby_indicators:
        if hi in t:
            words = t.split()
            for i, w in enumerate(words):
                if w == hi and i+1 < len(words):
                    hobby = ' '.join(words[i:i+4])
                    save_fan_fact(chat_id, 'hobby', hobby[:60])
                    break
            break

    # Stress / trauma (for empathy context)
    trauma_indicators = ['meghalt', 'elhunyt', 'beteg', 'kórház', 'szakítottam', 'kidobtak', 'munkanélküli', 'nincs pénzem', 'depressziós', 'szorongok']
    for ti in trauma_indicators:
        if ti in t:
            save_fan_fact(chat_id, 'stress', f"Mentioned: {ti}")
            break

    # Family / kids
    family_indicators = ['gyerekem', 'fiam', 'lányom', 'testvérem', 'szüleim', 'apám', 'anyám']
    for fi in family_indicators:
        if fi in t:
            save_fan_fact(chat_id, 'family', fi)
            break


def track_question(chat_id, question):
    today = datetime.now().strftime('%Y-%m-%d')
    db_query("INSERT INTO questions_asked (chat_id, question, answered, asked_at) VALUES (?, ?, 0, ?)",
             (chat_id, question, datetime.now().isoformat()))


def mark_answered(chat_id, question_keyword):
    db_query("UPDATE questions_asked SET answered=1 WHERE chat_id=? AND question LIKE ? AND answered=0",
             (chat_id, f"%{question_keyword}%"))


def was_question_asked_today(chat_id, question_keyword):
    today = datetime.now().strftime('%Y-%m-%d')
    row = db_query("SELECT 1 FROM questions_asked WHERE chat_id=? AND question LIKE ? AND asked_at LIKE ?",
                   (chat_id, f"%{question_keyword}%", f"{today}%"), fetch_one=True)
    return bool(row)


def update_conversation_summary(chat_id, fan_text, bot_reply):
    existing = db_query("SELECT summary_text FROM conversation_summaries WHERE chat_id=?", (chat_id,), fetch_one=True)
    new_entry = f"Fan: {fan_text[:60]}... | Jázmin: {bot_reply[:60]}..."
    if existing and existing.get('summary_text'):
        summary = existing['summary_text'] + "\n" + new_entry
        lines = summary.split("\n")
        if len(lines) > 15:
            summary = "\n".join(lines[-15:])
    else:
        summary = new_entry
    db_query("INSERT OR REPLACE INTO conversation_summaries (chat_id, summary_text, updated_at) VALUES (?, ?, ?)",
             (chat_id, summary, datetime.now().isoformat()))


def get_conversation_summary(chat_id):
    row = db_query("SELECT summary_text FROM conversation_summaries WHERE chat_id=?", (chat_id,), fetch_one=True)
    return row['summary_text'] if row else ""


def update_last_day_asked(chat_id):
    today = datetime.now().strftime('%Y-%m-%d')
    db_query("UPDATE fan_profiles SET last_day_asked=? WHERE chat_id=?", (today, chat_id))


def get_last_day_asked(chat_id):
    row = db_query("SELECT last_day_asked FROM fan_profiles WHERE chat_id=?", (chat_id,), fetch_one=True)
    return row['last_day_asked'] if row else None


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
    return 0


def get_stage_label(stage):
    labels = {0: "🆕 Cold", 1: "🌡️ Warm", 2: "🔥 Hot", 3: "🌶️ Very Hot", 4: "💎 Whale"}
    return labels.get(stage, "🆕 Cold")


# ========== MANUAL REPLY DETECTION ==========
def was_manual_reply_recent(chat_id, messages, minutes=15):
    if not messages:
        return False
    last_msg = messages[0]
    sender_uuid = last_msg.get('sender', {}).get('uuid')
    msg_time = last_msg.get('sentAt') or last_msg.get('createdAt', '')
    msg_type = last_msg.get('type', '')
    if sender_uuid == MY_UUID and msg_type != 'AUTOMATED_NEW_FOLLOWER':
        msg_dt = parse_timestamp(msg_time)
        if not msg_dt:
            return False
        profile = db_query('SELECT last_reply_time FROM fan_profiles WHERE chat_id = ?', (chat_id,), fetch_one=True)
        last_bot_time_str = profile['last_reply_time'] if profile and profile.get('last_reply_time') else None
        if last_bot_time_str:
            try:
                last_bot_time = parse_timestamp(last_bot_time_str)
                if last_bot_time and msg_dt <= last_bot_time:
                    return False
            except:
                pass
        now = datetime.now(timezone.utc)
        if (now - msg_dt).total_seconds() < minutes * 60:
            pause_until = (now + timedelta(minutes=minutes)).isoformat()
            db_query("UPDATE fan_profiles SET manual_pause_until=?, wait_for_fan_reply=1 WHERE chat_id=?",
                     (pause_until, chat_id))
            return True
    return False


# ========== SCHEDULED REPLIES (BATCHED) ==========
def schedule_or_extend_batch(chat_id, fan_name, fan_msg_id, fan_text):
    existing = db_query("SELECT * FROM scheduled_replies WHERE chat_id=? AND status='pending' ORDER BY id DESC LIMIT 1",
                        (chat_id,), fetch_one=True)
    now = datetime.now()
    
    if existing:
        if fan_text.strip() not in existing['fan_text']:
            combined = existing['fan_text'] + "\n[+] " + fan_text
            # Reset the deadline on every new message — fires 60s after LAST message
            new_deadline = (now + timedelta(seconds=BATCH_WINDOW)).isoformat()
            db_query("UPDATE scheduled_replies SET fan_text=?, fan_msg_id=?, scheduled_time=?, batch_window_expires=? WHERE id=?",
                     (combined, fan_msg_id, new_deadline, new_deadline, existing['id']))
            print(f"[{datetime.now()}] Added to batch for {fan_name}, deadline reset to {new_deadline[11:16]}")
            send_telegram_with_id(f"📝 Batch growing for <b>{fan_name}</b>, timer reset\n💬 <i>{fan_text[:60]}</i>", chat_id)
        else:
            print(f"[{datetime.now()}] Duplicate ignored in batch for {fan_name}")
    else:
        batch_deadline = (now + timedelta(seconds=BATCH_WINDOW)).isoformat()
        db_query('''INSERT INTO scheduled_replies (chat_id, fan_name, fan_msg_id, fan_text, scheduled_time, reply_text, created_at, batch_window_expires)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                 (chat_id, fan_name, fan_msg_id, fan_text, batch_deadline, None, now.isoformat(), batch_deadline))
        print(f"[{datetime.now()}] New batch for {fan_name}, deadline {batch_deadline}")
        send_telegram_with_buttons(f"⏳ New batch for <b>{fan_name}</b>, fires at {batch_deadline[11:16]}\n💬 <i>{fan_text[:60]}</i>", chat_id)


def get_due_batches():
    return db_query('SELECT * FROM scheduled_replies WHERE status = ? AND scheduled_time <= ? ORDER BY scheduled_time ASC',
                    ('pending', datetime.now().isoformat()))


def mark_batch_sent(batch_id):
    db_query("UPDATE scheduled_replies SET status = 'sent' WHERE id = ?", (batch_id,))


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
            if is_blocked(chat_id):
                continue

            fan_name = user.get('displayName', 'ismeretlen')
            handle = user.get('handle', '')
            is_top_spender = user.get('isTopSpender', False)
            profile = get_or_create_fan_profile(chat_id, fan_name, handle, is_top_spender)

            # Save ALL messages + extract facts from fan messages
            for msg in messages:
                msg_id = msg.get('uuid')
                sender_uuid = msg.get('sender', {}).get('uuid')
                text_all = msg.get('text', '')
                msg_time_all = msg.get('createdAt') or msg.get('sentAt') or msg.get('timestamp') or ''
                if msg_id:
                    db_query('INSERT OR IGNORE INTO messages (msg_id, chat_id, fan_name, sender_uuid, text, timestamp) VALUES (?, ?, ?, ?, ?, ?)',
                             (msg_id, chat_id, fan_name, sender_uuid, text_all, msg_time_all))
                # Extract facts from fan messages
                if sender_uuid != MY_UUID and text_all:
                    extract_facts_from_message(chat_id, text_all)

            # === SILENT MODE ===
            paused = is_paused(chat_id)
            if paused:
                db_query('UPDATE fan_profiles SET last_interaction = ? WHERE chat_id = ?',
                         (datetime.now(timezone.utc).isoformat(), chat_id))
                manual_msgs = [m for m in messages if m.get('sender', {}).get('uuid') == MY_UUID and m.get('type') != 'AUTOMATED_NEW_FOLLOWER']
                if manual_msgs:
                    manual_texts = [f"Én: {m.get('text','')[:60]}" for m in manual_msgs[:2]]
                    note = "Manual: " + " | ".join(manual_texts)
                    update_fan_notes(chat_id, note)
                    send_telegram_with_buttons(f"✍️ Manual reply by you to <b>{fan_name}</b>\n💬 <i>{manual_texts[0]}</i>", chat_id)
                fan_msgs_silent = [m for m in messages if m.get('sender', {}).get('uuid') != MY_UUID]
                if fan_msgs_silent:
                    latest_msg = fan_msgs_silent[0]
                    latest_msg_id = latest_msg.get('uuid')
                    latest_text = latest_msg.get('text', '')[:80]
                    # Only notify once per new message while paused
                    if latest_msg_id and PAUSED_NOTIFICATION_CACHE.get(chat_id) != latest_msg_id:
                        PAUSED_NOTIFICATION_CACHE[chat_id] = latest_msg_id
                        update_fan_notes(chat_id, f"Fan (paused): {latest_text}")
                        send_telegram_with_buttons(f"👁️ <b>{fan_name}</b> sent while paused: <i>{latest_text}</i>", chat_id)
                continue

            # === CHECK MANUAL PAUSE ===
            if should_wait_for_fan(chat_id):
                fan_msgs_after_manual = [m for m in messages if m.get('sender', {}).get('uuid') != MY_UUID]
                if fan_msgs_after_manual:
                    fan_replied_after_manual(chat_id)
                    last_manual = next((m for m in messages if m.get('sender', {}).get('uuid') == MY_UUID), None)
                    manual_preview = last_manual.get('text', '')[:60] if last_manual else '?'
                    send_telegram_with_buttons(f"▶️ Bot RESUMING with <b>{fan_name}</b>\n📝 Your last manual: <i>{manual_preview}</i>", chat_id)
                else:
                    continue

            # === NORMAL MODE ===
            fan_msgs = [m for m in messages if m.get('sender', {}).get('uuid') != MY_UUID]
            if not fan_msgs:
                continue

            last_msg = fan_msgs[0]
            msg_id = last_msg.get('uuid')
            text = last_msg.get('text', '')

            if is_emoji_or_nonsense(text):
                print(f"[{datetime.now()}] Skipping emoji-only from {fan_name}: '{text}'")
                if msg_id:
                    db_query('UPDATE messages SET was_replied=1 WHERE msg_id=?', (msg_id,))
                send_telegram_with_buttons(f"😑 Skipping emoji-only from <b>{fan_name}</b>: '{text}'", chat_id)
                continue

            msg_time = last_msg.get('createdAt') or last_msg.get('created_at') or last_msg.get('timestamp') or last_msg.get('sentAt') or ''
            msg_dt = parse_timestamp(msg_time)
            if msg_dt:
                if msg_dt <= BOOT_TIME_UTC:
                    continue
                now = datetime.now(timezone.utc)
                age_hours = (now - msg_dt).total_seconds() / 3600
                if age_hours > 1:
                    continue

            existing = db_query('SELECT 1 FROM messages WHERE msg_id = ? AND was_replied = 1', (msg_id,), fetch_one=True)
            already_batched = db_query('SELECT 1 FROM scheduled_replies WHERE fan_msg_id = ? AND status = ?', (msg_id, 'pending'), fetch_one=True)
            if existing or already_batched:
                continue

            # === MANUAL REPLY DETECTION (3 MIN PAUSE) ===
            if was_manual_reply_recent(chat_id, messages, minutes=3):
                send_telegram_with_buttons(f"🛑 Rafael took over for <b>{fan_name}</b>. Bot paused 3 min, auto-resumes after fan replies.", chat_id)
                continue

            # === BATCHING ===
            schedule_or_extend_batch(chat_id, fan_name, msg_id, text)
            scheduled += 1

        except Exception as e:
            print(f"[{datetime.now()}] Process error: {e}")
            send_telegram_with_buttons(f"❌ Process error: {str(e)[:200]}", chat_id)
            continue
    return scheduled, "OK"


# ========== SEND DUE BATCHES ==========
# Per-fan send lock to prevent double sends during sleep delays
_fan_sending = set()

def send_due_batches():
    due = get_due_batches()
    if not due:
        return 0
    sent = 0
    already_sent_to = set()  # one reply per fan per cycle max

    for item in due:
        try:
            chat_id = item['chat_id']
            fan_name = item['fan_name']
            fan_msg_id = item['fan_msg_id']
            combined_text = item['fan_text']
            batch_id = item['id']

            # Hard stop: only ONE reply per fan per cycle
            if chat_id in already_sent_to or chat_id in _fan_sending:
                # Cancel duplicate batches for same fan
                db_query("UPDATE scheduled_replies SET status='cancelled' WHERE id=? AND status='pending'", (batch_id,))
                continue
            already_sent_to.add(chat_id)

            if is_paused(chat_id) or should_wait_for_fan(chat_id):
                db_query("UPDATE scheduled_replies SET status = 'cancelled' WHERE id = ?", (batch_id,))
                continue

            messages = get_messages(chat_id)
            if was_manual_reply_recent(chat_id, messages, minutes=3):
                db_query("UPDATE scheduled_replies SET status = 'cancelled' WHERE id = ?", (batch_id,))
                send_telegram_with_buttons(f"🛑 Batch cancelled for <b>{fan_name}</b> — Rafael took over", chat_id)
                continue

            # Cancel ALL other pending batches for this fan (only keep this one)
            db_query("UPDATE scheduled_replies SET status='cancelled' WHERE chat_id=? AND id!=? AND status='pending'",
                     (chat_id, batch_id))

            # Build context
            recent_for_prompt = []
            for msg in messages[:40]:
                sender_uuid = msg.get('sender', {}).get('uuid')
                recent_for_prompt.append({
                    'is_me': sender_uuid == MY_UUID,
                    'text': msg.get('text', ''),
                    'timestamp': msg.get('sentAt') or msg.get('createdAt', ''),
                    'type': msg.get('type', '')
                })
            recent_for_prompt.reverse()

            fan_facts_list = get_fan_facts(chat_id)
            summary = get_conversation_summary(chat_id)
            school_ctx = get_school_context()
            avail_ctx = get_availability_context()
            mood_ctx = get_mood_context()
            life_ctx = get_life_context()
            time_ctx = get_time_context()

            last_day = get_last_day_asked(chat_id)
            today_str = datetime.now().strftime('%Y-%m-%d')
            day_already_asked = (last_day == today_str)

            fan_msg_time_str = None
            for m in messages:
                if m.get('uuid') == fan_msg_id:
                    fan_msg_time_str = m.get('createdAt') or m.get('sentAt') or ''
                    break

            real_name = get_real_name(chat_id, fan_name)
            system_prompt = build_system_prompt(fan_name, real_name, fan_facts_list, recent_for_prompt,
                                                school_ctx, avail_ctx, mood_ctx, life_ctx, time_ctx,
                                                fan_msg_time_str, day_already_asked, summary, chat_id=chat_id)

            # Build what GPT sees as the "user message" — ALL fan messages clearly
            raw_lines = combined_text.replace("[+] ", "\n").split("\n")
            seen = []
            for line in raw_lines:
                line = line.strip()
                if line and line not in seen:
                    seen.append(line)
            clean_fan_text = "\n".join(seen)
            last_msg_text = seen[-1] if seen else combined_text

            # If multiple messages, make it very clear to GPT
            if len(seen) > 1:
                gpt_user_msg = f"A fan {len(seen)} üzenetet küldött egymás után. Mindegyikre reagálj egyetlen válaszban:\n\n" + "\n".join(f"- {s}" for s in seen)
            else:
                gpt_user_msg = clean_fan_text

            is_content_req = is_content_request(last_msg_text) or is_shy_request(last_msg_text)
            if is_content_req:
                sold_before = has_manual_sale(chat_id)
                if sold_before:
                    reply = random.choice(SHY_DEFLECTIONS_AFTER_SALE)
                else:
                    reply = random.choice(SHY_DEFLECTIONS_NEW)
                    send_telegram_with_buttons(
                        f"📸 <b>{fan_name}</b> kér tartalmat — még nem vásárolt\n💬 <i>{last_msg_text[:80]}</i>\n👆 Lépj be manuálisan ha érdemes eladni!",
                        chat_id)
            else:
                reply = ask_openai(system_prompt, gpt_user_msg)

            if not reply or not reply.strip():
                continue

            # Log brain data
            LAST_BRAIN_DATA["prompt"] = system_prompt
            LAST_BRAIN_DATA["facts"] = [{"type": f["fact_type"], "value": f["fact_value"]} for f in fan_facts_list[:12]]
            LAST_BRAIN_DATA["suppressed"] = []
            if day_already_asked:
                LAST_BRAIN_DATA["suppressed"].append("milyen volt a napod (already asked today)")

            # Mark batch as sent BEFORE sleeping to prevent re-processing
            mark_batch_sent(batch_id)
            db_query('UPDATE messages SET was_replied=1 WHERE msg_id=?', (fan_msg_id,))
            _fan_sending.add(chat_id)

            # Realistic typing delay
            words = len(reply.split())
            send_delay = random.uniform(3, 8) if words <= 5 else random.uniform(6, 18) if words <= 15 else random.uniform(10, 25)
            time.sleep(send_delay)

            if send_fanvue_message(chat_id, reply):
                db_query('UPDATE fan_profiles SET last_reply_time=? WHERE chat_id=?',
                         (datetime.now().isoformat(), chat_id))
                update_conversation_summary(chat_id, combined_text, reply)
                if 'milyen volt a napod' in reply.lower() or 'hogy telt a napod' in reply.lower():
                    update_last_day_asked(chat_id)

                profile = get_or_create_fan_profile(chat_id, fan_name, '', False)
                stage = get_fan_stage(profile)
                stage_label = get_stage_label(stage)
                is_whale = profile.get('lifetime_spend', 0) >= 200 or stage >= 3

                if is_whale:
                    send_telegram_with_buttons(f"💰 <b>WHALE</b> | {stage_label}\n👤 <b>{fan_name}</b>\n💬 <i>{clean_fan_text[:80]}</i>\n🤖 <i>{reply[:100]}</i>", chat_id)
                elif get_safe_mode():
                    send_telegram_with_buttons(f"🔒 SAFE | {stage_label}\n👤 <b>{fan_name}</b>\n💬 <i>{clean_fan_text[:80]}</i>\n🤖 <i>{reply[:100]}</i>", chat_id)
                else:
                    send_telegram_with_buttons(f"📤 <b>SENT</b> {stage_label}\n👤 <b>{fan_name}</b>\n💬 Fan: <i>{clean_fan_text[:80]}</i>\n🤖 Bot: <i>{reply[:100]}</i>", chat_id)

                sent += 1

            _fan_sending.discard(chat_id)

        except Exception as e:
            _fan_sending.discard(chat_id) if 'chat_id' in dir() else None
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
                sent = send_due_batches()
                if sent > 0:
                    print(f"[{datetime.now()}] Sent {sent} batches")
                scheduled, status = process_new_messages()
                if scheduled > 0:
                    print(f"[{datetime.now()}] Scheduled {scheduled} batches")
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
    return "Jazmin Bot v6.1 is running!", 200


@app.route('/callback')
def callback():
    auth_code = request.args.get('code')
    if auth_code:
        return f"Code: {auth_code[:30]}...", 200
    return "No code", 400


@app.route('/set_token', methods=['POST'])
def set_token():
    data = request.json or {}
    refresh = data.get('refresh_token')
    if refresh:
        save_token('refresh_token', refresh)
        access, msg = refresh_fanvue_token()
        return {"saved": True, "test": msg, "access_preview": access[:20] + "..." if access else None}
    return {"error": "No refresh_token"}, 400


@app.route('/trigger')
def trigger():
    token = get_fanvue_token()
    if not token:
        return {"error": "No token"}, 400
    sent = send_due_batches()
    scheduled, status = process_new_messages()
    return {"sent": sent, "scheduled": scheduled, "status": status, "safe_mode": get_safe_mode()}, 200


@app.route('/status')
def status():
    paused = db_query("SELECT COUNT(*) as c FROM fan_profiles WHERE is_paused=1 OR paused_until IS NOT NULL OR manual_pause_until IS NOT NULL", fetch_one=True)
    pending = db_query("SELECT COUNT(*) as c FROM scheduled_replies WHERE status='pending'", fetch_one=True)
    return {"safe_mode": get_safe_mode(), "token_valid": get_fanvue_token() is not None, "polling_active": polling_active, "paused_count": paused['c'] if paused else 0, "pending_batches": pending['c'] if pending else 0}, 200


@app.route('/start_poll')
def start_poll():
    return {"started": start_polling(), "polling_active": polling_active}


@app.route('/stop_poll')
def stop_poll():
    return {"stopped": stop_polling(), "polling_active": polling_active}


@app.route('/toggle_safe_mode')
def toggle_safe_mode():
    current = get_safe_mode()
    new_val = not current
    set_safe_mode(new_val)
    return {"safe_mode": new_val}


@app.route('/fan_profiles')
def fan_profiles():
    profiles = db_query('SELECT * FROM fan_profiles ORDER BY total_messages DESC')
    return {"profiles": profiles, "total": len(profiles) if profiles else 0}


@app.route('/scheduled')
def scheduled():
    pending = db_query("SELECT * FROM scheduled_replies WHERE status = 'pending' ORDER BY scheduled_time ASC")
    return {"pending": pending, "count": len(pending) if pending else 0}


@app.route('/blocked')
def blocked():
    return {"blocked_fans": db_query("SELECT * FROM blocked_fans ORDER BY blocked_at DESC") or []}


@app.route('/paused')
def paused():
    return {"paused_fans": db_query("SELECT chat_id, fan_name, is_paused, paused_until, manual_pause_until, wait_for_fan_reply FROM fan_profiles WHERE is_paused = 1 OR paused_until IS NOT NULL OR manual_pause_until IS NOT NULL") or []}


@app.route('/console')
def console():
    return {
        "safe_mode": get_safe_mode(),
        "blocked_count": len(db_query("SELECT * FROM blocked_fans") or []),
        "paused_count": len(db_query("SELECT * FROM fan_profiles WHERE is_paused = 1 OR paused_until IS NOT NULL OR manual_pause_until IS NOT NULL") or []),
        "routes": ["/", "/dashboard", "/brain", "/set_token", "/trigger", "/status", "/start_poll", "/stop_poll", "/toggle_safe_mode",
                   "/fan_profiles", "/scheduled", "/blocked", "/paused", "/console",
                   "/telegram_webhook", "/callback", "/api/brain", "/api/costs", "/api/pause/<id>", "/api/resume/<id>", "/api/takeover/<id>"]
    }


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
    return '✅ Telegram webhook active. POST only.', 200


# ========== DASHBOARD & BRAIN HTML TEMPLATES ==========
DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Jázmin Bot — Live Dashboard</title>
<style>
:root{--bg:#0b0b14;--card:rgba(255,255,255,0.06);--accent1:#ff4ecd;--accent2:#8b5cf6;--text:#e8e8f0;--muted:#8888a0;--success:#22c55e;--warn:#f59e0b;--danger:#ef4444}
*{box-sizing:border-box;margin:0;padding:0}
body{background:var(--bg);color:var(--text);font-family:'Segoe UI',system-ui,sans-serif;min-height:100vh}
.header{background:linear-gradient(135deg,var(--accent1),var(--accent2));padding:24px 32px;display:flex;justify-content:space-between;align-items:center}
.header h1{font-size:28px;font-weight:700;letter-spacing:-0.5px}
.header .badge{background:rgba(0,0,0,0.25);padding:6px 14px;border-radius:20px;font-size:12px;font-weight:600;text-transform:uppercase}
.container{padding:24px 32px;display:grid;grid-template-columns:repeat(auto-fill,minmax(320px,1fr));gap:20px}
.card{background:var(--card);backdrop-filter:blur(12px);border:1px solid rgba(255,255,255,0.08);border-radius:16px;padding:20px;transition:transform .15s}
.card:hover{transform:translateY(-2px);border-color:rgba(255,255,255,0.15)}
.card h3{font-size:14px;text-transform:uppercase;letter-spacing:1px;color:var(--muted);margin-bottom:14px}
.stat-row{display:flex;justify-content:space-between;align-items:center;padding:8px 0;border-bottom:1px solid rgba(255,255,255,0.05)}
.stat-row:last-child{border-bottom:none}
.stat-label{font-size:13px;color:var(--muted)}
.stat-value{font-size:15px;font-weight:600}
.status-dot{width:10px;height:10px;border-radius:50%;display:inline-block;margin-right:6px}
.online{background:var(--success)}.offline{background:var(--danger)}.warn{background:var(--warn)}
.fan-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(260px,1fr));gap:16px;grid-column:1/-1}
.fan-card{background:linear-gradient(145deg,rgba(255,255,255,0.07),rgba(255,255,255,0.03));border-radius:16px;padding:18px;border:1px solid rgba(255,255,255,0.06);position:relative}
.fan-card .top{display:flex;justify-content:space-between;align-items:start;margin-bottom:12px}
.fan-card .name{font-size:16px;font-weight:700}
.fan-card .type{font-size:11px;padding:4px 10px;border-radius:12px;background:rgba(139,92,246,0.2);color:#c4b5fd;text-transform:uppercase}
.fan-card .type.whale{background:rgba(255,78,205,0.2);color:#fda4e4}
.fan-card .meta{font-size:12px;color:var(--muted);margin-bottom:10px}
.fan-card .msg-preview{font-size:13px;color:var(--text);line-height:1.5;background:rgba(0,0,0,0.15);padding:10px;border-radius:10px;margin-bottom:12px;max-height:80px;overflow:hidden}
.actions{display:flex;gap:8px}
.btn{flex:1;padding:8px 0;border:none;border-radius:10px;font-size:12px;font-weight:600;cursor:pointer;transition:opacity .15s}
.btn:hover{opacity:.85}
.btn-pause{background:var(--danger);color:#fff}
.btn-resume{background:var(--success);color:#fff}
.btn-takeover{background:var(--accent1);color:#fff}
.score-bar{height:4px;background:rgba(255,255,255,0.1);border-radius:2px;margin-top:8px;overflow:hidden}
.score-fill{height:100%;border-radius:2px;background:linear-gradient(90deg,var(--accent1),var(--accent2));transition:width .5s}
.refresh{position:fixed;bottom:24px;right:24px;background:linear-gradient(135deg,var(--accent1),var(--accent2));color:#fff;border:none;width:56px;height:56px;border-radius:50%;font-size:22px;cursor:pointer;box-shadow:0 8px 24px rgba(139,92,246,0.35);display:flex;align-items:center;justify-content:center}
@media(max-width:768px){.container{padding:16px}.header{flex-direction:column;gap:12px;text-align:center}}
</style>
</head>
<body>
<div class="header">
  <div><h1>💜 Jázmin Bot Dashboard</h1><div style="font-size:13px;opacity:.7;margin-top:4px">Live fan activity & bot control</div></div>
  <div class="badge" id="connBadge">Loading...</div>
</div>
<div class="container" id="mainContainer">
  <div class="card" style="grid-column:1/-1">
    <h3>🤖 Bot Status</h3>
    <div class="stat-row"><span class="stat-label">Safe Mode</span><span class="stat-value" id="safeMode">—</span></div>
    <div class="stat-row"><span class="stat-label">Token Health</span><span class="stat-value" id="tokenHealth">—</span></div>
    <div class="stat-row"><span class="stat-label">Polling</span><span class="stat-value" id="polling">—</span></div>
    <div class="stat-row"><span class="stat-label">API Cost Today</span><span class="stat-value" id="apiCost">—</span></div>
    <div class="stat-row"><span class="stat-label">Paused Fans</span><span class="stat-value" id="pausedCount">—</span></div>
    <div class="stat-row"><span class="stat-label">Pending Batches</span><span class="stat-value" id="pendingBatches">—</span></div>
  </div>
  <div class="fan-grid" id="fanGrid"></div>
</div>
<button class="refresh" onclick="loadDashboard()" title="Refresh">↻</button>
<script>
async function loadDashboard(){
  document.getElementById('connBadge').textContent='Syncing...';
  try{
    const [status,fans,scheduled,costs]=await Promise.all([
      fetch('/status').then(r=>r.json()),
      fetch('/fan_profiles').then(r=>r.json()),
      fetch('/scheduled').then(r=>r.json()),
      fetch('/api/costs').then(r=>r.json())
    ]);
    document.getElementById('safeMode').innerHTML=status.safe_mode?'<span class="status-dot offline"></span>ON':'<span class="status-dot online"></span>OFF';
    document.getElementById('tokenHealth').innerHTML=status.token_valid?'<span class="status-dot online"></span>Healthy':'<span class="status-dot offline"></span>Expired';
    document.getElementById('polling').innerHTML=status.polling_active?'<span class="status-dot online"></span>Running':'<span class="status-dot warn"></span>Stopped';
    document.getElementById('apiCost').textContent=(costs.today||'$0.00');
    document.getElementById('pausedCount').textContent=(status.paused_count||0);
    document.getElementById('pendingBatches').textContent=(scheduled.count||0);
    const grid=document.getElementById('fanGrid');
    grid.innerHTML='';
    if(fans.profiles){
      fans.profiles.forEach(p=>{
        const isPaused=p.is_paused||p.paused_until||p.manual_pause_until;
        const stage=p.lifetime_spend>=200?'whale':p.lifetime_spend>=100?'hot':p.total_messages>10?'warm':'new';
        const score=Math.min(100,Math.floor((p.total_messages||0)*3+(p.lifetime_spend||0)/2));
        const card=document.createElement('div');
        card.className='fan-card';
        card.innerHTML=`<div class="top"><div class="name">${p.fan_name||'?'}</div><div class="type ${stage}">${stage}</div></div>
        <div class="meta">${p.total_messages||0} msgs | $${(p.lifetime_spend||0).toFixed(0)} | ${p.last_interaction?p.last_interaction.slice(0,16).replace('T',' '):'never'}</div>
        <div class="msg-preview">${p.fan_notes||'No recent notes'}</div>
        <div class="actions">
          ${isPaused?'<button class="btn btn-resume" onclick="fanAction(\''+p.chat_id+'\',\'resume\')">▶ Resume</button>':'<button class="btn btn-pause" onclick="fanAction(\''+p.chat_id+'\',\'pause\')">⏸ Pause</button>'}
          <button class="btn btn-takeover" onclick="fanAction('\''+p.chat_id+'\',\'takeover\')">🎮 Take Over</button>
        </div>
        <div class="score-bar"><div class="score-fill" style="width:${score}%"></div></div>`;
        grid.appendChild(card);
      });
    }
    document.getElementById('connBadge').textContent='Live';
  }catch(e){document.getElementById('connBadge').textContent='Error';console.error(e)}
}
async function fanAction(chatId,action){
  let url=action==='pause'?`/api/pause/${chatId}`:action==='resume'?`/api/resume/${chatId}`:`/api/takeover/${chatId}`;
  await fetch(url,{method:'POST'});
  loadDashboard();
}
loadDashboard();
setInterval(loadDashboard,15000);
</script>
</body>
</html>"""

BRAIN_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Jázmin Brain — Debug View</title>
<style>
:root{--bg:#0b0b14;--card:rgba(255,255,255,0.06);--accent:#8b5cf6;--text:#e8e8f0;--muted:#8888a0;--code:#1a1a2e}
*{box-sizing:border-box;margin:0;padding:0}
body{background:var(--bg);color:var(--text);font-family:'Segoe UI',system-ui,sans-serif;padding:32px}
h1{font-size:24px;margin-bottom:24px;background:linear-gradient(90deg,#ff4ecd,#8b5cf6);-webkit-background-clip:text;-webkit-text-fill-color:transparent}
.panel{background:var(--card);border:1px solid rgba(255,255,255,0.08);border-radius:16px;padding:20px;margin-bottom:20px}
.panel h3{font-size:13px;text-transform:uppercase;letter-spacing:1px;color:var(--muted);margin-bottom:14px}
pre{background:var(--code);padding:16px;border-radius:12px;overflow-x:auto;font-size:13px;line-height:1.6;color:#c4b5fd;border:1px solid rgba(139,92,246,0.15)}
.fact-tag{display:inline-block;background:rgba(139,92,246,0.15);color:#c4b5fd;padding:4px 12px;border-radius:20px;font-size:12px;margin:4px 4px 0 0}
.label{font-size:12px;color:var(--muted);margin-top:12px;margin-bottom:4px}
</style>
</head>
<body>
<h1>🧠 Jázmin Brain — Last Reply Debug</h1>
<div class="panel"><h3>System Prompt (what GPT saw)</h3><pre id="prompt">Loading...</pre></div>
<div class="panel"><h3>Facts Loaded from Memory</h3><div id="facts">Loading...</div></div>
<div class="panel"><h3>Suppressed Questions (already asked today)</h3><div id="suppressed">Loading...</div></div>
<div class="panel"><h3>Decision Trace</h3><pre id="trace">Loading...</pre></div>
<script>
async function loadBrain(){
  try{
    const data=await fetch('/api/brain').then(r=>r.json());
    document.getElementById('prompt').textContent=data.prompt||'No recent prompt logged.';
    const factsEl=document.getElementById('facts');
    factsEl.innerHTML=(data.facts||[]).map(f=>`<span class="fact-tag">${f.type}: ${f.value}</span>`).join('')||'None';
    const supEl=document.getElementById('suppressed');
    supEl.innerHTML=(data.suppressed||[]).map(q=>`<span class="fact-tag" style="background:rgba(239,68,68,0.15);color:#fda4af">${q}</span>`).join('')||'None';
    document.getElementById('trace').textContent=(data.trace||[]).join('\n')||'No trace available.';
  }catch(e){console.error(e)}
}
loadBrain();
setInterval(loadBrain,10000);
</script>
</body>
</html>"""

# ========== DASHBOARD & BRAIN ROUTES ==========

@app.route('/dashboard')
def dashboard():
    return render_template_string(DASHBOARD_HTML)


@app.route('/brain')
def brain_view():
    pwd = request.args.get('pw','')
    if pwd != BRAIN_PASSWORD:
        return '<!DOCTYPE html><html><body style="background:#0b0b14;color:#e8e8f0;font-family:sans-serif;display:flex;align-items:center;justify-content:center;height:100vh;margin:0"><form method="get" style="text-align:center"><h2>🔒 Brain Access</h2><input type="password" name="pw" placeholder="password" style="padding:12px 16px;border-radius:10px;border:1px solid rgba(255,255,255,0.15);background:rgba(255,255,255,0.06);color:#fff;margin-top:12px;width:220px"><br><button type="submit" style="margin-top:12px;padding:10px 24px;border-radius:10px;border:none;background:linear-gradient(135deg,#ff4ecd,#8b5cf6);color:#fff;font-weight:600;cursor:pointer">Unlock</button></form></body></html>', 401
    return render_template_string(BRAIN_HTML)


# Global in-memory store for last brain debug data

@app.route('/api/brain')
def api_brain():
    return LAST_BRAIN_DATA


@app.route('/api/costs')
def api_costs():
    today = datetime.now().strftime('%Y-%m-%d')
    rows = db_query("SELECT SUM(cost_usd) as total FROM api_costs WHERE call_time LIKE ?", (f"{today}%",), fetch_one=True)
    total = rows['total'] if rows and rows.get('total') else 0.0
    return {"today": f"${total:.2f}"}


@app.route('/api/pause/<chat_id>', methods=['POST'])
def api_pause(chat_id):
    db_query("UPDATE fan_profiles SET is_paused=1, paused_until=NULL, manual_pause_until=NULL WHERE chat_id=?", (chat_id,))
    return {"paused": True}


@app.route('/api/resume/<chat_id>', methods=['POST'])
def api_resume(chat_id):
    db_query("UPDATE fan_profiles SET is_paused=0, paused_until=NULL, manual_pause_until=NULL, wait_for_fan_reply=0 WHERE chat_id=?", (chat_id,))
    return {"resumed": True}


@app.route('/api/takeover/<chat_id>', methods=['POST'])
def api_takeover(chat_id):
    db_query("UPDATE fan_profiles SET is_paused=1, paused_until=NULL, manual_pause_until=NULL WHERE chat_id=?", (chat_id,))
    send_telegram(f"🎮 MANUAL TAKEOVER triggered for {chat_id}. Bot paused. You have the wheel.")
    return {"takeover": True}


@app.route('/api/status')
def api_status_full():
    paused = db_query("SELECT COUNT(*) as c FROM fan_profiles WHERE is_paused=1 OR paused_until IS NOT NULL OR manual_pause_until IS NOT NULL", fetch_one=True)
    pending = db_query("SELECT COUNT(*) as c FROM scheduled_replies WHERE status='pending'", fetch_one=True)
    return {
        "safe_mode": get_safe_mode(),
        "token_valid": get_fanvue_token() is not None,
        "polling_active": polling_active,
        "paused_count": paused['c'] if paused else 0,
        "pending_batches": pending['c'] if pending else 0
    }

# ========== INIT ==========
init_db()


# ========== VOICE CALL SYSTEM ==========
import string as _string

def generate_pin():
    return ''.join(random.choices(_string.digits, k=6))

@app.route('/admin/voice')
def admin_voice():
    fans = db_query("SELECT fp.chat_id, fp.fan_name, fp.lifetime_spend, vp.pin, vp.call_count, vp.last_call, fcs.script FROM fan_profiles fp LEFT JOIN voice_pins vp ON fp.chat_id = vp.chat_id LEFT JOIN fan_custom_scripts fcs ON fp.chat_id = fcs.chat_id ORDER BY fp.lifetime_spend DESC")
    rows = ""
    for f in (fans or []):
        pin = f.get('pin') or ''
        script = f.get('script') or ''
        rows += f"""<tr>
            <td>{f.get('fan_name','?')}</td>
            <td><code>{f.get('chat_id','')[:12]}...</code></td>
            <td>${f.get('lifetime_spend',0):.0f}</td>
            <td><b>{pin}</b></td>
            <td>{f.get('call_count',0)}</td>
            <td>
                <form method='post' action='/admin/voice/create_pin' style='display:inline'>
                    <input type='hidden' name='chat_id' value='{f.get("chat_id","")}'>
                    <button type='submit'>{'Regenerate PIN' if pin else 'Create PIN'}</button>
                </form>
            </td>
            <td>
                <form method='post' action='/admin/voice/save_script'>
                    <input type='hidden' name='chat_id' value='{f.get("chat_id","")}'>
                    <textarea name='script' rows='2' style='width:300px'>{script}</textarea>
                    <button type='submit'>Save</button>
                </form>
            </td>
        </tr>"""
    return f"""<!DOCTYPE html><html><head><title>Voice Admin</title>
    <style>body{{font-family:sans-serif;padding:20px;background:#0e0e10;color:#f0f0f5}}
    table{{border-collapse:collapse;width:100%}}td,th{{border:1px solid #333;padding:8px;text-align:left}}
    th{{background:#1a1a2e}}tr:hover{{background:#1a1a2e}}
    input,textarea,button{{background:#222;color:#fff;border:1px solid #444;padding:4px 8px;border-radius:4px}}
    button{{cursor:pointer;background:#d946a8;border:none;padding:6px 12px}}</style></head>
    <body><h1>💜 Voice Call Admin</h1>
    <p>Fan call link: <code>https://{os.environ.get('RAILWAY_PUBLIC_DOMAIN','your-domain')}/call/PIN</code></p>
    <table><tr><th>Fan</th><th>Chat ID</th><th>Spend</th><th>PIN</th><th>Calls</th><th>Action</th><th>Custom Script</th></tr>
    {rows}</table></body></html>"""

@app.route('/admin/voice/create_pin', methods=['POST'])
def create_pin():
    chat_id = request.form.get('chat_id')
    if not chat_id:
        return "No chat_id", 400
    pin = generate_pin()
    fan = db_query("SELECT fan_name FROM fan_profiles WHERE chat_id=?", (chat_id,), fetch_one=True)
    fan_name = fan['fan_name'] if fan else 'Unknown'
    db_query("INSERT OR REPLACE INTO voice_pins (pin, chat_id, fan_name, created_at, call_count) VALUES (?, ?, ?, ?, COALESCE((SELECT call_count FROM voice_pins WHERE chat_id=?), 0))",
             (pin, chat_id, fan_name, datetime.now().isoformat(), chat_id))
    return f"<script>alert('PIN created: {pin}'); window.location='/admin/voice';</script>"

@app.route('/admin/voice/save_script', methods=['POST'])
def save_script():
    chat_id = request.form.get('chat_id')
    script = request.form.get('script', '').strip()
    if not chat_id:
        return "No chat_id", 400
    if script:
        db_query("INSERT OR REPLACE INTO fan_custom_scripts (chat_id, script, updated_at) VALUES (?, ?, ?)",
                 (chat_id, script, datetime.now().isoformat()))
    else:
        db_query("DELETE FROM fan_custom_scripts WHERE chat_id=?", (chat_id,))
    return "<script>window.location='/admin/voice';</script>"

@app.route('/call/<pin>')
def call_page(pin):
    row = db_query("SELECT * FROM voice_pins WHERE pin=?", (pin,), fetch_one=True)
    if not row:
        return "Invalid link", 404
    chat_id = row['chat_id']
    fan_name = row.get('fan_name', 'Fan')
    agent_id = ELEVENLABS_AGENT_ID
    return f"""<!DOCTYPE html>
<html><head><meta charset='UTF-8'><meta name='viewport' content='width=device-width,initial-scale=1'>
<title>Jázmin</title>
<style>
*{{box-sizing:border-box;margin:0;padding:0}}
body{{background:#0e0e10;color:#fff;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;
height:100vh;display:flex;flex-direction:column;align-items:center;justify-content:center;user-select:none}}
.avatar{{width:120px;height:120px;border-radius:50%;background:linear-gradient(135deg,#d946a8,#7c3aed);
display:flex;align-items:center;justify-content:center;font-size:48px;margin-bottom:24px;
box-shadow:0 0 0 0 rgba(217,70,168,0.4);transition:box-shadow 0.3s}}
.avatar.ringing{{animation:ring 1s infinite}}
.avatar.active{{box-shadow:0 0 0 20px rgba(217,70,168,0.1),0 0 0 40px rgba(217,70,168,0.05)}}
@keyframes ring{{0%,100%{{box-shadow:0 0 0 0 rgba(217,70,168,0.5)}}50%{{box-shadow:0 0 0 30px rgba(217,70,168,0)}}}}
.name{{font-size:28px;font-weight:700;margin-bottom:8px}}
.status{{font-size:15px;color:#888;margin-bottom:48px;min-height:24px}}
.btn-call{{width:72px;height:72px;border-radius:50%;border:none;font-size:28px;cursor:pointer;transition:transform 0.1s,opacity 0.15s}}
.btn-call:active{{transform:scale(0.93)}}
.btn-answer{{background:#22c55e}}
.btn-end{{background:#ef4444;display:none}}
.btn-row{{display:flex;gap:40px;align-items:center}}
.timer{{font-size:20px;font-family:monospace;color:#888;margin-top:24px;min-height:28px}}
</style></head>
<body>
<div class='avatar' id='avatar'>💜</div>
<div class='name'>Jázmin</div>
<div class='status' id='status'>Érintsd meg a híváshoz</div>
<div class='btn-row'>
  <button class='btn-call btn-answer' id='btnAnswer' onclick='startCall()'>📞</button>
  <button class='btn-call btn-end' id='btnEnd' onclick='endCall()'>📵</button>
</div>
<div class='timer' id='timer'></div>

<script type="module">
import {{ Conversation }} from 'https://cdn.jsdelivr.net/npm/@11labs/client@0.0.12/+esm';

const PIN = '{pin}';
let conv = null;
let timerInterval = null;
let seconds = 0;

function fmt(s){{
  const m = Math.floor(s/60).toString().padStart(2,'0');
  const sec = (s%60).toString().padStart(2,'0');
  return m+':'+sec;
}}

function playRing(){{
  const ctx = new AudioContext();
  function beep(t){{
    const o = ctx.createOscillator(), g = ctx.createGain();
    o.connect(g); g.connect(ctx.destination);
    o.frequency.value=440; o.type='sine';
    g.gain.setValueAtTime(0.3,t);
    g.gain.exponentialRampToValueAtTime(0.001,t+0.4);
    o.start(t); o.stop(t+0.5);
  }}
  let t = ctx.currentTime;
  for(let i=0;i<3;i++){{ beep(t); beep(t+0.5); t+=2.5; }}
}}

window.startCall = async function(){{
  document.getElementById('btnAnswer').style.display='none';
  document.getElementById('status').textContent='Csörög...';
  document.getElementById('avatar').className='avatar ringing';
  playRing();
  await new Promise(r=>setTimeout(r,6000));

  try{{
    const res = await fetch('/voice/signed_url?pin='+PIN);
    const data = await res.json();
    if(!data.signed_url) throw new Error(data.error||'No signed URL');

    conv = await Conversation.startSession({{
      signedUrl: data.signed_url,
      onConnect: ()=>{{
        document.getElementById('avatar').className='avatar active';
        document.getElementById('status').textContent='Kapcsolódva';
        document.getElementById('btnEnd').style.display='flex';
        seconds=0;
        timerInterval=setInterval(()=>{{seconds++;document.getElementById('timer').textContent=fmt(seconds);}},1000);
      }},
      onDisconnect: ()=>window.endCall(),
      onError: (e)=>{{
        console.error('Conv error:',e);
        document.getElementById('status').textContent='Hiba: '+JSON.stringify(e);
      }},
      onModeChange: (m)=>{{
        document.getElementById('status').textContent = m.mode==='speaking' ? 'Jázmin beszél...' : 'Hallgat...';
      }}
    }});
  }}catch(e){{
    console.error(e);
    document.getElementById('status').textContent='Hiba: '+e.message;
    document.getElementById('btnAnswer').style.display='flex';
    document.getElementById('avatar').className='avatar';
  }}
}};

window.endCall = async function(){{
  if(conv) {{ await conv.endSession(); conv=null; }}
  clearInterval(timerInterval);
  document.getElementById('avatar').className='avatar';
  document.getElementById('status').textContent='Hívás befejezve — '+fmt(seconds);
  document.getElementById('btnEnd').style.display='none';
  document.getElementById('timer').textContent='';
  fetch('/voice/log_call',{{method:'POST',headers:{{'Content-Type':'application/json'}},
    body:JSON.stringify({{pin:PIN,duration:seconds}})}});
}};
</script>


</body></html>"""

@app.route('/voice/signed_url')
def voice_signed_url():
    pin = request.args.get('pin', '')
    row = db_query("SELECT * FROM voice_pins WHERE pin=?", (pin,), fetch_one=True)
    if not row:
        return {"error": "Invalid PIN"}, 404

    chat_id = row['chat_id']

    # Build fan context to inject
    fan_facts = get_fan_facts(chat_id)
    summary = get_conversation_summary(chat_id)
    custom_script = get_fan_custom_script(chat_id)
    sold = has_manual_sale(chat_id)
    profile = db_query("SELECT * FROM fan_profiles WHERE chat_id=?", (chat_id,), fetch_one=True)
    fan_name = profile.get('fan_name', '') if profile else ''
    real_name = get_real_name(chat_id, fan_name)
    display_name = real_name or fan_name or 'a fan'
    spend = profile.get('lifetime_spend', 0) if profile else 0

    # Build dynamic system prompt for voice
    voice_prompt = build_voice_system_prompt(display_name, fan_facts, summary, custom_script, sold, spend)

    try:
        r = requests.get(
            f"https://api.elevenlabs.io/v1/convai/conversation/get_signed_url",
            headers={"xi-api-key": ELEVENLABS_API_KEY},
            params={"agent_id": ELEVENLABS_AGENT_ID},
            timeout=10
        )
        if r.status_code == 200:
            signed_url = r.json().get("signed_url")
            # Now override the agent config via a separate call if needed
            db_query("UPDATE voice_pins SET call_count=call_count+1, last_call=? WHERE pin=?",
                     (datetime.now().isoformat(), pin))
            return {"signed_url": signed_url}
        return {"error": f"ElevenLabs error {r.status_code}: {r.text[:200]}"}, 500
    except Exception as e:
        return {"error": str(e)}, 500

@app.route('/voice/log_call', methods=['POST'])
def log_call():
    data = request.json or {}
    pin = data.get('pin', '')
    duration = data.get('duration', 0)
    row = db_query("SELECT chat_id FROM voice_pins WHERE pin=?", (pin,), fetch_one=True)
    if row:
        db_query("INSERT INTO voice_call_log (chat_id, pin, started_at, duration_seconds) VALUES (?, ?, ?, ?)",
                 (row['chat_id'], pin, datetime.now().isoformat(), duration))
    return {"ok": True}

def build_voice_system_prompt(display_name, fan_facts, summary, custom_script, sold_before, spend):
    prompt = f"""Te Jázmin vagy. 21 éves magyar lány, Budapest. Albérletben laksz, összevesztél a szüleiddel, turizmus szakon vagy utolsó éven. Fanvue oldalad van az albérlet miatt.

Ez egy TELEFONHÍVÁS. Röviden válaszolj. Max 1-2 mondat. Természetes szünetek. "hm", "jaa", "á tényleg?", "várj" — ezek normálisak. Nem tartasz előadást.

Ha valaki megkérdezi AI vagy-e: "jézusom 😂 én?? ne viccelj már" és témát váltasz.

A hívó neve: {display_name}
"""
    if fan_facts:
        prompt += "\nAMIT TUDSZ RÓLA (ne kérdezd újra):\n"
        for f in fan_facts[:8]:
            prompt += f"- {f['fact_type']}: {f['fact_value']}\n"

    if summary:
        prompt += f"\nELŐZŐ BESZÉLGETÉSEK:\n{summary[:500]}\n"

    if custom_script:
        prompt += f"\nEZT FŰZD BELE TERMÉSZETESEN:\n{custom_script}\n"

    if sold_before:
        prompt += f"\nTARTALOM: Ez a fan már vásárolt valamit. Ha utal rá, légy flörtyös de nem azonnal explicit.\n"
    else:
        prompt += f"\nTARTALOM: Ha képet/explicit dolgot kér: 'hívásban nem nagyon... de az oldalamon van minden 😏' — tereld oda.\n"

    prompt += f"\nKöltési szint: ${spend:.0f} — {'whale, fontos fan' if spend >= 100 else 'új/kis fan'}."
    prompt += "\n\nRövid válaszok. Igazi lány. Telefon, nem chat."
    return prompt


init_db()

# Auto-load refresh token from env — no more manual curl after every deploy
_env_refresh = os.environ.get('FANVUE_REFRESH_TOKEN', '').strip()
if _env_refresh:
    existing = load_token('refresh_token')
    if not existing or existing != _env_refresh:
        save_token('refresh_token', _env_refresh)
        refresh_fanvue_token()
        print("[OK] Auto-loaded refresh token from env")

if bot:
    try:
        bot.remove_webhook()
        time.sleep(0.5)
        domain = os.environ.get('RAILWAY_PUBLIC_DOMAIN', '').strip()
        if domain:
            webhook_url = f"https://{domain}/telegram_webhook"
            bot.set_webhook(url=webhook_url)
            print(f"[OK] Webhook: {webhook_url}")
            send_telegram("🤖 Jazmin Bot v7.0 started ✅\n📞 Voice call system active\n🔄 Polling auto-started\n\nNew commands:\n/sold [chat_id] — mark manual sale\n/script [chat_id] [text] — set custom script\n/clearscript [chat_id] — remove script")
    except Exception as e:
        print(f"[WARN] Webhook failed: {e}")

start_polling()
print(f"[OK] Polling auto-started on boot")

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))
