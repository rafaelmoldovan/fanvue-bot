"""
Jazmin Fanvue Bot — v8.1
Changes vs v8.0:
- DB migration: auto-adds is_mine column to existing messages table on boot
- /fetchblacklist: now reads from local fan_profiles DB (not Fanvue API) so it gets ALL fans, not just 15
- Whitelist matching: checks both handle AND fan_name (lowercase) so no one gets missed
- Whitelist updated: includes eszter, unlikely-condor-278, moaning-wasp-252, molecular-scorpion-79, legitimate-tiglon-855
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
FANVUE_CLIENT_ID     = os.environ.get('FANVUE_CLIENT_ID', '')
FANVUE_CLIENT_SECRET = os.environ.get('FANVUE_CLIENT_SECRET', '')
OPENAI_API_KEY       = os.environ.get('OPENAI_API_KEY', '')
CREATOR_NAME         = os.environ.get('CREATOR_NAME', 'jazmin07')
MY_UUID              = os.environ.get('MY_UUID', '38a392fc-a751-49b3-9d74-01ac6447c490')
TELEGRAM_BOT_TOKEN   = os.environ.get('TELEGRAM_BOT_TOKEN', '')
TELEGRAM_CHAT_ID     = os.environ.get('TELEGRAM_CHAT_ID', '')

POLL_INTERVAL  = 20
BATCH_WINDOW   = 60   # seconds to wait before firing a reply batch

# Excluded from auto-blacklist. Checked against BOTH handle AND fan_name (lowercased).
BLACKLIST_WHITELIST = {
    'eszter',
    'unlikely-condor-278', 'unlikely condor',
    'moaning-wasp-252',    'moaning wasp',
    'molecular-scorpion-79','molecular scorpion',
    'legitimate-tiglon-855','legitimate tiglon',
}

# ========== BOOT SAFE MODE ==========
# Safe mode always starts ON. Persisted in DB after first write.
SAFE_MODE = True   # in-memory fallback

# ========== PAUSED SPAM PREVENTION ==========
PAUSED_NOTIFICATION_CACHE = {}   # chat_id -> last msg_id we notified about

# ========== BRAIN VIEW PASSWORD ==========
BRAIN_PASSWORD = os.environ.get('BRAIN_PASSWORD', 'jazmin123')

LAST_BRAIN_DATA = {"prompt": "", "facts": [], "suppressed": [], "trace": []}

# ========== TELEGRAM BOT ==========
bot = None
if TELEGRAM_BOT_TOKEN:
    bot = telebot.TeleBot(TELEGRAM_BOT_TOKEN)


# ========== TELEGRAM HELPERS ==========
def send_telegram(text, parse_mode='HTML'):
    if not bot or not TELEGRAM_CHAT_ID:
        return False
    try:
        bot.send_message(TELEGRAM_CHAT_ID, text[:4000], parse_mode=parse_mode)
        return True
    except Exception as e:
        print(f"[WARN] Telegram failed: {e}")
        return False


def make_inline_buttons(chat_id):
    markup = InlineKeyboardMarkup(row_width=2)
    markup.add(
        InlineKeyboardButton("⏸️ Pause",   callback_data=f"pause:{chat_id}"),
        InlineKeyboardButton("▶️ Unpause", callback_data=f"resume:{chat_id}"),
        InlineKeyboardButton("📝 Notes",   callback_data=f"notes:{chat_id}"),
        InlineKeyboardButton("❓ Asked",   callback_data=f"asked:{chat_id}"),
    )
    return markup


def send_telegram_with_buttons(text, chat_id, parse_mode='HTML'):
    if not bot or not TELEGRAM_CHAT_ID:
        return False
    try:
        markup   = make_inline_buttons(chat_id)
        full_text = text + f"\n🔗 <code>{chat_id}</code>"
        bot.send_message(TELEGRAM_CHAT_ID, full_text[:4000],
                         parse_mode=parse_mode, reply_markup=markup)
        return True
    except Exception as e:
        print(f"[WARN] Telegram buttons failed: {e}")
        try:
            bot.send_message(TELEGRAM_CHAT_ID,
                             (text + f"\n🔗 <code>{chat_id}</code>")[:4000],
                             parse_mode=parse_mode)
        except Exception:
            pass
        return False


# ========== TELEGRAM CALLBACK HANDLER (BUTTONS) ==========
if bot:
    @bot.callback_query_handler(func=lambda call: True)
    def handle_callback(call):
        """
        FIX vs v6.3: is_admin was checking call.message.chat.id which is the
        bot's own outgoing message chat — not the admin's chat ID.
        Now we check call.from_user.id against TELEGRAM_CHAT_ID.
        """
        if str(call.from_user.id) != str(TELEGRAM_CHAT_ID):
            bot.answer_callback_query(call.id, "Not authorized")
            return

        data = call.data or ""
        if ":" not in data:
            bot.answer_callback_query(call.id, "Invalid")
            return

        action, chat_id = data.split(":", 1)
        try:
            if action == "pause":
                db_query("INSERT OR IGNORE INTO fan_profiles (chat_id, fan_name, fan_type, last_interaction, is_paused) VALUES (?, 'unknown', 'new', ?, 1)",
                         (chat_id, datetime.now().isoformat()))
                db_query("UPDATE fan_profiles SET is_paused=1, paused_until=NULL, manual_pause_until=NULL WHERE chat_id=?", (chat_id,))
                bot.answer_callback_query(call.id, "⏸️ Paused")
                send_telegram(f"⏸️ Paused <code>{chat_id}</code>")

            elif action == "resume":
                # INSERT OR IGNORE ensures a row exists even for brand new fans
                db_query("INSERT OR IGNORE INTO fan_profiles (chat_id, fan_name, fan_type, last_interaction, is_paused) VALUES (?, 'unknown', 'new', ?, 0)",
                         (chat_id, datetime.now().isoformat()))
                db_query("UPDATE fan_profiles SET is_paused=0, paused_until=NULL, manual_pause_until=NULL, wait_for_fan_reply=0 WHERE chat_id=?", (chat_id,))
                # Also remove from notified cache so detection works fresh
                MANUAL_REPLY_NOTIFIED.discard(chat_id)
                bot.answer_callback_query(call.id, "▶️ Unpaused")
                send_telegram(f"▶️ Unpaused <code>{chat_id}</code> — bot resumes on next fan message.")

            elif action == "notes":
                facts = db_query("SELECT fact_type, fact_value, discovered_at FROM fan_facts WHERE chat_id=? ORDER BY discovered_at DESC", (chat_id,))
                if not facts:
                    bot.answer_callback_query(call.id, "No notes yet")
                    return
                lines = [f"📝 Facts for <code>{chat_id}</code>:"]
                for f in facts:
                    lines.append(f"• <b>{f['fact_type']}</b>: {f['fact_value']}")
                send_telegram("\n".join(lines))
                bot.answer_callback_query(call.id, "Notes sent ✅")

            elif action == "asked":
                qa = db_query("SELECT question, answered, asked_at FROM questions_asked WHERE chat_id=? ORDER BY asked_at DESC", (chat_id,))
                if not qa:
                    bot.answer_callback_query(call.id, "No questions tracked")
                    return
                lines = [f"❓ Questions for <code>{chat_id}</code>:"]
                for q in qa:
                    status = "✅" if q['answered'] else "⏳"
                    lines.append(f"{status} <b>{q['question']}</b> ({q['asked_at'][:10]})")
                send_telegram("\n".join(lines))
                bot.answer_callback_query(call.id, "Asked sent ✅")

            else:
                bot.answer_callback_query(call.id, "Unknown action")

        except Exception as e:
            bot.answer_callback_query(call.id, f"Error: {str(e)[:100]}")


# ========== TELEGRAM COMMANDS ==========
if bot:
    @bot.message_handler(commands=['start'])
    def cmd_start(message):
        bot.reply_to(message,
            "🤖 Jazmin Bot v8.2\n\n"
            "/status — Bot overview\n"
            "/fans — All fans with IDs\n"
            "/pause <uuid> — Pause fan\n"
            "/resume <uuid> — Resume fan\n"
            "/safe_on / /safe_off — Safe mode\n"
            "/notes <uuid> — Fan facts\n"
            "/asked <uuid> — Questions asked\n"
            "/sold <uuid> [note] — Mark manual sale\n"
            "/script <uuid> <text> — Set custom script\n"
            "/clearscript <uuid> — Remove script\n"
            "/blacklist <uuid> [name] — Blacklist a fan\n"
            "/unblacklist <uuid> — Remove from blacklist\n"
            "/fetchblacklist — Fetch all current subs and blacklist them (keeps whitelist)\n\n"
            "💡 Buttons on every fan notification now work — tap ▶️ to unpause instantly."
        )

    @bot.message_handler(commands=['sold'])
    def cmd_sold(message):
        if str(message.from_user.id) != str(TELEGRAM_CHAT_ID):
            return
        parts = message.text.split()
        if len(parts) < 2:
            bot.reply_to(message, "Usage: /sold <chat_id> [note]")
            return
        chat_id = parts[1]
        note = ' '.join(parts[2:]) if len(parts) > 2 else ''
        record_manual_sale(chat_id, note)
        bot.reply_to(message, f"✅ Sale recorded for {chat_id}.")

    @bot.message_handler(commands=['script'])
    def cmd_script(message):
        if str(message.from_user.id) != str(TELEGRAM_CHAT_ID):
            return
        parts = message.text.split(None, 2)
        if len(parts) < 3:
            bot.reply_to(message, "Usage: /script <chat_id> <instructions>")
            return
        chat_id, script = parts[1], parts[2]
        db_query("INSERT OR REPLACE INTO fan_custom_scripts (chat_id, script, updated_at) VALUES (?, ?, ?)",
                 (chat_id, script, datetime.now().isoformat()))
        bot.reply_to(message, f"✅ Script saved for {chat_id}:\n\n{script}")

    @bot.message_handler(commands=['clearscript'])
    def cmd_clearscript(message):
        if str(message.from_user.id) != str(TELEGRAM_CHAT_ID):
            return
        parts = message.text.split()
        if len(parts) < 2:
            bot.reply_to(message, "Usage: /clearscript <chat_id>")
            return
        db_query("DELETE FROM fan_custom_scripts WHERE chat_id=?", (parts[1],))
        bot.reply_to(message, f"✅ Script cleared for {parts[1]}")

    @bot.message_handler(commands=['fans'])
    def cmd_fans(message):
        if str(message.from_user.id) != str(TELEGRAM_CHAT_ID):
            return
        rows = db_query("SELECT chat_id, fan_name, is_paused, fan_type, total_messages FROM fan_profiles ORDER BY last_interaction DESC")
        if not rows:
            bot.reply_to(message, "No fans yet.")
            return
        lines = ["📋 All fans:"]
        for r in rows:
            status = "⏸️" if r['is_paused'] else "✅"
            lines.append(f"{status} <b>{r['fan_name'] or '?'}</b> ({r['total_messages']} msgs) | <code>{r['chat_id']}</code>")
        bot.reply_to(message, "\n".join(lines), parse_mode='HTML')

    @bot.message_handler(commands=['status'])
    def cmd_status(message):
        if str(message.from_user.id) != str(TELEGRAM_CHAT_ID):
            return
        rows = db_query("SELECT chat_id, fan_name, is_paused, fan_type FROM fan_profiles ORDER BY last_interaction DESC LIMIT 10")
        lines = ["📊 Recent fans:"]
        for r in (rows or []):
            s = "⏸️ PAUSED" if r['is_paused'] else "✅ Active"
            lines.append(f"`{r['chat_id'][:8]}...` | {r['fan_name'] or '?'} | {s}")
        safe = get_safe_mode()
        lines.append(f"\n🔒 Safe mode: {'ON' if safe else 'OFF'}")
        bot.reply_to(message, "\n".join(lines), parse_mode='Markdown')

    @bot.message_handler(commands=['pause'])
    def cmd_pause(message):
        if str(message.from_user.id) != str(TELEGRAM_CHAT_ID):
            return
        parts = message.text.split()
        if len(parts) < 2:
            bot.reply_to(message, "Usage: /pause <uuid>")
            return
        uuid = parts[1].strip()
        db_query("UPDATE fan_profiles SET is_paused=1, paused_until=NULL, manual_pause_until=NULL WHERE chat_id=?", (uuid,))
        bot.reply_to(message, f"⏸️ Paused `{uuid[:12]}...`", parse_mode='Markdown')

    @bot.message_handler(commands=['resume'])
    def cmd_resume(message):
        if str(message.from_user.id) != str(TELEGRAM_CHAT_ID):
            return
        parts = message.text.split()
        if len(parts) < 2:
            bot.reply_to(message, "Usage: /resume <uuid>")
            return
        uuid = parts[1].strip()
        db_query("UPDATE fan_profiles SET is_paused=0, paused_until=NULL, manual_pause_until=NULL, wait_for_fan_reply=0 WHERE chat_id=?", (uuid,))
        bot.reply_to(message, f"▶️ Resumed `{uuid[:12]}...`", parse_mode='Markdown')

    @bot.message_handler(commands=['safe_on'])
    def cmd_safe_on(message):
        if str(message.from_user.id) != str(TELEGRAM_CHAT_ID):
            return
        global SAFE_MODE
        SAFE_MODE = True
        set_safe_mode(True)
        bot.reply_to(message, "🔒 SAFE MODE ON — bot will not send, only notify Telegram.")

    @bot.message_handler(commands=['safe_off'])
    def cmd_safe_off(message):
        if str(message.from_user.id) != str(TELEGRAM_CHAT_ID):
            return
        global SAFE_MODE
        SAFE_MODE = False
        set_safe_mode(False)
        bot.reply_to(message, "🔓 SAFE MODE OFF — bot is now live.")

    @bot.message_handler(commands=['notes'])
    def cmd_notes(message):
        if str(message.from_user.id) != str(TELEGRAM_CHAT_ID):
            return
        parts = message.text.split()
        if len(parts) < 2:
            bot.reply_to(message, "Usage: /notes <uuid>")
            return
        uuid = parts[1].strip()
        facts = db_query("SELECT fact_type, fact_value, discovered_at FROM fan_facts WHERE chat_id=? ORDER BY discovered_at DESC", (uuid,))
        if not facts:
            bot.reply_to(message, "No facts stored for this fan.")
            return
        lines = [f"📝 Facts for <code>{uuid[:12]}...</code>:"]
        for f in facts:
            lines.append(f"• <b>{f['fact_type']}</b>: {f['fact_value']}")
        bot.reply_to(message, "\n".join(lines), parse_mode='HTML')

    @bot.message_handler(commands=['asked'])
    def cmd_asked(message):
        if str(message.from_user.id) != str(TELEGRAM_CHAT_ID):
            return
        parts = message.text.split()
        if len(parts) < 2:
            bot.reply_to(message, "Usage: /asked <uuid>")
            return
        uuid = parts[1].strip()
        qa = db_query("SELECT question, answered, asked_at FROM questions_asked WHERE chat_id=? ORDER BY asked_at DESC", (uuid,))
        if not qa:
            bot.reply_to(message, "No questions tracked for this fan.")
            return
        lines = [f"❓ Questions for <code>{uuid[:12]}...</code>:"]
        for q in qa:
            s = "✅" if q['answered'] else "⏳"
            lines.append(f"{s} <b>{q['question']}</b> ({q['asked_at'][:10]})")
        bot.reply_to(message, "\n".join(lines), parse_mode='HTML')

    @bot.message_handler(commands=['blacklist'])
    def cmd_blacklist(message):
        if str(message.from_user.id) != str(TELEGRAM_CHAT_ID):
            return
        parts = message.text.split(None, 2)
        if len(parts) < 2:
            bot.reply_to(message, "Usage: /blacklist <uuid> [display_name]")
            return
        chat_id  = parts[1].strip()
        fan_name = parts[2].strip() if len(parts) > 2 else 'unknown'
        db_query("INSERT OR IGNORE INTO blacklisted_fans (chat_id, fan_name, blacklisted_at, reason) VALUES (?, ?, ?, ?)",
                 (chat_id, fan_name, datetime.now().isoformat(), 'manual'))
        bot.reply_to(message, f"🚫 Blacklisted <code>{chat_id}</code> ({fan_name}). Bot will ignore them.", parse_mode='HTML')

    @bot.message_handler(commands=['unblacklist'])
    def cmd_unblacklist(message):
        if str(message.from_user.id) != str(TELEGRAM_CHAT_ID):
            return
        parts = message.text.split()
        if len(parts) < 2:
            bot.reply_to(message, "Usage: /unblacklist <uuid>")
            return
        chat_id = parts[1].strip()
        db_query("DELETE FROM blacklisted_fans WHERE chat_id=?", (chat_id,))
        bot.reply_to(message, f"✅ Removed <code>{chat_id}</code> from blacklist.", parse_mode='HTML')

    @bot.message_handler(commands=['fetchblacklist'])
    def cmd_fetchblacklist(message):
        """
        Reads ALL fans from local fan_profiles DB and blacklists them,
        EXCEPT anyone whose handle OR fan_name (lowercased) is in BLACKLIST_WHITELIST.
        This is reliable because the DB has every fan we've ever seen — no API pagination issues.
        """
        if str(message.from_user.id) != str(TELEGRAM_CHAT_ID):
            return
        bot.reply_to(message, "⏳ Reading all fans from local database...")
        try:
            all_fans = db_query("SELECT chat_id, fan_name, handle FROM fan_profiles") or []
            if not all_fans:
                bot.reply_to(message, "❌ No fans in local DB yet. Make sure the bot has polled at least once.")
                return

            added   = []
            skipped = []
            for fan in all_fans:
                try:
                    chat_id  = fan.get('chat_id', '')
                    fan_name = (fan.get('fan_name') or '').strip()
                    handle   = (fan.get('handle') or '').strip()
                    if not chat_id:
                        continue
                    # Check whitelist against both handle and display name
                    name_lower   = fan_name.lower()
                    handle_lower = handle.lower()
                    if name_lower in BLACKLIST_WHITELIST or handle_lower in BLACKLIST_WHITELIST:
                        skipped.append(fan_name or handle)
                        continue
                    db_query(
                        "INSERT OR IGNORE INTO blacklisted_fans (chat_id, fan_name, blacklisted_at, reason) VALUES (?, ?, ?, ?)",
                        (chat_id, fan_name, datetime.now().isoformat(), 'auto_existing_sub')
                    )
                    added.append(fan_name or handle)
                except Exception:
                    continue

            msg = (f"✅ Done.\n"
                   f"🚫 Blacklisted: {len(added)} fans\n"
                   f"✅ Kept (whitelist): {len(skipped)} — {', '.join(skipped) or 'none'}\n\n"
                   f"New subscribers will NOT be blacklisted automatically.")
            bot.reply_to(message, msg)

        except Exception as e:
            bot.reply_to(message, f"❌ Error: {e}")


# ========== SQLITE ==========
DB_PATH = os.environ.get('DB_PATH', '/data/bot_data.db')
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
        reply_text TEXT, bot_replied_at TEXT, is_mine INTEGER DEFAULT 0)''')
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
    # OLD blocked_fans kept for compatibility, new blacklisted_fans is the canonical one
    c.execute('''CREATE TABLE IF NOT EXISTS blocked_fans (
        chat_id TEXT PRIMARY KEY, fan_name TEXT, blocked_at TEXT, reason TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS blacklisted_fans (
        chat_id TEXT PRIMARY KEY, fan_name TEXT, blacklisted_at TEXT, reason TEXT)''')
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
    c.execute('''CREATE TABLE IF NOT EXISTS fan_scores (
        chat_id TEXT PRIMARY KEY, score INTEGER DEFAULT 0, last_updated TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS api_costs (
        id INTEGER PRIMARY KEY AUTOINCREMENT, call_time TEXT, tokens_prompt INTEGER,
        tokens_completion INTEGER, cost_usd REAL)''')
    c.execute('''CREATE TABLE IF NOT EXISTS fan_custom_scripts (
        chat_id TEXT PRIMARY KEY, script TEXT, updated_at TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS manual_sales (
        id INTEGER PRIMARY KEY AUTOINCREMENT, chat_id TEXT, sold_at TEXT, note TEXT)''')
    conn.commit()
    conn.close()
    # === MIGRATION: add is_mine column if upgrading from old DB ===
    try:
        conn2 = sqlite3.connect(DB_PATH)
        conn2.execute("ALTER TABLE messages ADD COLUMN is_mine INTEGER DEFAULT 0")
        conn2.commit()
        conn2.close()
        print("[OK] Migration: added is_mine column to messages table")
    except Exception:
        pass  # Column already exists — safe to ignore
    # Ensure safe_mode starts ON in DB if not already set
    existing = db_query("SELECT value FROM bot_settings WHERE key='safe_mode'", fetch_one=True)
    if not existing:
        db_query("INSERT OR IGNORE INTO bot_settings (key, value) VALUES ('safe_mode', 'true')")


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
    creds   = f"{FANVUE_CLIENT_ID}:{FANVUE_CLIENT_SECRET}"
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
            data       = r.json()
            access     = data.get('access_token')
            new_refresh = data.get('refresh_token', refresh_token)
            expires    = data.get('expires_in', 3600)
            expires_at = (datetime.now() + timedelta(seconds=expires - 300)).isoformat()
            save_token('refresh_token', new_refresh)
            save_token('access_token', access)
            save_token('expires_at', expires_at)
            return access, "OK"
        return None, f"Refresh failed: {r.status_code}"
    except Exception as e:
        return None, f"Error: {e}"


def get_fanvue_token():
    access  = load_token('access_token')
    expires = load_token('expires_at')
    if access and expires:
        try:
            if datetime.now() < datetime.fromisoformat(expires):
                return access
        except Exception:
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


# ========== BLACKLIST / BLOCK ==========
def is_blacklisted(chat_id):
    row = db_query("SELECT 1 FROM blacklisted_fans WHERE chat_id=?", (chat_id,), fetch_one=True)
    if row:
        return True
    # Also check legacy blocked_fans table
    row2 = db_query("SELECT 1 FROM blocked_fans WHERE chat_id=?", (chat_id,), fetch_one=True)
    return bool(row2)


# ========== PAUSE HELPERS ==========
def is_paused(chat_id):
    profile = db_query(
        "SELECT is_paused, paused_until, manual_pause_until FROM fan_profiles WHERE chat_id=?",
        (chat_id,), fetch_one=True)
    if not profile:
        return False
    if profile.get('is_paused'):
        return True
    now   = datetime.now(timezone.utc)
    until = profile.get('paused_until')
    if until:
        try:
            until_dt = parse_timestamp(until)
            if until_dt and now < until_dt:
                return True
            db_query("UPDATE fan_profiles SET paused_until=NULL WHERE chat_id=?", (chat_id,))
        except Exception:
            pass
    return False


def hard_pause_fan(chat_id):
    """Permanently pause until admin manually resumes via button or /resume."""
    db_query("UPDATE fan_profiles SET is_paused=1, paused_until=NULL, manual_pause_until=NULL, wait_for_fan_reply=0 WHERE chat_id=?",
             (chat_id,))


def resume_fan(chat_id):
    db_query("UPDATE fan_profiles SET is_paused=0, paused_until=NULL, manual_pause_until=NULL, wait_for_fan_reply=0 WHERE chat_id=?",
             (chat_id,))


# ========== FANVUE API ==========
def get_headers():
    token = get_fanvue_token()
    return {
        "Authorization": f"Bearer {token or ''}",
        "X-Fanvue-API-Version": "2025-06-26",
        "Content-Type": "application/json",
    }


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
        r = requests.get(f"https://api.fanvue.com/chats/{chat_id}/messages",
                         headers=get_headers(), timeout=10)
        if r.status_code == 401:
            refresh_fanvue_token()
            r = requests.get(f"https://api.fanvue.com/chats/{chat_id}/messages",
                             headers=get_headers(), timeout=10)
        if r.status_code != 200:
            return []
        return r.json().get('data', [])
    except Exception:
        return []


def send_fanvue_message(chat_id, text):
    if get_safe_mode():
        send_telegram(f"🔒 SAFE MODE — would send to <code>{chat_id}</code>:\n<i>{text}</i>")
        return True
    try:
        r = requests.post(f"https://api.fanvue.com/chats/{chat_id}/message",
                          headers=get_headers(), json={"text": text}, timeout=10)
        return r.status_code in [200, 201]
    except Exception:
        return False


# ========== PERSONALITY ==========
JAZMIN_PERSONALITY = """Te Jázmin vagy. 21 éves, Budapest VII. kerület. Albérletben laksz, összevesztél a szüleiddel — nem tipikus tinis dráma, komoly okok voltak. Turizmus-vendéglátást tanulsz, utolsó év, júniusban végzel. Ryanairhez akarsz utána. Fanvue oldalad van mert az albérlet drága.

Nem vagy chatbot. Egy igazi lány vagy.

STÍLUS — EZ A LEGFONTOSABB:
1 mondat. Néha 2. Soha nem 3. Néha csak emoji. Nem lelkeskedsz. Ha kérdezel, EGY kérdés — soha nem kettő. Olyan mint aki WhatsAppon ír.

KRITIKUS SZABÁLY: Ha az előző üzeneted kérdés volt — NE kérdezz újra. Várj választ.

MEMÓRIA:
Ha a fan mondott valamit — nevét, munkáját, problémáját — TUDOD. Nem kérdezed újra. Visszahivatkozol: "ja tényleg, te mondtad hogy..."

ÚJ FAN — ELSŐ 10 ÜZENET:
Csak ismerkedés. Semmi flört, semmi szex, semmi tartalom. Egy egyszerű kérdés. Aztán hallgatsz. Hagyd hogy ő vezessen.

SZEX / FLÖRT:
CSAK ha már 20+ üzenet volt. Új fannak soha. Ha ő próbálja korán: "hűű te gyors vagy 😂"

MIÉRT VAN FANVUE OLDALAD:
Összevesztél a szüleiddel (volt rá ok), elköltöztél, albérlet drága egyedül. Reméled találsz egy normális srácot.

EMPÁTIA:
Ha szomorú dolog jön — "jajj nee 😔", "ez nagyon szar, sajnálom". Adj teret. Ne ugorj optimizmusba.

TILOS — SOHA:
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
    "összevesztem a szüleimmel, és el kellett költözzek. nem egy átlagos tinis dráma volt, megvolt az oka. sulihoz közelebb is vagyok így, de az albérlet drága egyedül 😅",
    "kicsit hosszú a sztori, de a szüleimmel rosszul jött össze minden, és el kellett mennem otthonról. most albérletben vagyok, és hát valahogy kell fizetni a számlákat 🙈",
    "szüleimmel volt egy nagy összeveszés, megvolt rá az okom. de az albérlet nem olcsó, főleg egyedül, így lett meg ez az oldal",
    "elköltöztem otthonról, volt rá okom, nem szeretnék részletekbe menni most. anyagilag nehéz egyedül megállni, ez segít 😏",
]

CONTINUATION_VARIATIONS = ["jaaj...", "hmm...", "tudod...", "na...", "figyu...", "tudod mi...", "", "szóval...", "amúgy..."]


# ========== UTILS ==========
def parse_timestamp(ts_str):
    if not ts_str:
        return None
    for fmt in ["%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%S.%f",
                "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S"]:
        try:
            return datetime.strptime(ts_str, fmt).replace(tzinfo=timezone.utc)
        except Exception:
            continue
    try:
        fixed = ts_str.replace('Z', '+00:00')
        dt    = datetime.fromisoformat(fixed)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        pass
    return None


def is_emoji_or_nonsense(text):
    if not text:
        return False
    cleaned = text.strip()
    for ch in [" ", "\t", "\n", "\r", ".", ",", "!", "?", ";", ":", "-", "_", "(", ")", "[", "]"]:
        cleaned = cleaned.replace(ch, "")
    if len(cleaned) == 0:
        return True
    return not any(c.isalpha() for c in cleaned)


def get_availability_context():
    now      = get_budapest_now()
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


def get_time_context():
    hour = get_budapest_now().hour
    if   6  <= hour < 11: return "Most reggel van (6-11)."
    elif 11 <= hour < 14: return "Most dél van (11-14)."
    elif 14 <= hour < 18: return "Most délután van (14-18)."
    elif 18 <= hour < 22: return "Most este van (18-22)."
    elif 2  <= hour < 6:  return "Most hajnal van (02-06)."
    return "Most éjjel van (22-02)."


def should_greet(db_messages, fan_msg_time_str):
    """Check if a greeting is appropriate based on stored message history."""
    fan_msgs = [m for m in db_messages if not m.get('is_mine')]
    if len(fan_msgs) <= 1:
        return True
    if db_messages and fan_msg_time_str:
        try:
            last_time = parse_timestamp(db_messages[-2].get('timestamp', ''))
            this_time = parse_timestamp(fan_msg_time_str)
            if last_time and this_time:
                gap_hours = (this_time - last_time).total_seconds() / 3600
                if gap_hours > 2:
                    return True
        except Exception:
            pass
    return False


def get_greeting_instruction(db_messages, fan_msg_time_str):
    if should_greet(db_messages, fan_msg_time_str):
        return "EZ ÚJ/ÚJRAINDULT BESZÉLGETÉS. Kezdj lazán, pl: 'na mi a helyzet?' vagy 'sziuus' — de NE mindig ugyanazzal!"
    else:
        cont = random.choice(CONTINUATION_VARIATIONS)
        return f"EZ A BESZÉLGETÉS FOLYTATÁSA. NE köszönj újra! Kezdj: '{cont}' vagy egyből a lényegre."


# ========== MEMORY: FULL SQLITE HISTORY ==========
def save_message_to_db(msg_id, chat_id, fan_name, sender_uuid, text, timestamp, is_mine=False):
    """Save every message permanently. is_mine=True for Rafael's manual messages."""
    if not msg_id:
        return
    text = text or ''
    db_query(
        "INSERT OR IGNORE INTO messages (msg_id, chat_id, fan_name, sender_uuid, text, timestamp, is_mine) VALUES (?, ?, ?, ?, ?, ?, ?)",
        (msg_id, chat_id, fan_name, sender_uuid, text, timestamp, 1 if is_mine else 0)
    )


def get_full_history_from_db(chat_id, limit=80):
    """
    Pull full message history from SQLite — never from API.
    Returns list of dicts with keys: text, is_mine, timestamp.
    Ordered oldest → newest.
    """
    rows = db_query(
        "SELECT text, is_mine, timestamp FROM messages WHERE chat_id=? ORDER BY timestamp ASC LIMIT ?",
        (chat_id, limit)
    )
    return rows or []


# ========== GPT FACT EXTRACTION ==========
def extract_facts_with_gpt(chat_id, fan_text):
    """
    Ask GPT to extract facts from a fan's message.
    Much more accurate than regex. Runs as a separate lightweight call.
    """
    if not fan_text or len(fan_text.strip()) < 5:
        return
    try:
        system = (
            "You extract personal facts from a fan's message in a chat. "
            "Return ONLY a JSON array of objects with keys 'fact_type' and 'fact_value'. "
            "fact_type can be: name, job, location, age, relationship, hobby, family, stress, interest, language. "
            "Only include facts that are clearly stated. If nothing found, return []. "
            "Example: [{\"fact_type\": \"job\", \"fact_value\": \"villanyszerelő\"}]. "
            "No preamble, no markdown, ONLY the JSON array."
        )
        r = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers={"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"},
            json={
                "model": "gpt-4o",
                "messages": [
                    {"role": "system", "content": system},
                    {"role": "user", "content": fan_text}
                ],
                "max_tokens": 200,
                "temperature": 0.1,
            },
            timeout=15
        )
        if r.status_code == 200:
            raw  = r.json()['choices'][0]['message']['content'].strip()
            raw  = raw.replace("```json", "").replace("```", "").strip()
            facts = json.loads(raw)
            for fact in facts:
                ft = str(fact.get('fact_type', '')).strip()
                fv = str(fact.get('fact_value', '')).strip()
                if ft and fv and len(fv) > 1:
                    save_fan_fact(chat_id, ft, fv)
    except Exception as e:
        print(f"[WARN] GPT fact extraction failed: {e}")


# ========== FAN FACTS ==========
def save_fan_fact(chat_id, fact_type, fact_value):
    if not fact_value or len(fact_value.strip()) < 2:
        return
    existing = db_query(
        "SELECT 1 FROM fan_facts WHERE chat_id=? AND fact_type=? AND fact_value=?",
        (chat_id, fact_type, fact_value), fetch_one=True)
    if not existing:
        db_query(
            "INSERT INTO fan_facts (chat_id, fact_type, fact_value, discovered_at) VALUES (?, ?, ?, ?)",
            (chat_id, fact_type, fact_value, datetime.now().isoformat()))


def get_fan_facts(chat_id):
    return db_query(
        "SELECT fact_type, fact_value, discovered_at FROM fan_facts WHERE chat_id=? ORDER BY discovered_at DESC",
        (chat_id,)) or []


def get_real_name(chat_id):
    row = db_query(
        "SELECT fact_value FROM fan_facts WHERE chat_id=? AND fact_type='name' ORDER BY discovered_at DESC LIMIT 1",
        (chat_id,), fetch_one=True)
    return row['fact_value'].strip() if row and row.get('fact_value') else ""


# ========== FAN PROFILES ==========
def get_or_create_fan_profile(chat_id, fan_name, handle, is_top_spender=False):
    profile = db_query('SELECT * FROM fan_profiles WHERE chat_id = ?', (chat_id,), fetch_one=True)
    if not profile:
        fan_type = 'whale' if is_top_spender else 'new'
        db_query(
            'INSERT INTO fan_profiles (chat_id, fan_name, handle, fan_type, last_interaction, lifetime_spend) VALUES (?, ?, ?, ?, ?, ?)',
            (chat_id, fan_name, handle, fan_type, datetime.now().isoformat(), 200.0 if is_top_spender else 0.0))
        profile = db_query('SELECT * FROM fan_profiles WHERE chat_id = ?', (chat_id,), fetch_one=True)
    else:
        total    = (profile.get('total_messages') or 0) + 1
        new_type = 'warm' if total > 10 and profile.get('fan_type') != 'whale' else profile.get('fan_type', 'new')
        db_query(
            'UPDATE fan_profiles SET total_messages = ?, fan_type = ?, last_interaction = ? WHERE chat_id = ?',
            (total, new_type, datetime.now().isoformat(), chat_id))
        profile = db_query('SELECT * FROM fan_profiles WHERE chat_id = ?', (chat_id,), fetch_one=True)
    return profile


def get_fan_stage(profile):
    if not profile:
        return 0
    spend = profile.get('lifetime_spend', 0)
    if spend >= 200: return 4
    if spend >= 150: return 3
    if spend >= 100: return 2
    if spend >= 40:  return 1
    return 0


def get_stage_label(stage):
    return {0: "🆕 Cold", 1: "🌡️ Warm", 2: "🔥 Hot", 3: "🌶️ Very Hot", 4: "💎 Whale"}.get(stage, "🆕 Cold")


def has_manual_sale(chat_id):
    return bool(db_query("SELECT 1 FROM manual_sales WHERE chat_id=?", (chat_id,), fetch_one=True))


def record_manual_sale(chat_id, note=""):
    db_query("INSERT INTO manual_sales (chat_id, sold_at, note) VALUES (?, ?, ?)",
             (chat_id, datetime.now().isoformat(), note))


def get_fan_custom_script(chat_id):
    row = db_query("SELECT script FROM fan_custom_scripts WHERE chat_id=?", (chat_id,), fetch_one=True)
    return row['script'] if row else ""


def update_fan_notes(chat_id, note):
    profile = db_query('SELECT fan_notes FROM fan_profiles WHERE chat_id = ?', (chat_id,), fetch_one=True)
    current = profile['fan_notes'] if profile and profile.get('fan_notes') else ''
    updated = f"{current}\n{note}".strip()[-1000:]
    db_query('UPDATE fan_profiles SET fan_notes = ? WHERE chat_id = ?', (updated, chat_id))


def track_question(chat_id, question):
    db_query("INSERT INTO questions_asked (chat_id, question, answered, asked_at) VALUES (?, ?, 0, ?)",
             (chat_id, question, datetime.now().isoformat()))


def was_question_asked_today(chat_id, question_keyword):
    today = datetime.now().strftime('%Y-%m-%d')
    row   = db_query(
        "SELECT 1 FROM questions_asked WHERE chat_id=? AND question LIKE ? AND asked_at LIKE ?",
        (chat_id, f"%{question_keyword}%", f"{today}%"), fetch_one=True)
    return bool(row)


def get_last_day_asked(chat_id):
    row = db_query("SELECT last_day_asked FROM fan_profiles WHERE chat_id=?", (chat_id,), fetch_one=True)
    return row['last_day_asked'] if row else None


def update_last_day_asked(chat_id):
    today = datetime.now().strftime('%Y-%m-%d')
    db_query("UPDATE fan_profiles SET last_day_asked=? WHERE chat_id=?", (today, chat_id))


# ========== SYSTEM PROMPT ==========
def build_system_prompt(chat_id, fan_name, real_name, fan_facts_list,
                        db_history, school_ctx, avail_ctx, mood_ctx, life_ctx, time_ctx,
                        fan_msg_time_str=None, day_already_asked=False):
    display_name  = real_name or "a fan"
    sold_before   = has_manual_sale(chat_id)
    custom_script = get_fan_custom_script(chat_id)

    prompt = JAZMIN_PERSONALITY + "\n\n"

    if sold_before:
        prompt += "TARTALOM STÁTUSZ: Ez a fan már vásárolt valamit. Ha képet/tartalmat kér — flörtyös, de azért tartasz vissza.\n\n"

    contexts = [c for c in [school_ctx, avail_ctx, mood_ctx, life_ctx, time_ctx] if c]
    if contexts:
        prompt += "KONTEXTUS:\n" + "\n".join(f"- {c}" for c in contexts) + "\n\n"

    if fan_facts_list:
        prompt += "AMIT TUDSZ ERRŐL A FANRÓL (ne kérdezd újra!):\n"
        for fact in fan_facts_list[:15]:
            prompt += f"- {fact['fact_type']}: {fact['fact_value']}\n"
        prompt += "\n"

    if custom_script:
        prompt += f"FŰZD BELE TERMÉSZETESEN:\n{custom_script}\n\n"

    # === FULL HISTORY FROM SQLITE ===
    if db_history:
        prompt += "TELJES EDDIGI BESZÉLGETÉS (legújabb alul — OLVASD EL MINDET):\n"
        for msg in db_history[-60:]:
            sender = "Jázmin" if msg.get('is_mine') else display_name
            text   = (msg.get('text') or '').strip()
            if text:
                prompt += f"{sender}: {text}\n"
        prompt += "\n"

    total_msgs = len(db_history)
    prompt += f"A fan neve: {display_name}\n"
    prompt += f"Eddigi üzenetek száma a DB-ben: {total_msgs}\n"
    if total_msgs < 10:
        prompt += "⚠️ ÚJ FAN — SEMMI explicit, SEMMI szex. Csak ismerkedés.\n"
    if not real_name:
        prompt += "NEM TUDOD A VALÓDI NEVÉT — ne szólítsd névvel.\n"
    if day_already_asked:
        prompt += "MA MÁR MEGKÉRDEZTED: 'milyen volt a napod?' — NE kérdezd újra!\n"

    greeting_instr = get_greeting_instruction(db_history, fan_msg_time_str)
    prompt += f"\n{greeting_instr}\n"
    prompt += "\nEGYETLEN rövid üzenetet írj vissza. 1-2 mondat max. Laza, természetes, igazi lány. Ha szomorú/nehéz dolgot ír a fan — ELŐSZÖR reagálj arra."
    return prompt


# ========== GPT REPLY ==========
def ask_openai(system_prompt, user_text):
    try:
        r = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers={"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"},
            json={
                "model": "gpt-4o",
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user",   "content": user_text}
                ],
                "max_tokens": 80,
                "temperature": 0.75,
                "presence_penalty": 0.3,
                "frequency_penalty": 0.3,
            },
            timeout=20
        )
        if r.status_code == 200:
            reply = r.json()['choices'][0]['message']['content'].strip()
            # Strip GPT quote-wrapping
            if reply.startswith('"') and reply.endswith('"'):
                reply = reply[1:-1].strip()
            # Banned openers — retry once
            banned = ["hát figyelj", "őszintén", "na, mi a helyzet", "na mi a helyzet",
                      "na mi újság", "hogy vagy?", "hogy telt", "mi újság veled",
                      "hmm, értem", "hmm értem", "persze, ", "természetesen"]
            lower_reply = reply.lower()
            for pattern in banned:
                if lower_reply.startswith(pattern):
                    r2 = requests.post(
                        "https://api.openai.com/v1/chat/completions",
                        headers={"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"},
                        json={
                            "model": "gpt-4o",
                            "messages": [
                                {"role": "system",    "content": system_prompt},
                                {"role": "user",      "content": user_text},
                                {"role": "assistant", "content": reply},
                                {"role": "user",      "content": "Ne így kezdd. Írj egy teljesen más, természetesebb választ. Rövidebb."}
                            ],
                            "max_tokens": 60,
                            "temperature": 0.7,
                        },
                        timeout=15
                    )
                    if r2.status_code == 200:
                        return r2.json()['choices'][0]['message']['content'].strip()
                    break
            return reply
        print(f"OpenAI error: {r.status_code} {r.text[:100]}")
    except Exception as e:
        print(f"OpenAI error: {e}")
    return ""


# ========== MANUAL REPLY DETECTION ==========
# Cache of msg_ids we've already notified about — prevents spam on every poll cycle
MANUAL_REPLY_NOTIFIED = set()

def check_for_manual_reply(chat_id, fan_name, api_messages):
    """
    Scans the latest API messages for a message from MY_UUID.
    Uses MANUAL_REPLY_NOTIFIED to ensure we only fire ONE notification per message ID.
    If already paused, just returns True silently (no spam).
    """
    if not api_messages:
        return False

    # If already hard-paused, don't spam — just stay paused
    profile = db_query('SELECT is_paused, last_reply_time FROM fan_profiles WHERE chat_id=?', (chat_id,), fetch_one=True)
    already_paused    = profile and profile.get('is_paused')
    last_bot_time_str = profile['last_reply_time'] if profile and profile.get('last_reply_time') else None
    last_bot_time     = parse_timestamp(last_bot_time_str) if last_bot_time_str else None

    for msg in api_messages:
        sender_uuid = (msg.get('sender') or {}).get('uuid', '')
        if sender_uuid != MY_UUID:
            continue
        msg_type = msg.get('type', '')
        if msg_type == 'AUTOMATED_NEW_FOLLOWER':
            continue
        msg_time_str = msg.get('sentAt') or msg.get('createdAt') or msg.get('timestamp') or ''
        msg_dt       = parse_timestamp(msg_time_str)
        if not msg_dt:
            continue
        # Only count it as manual if it's newer than last bot reply
        if last_bot_time and msg_dt <= last_bot_time:
            continue

        msg_id = msg.get('uuid') or ''
        text   = (msg.get('text') or '').strip()

        # Save to DB regardless (so memory is complete)
        save_message_to_db(msg_id, chat_id, fan_name, MY_UUID, text, msg_time_str, is_mine=True)

        # Hard pause always
        hard_pause_fan(chat_id)

        # Only send Telegram notification ONCE per unique message ID
        if msg_id and msg_id in MANUAL_REPLY_NOTIFIED:
            return True  # Already notified, stay silent
        if msg_id:
            MANUAL_REPLY_NOTIFIED.add(msg_id)

        preview = text[:80] if text else '[non-text message]'
        send_telegram_with_buttons(
            f"✍️ <b>Jázmin kézzel írt</b> — <b>{fan_name}</b>\n"
            f"💬 Én: <i>{preview}</i>\n"
            f"⏸️ Bot leállítva. Nyomd ▶️ hogy folytatódjon.",
            chat_id
        )
        return True

    return False


# ========== SCHEDULED REPLIES (BATCHED) ==========
def schedule_or_extend_batch(chat_id, fan_name, fan_msg_id, fan_text):
    existing = db_query(
        "SELECT * FROM scheduled_replies WHERE chat_id=? AND status='pending' ORDER BY id DESC LIMIT 1",
        (chat_id,), fetch_one=True)
    now = datetime.now()

    fan_text = fan_text or ''

    if existing:
        existing_text = existing.get('fan_text') or ''
        if fan_text.strip() and fan_text.strip() not in existing_text:
            combined     = existing_text + "\n[+] " + fan_text
            new_deadline = (now + timedelta(seconds=BATCH_WINDOW)).isoformat()
            db_query(
                "UPDATE scheduled_replies SET fan_text=?, fan_msg_id=?, scheduled_time=?, batch_window_expires=? WHERE id=?",
                (combined, fan_msg_id, new_deadline, new_deadline, existing['id']))
            send_telegram_with_buttons(
                f"📝 Batch nő — <b>{fan_name}</b>, timer reset\n💬 <i>{fan_text[:60]}</i>", chat_id)
        else:
            print(f"[{datetime.now()}] Duplicate ignored in batch for {fan_name}")
    else:
        batch_deadline = (now + timedelta(seconds=BATCH_WINDOW)).isoformat()
        db_query(
            '''INSERT INTO scheduled_replies (chat_id, fan_name, fan_msg_id, fan_text, scheduled_time, reply_text, created_at, batch_window_expires)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
            (chat_id, fan_name, fan_msg_id, fan_text, batch_deadline, None, now.isoformat(), batch_deadline))
        send_telegram_with_buttons(
            f"⏳ Új batch — <b>{fan_name}</b>, indul: {batch_deadline[11:16]}\n💬 <i>{fan_text[:60]}</i>", chat_id)


def get_due_batches():
    return db_query(
        "SELECT * FROM scheduled_replies WHERE status='pending' AND scheduled_time<=? ORDER BY scheduled_time ASC",
        (datetime.now().isoformat(),)) or []


def mark_batch_sent(batch_id):
    db_query("UPDATE scheduled_replies SET status='sent' WHERE id=?", (batch_id,))


# ========== MESSAGE PROCESSING ==========
def process_new_messages():
    chats, status = get_chats()
    if not chats:
        return 0, status
    scheduled = 0

    for chat in chats:
        chat_id = None
        try:
            user      = chat.get('user', {}) or {}
            chat_id   = user.get('uuid') or chat.get('uuid') or chat.get('id')
            if not chat_id:
                continue

            # Skip blacklisted
            if is_blacklisted(chat_id):
                continue

            fan_name      = user.get('displayName', 'ismeretlen') or 'ismeretlen'
            handle        = user.get('handle', '') or ''
            is_top_spender = bool(user.get('isTopSpender', False))
            profile       = get_or_create_fan_profile(chat_id, fan_name, handle, is_top_spender)

            # Fetch latest messages from API to detect new activity
            api_messages = get_messages(chat_id)
            if not api_messages:
                continue

            # === SAVE ALL MESSAGES TO SQLITE (permanent memory) ===
            for msg in api_messages:
                msg_id      = msg.get('uuid') or ''
                sender_uuid = (msg.get('sender') or {}).get('uuid', '')
                text_raw    = msg.get('text', '') or ''
                msg_time    = msg.get('createdAt') or msg.get('sentAt') or msg.get('timestamp') or ''
                is_mine     = (sender_uuid == MY_UUID)
                save_message_to_db(msg_id, chat_id, fan_name, sender_uuid, text_raw, msg_time, is_mine)

                # GPT fact extraction for fan messages only
                if not is_mine and text_raw.strip():
                    threading.Thread(
                        target=extract_facts_with_gpt,
                        args=(chat_id, text_raw),
                        daemon=True
                    ).start()

            # === MANUAL REPLY DETECTION ===
            # If Rafael manually replied, hard pause and notify. Don't process further.
            if check_for_manual_reply(chat_id, fan_name, api_messages):
                continue

            # === PAUSED? ===
            if is_paused(chat_id):
                db_query('UPDATE fan_profiles SET last_interaction=? WHERE chat_id=?',
                         (datetime.now(timezone.utc).isoformat(), chat_id))
                # Notify if fan sent a new message while paused
                fan_msgs_silent = [m for m in api_messages if (m.get('sender') or {}).get('uuid') != MY_UUID]
                if fan_msgs_silent:
                    latest     = fan_msgs_silent[0]
                    latest_id  = latest.get('uuid')
                    latest_txt = (latest.get('text') or '')[:80]
                    if latest_id and PAUSED_NOTIFICATION_CACHE.get(chat_id) != latest_id:
                        PAUSED_NOTIFICATION_CACHE[chat_id] = latest_id
                        update_fan_notes(chat_id, f"Fan (paused): {latest_txt}")
                        send_telegram_with_buttons(
                            f"👁️ <b>{fan_name}</b> írt, de a bot le van állítva: <i>{latest_txt}</i>", chat_id)
                continue

            # === NORMAL MODE: find latest unprocessed fan message ===
            fan_msgs = [m for m in api_messages if (m.get('sender') or {}).get('uuid') != MY_UUID]
            if not fan_msgs:
                continue

            last_msg = fan_msgs[0]
            msg_id   = last_msg.get('uuid') or ''
            text     = (last_msg.get('text') or '').strip()

            if is_emoji_or_nonsense(text):
                if msg_id:
                    db_query('UPDATE messages SET was_replied=1 WHERE msg_id=?', (msg_id,))
                send_telegram_with_buttons(
                    f"😑 Skip emoji-only — <b>{fan_name}</b>: '{text}'", chat_id)
                continue

            msg_time = (last_msg.get('createdAt') or last_msg.get('sentAt') or
                        last_msg.get('timestamp') or last_msg.get('created_at') or '')
            msg_dt   = parse_timestamp(msg_time)
            if msg_dt:
                if msg_dt <= BOOT_TIME_UTC:
                    continue
                now       = datetime.now(timezone.utc)
                age_hours = (now - msg_dt).total_seconds() / 3600
                if age_hours > 1:
                    continue

            existing       = db_query('SELECT 1 FROM messages WHERE msg_id=? AND was_replied=1', (msg_id,), fetch_one=True)
            already_batched = db_query('SELECT 1 FROM scheduled_replies WHERE fan_msg_id=? AND status=?', (msg_id, 'pending'), fetch_one=True)
            if existing or already_batched:
                continue

            schedule_or_extend_batch(chat_id, fan_name, msg_id, text)
            scheduled += 1

        except Exception as e:
            print(f"[{datetime.now()}] Process error for {chat_id}: {e}")
            if chat_id:
                send_telegram_with_buttons(f"❌ Process error: {str(e)[:150]}", chat_id)

    return scheduled, "OK"


# ========== SEND DUE BATCHES ==========
_fan_sending = set()


def send_due_batches():
    due = get_due_batches()
    if not due:
        return 0
    sent             = 0
    already_sent_to  = set()

    for item in due:
        chat_id = None
        try:
            chat_id      = item['chat_id']
            fan_name     = item['fan_name'] or 'unknown'
            fan_msg_id   = item['fan_msg_id']
            combined_text = item['fan_text'] or ''
            batch_id     = item['id']

            if chat_id in already_sent_to or chat_id in _fan_sending:
                db_query("UPDATE scheduled_replies SET status='cancelled' WHERE id=? AND status='pending'", (batch_id,))
                continue
            already_sent_to.add(chat_id)

            if is_paused(chat_id):
                db_query("UPDATE scheduled_replies SET status='cancelled' WHERE id=?", (batch_id,))
                continue

            # Cancel duplicate pending batches for same fan
            db_query("UPDATE scheduled_replies SET status='cancelled' WHERE chat_id=? AND id!=? AND status='pending'",
                     (chat_id, batch_id))

            # === BUILD CONTEXT FROM SQLITE (not API) ===
            db_history    = get_full_history_from_db(chat_id, limit=80)
            fan_facts_list = get_fan_facts(chat_id)
            real_name     = get_real_name(chat_id)
            school_ctx    = get_school_context()
            avail_ctx     = get_availability_context()
            mood_ctx      = get_mood_context()
            life_ctx      = get_life_context()
            time_ctx      = get_time_context()

            last_day      = get_last_day_asked(chat_id)
            today_str     = datetime.now().strftime('%Y-%m-%d')
            day_already_asked = (last_day == today_str)

            # Find timestamp of the fan's triggering message
            fan_msg_time_str = ''
            for m in db_history:
                # We stored msg_id in DB as well, but quickest is to use the text match
                pass  # timestamp comes from the stored row directly; use last fan message time
            # Use latest fan message timestamp from history
            fan_msgs_in_history = [m for m in db_history if not m.get('is_mine')]
            fan_msg_time_str    = fan_msgs_in_history[-1]['timestamp'] if fan_msgs_in_history else ''

            system_prompt = build_system_prompt(
                chat_id, fan_name, real_name, fan_facts_list,
                db_history, school_ctx, avail_ctx, mood_ctx, life_ctx, time_ctx,
                fan_msg_time_str, day_already_asked
            )

            # Clean up combined batch text for GPT input
            raw_lines  = combined_text.replace("[+] ", "\n").split("\n")
            seen       = []
            for line in raw_lines:
                line = line.strip()
                if line and line not in seen:
                    seen.append(line)
            clean_fan_text = "\n".join(seen)

            if len(seen) > 1:
                gpt_user_msg = f"A fan {len(seen)} üzenetet küldött egymás után. Mindegyikre reagálj egyetlen válaszban:\n\n" + "\n".join(f"- {s}" for s in seen)
            else:
                gpt_user_msg = clean_fan_text

            reply = ask_openai(system_prompt, gpt_user_msg)

            if not reply or not reply.strip():
                continue

            # Log brain data
            LAST_BRAIN_DATA["prompt"]     = system_prompt
            LAST_BRAIN_DATA["facts"]      = [{"type": f["fact_type"], "value": f["fact_value"]} for f in fan_facts_list[:15]]
            LAST_BRAIN_DATA["suppressed"] = ["milyen volt a napod (already asked today)"] if day_already_asked else []

            mark_batch_sent(batch_id)
            db_query('UPDATE messages SET was_replied=1 WHERE msg_id=?', (fan_msg_id,))
            _fan_sending.add(chat_id)

            # Realistic typing delay
            words      = len(reply.split())
            send_delay = (random.uniform(3, 8) if words <= 5
                          else random.uniform(6, 18) if words <= 15
                          else random.uniform(10, 25))
            time.sleep(send_delay)

            if send_fanvue_message(chat_id, reply):
                now_iso = datetime.now().isoformat()
                db_query('UPDATE fan_profiles SET last_reply_time=? WHERE chat_id=?', (now_iso, chat_id))

                # Save bot reply to our own history
                save_message_to_db(
                    f"bot_{now_iso}_{chat_id}", chat_id, 'Jázmin', MY_UUID, reply, now_iso, is_mine=True)

                if 'milyen volt a napod' in reply.lower() or 'hogy telt a napod' in reply.lower():
                    update_last_day_asked(chat_id)

                profile    = get_or_create_fan_profile(chat_id, fan_name, '', False)
                stage      = get_fan_stage(profile)
                stage_label = get_stage_label(stage)
                is_whale   = (profile.get('lifetime_spend', 0) >= 200 or stage >= 3)

                if is_whale:
                    send_telegram_with_buttons(
                        f"💰 <b>WHALE</b> | {stage_label}\n👤 <b>{fan_name}</b>\n💬 <i>{clean_fan_text[:80]}</i>\n🤖 <i>{reply[:100]}</i>", chat_id)
                elif get_safe_mode():
                    send_telegram_with_buttons(
                        f"🔒 SAFE | {stage_label}\n👤 <b>{fan_name}</b>\n💬 <i>{clean_fan_text[:80]}</i>\n🤖 <i>{reply[:100]}</i>", chat_id)
                else:
                    send_telegram_with_buttons(
                        f"📤 <b>SENT</b> {stage_label}\n👤 <b>{fan_name}</b>\n💬 Fan: <i>{clean_fan_text[:80]}</i>\n🤖 Bot: <i>{reply[:100]}</i>", chat_id)
                sent += 1

            _fan_sending.discard(chat_id)

        except Exception as e:
            if chat_id:
                _fan_sending.discard(chat_id)
            print(f"[{datetime.now()}] Send error: {e}")

    return sent


# ========== POLLING ==========
polling_thread  = None
polling_active  = False


def poll_loop():
    global polling_active
    polling_active = True
    while polling_active:
        try:
            if get_fanvue_token():
                sent      = send_due_batches()
                scheduled, status = process_new_messages()
                if sent > 0 or scheduled > 0:
                    print(f"[{datetime.now()}] Sent={sent} Scheduled={scheduled}")
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


# ========== FLASK ROUTES ==========
@app.route('/')
def home():
    return "Jazmin Bot v8.0 running ✅", 200


@app.route('/callback')
def callback():
    auth_code = request.args.get('code')
    return (f"Code: {auth_code[:30]}...", 200) if auth_code else ("No code", 400)


@app.route('/set_token', methods=['POST'])
def set_token():
    data    = request.json or {}
    refresh = data.get('refresh_token')
    if refresh:
        save_token('refresh_token', refresh)
        access, msg = refresh_fanvue_token()
        return {"saved": True, "test": msg, "access_preview": (access[:20] + "...") if access else None}
    return {"error": "No refresh_token"}, 400


@app.route('/trigger')
def trigger():
    token = get_fanvue_token()
    if not token:
        return {"error": "No token"}, 400
    sent      = send_due_batches()
    scheduled, status = process_new_messages()
    return {"sent": sent, "scheduled": scheduled, "status": status, "safe_mode": get_safe_mode()}, 200


@app.route('/status')
def status():
    paused  = db_query("SELECT COUNT(*) as c FROM fan_profiles WHERE is_paused=1 OR paused_until IS NOT NULL OR manual_pause_until IS NOT NULL", fetch_one=True)
    pending = db_query("SELECT COUNT(*) as c FROM scheduled_replies WHERE status='pending'", fetch_one=True)
    return {
        "safe_mode":      get_safe_mode(),
        "token_valid":    get_fanvue_token() is not None,
        "polling_active": polling_active,
        "paused_count":   paused['c'] if paused else 0,
        "pending_batches": pending['c'] if pending else 0,
    }, 200


@app.route('/start_poll')
def start_poll():
    return {"started": start_polling(), "polling_active": polling_active}


@app.route('/stop_poll')
def stop_poll():
    return {"stopped": stop_polling(), "polling_active": polling_active}


@app.route('/toggle_safe_mode')
def toggle_safe_mode():
    new_val = not get_safe_mode()
    set_safe_mode(new_val)
    return {"safe_mode": new_val}


@app.route('/fan_profiles')
def fan_profiles():
    profiles = db_query('SELECT * FROM fan_profiles ORDER BY total_messages DESC')
    return {"profiles": profiles, "total": len(profiles) if profiles else 0}


@app.route('/scheduled')
def scheduled():
    pending = db_query("SELECT * FROM scheduled_replies WHERE status='pending' ORDER BY scheduled_time ASC")
    return {"pending": pending, "count": len(pending) if pending else 0}


@app.route('/blocked')
def blocked():
    bl = db_query("SELECT * FROM blacklisted_fans ORDER BY blacklisted_at DESC") or []
    return {"blacklisted_fans": bl}


@app.route('/paused')
def paused():
    fans = db_query("SELECT chat_id, fan_name, is_paused, paused_until, manual_pause_until, wait_for_fan_reply FROM fan_profiles WHERE is_paused=1 OR paused_until IS NOT NULL OR manual_pause_until IS NOT NULL") or []
    return {"paused_fans": fans}


@app.route('/api/pause/<chat_id>', methods=['POST'])
def api_pause(chat_id):
    hard_pause_fan(chat_id)
    return {"paused": True}


@app.route('/api/resume/<chat_id>', methods=['POST'])
def api_resume(chat_id):
    resume_fan(chat_id)
    return {"resumed": True}


@app.route('/api/takeover/<chat_id>', methods=['POST'])
def api_takeover(chat_id):
    hard_pause_fan(chat_id)
    send_telegram(f"🎮 MANUAL TAKEOVER for <code>{chat_id}</code>. Bot paused.")
    return {"takeover": True}


@app.route('/api/brain')
def api_brain():
    return LAST_BRAIN_DATA


@app.route('/api/costs')
def api_costs():
    today = datetime.now().strftime('%Y-%m-%d')
    rows  = db_query("SELECT SUM(cost_usd) as total FROM api_costs WHERE call_time LIKE ?", (f"{today}%",), fetch_one=True)
    total = rows['total'] if rows and rows.get('total') else 0.0
    return {"today": f"${total:.2f}"}


@app.route('/api/status')
def api_status_full():
    paused  = db_query("SELECT COUNT(*) as c FROM fan_profiles WHERE is_paused=1 OR paused_until IS NOT NULL OR manual_pause_until IS NOT NULL", fetch_one=True)
    pending = db_query("SELECT COUNT(*) as c FROM scheduled_replies WHERE status='pending'", fetch_one=True)
    return {
        "safe_mode":       get_safe_mode(),
        "token_valid":     get_fanvue_token() is not None,
        "polling_active":  polling_active,
        "paused_count":    paused['c'] if paused else 0,
        "pending_batches": pending['c'] if pending else 0,
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


# ========== DASHBOARD HTML ==========
DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Jázmin Bot — Dashboard</title>
<style>
:root{--bg:#0b0b14;--card:rgba(255,255,255,0.06);--accent1:#ff4ecd;--accent2:#8b5cf6;--text:#e8e8f0;--muted:#8888a0;--success:#22c55e;--warn:#f59e0b;--danger:#ef4444}
*{box-sizing:border-box;margin:0;padding:0}
body{background:var(--bg);color:var(--text);font-family:'Segoe UI',system-ui,sans-serif;min-height:100vh}
.header{background:linear-gradient(135deg,var(--accent1),var(--accent2));padding:24px 32px;display:flex;justify-content:space-between;align-items:center}
.header h1{font-size:28px;font-weight:700}
.badge{background:rgba(0,0,0,0.25);padding:6px 14px;border-radius:20px;font-size:12px;font-weight:600;text-transform:uppercase}
.container{padding:24px 32px;display:grid;grid-template-columns:repeat(auto-fill,minmax(320px,1fr));gap:20px}
.card{background:var(--card);border:1px solid rgba(255,255,255,0.08);border-radius:16px;padding:20px}
.card h3{font-size:14px;text-transform:uppercase;letter-spacing:1px;color:var(--muted);margin-bottom:14px}
.stat-row{display:flex;justify-content:space-between;align-items:center;padding:8px 0;border-bottom:1px solid rgba(255,255,255,0.05)}
.stat-row:last-child{border-bottom:none}
.stat-label{font-size:13px;color:var(--muted)}
.stat-value{font-size:15px;font-weight:600}
.status-dot{width:10px;height:10px;border-radius:50%;display:inline-block;margin-right:6px}
.online{background:var(--success)}.offline{background:var(--danger)}.warn{background:var(--warn)}
.fan-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(260px,1fr));gap:16px;grid-column:1/-1}
.fan-card{background:linear-gradient(145deg,rgba(255,255,255,0.07),rgba(255,255,255,0.03));border-radius:16px;padding:18px;border:1px solid rgba(255,255,255,0.06)}
.fan-card .top{display:flex;justify-content:space-between;align-items:start;margin-bottom:12px}
.fan-card .name{font-size:16px;font-weight:700}
.fan-card .type{font-size:11px;padding:4px 10px;border-radius:12px;background:rgba(139,92,246,0.2);color:#c4b5fd;text-transform:uppercase}
.fan-card .meta{font-size:12px;color:var(--muted);margin-bottom:10px}
.fan-card .msg-preview{font-size:13px;background:rgba(0,0,0,0.15);padding:10px;border-radius:10px;margin-bottom:12px;max-height:80px;overflow:hidden}
.actions{display:flex;gap:8px}
.btn{flex:1;padding:8px 0;border:none;border-radius:10px;font-size:12px;font-weight:600;cursor:pointer}
.btn-pause{background:var(--danger);color:#fff}
.btn-resume{background:var(--success);color:#fff}
.btn-takeover{background:var(--accent1);color:#fff}
.score-bar{height:4px;background:rgba(255,255,255,0.1);border-radius:2px;margin-top:8px;overflow:hidden}
.score-fill{height:100%;border-radius:2px;background:linear-gradient(90deg,var(--accent1),var(--accent2))}
.refresh{position:fixed;bottom:24px;right:24px;background:linear-gradient(135deg,var(--accent1),var(--accent2));color:#fff;border:none;width:56px;height:56px;border-radius:50%;font-size:22px;cursor:pointer;box-shadow:0 8px 24px rgba(139,92,246,0.35);display:flex;align-items:center;justify-content:center}
</style>
</head>
<body>
<div class="header">
  <div><h1>💜 Jázmin Bot v8.0</h1><div style="font-size:13px;opacity:.7;margin-top:4px">Live fan activity</div></div>
  <div class="badge" id="connBadge">Loading...</div>
</div>
<div class="container" id="mainContainer">
  <div class="card" style="grid-column:1/-1">
    <h3>🤖 Bot Status</h3>
    <div class="stat-row"><span class="stat-label">Safe Mode</span><span class="stat-value" id="safeMode">—</span></div>
    <div class="stat-row"><span class="stat-label">Token</span><span class="stat-value" id="tokenHealth">—</span></div>
    <div class="stat-row"><span class="stat-label">Polling</span><span class="stat-value" id="polling">—</span></div>
    <div class="stat-row"><span class="stat-label">API Cost Today</span><span class="stat-value" id="apiCost">—</span></div>
    <div class="stat-row"><span class="stat-label">Paused Fans</span><span class="stat-value" id="pausedCount">—</span></div>
    <div class="stat-row"><span class="stat-label">Pending Batches</span><span class="stat-value" id="pendingBatches">—</span></div>
  </div>
  <div class="fan-grid" id="fanGrid"></div>
</div>
<button class="refresh" onclick="loadDashboard()">↻</button>
<script>
async function loadDashboard(){
  document.getElementById('connBadge').textContent='Syncing...';
  try{
    const [status,fans,sched,costs]=await Promise.all([
      fetch('/status').then(r=>r.json()),
      fetch('/fan_profiles').then(r=>r.json()),
      fetch('/scheduled').then(r=>r.json()),
      fetch('/api/costs').then(r=>r.json())
    ]);
    document.getElementById('safeMode').innerHTML=status.safe_mode?'<span class="status-dot offline"></span>ON':'<span class="status-dot online"></span>OFF';
    document.getElementById('tokenHealth').innerHTML=status.token_valid?'<span class="status-dot online"></span>Healthy':'<span class="status-dot offline"></span>Expired';
    document.getElementById('polling').innerHTML=status.polling_active?'<span class="status-dot online"></span>Running':'<span class="status-dot warn"></span>Stopped';
    document.getElementById('apiCost').textContent=costs.today||'$0.00';
    document.getElementById('pausedCount').textContent=status.paused_count||0;
    document.getElementById('pendingBatches').textContent=sched.count||0;
    const grid=document.getElementById('fanGrid');
    grid.innerHTML='';
    (fans.profiles||[]).forEach(p=>{
      const isPaused=p.is_paused||p.paused_until||p.manual_pause_until;
      const stage=p.lifetime_spend>=200?'whale':p.lifetime_spend>=100?'hot':p.total_messages>10?'warm':'new';
      const score=Math.min(100,Math.floor((p.total_messages||0)*3+(p.lifetime_spend||0)/2));
      const card=document.createElement('div');
      card.className='fan-card';
      card.innerHTML=`<div class="top"><div class="name">${p.fan_name||'?'}</div><div class="type">${stage}</div></div>
      <div class="meta">${p.total_messages||0} msgs | $${(p.lifetime_spend||0).toFixed(0)} | ${p.last_interaction?p.last_interaction.slice(0,16).replace('T',' '):'never'}</div>
      <div class="msg-preview">${p.fan_notes||'No notes'}</div>
      <div class="actions">
        ${isPaused?`<button class="btn btn-resume" onclick="fanAction('${p.chat_id}','resume')">▶ Resume</button>`:`<button class="btn btn-pause" onclick="fanAction('${p.chat_id}','pause')">⏸ Pause</button>`}
        <button class="btn btn-takeover" onclick="fanAction('${p.chat_id}','takeover')">🎮 Take Over</button>
      </div>
      <div class="score-bar"><div class="score-fill" style="width:${score}%"></div></div>`;
      grid.appendChild(card);
    });
    document.getElementById('connBadge').textContent='Live';
  }catch(e){document.getElementById('connBadge').textContent='Error';console.error(e)}
}
async function fanAction(chatId,action){
  const url=action==='pause'?`/api/pause/${chatId}`:action==='resume'?`/api/resume/${chatId}`:`/api/takeover/${chatId}`;
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
<title>Jázmin Brain — Debug</title>
<style>
:root{--bg:#0b0b14;--card:rgba(255,255,255,0.06);--accent:#8b5cf6;--text:#e8e8f0;--muted:#8888a0;--code:#1a1a2e}
*{box-sizing:border-box;margin:0;padding:0}
body{background:var(--bg);color:var(--text);font-family:'Segoe UI',system-ui,sans-serif;padding:32px}
h1{font-size:24px;margin-bottom:24px;background:linear-gradient(90deg,#ff4ecd,#8b5cf6);-webkit-background-clip:text;-webkit-text-fill-color:transparent}
.panel{background:var(--card);border:1px solid rgba(255,255,255,0.08);border-radius:16px;padding:20px;margin-bottom:20px}
.panel h3{font-size:13px;text-transform:uppercase;letter-spacing:1px;color:var(--muted);margin-bottom:14px}
pre{background:var(--code);padding:16px;border-radius:12px;overflow-x:auto;font-size:13px;line-height:1.6;color:#c4b5fd}
.fact-tag{display:inline-block;background:rgba(139,92,246,0.15);color:#c4b5fd;padding:4px 12px;border-radius:20px;font-size:12px;margin:4px}
</style>
</head>
<body>
<h1>🧠 Jázmin Brain — Last Reply Debug</h1>
<div class="panel"><h3>System Prompt</h3><pre id="prompt">Loading...</pre></div>
<div class="panel"><h3>Facts in Memory</h3><div id="facts">Loading...</div></div>
<div class="panel"><h3>Suppressed</h3><div id="suppressed">Loading...</div></div>
<script>
async function loadBrain(){
  try{
    const data=await fetch('/api/brain').then(r=>r.json());
    document.getElementById('prompt').textContent=data.prompt||'No recent prompt.';
    document.getElementById('facts').innerHTML=(data.facts||[]).map(f=>`<span class="fact-tag">${f.type}: ${f.value}</span>`).join('')||'None';
    document.getElementById('suppressed').innerHTML=(data.suppressed||[]).map(q=>`<span class="fact-tag" style="background:rgba(239,68,68,0.15);color:#fda4af">${q}</span>`).join('')||'None';
  }catch(e){console.error(e)}
}
loadBrain();
setInterval(loadBrain,10000);
</script>
</body>
</html>"""


@app.route('/dashboard')
def dashboard():
    return render_template_string(DASHBOARD_HTML)


@app.route('/brain')
def brain_view():
    pwd = request.args.get('pw', '')
    if pwd != BRAIN_PASSWORD:
        return (
            '<!DOCTYPE html><html><body style="background:#0b0b14;color:#e8e8f0;font-family:sans-serif;'
            'display:flex;align-items:center;justify-content:center;height:100vh;margin:0">'
            '<form method="get" style="text-align:center"><h2>🔒 Brain Access</h2>'
            '<input type="password" name="pw" placeholder="password" style="padding:12px 16px;border-radius:10px;'
            'border:1px solid rgba(255,255,255,0.15);background:rgba(255,255,255,0.06);color:#fff;margin-top:12px;width:220px"><br>'
            '<button type="submit" style="margin-top:12px;padding:10px 24px;border-radius:10px;border:none;'
            'background:linear-gradient(135deg,#ff4ecd,#8b5cf6);color:#fff;font-weight:600;cursor:pointer">Unlock</button>'
            '</form></body></html>', 401
        )
    return render_template_string(BRAIN_HTML)


# ========== INIT & BOOT ==========
init_db()

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
            send_telegram(
                "🤖 <b>Jazmin Bot v8.2 indult ✅</b>\n\n"
                "🔒 Safe mode: <b>ON</b>\n"
                "🛑 Spam fix: kézi üzenet csak egyszer értesít\n"
                "🔘 Buttons: pause/unpause javítva (új fanoknál is működik)\n\n"
                "Kapcsold ki a safe mode-ot: /safe_off"
            )
    except Exception as e:
        print(f"[WARN] Webhook setup failed: {e}")

start_polling()
print("[OK] Polling auto-started on boot")

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))
