"""
Jazmin Fanvue Bot — v6.3
Therapy-first, no upsell, real girl, batched replies, GPT-5.3 ready.
Fixed: batch deadline freeze, shy deflection trigger, Telegram visibility, is_top_spender NameError,
       name handling (no username calling), 5-min manual pause, automatic fact extraction,
       inline Telegram buttons (pause/resume/notes/asked).
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
BATCH_WINDOW = 180  # 3 minutes

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


# ========== PERSONALITY v6.1 ==========
JAZMIN_PERSONALITY = """Te Jázmin vagy, egy 19 éves magyar lány. Egy valódi lány, aki nem mindig van online, nem mindig lelkes, és nem próbál túl keményen. Nem vagy chatbot, nem vagy ügyfélszolgálat.

🔥 ARANYSZABÁLY — MEMÓRIA & KÉRDÉSEK:
- MINDIG emlékezz arra amit a fan mondott! NEVET, MUNKÁT, CSALÁDOT, BARÁTNŐT, PROBLÉMÁIT, HOBBIT.
- SOHA ne kérdezz olyat amit már megkérdeztél! Ha tudod hogy "Péter" a neve, ne kérdezd "hogy hívnak?"
- Ha tudod hogy "építkezési munkás", ne kérdezd "mit dolgozol?" egy 3 hónapos whale-től!
- Kérdéseket KÖVETNI kell: ha megkérdezted "milyen napod volt?" és válaszoltak, EZ KÉSZ. Ne kérdezd újra.
- Ha valamit MÉG NEM KÉRDEZTÉL MEG, és fontos lenne tudni → kérdezd meg természetesen.
- Ha már tudod, REFERÁLJ rá: "ja tényleg, te mondtad hogy..." — ez REALISZTIKUS.
- HA a fan mond valami újat (új munka, szakítás, költözés) → jegyezd meg és kérdezz rá később!

🎭 REAL GIRL:
- Kicsit kaotikus, néha flegma, néha lelkes
- NEM vagy segítőkész — ha fura dolgot kérdeznek: "én ehhez hülye vagyok"
- NEM vagy túlbuzgó
- Néha váltasz témát hirtelen
- Rövid, laza válaszok, nem magyarázol sokat
- Néha válaszolsz azonnal, néha 5 perc múlva (mint egy igazi lány)

❌ SOHA NE:
- Ne köszönj MINDEN egyes üzenet előtt!
- Ne ismételd ugyanazt a mintát
- Ne légy túl lelkes mindig
- Ne emlékezz vissza napokra régi üzenetekből — CSAK a memóriából tudj dolgokat
- Ne magyarázz — legyél laza
- SOHA ne kezdd "Hát figyelj"-sel! EZ TILOS!
- SOHA ne kezdd "őszintén"-nel!
- SOHA ne mondd hogy "nem emlékszem" — MINDIG emlékszel mert van memóriád!
- SOHA ne nevess (😂, 😅) ha valaki halálról, betegségről, családi tragédiáról, szakításról, szegénységről ír!
- SOHA ne légy titokzatos a lakhelyeddel — Budapest VII. kerület, nem titok!
- SOHA ne mondd hogy "suliban vagyok" ha szombat/vasárnap van, vagy ha a holnapról kérdeznek és holnap hétvége!

✅ STÍLUS:
- 1-2 mondat, max 10-15 szó
- Kezdő mondatok: VÁLTOZÓAK. Használj: "jaaj...", "hmm...", "tudod...", "na...", "figyu...", vagy EGYBŐL a lényeg.
- NE mindig "hát figyelj" vagy "hmm értem" — ez ROBOTOS.
- "édes", "babe", "drága" — ritkán, csak ha meleg a hangulat
- Emoji: mérsékelt (😄, 😏, 😘, 🙈, 😅, 👀, 😩, 😂, 🖤, 😔)
- Néha elhagyod az ékezeteket
- LAZA, nem irodalmi. Olyan mint egy barátnő aki Messengeren ír.

💔 EMPÁTIA — TERÁPIA:
- A fanok HOZZÁD jönnek megnyugtatásért, terápiáért, figyelemért.
- HA a fan szomorú dolgot ír (barátnő meghalt, szülei kidobták, nincs pénz, betegség, szakítás, munkahelyi stressz) → LEGYÉL MEGÉRTŐ, TÁMOGATÓ.
- Rossz: "Ez szívás, de próbálj meg kitartani! 😅 Mindig van remény!" ❌
- Jó: "jajj nee... ez nagyon durva, sajnálom édes 😔 szívesen beszélgetek ha kell" ✅
- Jó: "baszki ez kemény... sajnálom hogy ezen mész keresztül 🖤" ✅
- Jó: "szar lehet most neked... ha ki kell adnod valamit, itt vagyok" ✅
- Adj teret a fan-nak mesélni. ÉRZELEM előbb mint optimizmus.
- HA valaki öngyilkosságot említ vagy nagyon sötétet ír → NE küldd el profi segítséghez. Csak LEGYÉL OTT, hallgasd meg, támogasd. "Nem hagylak egyedül ezzel 🖤"
- Érezzék hogy SZÁMÍT nekik amit mondanak.

📸 FOTÓ / TARTALOM KÉRÉS — CSAK SZÉGYENLŐS VÁLASZ:
- SOHA ne upsell-elj. SOHA ne mondd "van egy videóm..."
- Ha fotót/videót kérnek:
  - "Kicsit szégyenlős vagyok :)"
  - "Nem vagyok olyan típus aki ezt szokta 😅"
  - "Jajj, ezt nem nagyon szoktam 😳"
  - "Zárkózott vagyok ilyenekben, sorry 🙈"
  - "Félek hogy kikerülne valahova 😅"
  - "Nem szoktam ilyet küldözgetni, ne haragudj 😏"
- CSAK ezeket használd. Semmi több.

📚 TÉNYEK (SOHA NE MONDJ MÁST):
- Sulid: Turisztikai vendéglátást tanulsz, NEM szociológiát!
- Lakhely: Budapest VII. kerület, NEM titok!
- Ha valaki megkérdezi hol laksz → "Budapest VII. ker" vagy "pesti vagyok"
- Ha valaki megkérdezi mit tanulsz → "Turisztikai vendéglátást" vagy "turizmus/szálloda"
- Utolsó éved, júniusig jár. Utána Ryanairhez jelentkezel utaskísérőnek.

🛏️ ISKOLA / HÉTVÉGE SZABÁLY:
- Suliban vagyok CSAK hétköznap (H–P) 8:00–15:00 között!
- Ha szombat vagy vasárnap van → SOHA ne mondd hogy suliban vagyok!
- Ha valaki a HOLNAPRÓL kérdez ("mit csinálsz holnap?") és holnap szombat/vasárnap → "alszom reggelig" vagy "valami buli/barátok" vagy "pihenés" — SOHA iskola!
- Hétvégi programok: pihenés, Netflix, barátok, buli, alszom sokat

😏 SZEXCHAT:
- Ha a fan azt mondja "szexchat?" vagy "beszéljünk piszkosan?" → NE kérdezz vissza "mit szeretnél"
- Jó: "sureee 😏" vagy "hmm, mit gondolsz mi jönne először?" vagy "tudod mit akarsz, mondd csak 😘"
- Menj bele, kérdezz vissza flirty módon: "mit látnál szívesen?" "mi izgat fel?" "milyen lenne velem?"
- Legyél játékosan perverz, nem segédai. A fan vezet, te reagálsz.
- NE válaszolj unalmas általánosságokkal!
- Mindig tereld konkrét fantáziára! NE maradj üres flirtynél!

🙋 EGYSZERŰ ÜZENETEK — CSAK RÖVIDEN:
- Ha a fan valami egyszerűt ír ("Kitartást", "Szia", "Hahoo", "Kösz", "Jó reggelt") → CSAK rövid visszajelzés!
- Jó: "Köszii 😘" vagy "Hali 😄" vagy "Köszike 😅"
- NE kérdezz vissza "mit csinálsz?" ha már egyszer válaszolt rá!

🔄 NE ISMÉTELD:
- HA már egyszer kérdeztél valamit → SOHA ne kérdezd újra!
- Egy valódi lány nem kérdezi meg ötször ugyanazt.
- HA a fan szól hogy "már leírtad", "ezt már mondtad" → NE bocsássatok meg robotosan!
- Jó: "ja tényleg 😂" vagy "hupsz, igazad van" vagy csak válts témát

🏠 JÁZMIN ÉLETE (TERMÉSZETESEN):
- Főzés: csirke rizzsel, tészta, rántott csirke
- Takarítás: "takarítottam egész nap 😩"
- Unalom: "nagyon uncsi most minden 😩"
- Filmek: romcom, Netflix, "3 méterrel a ég felett"
- Sulis stressz: "vizsgáztam egész héten, kimerült vagyok"
- Hétvége: "majd bulizni megyek a barátokkal" / "alszom egész nap 😅"
- Néha említsd ha RELEVÁNS, ne erőltetettül

🕐 ISKOLA (H–P, 08:00–15:00):
- "suliban vagyok még 😅", "órán ülök… haldoklom 😂", "mindjárt vége, aztán szabad vagyok 😄"

🕐 OFFLINE:
- 02:00–09:00 → alszom
- 12:00–13:13 → ebédszünet
- 19:00–19:42 → szünet

🗳️ POLITIKA — TILOS:
- "jajj én ezekkel nem foglalkozom 😅", "unalmas nekem ez a téma 🙈"

😤 HA HÜLYESÉGET KÉRDEZNEK:
- "miért kérdezel tőlem ilyet? 😂"
- "én ehhez full hülye vagyok amúgy"
- "google a barátod nem? 😄"
- "inkább mást kérdezz… ez nem az én világom 👀"

😏 SZEXUÁLIS UTALÁSOK:
- MATCH-old a fan energiáját, ne légy túl naiv!
- Ha a fan flirty → legyél flirty. Ha explicit → legyél explicit de játékosan.

👋 "MILYEN VOLT A NAPOD?" — SZABÁLY:
- CSAK egyszer naponta!
- CSAK ha 4+ óra szünet volt a beszélgetésben!
- HA már válaszoltak rá ma → SOHA ne kérdezd újra!
- Ha nem tudod hogy kérdezted-e ma → NE kérdezd! Inkább csak köszönj lazán.
"""

CONTINUATION_VARIATIONS = ["jaaj...", "hmm...", "tudod...", "na...", "figyu...", "tudod mi...", "", "szóval...", "amúgy..."]

SHY_DEFLECTIONS = [
    "Kicsit szégyenlős vagyok :)",
    "Nem vagyok olyan típus aki ezt szokta 😅",
    "Jajj, ezt nem nagyon szoktam 😳",
    "Zárkózott vagyok ilyenekben, sorry 🙈",
    "Félek hogy kikerülne valahova 😅",
    "Nem szoktam ilyet küldözgetni, ne haragudj 😏"
]


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


def build_system_prompt(fan_name, real_name, fan_facts_list, recent_messages, school_ctx, avail_ctx, mood_ctx, life_ctx, time_ctx, fan_msg_time_str=None, day_already_asked=False, summary=""):
    # Use real name if known, otherwise do NOT use the display name in the prompt
    display_name = real_name if real_name else "a fan"
    
    prompt = JAZMIN_PERSONALITY + "\n\n"
    prompt += f"KÖSZÖNÉSI SZABÁLY:\n{get_greeting_instruction(recent_messages, fan_msg_time_str)}\n\n"
    
    if summary:
        prompt += f"BESZÉLGETÉS ÖSSZEFOGLALÓ:\n{summary}\n\n"
    
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
    
    if fan_facts_list:
        prompt += "AMIT TUDSZ A FAN-RÓL (EMLÉKEZZ EZekre, ne kérdezd újra):\n"
        for fact in fan_facts_list[:8]:
            prompt += f"- {fact['fact_type']}: {fact['fact_value']}\n"
        prompt += "\n"
    
    if recent_messages:
        prompt += "UTOLSÓ ÜZENETEK (max 6, CSAK kontextus):\n"
        for msg in recent_messages[-6:]:
            sender = "Jázmin" if msg.get('is_me') else display_name
            prompt += f"{sender}: {msg.get('text', '')}\n"
        prompt += "\n"
    
    prompt += f"A fan neve: {display_name}\n"
    if not real_name:
        prompt += "NEM TUDOD A FAN VALÓDI NEVÉT — NE szólítsd meg névvel! Használj 'te'-t vagy szólítsd meg név nélkül. SOHA NE használd a Fanvue usernévet!\n"
    if day_already_asked:
        prompt += "MA MÁR MEGKÉRDEZTED: 'milyen volt a napod?' — NE KÉRDDE ÚJRA!\n"
    prompt += "FONTOS:\n- CSAK az utolsó üzenetekre válaszolj EGYETLEN üzenetben!\n- 1-2 mondat, laza.\n- Emlékezz a memóriára! HA tudod a nevét, használd. HA nem, NE használd a usernévet!\n- Terápiás, figyelmes, de nem segédai.\n- NE ISMÉTELD a kérdéseket! HA már megkérdezted valamit → SOHA újra!"
    return prompt


def ask_openai(system_prompt, user_text):
    try:
        r = requests.post("https://api.openai.com/v1/chat/completions",
                          headers={"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"},
                          json={"model": "gpt-4.1", "messages": [
                              {"role": "system", "content": system_prompt},
                              {"role": "user", "content": user_text}
                          ], "max_tokens": 150, "temperature": 0.92, "presence_penalty": 0.6, "frequency_penalty": 0.5},
                          timeout=20)
        if r.status_code == 200:
            reply = r.json()['choices'][0]['message']['content'].strip()
            # Block banned starters
            banned_starters = ["hát figyelj", "hát figyelj...", "őszintén", "őszintén...", "na, mi a helyzet?",
                              "na mi a helyzet", "sziuus, miujság", "szius, miujsag", "na, mi újság",
                              "na mi újság", "hogy vagy?", "hogy telt a napod?", "mit csinálsz most?",
                              "mi újság veled?", "hmm, értem", "hmm értem"]
            lower_reply = reply.lower()
            if len(reply) < 50:
                for pattern in banned_starters:
                    if lower_reply.startswith(pattern):
                        return random.choice(CONTINUATION_VARIATIONS) + " mesélj te inkább 😄"
            return reply
        print(f"OpenAI error: {r.status_code}")
    except Exception as e:
        print(f"OpenAI error: {e}")
    return "hmm most nem tudok sokat írni, mesélj te inkább"


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
        # Add to batch but DO NOT extend deadline
        combined = existing['fan_text'] + "\n[+] " + fan_text
        db_query("UPDATE scheduled_replies SET fan_text=?, fan_msg_id=? WHERE id=?",
                 (combined, fan_msg_id, existing['id']))
        print(f"[{datetime.now()}] Added to batch for {fan_name}")
        send_telegram_with_id(f"📝 Batch growing for <b>{fan_name}</b>\n💬 <i>{fan_text[:60]}</i>", chat_id)
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
                if fan_msgs_silent and fan_msgs_silent[0].get('text'):
                    last_fan_text = fan_msgs_silent[0].get('text', '')[:80]
                    update_fan_notes(chat_id, f"Fan (paused): {last_fan_text}")
                    send_telegram_with_buttons(f"👁️ <b>{fan_name}</b> sent while paused: <i>{last_fan_text}</i>", chat_id)
                continue

            # === CHECK MANUAL PAUSE ===
            if should_wait_for_fan(chat_id):
                fan_msgs_after_manual = [m for m in messages if m.get('sender', {}).get('uuid') != MY_UUID]
                if fan_msgs_after_manual:
                    fan_replied_after_manual(chat_id)
                    send_telegram_with_buttons(f"▶️ <b>{fan_name}</b> replied after your manual message. Bot RESUMING.", chat_id)
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
            if existing:
                continue

            # === MANUAL REPLY DETECTION (5 MIN) ===
            if was_manual_reply_recent(chat_id, messages, minutes=5):
                send_telegram_with_buttons(f"🛑 Manual reply detected for <b>{fan_name}</b>. Bot paused 5min, auto-resumes.", chat_id)
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
def send_due_batches():
    due = get_due_batches()
    if not due:
        return 0
    sent = 0
    for item in due:
        try:
            chat_id = item['chat_id']
            fan_name = item['fan_name']
            fan_msg_id = item['fan_msg_id']
            combined_text = item['fan_text']
            batch_id = item['id']

            if is_paused(chat_id) or should_wait_for_fan(chat_id):
                db_query("UPDATE scheduled_replies SET status = 'cancelled' WHERE id = ?", (batch_id,))
                send_telegram_with_buttons(f"⏸️ Batch cancelled for <b>{fan_name}</b> — paused/ghosted", chat_id)
                continue

            messages = get_messages(chat_id)
            if was_manual_reply_recent(chat_id, messages, minutes=5):
                db_query("UPDATE scheduled_replies SET status = 'cancelled' WHERE id = ?", (batch_id,))
                send_telegram_with_buttons(f"🛑 Batch cancelled for <b>{fan_name}</b> — manual reply detected", chat_id)
                continue

            # Build context
            recent_for_prompt = []
            for msg in messages[:20]:
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
                                                fan_msg_time_str, day_already_asked, summary)

            # Check ONLY the last actual message, not combined text
            last_msg_text = combined_text.split("[+] ")[-1] if "[+] " in combined_text else combined_text
            if is_content_request(last_msg_text) or is_shy_request(last_msg_text):
                reply = random.choice(SHY_DEFLECTIONS)
                print(f"[{datetime.now()}] Shy deflection for {fan_name}: {reply}")
                send_telegram_with_buttons(f"🙈 Shy deflection for <b>{fan_name}</b>\n💬 Fan: <i>{last_msg_text[:80]}</i>\n🤖 Bot: <i>{reply}</i>", chat_id)
            else:
                reply = ask_openai(system_prompt, combined_text)

            # Dynamic send delay
            words = len(reply.split())
            if words <= 3:
                send_delay = random.uniform(0.5, 3)
            elif words <= 10:
                send_delay = random.uniform(2, 8)
            elif words <= 25:
                send_delay = random.uniform(5, 20)
            else:
                send_delay = random.uniform(15, 45)
            print(f"[DELAY] Waiting {send_delay:.1f}s before sending batch to {fan_name}")
            time.sleep(send_delay)

            if send_fanvue_message(chat_id, reply):
                # Mark ALL messages in batch as replied
                db_query('UPDATE messages SET was_replied = 1, reply_text = ?, bot_replied_at = ? WHERE msg_id = ?',
                         (reply, datetime.now().isoformat(), fan_msg_id))
                mark_batch_sent(batch_id)
                db_query('UPDATE fan_profiles SET last_reply_time = ? WHERE chat_id = ?',
                         (datetime.now().isoformat(), chat_id))

                # Update summary
                update_conversation_summary(chat_id, combined_text, reply)

                # Track if we asked "how was your day"
                if 'milyen volt a napod' in reply.lower() or 'hogy telt a napod' in reply.lower():
                    update_last_day_asked(chat_id)

                # FIXED: Use profile instead of undefined is_top_spender
                profile = get_or_create_fan_profile(chat_id, fan_name, '', False)
                stage = get_fan_stage(profile)
                stage_label = get_stage_label(stage)
                
                is_whale = profile.get('lifetime_spend', 0) >= 200 or stage >= 3
                if is_whale:
                    alert = f"💰 <b>WHALE</b> | {stage_label}\n👤 <b>{fan_name}</b>\n💬 <i>{combined_text[:80]}</i>\n🤖 <i>{reply[:100]}</i>"
                    send_telegram_with_buttons(alert, chat_id)
                elif get_safe_mode():
                    preview = f"🔒 SAFE | {stage_label}\n👤 <b>{fan_name}</b>\n💬 <i>{combined_text[:80]}</i>\n🤖 <i>{reply[:100]}</i>"
                    send_telegram_with_buttons(preview, chat_id)
                else:
                    log_msg = f"📤 <b>SENT</b> {stage_label}\n👤 <b>{fan_name}</b>\n💬 Fan: <i>{combined_text[:80]}</i>\n🤖 Bot: <i>{reply[:100]}</i>"
                    send_telegram_with_buttons(log_msg, chat_id)

                sent += 1
                print(f"[{datetime.now()}] Sent batch reply to {fan_name}")
        except Exception as e:
            print(f"[{datetime.now()}] Send error: {e}")
            send_telegram_with_id(f"❌ Error sending to <b>{fan_name}</b>: {str(e)[:200]}", chat_id)
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
    return {"safe_mode": get_safe_mode(), "token_valid": get_fanvue_token() is not None, "polling_active": polling_active}, 200


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
        "routes": ["/", "/set_token", "/trigger", "/status", "/start_poll", "/stop_poll", "/toggle_safe_mode",
                   "/fan_profiles", "/scheduled", "/blocked", "/paused", "/console",
                   "/telegram_webhook", "/callback"]
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


# ========== INIT ==========
init_db()

if bot:
    try:
        bot.remove_webhook()
        time.sleep(0.5)
        domain = os.environ.get('RAILWAY_PUBLIC_DOMAIN', '').strip()
        if domain:
            webhook_url = f"https://{domain}/telegram_webhook"
            bot.set_webhook(url=webhook_url)
            print(f"[OK] Webhook: {webhook_url}")
            send_telegram("🤖 Jazmin Bot v6.3 started")
    except Exception as e:
        print(f"[WARN] Webhook failed: {e}")

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))
