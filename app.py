from flask import Flask, request
import requests
import os
import json
import base64
import sqlite3
import threading
import time
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

# ========== TELEGRAM ==========
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
    if SAFE_MODE:
        preview = f"🔒 <b>SAFE MODE</b>\n<b>To:</b> {chat_id}\n<b>Message:</b>\n{text}"
        send_telegram(preview)
        return True
    try:
        r = requests.post(f"https://api.fanvue.com/chats/{chat_id}/message",
            headers=get_headers(), json={"text": text}, timeout=10)
        return r.status_code in [200, 201]
    except Exception as e:
        return False

# ========== PERSONALITY v4.0 — REAL GIRL ==========
JAZMIN_PERSONALITY = """Te Jazmin vagy, egy 19 éves magyar lány. Egy valódi lány, aki nem mindig van online, nem mindig lelkes, és nem próbál túl keményen. Nem vagy chatbot, nem vagy ügyfélszolgálat.

🔥 ARANYSZABÁLY:
- CSAK az utolsó üzenetre válaszolj!
- A korábbi üzenetek CSAK kontextus — ne hozzál fel régi témákat!
- SOHA ne kezdd így: "ja emlékszem tegnap azt mondtad..." — NEM emlékezel vissza napokra!

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

✅ STÍLUS:
- 1-2 mondat, max 10-15 szó
- "hát figyelj..." — de NE minden üzenetnél!
- "édes", "babe" — ritkán
- Emoji: mérsékelt (😄, 😏, 😘, 🙈, 😅, 👀, 😩, 😂)
- Néha elhagyod az ékezeteket

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
"""

# ========== SMART GREETING ==========
CONTINUATION_VARIATIONS = [
    "hát figyelj...", "na...", "őszintén?", "hmm...", "jaaj...", "figyu...", ""
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
                "model": "gpt-4o-mini",
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
                      "mit csinálsz most?", "mi újság veled?"]
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

# ========== MANUAL REPLY DETECTION ==========
def was_manual_reply_recent(chat_id, messages, minutes=30):
    if not messages:
        return False
    last_msg = messages[-1]
    sender_uuid = last_msg.get('sender', {}).get('uuid')
    msg_time = last_msg.get('createdAt', '')
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
            fan_name = user.get('displayName', 'ismeretlen')
            handle = user.get('handle', '')
            is_top_spender = user.get('isTopSpender', False)
            profile = get_or_create_fan_profile(chat_id, fan_name, handle, is_top_spender)
            fan_msgs = [m for m in messages if m.get('sender', {}).get('uuid') != MY_UUID]
            if not fan_msgs:
                continue
            
            # DEBUG: Log all fan message timestamps to understand API ordering
            all_fan_times = []
            for m in fan_msgs:
                t = m.get('sentAt') or m.get('createdAt') or m.get('timestamp') or ''
                txt = m.get('text', '')[:30]
                all_fan_times.append(f"{t}:{txt}")
            print(f"[{datetime.now()}] DEBUG {fan_name} all {len(fan_msgs)} fan msgs: {all_fan_times[-3:]}")
            
            last_msg = fan_msgs[-1]
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

            # Build context
            recent_for_prompt = []
            for msg in messages[-5:]:
                sender_uuid = msg.get('sender', {}).get('uuid')
                recent_for_prompt.append({
                    'is_me': sender_uuid == MY_UUID,
                    'text': msg.get('text', ''),
                    'timestamp': msg.get('createdAt', ''),
                    'type': msg.get('type', '')
                })

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

            # Content request alert
            if content_request:
                pref_prompt = JAZMIN_PERSONALITY
                if avail_ctx:
                    pref_prompt += f"\n\n{avail_ctx}"
                if school_ctx:
                    pref_prompt += f"\n\n{school_ctx}"
                pref_prompt += f"\n\nA fan tartalmat kér: '{text}'. Kérdezd meg mit akar látni, de ne ígérj semmit. 1-2 mondat, laza stílus."
                reply = ask_openai(pref_prompt, "mit akarsz látni?")
                alert = f"""🎯 <b>TARTALOMKÉRÉS</b>
👤 <b>{fan_name}</b> (@{handle})
💬 <i>{text[:100]}</i>
🤖 Bot javaslat: <i>{reply[:100]}</i>
🔗 Chat ID: <code>{chat_id}</code>"""
                send_telegram(alert)
                new_count = profile.get('content_ask_count', 0) + 1
                db_query('UPDATE fan_profiles SET content_ask_count = ? WHERE chat_id = ?', (new_count, chat_id))
                update_fan_notes(chat_id, f"Tartalmat kért ({new_count}. alkalom): '{text[:50]}'")

            # Whale alert
            elif is_top_spender or (profile and profile.get('lifetime_spend', 0) > 200):
                alert = f"""💰 <b>WHALE ALERT</b>
👤 <b>{fan_name}</b> (@{handle})
💰 Top Spender / $200+
💬 <i>{text[:100]}</i>
🤖 Bot javaslat: <i>{reply[:100]}</i>
🔗 Chat ID: <code>{chat_id}</code>"""
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
                if SAFE_MODE:
                    send_telegram(f"✅ <b>Válasz {fan_name}-nak (küldve)</b>\n<i>{reply_text[:100]}</i>")
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
        "status": "Jazmin Bot v4.1 — Boot Watermark",
        "safe_mode": SAFE_MODE,
        "boot_time_utc": BOOT_TIME_UTC.isoformat(),
        "token_valid": get_fanvue_token() is not None,
        "polling_active": polling_active,
    }

@app.route('/status')
def status():
    return {
        "safe_mode": SAFE_MODE,
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
    return {"sent": sent, "scheduled": scheduled, "status": status_msg, "safe_mode": SAFE_MODE, "boot_time_utc": BOOT_TIME_UTC.isoformat()}

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
