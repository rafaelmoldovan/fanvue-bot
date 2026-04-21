from flask import Flask, request
import requests
import os
import json
import base64
import time
import threading
import psycopg2
from datetime import datetime, timedelta

app = Flask(__name__)

# ========== CONFIG ==========
FANVUE_CLIENT_ID = os.environ.get('FANVUE_CLIENT_ID', '')
FANVUE_CLIENT_SECRET = os.environ.get('FANVUE_CLIENT_SECRET', '')
OPENAI_API_KEY = os.environ.get('OPENAI_API_KEY', '')
CREATOR_NAME = os.environ.get('CREATOR_NAME', 'Jazmin')
RAILWAY_REFRESH_TOKEN = os.environ.get('FANVUE_REFRESH_TOKEN', '')
DATABASE_URL = os.environ.get('DATABASE_URL', '')
TELEGRAM_BOT_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN', '')
TELEGRAM_CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID', '')

MY_UUID = '38a392fc-a751-49b3-9d74-01ac6447c490'
MY_HANDLE = 'jazmin07'

# SAFE MODE: NEVER send messages to Fanvue
SAFE_MODE = True  # Set to False only when ready to go live

bot_status = {
    "started": datetime.now().isoformat(),
    "last_check": "never",
    "messages_found": 0,
    "replies_sent": 0,
    "errors": [],
    "paused": True,  # Start PAUSED — manually resume when ready
    "blocked_users": set(),
    "safe_mode": SAFE_MODE
}

memory_tokens = {
    "refresh_token": RAILWAY_REFRESH_TOKEN,
    "access_token": None,
    "expires_at": None
}

# In-memory fallback (used if PostgreSQL fails)
_in_memory_db = {
    "messages": {},
    "fan_profiles": {},
    "tokens": {},
    "stats": {"messages_found": 0, "replies_sent": 0},
    "cooldown": {},
    "processed_ids": set(),
    "last_bot_reply": {}  # chat_id -> timestamp
}

# ========== ACCENT STRIPPER ==========
HUNGARIAN_MAP = {
    'á': 'a', 'é': 'e', 'í': 'i', 'ó': 'o', 'ö': 'o', 'ő': 'o',
    'ú': 'u', 'ü': 'u', 'ű': 'u',
    'Á': 'A', 'É': 'E', 'Í': 'I', 'Ó': 'O', 'Ö': 'O', 'Ő': 'O',
    'Ú': 'U', 'Ü': 'U', 'Ű': 'U'
}

def strip_accents(text):
    """Replace Hungarian accents with plain letters"""
    result = []
    for char in text:
        result.append(HUNGARIAN_MAP.get(char, char))
    return ''.join(result)

# ========== LOGGING ==========
def log(msg):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{timestamp}] {msg}"
    print(line, flush=True)
    bot_status["errors"].append(line)
    if len(bot_status["errors"]) > 100:
        bot_status["errors"] = bot_status["errors"][-100:]

# ========== TELEGRAM ==========
def send_telegram(text):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": text[:4000]}, timeout=10)
    except:
        pass

# ========== POSTGRESQL ==========
_db_available = False

def init_database():
    global _db_available
    if not DATABASE_URL:
        log("WARNING: No DATABASE_URL. Using in-memory only.")
        _db_available = False
        return
    
    try:
        conn = psycopg2.connect(DATABASE_URL, sslmode='require')
        c = conn.cursor()
        
        c.execute('''
            CREATE TABLE IF NOT EXISTS messages (
                msg_id VARCHAR(255) PRIMARY KEY,
                chat_id VARCHAR(255) NOT NULL,
                fan_name VARCHAR(255),
                sender_uuid VARCHAR(255),
                text TEXT,
                timestamp TIMESTAMP,
                was_replied BOOLEAN DEFAULT FALSE,
                reply_text TEXT,
                bot_replied_at TIMESTAMP
            )
        ''')
        
        c.execute('''
            CREATE TABLE IF NOT EXISTS fan_profiles (
                chat_id VARCHAR(255) PRIMARY KEY,
                fan_name VARCHAR(255),
                handle VARCHAR(255),
                total_messages INTEGER DEFAULT 0,
                fan_type VARCHAR(50) DEFAULT 'new',
                last_interaction TIMESTAMP,
                last_reply_time TIMESTAMP,
                content_ask_count INTEGER DEFAULT 0,
                meetup_ask_count INTEGER DEFAULT 0
            )
        ''')
        
        c.execute('''
            CREATE TABLE IF NOT EXISTS tokens (
                key VARCHAR(255) PRIMARY KEY,
                value TEXT
            )
        ''')
        
        conn.commit()
        c.close()
        conn.close()
        _db_available = True
        log("PostgreSQL connected and tables ready")
    except Exception as e:
        log(f"PostgreSQL failed: {e}")
        _db_available = False

# ========== DB HELPERS (PostgreSQL + Memory Fallback) ==========
def db_save_message(msg_id, chat_id, fan_name, sender_uuid, text, timestamp):
    _in_memory_db["messages"][msg_id] = {
        "msg_id": msg_id, "chat_id": chat_id, "fan_name": fan_name,
        "sender_uuid": sender_uuid, "text": text, "timestamp": timestamp,
        "was_replied": False, "reply_text": None, "bot_replied_at": None
    }
    if not _db_available:
        return
    try:
        conn = psycopg2.connect(DATABASE_URL, sslmode='require')
        c = conn.cursor()
        c.execute('''
            INSERT INTO messages (msg_id, chat_id, fan_name, sender_uuid, text, timestamp)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (msg_id) DO NOTHING
        ''', (msg_id, chat_id, fan_name, sender_uuid, text, timestamp))
        conn.commit()
        c.close()
        conn.close()
    except Exception as e:
        log(f"DB save msg error: {e}")

def db_get_chat_history(chat_id, limit=15):
    msgs = [m for m in _in_memory_db["messages"].values() if m["chat_id"] == chat_id]
    msgs.sort(key=lambda x: x["timestamp"])
    msgs = msgs[-limit:]
    if msgs:
        return [(m["fan_name"], m["text"], m["was_replied"], m["reply_text"], m["timestamp"]) for m in msgs]
    if not _db_available:
        return []
    try:
        conn = psycopg2.connect(DATABASE_URL, sslmode='require')
        c = conn.cursor()
        c.execute('''
            SELECT fan_name, text, was_replied, reply_text, timestamp
            FROM messages WHERE chat_id = %s ORDER BY timestamp DESC LIMIT %s
        ''', (chat_id, limit))
        rows = c.fetchall()
        c.close()
        conn.close()
        return list(reversed(rows))
    except Exception as e:
        log(f"DB history error: {e}")
        return []

def db_update_fan_profile(chat_id, fan_name, handle):
    if chat_id not in _in_memory_db["fan_profiles"]:
        _in_memory_db["fan_profiles"][chat_id] = {
            "chat_id": chat_id, "fan_name": fan_name, "handle": handle,
            "total_messages": 0, "fan_type": "new", "last_interaction": datetime.now(),
            "last_reply_time": None, "content_ask_count": 0, "meetup_ask_count": 0
        }
    p = _in_memory_db["fan_profiles"][chat_id]
    p["total_messages"] += 1
    p["last_interaction"] = datetime.now()
    if p["total_messages"] >= 15:
        p["fan_type"] = "whale"
    elif p["total_messages"] >= 3:
        p["fan_type"] = "warm"
    
    if not _db_available:
        return
    try:
        conn = psycopg2.connect(DATABASE_URL, sslmode='require')
        c = conn.cursor()
        c.execute('SELECT total_messages FROM fan_profiles WHERE chat_id = %s', (chat_id,))
        row = c.fetchone()
        if row:
            new_count = row[0] + 1
            fan_type = 'whale' if new_count >= 15 else ('warm' if new_count >= 3 else 'new')
            c.execute('UPDATE fan_profiles SET total_messages = %s, fan_type = %s, last_interaction = NOW(), fan_name = %s WHERE chat_id = %s',
                (new_count, fan_type, fan_name, chat_id))
        else:
            c.execute('INSERT INTO fan_profiles (chat_id, fan_name, handle, total_messages, last_interaction, fan_type) VALUES (%s, %s, %s, 1, NOW(), %s)',
                (chat_id, fan_name, handle, 'new'))
        conn.commit()
        c.close()
        conn.close()
    except Exception as e:
        log(f"DB profile error: {e}")

def db_get_fan_profile(chat_id):
    if chat_id in _in_memory_db["fan_profiles"]:
        p = _in_memory_db["fan_profiles"][chat_id]
        return {
            'fan_name': p["fan_name"], 'total_messages': p["total_messages"],
            'fan_type': p["fan_type"], 'content_ask_count': p["content_ask_count"],
            'meetup_ask_count': p["meetup_ask_count"], 'last_reply_time': p["last_reply_time"]
        }
    if not _db_available:
        return None
    try:
        conn = psycopg2.connect(DATABASE_URL, sslmode='require')
        c = conn.cursor()
        c.execute('SELECT fan_name, total_messages, fan_type, content_ask_count, meetup_ask_count, last_reply_time FROM fan_profiles WHERE chat_id = %s', (chat_id,))
        row = c.fetchone()
        c.close()
        conn.close()
        if row:
            return {'fan_name': row[0], 'total_messages': row[1], 'fan_type': row[2], 'content_ask_count': row[3], 'meetup_ask_count': row[4], 'last_reply_time': row[5]}
        return None
    except Exception as e:
        log(f"DB get profile error: {e}")
        return None

def db_mark_replied(msg_id, reply_text):
    if msg_id in _in_memory_db["messages"]:
        _in_memory_db["messages"][msg_id]["was_replied"] = True
        _in_memory_db["messages"][msg_id]["reply_text"] = reply_text
        _in_memory_db["messages"][msg_id]["bot_replied_at"] = datetime.now()
    _in_memory_db["processed_ids"].add(msg_id)
    
    if not _db_available:
        return
    try:
        conn = psycopg2.connect(DATABASE_URL, sslmode='require')
        c = conn.cursor()
        c.execute('UPDATE messages SET was_replied = TRUE, reply_text = %s, bot_replied_at = NOW() WHERE msg_id = %s', (reply_text, msg_id))
        conn.commit()
        c.close()
        conn.close()
    except Exception as e:
        log(f"DB mark replied error: {e}")

def db_update_last_reply_time(chat_id):
    _in_memory_db["last_bot_reply"][chat_id] = datetime.now()
    if chat_id in _in_memory_db["fan_profiles"]:
        _in_memory_db["fan_profiles"][chat_id]["last_reply_time"] = datetime.now()
    if not _db_available:
        return
    try:
        conn = psycopg2.connect(DATABASE_URL, sslmode='require')
        c = conn.cursor()
        c.execute('UPDATE fan_profiles SET last_reply_time = NOW() WHERE chat_id = %s', (chat_id,))
        conn.commit()
        c.close()
        conn.close()
    except Exception as e:
        log(f"DB update reply time error: {e}")

def db_is_message_processed(msg_id):
    if msg_id in _in_memory_db["processed_ids"]:
        return True
    if msg_id in _in_memory_db["messages"] and _in_memory_db["messages"][msg_id]["was_replied"]:
        return True
    if not _db_available:
        return False
    try:
        conn = psycopg2.connect(DATABASE_URL, sslmode='require')
        c = conn.cursor()
        c.execute('SELECT 1 FROM messages WHERE msg_id = %s AND was_replied = TRUE', (msg_id,))
        result = c.fetchone()
        c.close()
        conn.close()
        return result is not None
    except Exception as e:
        log(f"DB check processed error: {e}")
        return False

def db_save_token(key, value):
    _in_memory_db["tokens"][key] = value
    if not _db_available:
        return
    try:
        conn = psycopg2.connect(DATABASE_URL, sslmode='require')
        c = conn.cursor()
        c.execute('INSERT INTO tokens (key, value) VALUES (%s, %s) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value', (key, value))
        conn.commit()
        c.close()
        conn.close()
    except Exception as e:
        log(f"DB save token error: {e}")

def db_load_token(key):
    if key in _in_memory_db["tokens"]:
        return _in_memory_db["tokens"][key]
    if not _db_available:
        return None
    try:
        conn = psycopg2.connect(DATABASE_URL, sslmode='require')
        c = conn.cursor()
        c.execute('SELECT value FROM tokens WHERE key = %s', (key,))
        row = c.fetchone()
        c.close()
        conn.close()
        return row[0] if row else None
    except Exception as e:
        log(f"DB load token error: {e}")
        return None

# ========== FANVUE AUTH ==========
def get_basic_auth_header():
    credentials = f"{FANVUE_CLIENT_ID}:{FANVUE_CLIENT_SECRET}"
    encoded = base64.b64encode(credentials.encode('utf-8')).decode('utf-8')
    return f"Basic {encoded}"

def save_token(refresh_token, access_token=None, expires_at=None):
    memory_tokens["refresh_token"] = refresh_token
    memory_tokens["access_token"] = access_token
    memory_tokens["expires_at"] = expires_at
    db_save_token('refresh_token', refresh_token)
    if access_token:
        db_save_token('access_token', access_token)
    if expires_at:
        db_save_token('expires_at', expires_at)

def load_token():
    if memory_tokens["refresh_token"]:
        return memory_tokens
    db_refresh = db_load_token('refresh_token')
    if db_refresh:
        memory_tokens["refresh_token"] = db_refresh
        memory_tokens["access_token"] = db_load_token('access_token')
        memory_tokens["expires_at"] = db_load_token('expires_at')
        return memory_tokens
    if RAILWAY_REFRESH_TOKEN:
        memory_tokens["refresh_token"] = RAILWAY_REFRESH_TOKEN
        db_save_token('refresh_token', RAILWAY_REFRESH_TOKEN)
        return memory_tokens
    return {}

def refresh_fanvue_token():
    tokens = load_token()
    refresh_token = tokens.get('refresh_token')
    if not refresh_token:
        log("No refresh token!")
        return None
    try:
        r = requests.post("https://auth.fanvue.com/oauth2/token",
            data={"grant_type": "refresh_token", "refresh_token": refresh_token},
            headers={"Content-Type": "application/x-www-form-urlencoded", "Authorization": get_basic_auth_header()},
            timeout=10)
        log(f"Refresh status: {r.status_code}")
        if r.status_code == 200:
            data = r.json()
            access_token = data.get('access_token')
            new_refresh = data.get('refresh_token')
            expires_in = data.get('expires_in', 3600)
            expires_at = (datetime.now() + timedelta(seconds=expires_in - 60)).isoformat()
            save_token(new_refresh or refresh_token, access_token, expires_at)
            log("Got new access token")
            return access_token
        else:
            log(f"Refresh error: {r.status_code} - {r.text[:200]}")
            return None
    except Exception as e:
        log(f"Refresh exception: {e}")
        return None

def get_fanvue_token():
    tokens = load_token()
    access_token = tokens.get('access_token')
    expires_at = tokens.get('expires_at')
    if access_token and expires_at:
        try:
            if datetime.now() < datetime.fromisoformat(expires_at):
                return access_token
        except:
            pass
    return refresh_fanvue_token()

def get_headers():
    return {"Authorization": f"Bearer {get_fanvue_token() or ''}", "X-Fanvue-API-Version": "2025-06-26", "Content-Type": "application/json"}

# ========== FANVUE API (SAFE — no sending) ==========
def get_chats():
    try:
        r = requests.get("https://api.fanvue.com/chats", headers=get_headers(), timeout=10)
        if r.status_code == 401:
            save_token(load_token().get('refresh_token', ''), None, None)
            r = requests.get("https://api.fanvue.com/chats", headers=get_headers(), timeout=10)
        if r.status_code != 200:
            log(f"Chats error: {r.status_code}")
            return []
        return r.json().get('data', [])
    except Exception as e:
        log(f"Get chats error: {e}")
        return []

def get_messages(chat_id):
    try:
        r = requests.get(f"https://api.fanvue.com/chats/{chat_id}/messages", headers=get_headers(), timeout=10)
        if r.status_code == 401:
            save_token(load_token().get('refresh_token', ''), None, None)
            r = requests.get(f"https://api.fanvue.com/chats/{chat_id}/messages", headers=get_headers(), timeout=10)
        if r.status_code != 200:
            return []
        return r.json().get('data', [])
    except Exception as e:
        log(f"Get messages error: {e}")
        return []

def send_fanvue_message(chat_id, text):
    if SAFE_MODE:
        log(f"SAFE MODE: Would send to {chat_id}: {text[:60]}")
        return True  # Pretend it sent
    url = f"https://api.fanvue.com/chats/{chat_id}/message"
    headers = {"Authorization": f"Bearer {get_fanvue_token() or ''}", "Content-Type": "application/json"}
    try:
        r = requests.post(url, headers=headers, json={"text": text}, timeout=10)
        log(f"Send status: {r.status_code}")
        if r.status_code in [200, 201]:
            return True
        log(f"Send error: {r.status_code}")
        return False
    except Exception as e:
        log(f"Send error: {e}")
        return False

# ========== OPENAI ==========
def ask_openai(prompt, fan_name=""):
    url = "https://api.openai.com/v1/chat/completions"
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"}
    data = {
        "model": "gpt-4o-mini",
        "messages": [
            {"role": "system", "content": prompt},
            {"role": "user", "content": "irj vissza"}
        ],
        "max_tokens": 250,
        "temperature": 0.7
    }
    try:
        r = requests.post(url, headers=headers, json=data, timeout=20)
        if r.status_code == 200:
            content = r.json()['choices'][0]['message']['content']
            # Clean smart quotes
            content = content.replace(chr(8216), "'").replace(chr(8217), "'").replace(chr(8220), '"').replace(chr(8221), '"')
            # Strip Hungarian accents
            content = strip_accents(content)
            return content.strip()
        else:
            log(f"OpenAI error: {r.status_code}")
    except Exception as e:
        log(f"OpenAI error: {e}")
    return "hmm most nem tudok sokat irni, meselj te inkabb"

# ========== QUESTION VARIATIONS ==========
QUESTION_VARIATIONS = {
    "how_are_you": ["na, mi a helyzet?", "helyzet?", "mizu?", "meselj", "na?", "hogy vagy?", "mi ujsag?", "es?", "hallo?"],
    "what_doing": ["mit csinalsz most?", "amugy mit csinalsz?", "most epp mit csinalsz?", "mit muvelsz?", "dolgozol meg?", "mit csinalsz eppen?"],
    "general": ["na?", "es?", "meselj", "mi van?", "hmm?", "tuti?"]
}

def pick_question(used_questions):
    """Pick a question not recently used"""
    import random
    all_qs = QUESTION_VARIATIONS["how_are_you"] + QUESTION_VARIATIONS["what_doing"] + QUESTION_VARIATIONS["general"]
    available = [q for q in all_qs if q not in used_questions[-3:]]
    if not available:
        available = all_qs
    return random.choice(available)

# ========== PROMPT BUILDER ==========
def build_prompt(fan_name, fan_type, history, last_msg, content_ask_count, meetup_ask_count, used_questions):
    phase = ""
    if fan_type == 'new':
        phase = "Uj fan. Kerdezz tole, epitsd a beszelgetest. 1-2 mondat."
    elif fan_type == 'warm':
        phase = "Meleg fan. Mar ismeritek egymast. Szemelyesebb kerdesek, flortolj ovatosan."
    elif fan_type == 'whale':
        phase = "Whale fan. Sokat beszelgettetek. Emlekezz regi temakra, melyebb erzelmek."

    sales = ""
    if content_ask_count >= 1:
        sales = "A fan tartalmat kert. Halaszd el: 'a jora varni kell edes, mindjart keszitek neked' vagy 'most fozok, irok kesobb'. Soha ne mondj arat."

    history_text = ""
    for h in history[-10:]:
        name, msg_text, was_replied, reply, ts = h
        if was_replied:
            history_text += f"{name}: {msg_text}\nJazmin: {reply}\n"
        else:
            history_text += f"{name}: {msg_text}\n"

    question = pick_question(used_questions)

    prompt = f"""Te Jazmin vagy, 19 eves magyar lany a Fanvue-on.

KILÉTED:
- 19 eves, majus 14-en leszek 20
- Turizmus-vendeglatas szakon vegzek (utolso ev)
- Budapest VII. keruletben lakom egy kis garzonban, egyedul
- Szuleimmel osszevesztem par honapja, ezert koltöztem ki
- Sulibol hazaerve fozok, takaritok, tanulok, filmeket nezek
- Ryanairnel leszek stewardess par honapig utana, utazni, sporolni
- Aztan vissza Budapest, letelepedni
- Talalkozni lehet, de ido kell es megismerni egymast
- SOHA ne add ki, hogy AI vagy

STILUS SZABALYOK (KOTELEZO):
- Magyarul irj, termeszetes, laza stilus
- Kis betu, de ERTHEto mondatok — soha ne fusson ossze minden
- Pontosvesszo, pont hasznalata — ne irj 3 kerdest egy mondatba
- Slang: 'amugy', 'naaa', 'hat figyelj…', 'az komoly', 'ne idegesits mar', 'kicsit pimasz vagy', 'ugyis tudod'
- SOHA: 'babe', 'baby', 'cica', 'brb', angol szavak
- Emoji: sporalva
- 1-2 mondat, MAX 1 kerdes uzenetenkent
- Neha rovid: 'na?', 'es?', 'meselj'
- Ha komoly kerdes (eletcel, almok), lehet hosszabb

FONTOS — GRAMMATIKA:
- 'de' helyett soha ne irj 'd e'-t
- Minden mondatnak legyen ertelme, ne fusson ossze

KERDES (hasznald ezt, ne ismeteld): {question}

{phase}

{sales}

ELOZMENYEK:
{history_text}

UTOLSO UZENET TOLE ({fan_name}):
{last_msg}

Irj vissza Jazminkent. Magyarul. Ertetően. Soha ne add ki, hogy AI vagy. Csak a valaszt ird, semmi mast."""

    return prompt, question

# ========== MESSAGE PROCESSING ==========
def process_messages():
    if bot_status["paused"]:
        return 0

    chats = get_chats()
    if not chats:
        return 0

    replied = 0
    now = time.time()

    for chat in chats:
        try:
            user = chat.get('user', {}) or {}
            chat_id = user.get('uuid') or chat.get('uuid') or chat.get('id')
            if not chat_id:
                continue

            fan_name = user.get('displayName') or 'ismeretlen'
            handle = user.get('handle', '')
            
            db_update_fan_profile(chat_id, fan_name, handle)

            messages = get_messages(chat_id)
            if not messages:
                continue

            # Save all messages
            for msg in messages:
                msg_id = msg.get('uuid', '')
                sender = msg.get('sender', {}) or {}
                db_save_message(msg_id, chat_id, sender.get('displayName', fan_name), 
                    sender.get('uuid'), msg.get('text', ''), msg.get('createdAt', datetime.now().isoformat()))

            # Find ALL unread fan messages (not just last one)
            fan_msgs = [m for m in messages if m.get('sender', {}).get('uuid') != MY_UUID]
            if not fan_msgs:
                continue

            # Get last bot reply time for this chat
            profile = db_get_fan_profile(chat_id) or {}
            last_reply_time = profile.get('last_reply_time')
            
            # Check if we already replied to the MOST RECENT fan message
            last_fan_msg = fan_msgs[-1]
            last_msg_id = last_fan_msg.get('uuid')
            
            if db_is_message_processed(last_msg_id):
                # We replied to last message, but check if fan sent something EVEN NEWER
                # (This shouldn't happen if we process in order, but safety check)
                continue

            # Check if fan has sent multiple messages since our last reply
            # Only reply to the LATEST one, but acknowledge we saw them all
            msgs_since_reply = []
            for msg in fan_msgs:
                msg_time = msg.get('createdAt', '')
                if last_reply_time and msg_time:
                    try:
                        msg_dt = datetime.fromisoformat(str(msg_time).replace('Z', '+00:00'))
                        if msg_dt > last_reply_time:
                            msgs_since_reply.append(msg)
                    except:
                        msgs_since_reply.append(msg)
                else:
                    msgs_since_reply.append(msg)

            if not msgs_since_reply:
                continue

            # Only process the LAST message from fan
            target_msg = msgs_since_reply[-1]
            msg_id = target_msg.get('uuid')
            sender = target_msg.get('sender', {}) or {}
            text = target_msg.get('text') or ''

            if db_is_message_processed(msg_id):
                continue

            # Check triggers
            content_triggers = ['kepet', 'videot', 'tartalmat', 'extrat', 'mennyibe', 'ar', 'fizetek', 'mutass', 'küldj', 'picit', 'doboz', 'csomag', 'premium', 'exkluziv']
            if any(t in text.lower() for t in content_triggers):
                log(f"CONTENT ASK from {fan_name}: {text[:50]}")

            meetup_triggers = ['talalkoz', 'talalkozzunk', 'mikor', 'hol', 'helyszin', 'cim', 'lakcim', 'telefonszam', 'szamot', 'whatsapp', 'insta', 'instagram']
            if any(t in text.lower() for t in meetup_triggers):
                log(f"MEETUP ASK from {fan_name}: {text[:50]}")

            # Build history and get used questions
            history = db_get_chat_history(chat_id, limit=15)
            used_questions = []
            for h in history:
                if h[3]:  # reply_text exists
                    # Extract question from reply (rough heuristic)
                    reply_lower = h[3].lower()
                    for q_list in QUESTION_VARIATIONS.values():
                        for q in q_list:
                            if q.lower() in reply_lower:
                                used_questions.append(q)

            prompt, question = build_prompt(
                fan_name=fan_name,
                fan_type=profile.get('fan_type', 'new'),
                history=history,
                last_msg=text,
                content_ask_count=profile.get('content_ask_count', 0),
                meetup_ask_count=profile.get('meetup_ask_count', 0),
                used_questions=used_questions
            )

            reply = ask_openai(prompt, fan_name)

            if reply and reply.strip():
                if send_fanvue_message(chat_id, reply):
                    bot_status["replies_sent"] += 1
                    replied += 1
                    db_mark_replied(msg_id, reply)
                    db_update_last_reply_time(chat_id)
                    log(f"✅ Replied to {fan_name}: {reply[:60]}")
                else:
                    log(f"❌ Failed to send to {fan_name}")

            time.sleep(1)

        except Exception as e:
            log(f"Process error: {e}")
            continue

    return replied

# ========== ROUTES ==========
@app.route('/')
def home():
    return f"SAFE MODE: {SAFE_MODE}. Paused: {bot_status['paused']}. Replies: {bot_status['replies_sent']}. Use /safe_fetch /trigger /pause /resume /status /test_telegram"

@app.route('/status')
def status():
    return {
        "started": bot_status["started"],
        "last_check": bot_status["last_check"],
        "messages_found": bot_status["messages_found"],
        "replies_sent": bot_status["replies_sent"],
        "paused": bot_status["paused"],
        "safe_mode": SAFE_MODE,
        "db_available": _db_available,
        "blocked_users": list(bot_status["blocked_users"]),
        "recent_logs": bot_status["errors"][-10:]
    }

@app.route('/safe_fetch')
def safe_fetch():
    """Fetch data from Fanvue WITHOUT sending replies"""
    try:
        token = get_fanvue_token()
        if not token:
            return {"error": "No token"}
        
        chats = get_chats()
        samples = []
        
        for chat in chats[:5]:
            user = chat.get('user', {}) or {}
            chat_id = user.get('uuid') or chat.get('uuid') or chat.get('id')
            if not chat_id:
                continue
            
            msgs = get_messages(chat_id)
            samples.append({
                "chat_id": chat_id,
                "fan_name": user.get('displayName'),
                "handle": user.get('handle'),
                "msg_count": len(msgs),
                "first_msg": msgs[0] if msgs else None,
                "last_msg": msgs[-1] if msgs else None,
                "sample_texts": [m.get('text', '')[:50] for m in msgs[-3:]] if msgs else []
            })
        
        return {
            "status": "SAFE_FETCH_OK",
            "total_chats": len(chats),
            "samples": samples,
            "warning": "NO MESSAGES SENT — SAFE MODE ACTIVE"
        }
    except Exception as e:
        return {"error": str(e)}

@app.route('/trigger')
def trigger():
    if not SAFE_MODE:
        return {"error": "Not in safe mode! Use /safe_fetch or set SAFE_MODE=True"}
    try:
        bot_status["last_check"] = datetime.now().isoformat()
        count = process_messages()
        return {"status": "safe_mode_test", "replied": count, "note": "Replies logged but NOT sent to Fanvue"}
    except Exception as e:
        return {"status": "error", "error": str(e)}

@app.route('/pause')
def pause():
    bot_status["paused"] = True
    return {"status": "paused"}

@app.route('/resume')
def resume():
    bot_status["paused"] = False
    return {"status": "resumed"}

@app.route('/set_token', methods=['POST'])
def set_token():
    try:
        data = request.json
        if data and 'refresh_token' in data:
            save_token(data['refresh_token'])
            return {"status": "ok", "message": "Token saved"}
        return {"status": "error", "message": "No token"}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.route('/get_current_token')
def get_current_token():
    tokens = load_token()
    refresh = tokens.get('refresh_token', '')
    return {"refresh_token": refresh[:50] + "..." if len(refresh) > 50 else refresh}

@app.route('/test_telegram', methods=['POST'])
def test_telegram():
    """Test Telegram alert"""
    send_telegram("🔥 Test alert from Jazmin bot!")
    return {"status": "telegram_sent"}

# ========== INIT ==========
try:
    init_database()
except Exception as e:
    log(f"Init error: {e}")

log("=" * 50)
log("JAZMIN BOT STARTING — SAFE MODE")
log(f"SAFE_MODE = {SAFE_MODE} (replies are logged but NOT sent)")
log("=" * 50)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
