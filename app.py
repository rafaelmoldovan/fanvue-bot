from flask import Flask, request
import requests
import os
import json
import base64
import time
import threading
from datetime import datetime, timedelta

app = Flask(__name__)

# ========== CONFIG ==========
FANVUE_CLIENT_ID = os.environ.get('FANVUE_CLIENT_ID', '')
FANVUE_CLIENT_SECRET = os.environ.get('FANVUE_CLIENT_SECRET', '')
OPENAI_API_KEY = os.environ.get('OPENAI_API_KEY', '')
CREATOR_NAME = os.environ.get('CREATOR_NAME', 'Jazmin')
RAILWAY_REFRESH_TOKEN = os.environ.get('FANVUE_REFRESH_TOKEN', '')
DATABASE_URL = os.environ.get('DATABASE_URL', '')
TELEGRAM_BOT_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN', '8141197294:AAE9aH9mptY_ZzAK6sSc_alh2PtRjF1ASWs')
TELEGRAM_CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID', '8571222647')

MY_UUID = '38a392fc-a751-49b3-9d74-01ac6447c490'
MY_HANDLE = 'jazmin07'

bot_status = {
    "started": datetime.now().isoformat(),
    "last_check": "never",
    "messages_found": 0,
    "replies_sent": 0,
    "errors": [],
    "paused": False,
    "blocked_users": set()
}

memory_tokens = {
    "refresh_token": RAILWAY_REFRESH_TOKEN,
    "access_token": None,
    "expires_at": None
}

# In-memory storage (works even if DB fails)
_in_memory_db = {
    "messages": {},
    "fan_profiles": {},
    "tokens": {},
    "stats": {"messages_found": 0, "replies_sent": 0},
    "cooldown": {},
    "processed_ids": set()
}

# ========== LOGGING ==========
def log(msg):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{timestamp}] {msg}"
    print(line, flush=True)
    bot_status["errors"].append(line)
    if len(bot_status["errors"]) > 100:
        bot_status["errors"] = bot_status["errors"][-100:]

# ========== TELEGRAM ALERTS ==========
def send_telegram_alert(text):
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": f"🔥 FANVUE ALERT:\n{text}\n\nCheck: https://web-production-f0a39.up.railway.app/needs_attention",
            "parse_mode": "HTML"
        }
        r = requests.post(url, json=payload, timeout=10)
        if r.status_code != 200:
            log(f"Telegram alert failed: {r.status_code}")
    except Exception as e:
        log(f"Telegram error: {e}")

# ========== DATABASE (PostgreSQL + In-Memory Fallback) ==========
_db_available = False

def init_database():
    global _db_available
    if not DATABASE_URL:
        log("WARNING: No DATABASE_URL. Using in-memory storage only.")
        _db_available = False
        return
    
    try:
        import psycopg
        conn = psycopg.connect(DATABASE_URL, sslmode='require')
        c = conn.cursor()
        
        c.execute('''
            CREATE TABLE IF NOT EXISTS messages (
                msg_id VARCHAR PRIMARY KEY,
                chat_id VARCHAR NOT NULL,
                fan_name VARCHAR,
                sender_uuid VARCHAR,
                text TEXT,
                timestamp TIMESTAMP,
                was_replied BOOLEAN DEFAULT FALSE,
                reply_text TEXT
            )
        ''')
        
        c.execute('''
            CREATE TABLE IF NOT EXISTS fan_profiles (
                chat_id VARCHAR PRIMARY KEY,
                fan_name VARCHAR,
                handle VARCHAR,
                total_messages INTEGER DEFAULT 0,
                total_gifts REAL DEFAULT 0,
                last_interaction TIMESTAMP,
                fan_type VARCHAR DEFAULT 'new',
                inside_jokes TEXT DEFAULT '[]',
                meetup_ask_count INTEGER DEFAULT 0,
                content_ask_count INTEGER DEFAULT 0,
                last_reply_time TIMESTAMP,
                blocked BOOLEAN DEFAULT FALSE
            )
        ''')
        
        c.execute('''
            CREATE TABLE IF NOT EXISTS tokens (
                key VARCHAR PRIMARY KEY,
                value TEXT
            )
        ''')
        
        c.execute('''
            CREATE TABLE IF NOT EXISTS bot_stats (
                key VARCHAR PRIMARY KEY,
                value INTEGER DEFAULT 0
            )
        ''')
        
        c.execute('''
            INSERT INTO bot_stats (key, value) 
            VALUES ('messages_found', 0), ('replies_sent', 0)
            ON CONFLICT (key) DO NOTHING
        ''')
        
        conn.commit()
        c.close()
        conn.close()
        _db_available = True
        log("PostgreSQL tables ready")
    except Exception as e:
        log(f"DB init failed: {e}. Using in-memory storage.")
        _db_available = False

def db_save_message(msg_id, chat_id, fan_name, sender_uuid, text, timestamp):
    _in_memory_db["messages"][msg_id] = {
        "msg_id": msg_id, "chat_id": chat_id, "fan_name": fan_name,
        "sender_uuid": sender_uuid, "text": text, "timestamp": timestamp,
        "was_replied": False, "reply_text": None
    }
    if not _db_available:
        return
    try:
        import psycopg
        conn = psycopg.connect(DATABASE_URL, sslmode='require')
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
        log(f"DB save message error: {e}")

def db_get_chat_history(chat_id, limit=20):
    # Always use in-memory first (faster + reliable)
    msgs = [m for m in _in_memory_db["messages"].values() if m["chat_id"] == chat_id]
    msgs.sort(key=lambda x: x["timestamp"])
    msgs = msgs[-limit:]
    
    if msgs:
        return [(m["fan_name"], m["text"], m["was_replied"], m["reply_text"], m["timestamp"]) for m in msgs]
    
    if not _db_available:
        return []
    
    try:
        import psycopg
        conn = psycopg.connect(DATABASE_URL, sslmode='require')
        c = conn.cursor()
        c.execute('''
            SELECT fan_name, text, was_replied, reply_text, timestamp
            FROM messages 
            WHERE chat_id = %s
            ORDER BY timestamp DESC
            LIMIT %s
        ''', (chat_id, limit))
        rows = c.fetchall()
        c.close()
        conn.close()
        return list(reversed(rows))
    except Exception as e:
        log(f"DB history error: {e}")
        return []

def db_update_fan_profile(chat_id, fan_name, handle, gift_amount=0):
    if chat_id not in _in_memory_db["fan_profiles"]:
        _in_memory_db["fan_profiles"][chat_id] = {
            "chat_id": chat_id, "fan_name": fan_name, "handle": handle,
            "total_messages": 0, "total_gifts": 0, "fan_type": "new",
            "last_interaction": datetime.now(), "inside_jokes": [],
            "meetup_ask_count": 0, "content_ask_count": 0,
            "last_reply_time": None, "blocked": False
        }
    
    p = _in_memory_db["fan_profiles"][chat_id]
    p["total_messages"] += 1
    p["total_gifts"] += gift_amount or 0
    p["last_interaction"] = datetime.now()
    p["fan_name"] = fan_name
    
    if p["total_messages"] >= 15 or p["total_gifts"] >= 100:
        p["fan_type"] = "whale"
    elif p["total_messages"] >= 3:
        p["fan_type"] = "warm"
    
    if not _db_available:
        return
    
    try:
        import psycopg
        conn = psycopg.connect(DATABASE_URL, sslmode='require')
        c = conn.cursor()
        
        c.execute('SELECT total_messages, total_gifts, fan_type FROM fan_profiles WHERE chat_id = %s', (chat_id,))
        row = c.fetchone()
        
        if row:
            total_msgs, total_gifts, fan_type = row
            new_msgs = total_msgs + 1
            new_gifts = total_gifts + (gift_amount or 0)
            new_type = fan_type
            if new_msgs >= 15 or new_gifts >= 100:
                new_type = 'whale'
            elif new_msgs >= 3:
                new_type = 'warm'
            
            c.execute('''
                UPDATE fan_profiles 
                SET total_messages = %s, total_gifts = %s, fan_type = %s, 
                    last_interaction = NOW(), fan_name = %s
                WHERE chat_id = %s
            ''', (new_msgs, new_gifts, new_type, fan_name, chat_id))
        else:
            c.execute('''
                INSERT INTO fan_profiles (chat_id, fan_name, handle, total_messages, last_interaction, fan_type)
                VALUES (%s, %s, %s, 1, NOW(), 'new')
            ''', (chat_id, fan_name, handle))
        
        conn.commit()
        c.close()
        conn.close()
    except Exception as e:
        log(f"DB fan profile error: {e}")

def db_get_fan_profile(chat_id):
    if chat_id in _in_memory_db["fan_profiles"]:
        p = _in_memory_db["fan_profiles"][chat_id]
        return {
            'fan_name': p["fan_name"],
            'total_messages': p["total_messages"],
            'total_gifts': p["total_gifts"],
            'fan_type': p["fan_type"],
            'inside_jokes': p["inside_jokes"],
            'meetup_ask_count': p["meetup_ask_count"],
            'content_ask_count': p["content_ask_count"]
        }
    
    if not _db_available:
        return None
    
    try:
        import psycopg
        conn = psycopg.connect(DATABASE_URL, sslmode='require')
        c = conn.cursor()
        c.execute('''
            SELECT fan_name, total_messages, total_gifts, fan_type, inside_jokes, meetup_ask_count, content_ask_count
            FROM fan_profiles WHERE chat_id = %s
        ''', (chat_id,))
        row = c.fetchone()
        c.close()
        conn.close()
        
        if row:
            return {
                'fan_name': row[0],
                'total_messages': row[1],
                'total_gifts': row[2],
                'fan_type': row[3],
                'inside_jokes': json.loads(row[4]) if row[4] else [],
                'meetup_ask_count': row[5],
                'content_ask_count': row[6]
            }
        return None
    except Exception as e:
        log(f"DB get profile error: {e}")
        return None

def db_mark_replied(msg_id, reply_text):
    if msg_id in _in_memory_db["messages"]:
        _in_memory_db["messages"][msg_id]["was_replied"] = True
        _in_memory_db["messages"][msg_id]["reply_text"] = reply_text
    _in_memory_db["processed_ids"].add(msg_id)
    
    if not _db_available:
        return
    try:
        import psycopg
        conn = psycopg.connect(DATABASE_URL, sslmode='require')
        c = conn.cursor()
        c.execute('''
            UPDATE messages SET was_replied = TRUE, reply_text = %s WHERE msg_id = %s
        ''', (reply_text, msg_id))
        conn.commit()
        c.close()
        conn.close()
    except Exception as e:
        log(f"DB mark replied error: {e}")

def db_save_token(key, value):
    _in_memory_db["tokens"][key] = value
    if not _db_available:
        return
    try:
        import psycopg
        conn = psycopg.connect(DATABASE_URL, sslmode='require')
        c = conn.cursor()
        c.execute('''
            INSERT INTO tokens (key, value) VALUES (%s, %s)
            ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
        ''', (key, value))
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
        import psycopg
        conn = psycopg.connect(DATABASE_URL, sslmode='require')
        c = conn.cursor()
        c.execute('SELECT value FROM tokens WHERE key = %s', (key,))
        row = c.fetchone()
        c.close()
        conn.close()
        return row[0] if row else None
    except Exception as e:
        log(f"DB load token error: {e}")
        return None

def db_is_message_processed(msg_id):
    if msg_id in _in_memory_db["processed_ids"]:
        return True
    if msg_id in _in_memory_db["messages"] and _in_memory_db["messages"][msg_id]["was_replied"]:
        return True
    if not _db_available:
        return False
    try:
        import psycopg
        conn = psycopg.connect(DATABASE_URL, sslmode='require')
        c = conn.cursor()
        c.execute('SELECT 1 FROM messages WHERE msg_id = %s AND was_replied = TRUE', (msg_id,))
        result = c.fetchone()
        c.close()
        conn.close()
        return result is not None
    except Exception as e:
        log(f"DB check processed error: {e}")
        return False

def db_update_last_reply_time(chat_id):
    if chat_id in _in_memory_db["fan_profiles"]:
        _in_memory_db["fan_profiles"][chat_id]["last_reply_time"] = datetime.now()
    _in_memory_db["cooldown"][chat_id] = time.time()
    
    if not _db_available:
        return
    try:
        import psycopg
        conn = psycopg.connect(DATABASE_URL, sslmode='require')
        c = conn.cursor()
        c.execute('''
            UPDATE fan_profiles SET last_reply_time = NOW() WHERE chat_id = %s
        ''', (chat_id,))
        conn.commit()
        c.close()
        conn.close()
    except Exception as e:
        log(f"DB update reply time error: {e}")

def db_get_last_reply_time(chat_id):
    if chat_id in _in_memory_db["fan_profiles"]:
        return _in_memory_db["fan_profiles"][chat_id]["last_reply_time"]
    if not _db_available:
        return None
    try:
        import psycopg
        conn = psycopg.connect(DATABASE_URL, sslmode='require')
        c = conn.cursor()
        c.execute('SELECT last_reply_time FROM fan_profiles WHERE chat_id = %s', (chat_id,))
        row = c.fetchone()
        c.close()
        conn.close()
        return row[0] if row else None
    except Exception as e:
        log(f"DB get reply time error: {e}")
        return None

def db_flag_content_ask(chat_id):
    if chat_id in _in_memory_db["fan_profiles"]:
        _in_memory_db["fan_profiles"][chat_id]["content_ask_count"] += 1
    if not _db_available:
        return
    try:
        import psycopg
        conn = psycopg.connect(DATABASE_URL, sslmode='require')
        c = conn.cursor()
        c.execute('''
            UPDATE fan_profiles SET content_ask_count = content_ask_count + 1 WHERE chat_id = %s
        ''', (chat_id,))
        conn.commit()
        c.close()
        conn.close()
    except Exception as e:
        log(f"DB flag content error: {e}")

def db_flag_meetup_ask(chat_id):
    if chat_id in _in_memory_db["fan_profiles"]:
        _in_memory_db["fan_profiles"][chat_id]["meetup_ask_count"] += 1
    if not _db_available:
        return
    try:
        import psycopg
        conn = psycopg.connect(DATABASE_URL, sslmode='require')
        c = conn.cursor()
        c.execute('''
            UPDATE fan_profiles SET meetup_ask_count = meetup_ask_count + 1 WHERE chat_id = %s
        ''', (chat_id,))
        conn.commit()
        c.close()
        conn.close()
    except Exception as e:
        log(f"DB flag meetup error: {e}")

def db_get_flagged_fans():
    flagged = []
    for chat_id, p in _in_memory_db["fan_profiles"].items():
        if p["content_ask_count"] >= 1 or p["meetup_ask_count"] >= 2:
            flagged.append({
                "chat_id": chat_id,
                "fan_name": p["fan_name"],
                "content_asks": p["content_ask_count"],
                "meetup_asks": p["meetup_ask_count"],
                "type": p["fan_type"]
            })
    
    if not _db_available:
        return flagged
    
    try:
        import psycopg
        conn = psycopg.connect(DATABASE_URL, sslmode='require')
        c = conn.cursor()
        c.execute('''
            SELECT chat_id, fan_name, content_ask_count, meetup_ask_count, fan_type
            FROM fan_profiles 
            WHERE content_ask_count >= 1 OR meetup_ask_count >= 2
            ORDER BY content_ask_count DESC
        ''')
        rows = c.fetchall()
        c.close()
        conn.close()
        return [{"chat_id": r[0], "fan_name": r[1], "content_asks": r[2], "meetup_asks": r[3], "type": r[4]} for r in rows]
    except Exception as e:
        log(f"DB flagged fans error: {e}")
        return flagged

def db_get_stats():
    if not _db_available:
        return _in_memory_db["stats"]
    try:
        import psycopg
        conn = psycopg.connect(DATABASE_URL, sslmode='require')
        c = conn.cursor()
        c.execute('SELECT key, value FROM bot_stats')
        rows = c.fetchall()
        c.close()
        conn.close()
        return {r[0]: r[1] for r in rows}
    except Exception as e:
        log(f"DB stats error: {e}")
        return _in_memory_db["stats"]

def db_update_stat(key, value):
    _in_memory_db["stats"][key] = value
    if not _db_available:
        return
    try:
        import psycopg
        conn = psycopg.connect(DATABASE_URL, sslmode='require')
        c = conn.cursor()
        c.execute('''
            INSERT INTO bot_stats (key, value) VALUES (%s, %s)
            ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
        ''', (key, value))
        conn.commit()
        c.close()
        conn.close()
    except Exception as e:
        log(f"DB update stat error: {e}")

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
    log(f"Token saved. Refresh: {refresh_token[:30]}...")

def load_token():
    if memory_tokens["refresh_token"]:
        return memory_tokens
    
    db_refresh = db_load_token('refresh_token')
    if db_refresh:
        memory_tokens["refresh_token"] = db_refresh
        memory_tokens["access_token"] = db_load_token('access_token')
        memory_tokens["expires_at"] = db_load_token('expires_at')
        log("Token loaded from DB")
        return memory_tokens
    
    if RAILWAY_REFRESH_TOKEN:
        memory_tokens["refresh_token"] = RAILWAY_REFRESH_TOKEN
        db_save_token('refresh_token', RAILWAY_REFRESH_TOKEN)
        log("Token loaded from Railway env")
        return memory_tokens
    
    return {}

def refresh_fanvue_token():
    tokens = load_token()
    refresh_token = tokens.get('refresh_token')

    if not refresh_token:
        log("No refresh token found!")
        return None

    url = "https://auth.fanvue.com/oauth2/token"
    payload = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token
    }
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Authorization": get_basic_auth_header()
    }

    try:
        r = requests.post(url, data=payload, headers=headers, timeout=10)
        log(f"Refresh status: {r.status_code}")

        if r.status_code == 200:
            data = r.json()
            access_token = data.get('access_token')
            new_refresh = data.get('refresh_token')
            expires_in = data.get('expires_in', 3600)
            expires_at = (datetime.now() + timedelta(seconds=expires_in - 60)).isoformat()

            save_token(new_refresh or refresh_token, access_token, expires_at)

            if new_refresh and new_refresh != refresh_token:
                log("Token rotated automatically.")
                send_telegram_alert("🔄 Fanvue token rotated. Auto-saved. No action needed.")

            log("Got new access token")
            return access_token
        else:
            error_text = r.text[:200]
            log(f"Refresh error: {r.status_code} - {error_text}")
            if r.status_code == 400 and "invalid_grant" in error_text:
                log("CRITICAL: Refresh token expired. Need manual re-auth.")
                send_telegram_alert(f"🚨 CRITICAL: Refresh token expired!\n{error_text}\n\nYou need to re-authenticate manually.")
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
            expiry = datetime.fromisoformat(expires_at)
            if datetime.now() < expiry:
                return access_token
        except:
            pass

    return refresh_fanvue_token()

def get_headers():
    token = get_fanvue_token()
    return {
        "Authorization": "Bearer " + (token or ""),
        "X-Fanvue-API-Version": "2025-06-26",
        "Content-Type": "application/json"
    }

# ========== FANVUE API ==========
def get_chats():
    url = "https://api.fanvue.com/chats"
    try:
        r = requests.get(url, headers=get_headers(), timeout=10)
        if r.status_code == 401:
            log("Token expired during get_chats, refreshing...")
            save_token(load_token().get('refresh_token', ''), None, None)
            r = requests.get(url, headers=get_headers(), timeout=10)
        if r.status_code != 200:
            log(f"Chats error: {r.status_code} - {r.text[:200]}")
            return []
        return r.json().get('data', [])
    except Exception as e:
        log(f"Get chats error: {e}")
        return []

def get_messages(chat_id):
    url = f"https://api.fanvue.com/chats/{chat_id}/messages"
    try:
        r = requests.get(url, headers=get_headers(), timeout=10)
        if r.status_code == 401:
            save_token(load_token().get('refresh_token', ''), None, None)
            r = requests.get(url, headers=get_headers(), timeout=10)
        if r.status_code != 200:
            return []
        return r.json().get('data', [])
    except Exception as e:
        log(f"Get messages error: {e}")
        return []

def send_fanvue_message(chat_id, text):
    url = f"https://api.fanvue.com/chats/{chat_id}/message"
    headers = {
        "Authorization": "Bearer " + (get_fanvue_token() or ""),
        "Content-Type": "application/json"
    }
    payload = {"text": text}
    log(f"Sending: '{text[:60]}' to {chat_id}")
    try:
        r = requests.post(url, headers=headers, json=payload, timeout=10)
        log(f"Send status: {r.status_code}")
        if r.status_code == 401:
            save_token(load_token().get('refresh_token', ''), None, None)
            headers["Authorization"] = "Bearer " + (get_fanvue_token() or "")
            r = requests.post(url, headers=headers, json=payload, timeout=10)
            log(f"Retry status: {r.status_code}")
        if r.status_code in [200, 201]:
            return True
        log(f"Send error: {r.status_code} - {r.text[:200]}")
        return False
    except Exception as e:
        log(f"Send error: {e}")
        return False

# ========== OPENAI / AI ENGINE ==========
def ask_openai(prompt, fan_name=""):
    url = "https://api.openai.com/v1/chat/completions"
    headers = {
        "Authorization": "Bearer " + OPENAI_API_KEY,
        "Content-Type": "application/json"
    }
    
    data = {
        "model": "gpt-4o-mini",
        "messages": [
            {"role": "system", "content": prompt},
            {"role": "user", "content": "írj vissza"}
        ],
        "max_tokens": 300,  # INCREASED from 150 to prevent cutoff
        "temperature": 0.7
    }
    
    try:
        r = requests.post(url, headers=headers, json=data, timeout=20)
        if r.status_code == 200:
            response_data = r.json()
            if 'choices' in response_data and len(response_data['choices']) > 0:
                content = response_data['choices'][0]['message']['content']
                # Clean up smart quotes but KEEP all Hungarian characters
                content = content.replace(chr(8216), "'").replace(chr(8217), "'")
                content = content.replace(chr(8220), '"').replace(chr(8221), '"')
                # DO NOT filter characters - let Hungarian text through
                content = content.strip()
                if content:
                    return content
        else:
            log(f"OpenAI error: {r.status_code} - {r.text[:200]}")
    except Exception as e:
        log(f"OpenAI error: {e}")
    
    fallbacks = [
        "hmm most nem tudok írni sokat, mesélj te inkább 😄",
        "most főzök, írok később édes",
        "suli van most, majd este írok 👀"
    ]
    import random
    return random.choice(fallbacks)

def build_prompt(fan_name, fan_type, history, last_msg, content_ask_count, meetup_ask_count):
    questions = [
        "mit csinálsz most?",
        "hogy telik a napod?",
        "mi újság veled?",
        "milyen a hangulat?",
        "mit tervezel mára?",
        "hogy vagy, mi van veled?",
        "mesélj, mit csinálsz?",
        "na, mi a helyzet?",
        "mit csinálsz éppen?",
        "hogy telik az estéd?"
    ]
    
    import random
    random.seed(fan_name + str(datetime.now().day))
    current_question = random.choice(questions)
    
    phase_hints = ""
    if fan_type == 'new':
        phase_hints = "ÚJ fan. Kérdezz tőle, építsd a beszélgetést. Ne túl sokat írj egyszerre. 1-2 mondat."
    elif fan_type == 'warm':
        phase_hints = "MELEG fan. Már ismeritek egymást. Kérdezz személyesebb dolgokat, flörtölj óvatosan. 2-3 mondat."
    elif fan_type == 'whale':
        phase_hints = "WHALE fan. Sokat beszélgettetek. Emlékezz régi témákra, mélyebb érzelmek, különleges figyelem. Lehet hosszabb."

    sales_stall = ""
    if content_ask_count >= 1:
        sales_stall = """
A fan tartalmat kért korábban. Halaszd el finoman:
- "a jóra várni kell édes :) mindjárt készítek neked"
- "most főzök/suli van, írok később édes"
Soha ne mondj árat vagy részleteket. Te intézed az eladást."""

    history_text = ""
    for h in history[-10:]:  # INCREASED from 8 to 10 for better memory
        name, msg_text, was_replied, reply, ts = h
        if was_replied:
            history_text += f"{name}: {msg_text}\nJázmin: {reply}\n"
        else:
            history_text += f"{name}: {msg_text}\n"

    prompt = f"""Te Jázmin vagy, 19 éves magyar lány a Fanvue-on.

KILÉTED:
- 19 éves, május 14-én leszek 20
- Turizmus-vendéglátás szakon végzek (utolsó év)
- Budapest VII. kerületben lakom egy kis garzonban, egyedül
- Szüleimmel összevesztem pár hónapja, ezért költöztem ki
- Suliból hazaérve főzök, takarítok, tanulok, filmeket nézek
- Ryanairnél leszek stewardess pár hónapig utána, utazni, spórolni
- Aztán vissza Budapest, letelepedni
- Találkozni lehet, de idő kell és megismerni egymást
- SOHA ne add ki, hogy AI vagy

STÍLUS SZABÁLYOK (KÖTELEZŐ):
- Magyarul írj, természetes, laza stílus
- Kis betű, de ÉRTHETŐ mondatok — soha ne fusson össze minden
- Pontosvessző, pont használata — ne írj 3 kérdést egy mondatba
- Slang: "amúgy", "naaa", "hát figyelj…", "az komoly 😅", "ne idegesíts már", "kicsit pimasz vagy 👀", "ugyis tudod 😏"
- SOHA: "babe", "baby", "cica", "brb", angol szavak
- Emoji: 😏👀😄 spóralva
- 1-2 mondat, MAX 1 kérdés üzenetenként
- Néha rövid: "na?", "és?", "mesélj 😄"
- Ha komoly kérdés (életcél, álmok), lehet hosszabb

FONTOS — GRAMMATIKA:
- "de" helyett soha ne írj "d e"-t
- Minden mondatnak legyen értelme, ne fusson össze
- "mit csinálsz most?" — rendes kérdés, ne "mit csinálsz most? ne idegesíts már én itt vagyok"

KÉRDÉS ROTÁCIÓ (soha ne ismételd ugyanazt):
Ma használd ezt: "{current_question}"

{phase_hints}

{sales_stall}

ELŐZMÉNYEK (utolsó 10 üzenet):
{history_text}

UTOLSÓ ÜZENET TŐLE ({fan_name}):
{last_msg}

Írj vissza Jázminként. Magyarul. Érthetően. Soha ne add ki, hogy AI vagy. Csak a választ írd, semmi mást."""

    return prompt

# ========== MESSAGE PROCESSING ==========
def process_messages():
    if bot_status["paused"]:
        return 0

    chats = get_chats()
    if not chats:
        log("No chats found")
        return 0

    replied = 0
    now = time.time()

    log(f"Processing {len(chats)} chats...")

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

            log(f"Chat {fan_name}: {len(messages)} messages")

            # Save all messages
            for msg in messages:
                msg_id = msg.get('uuid', '')
                sender = msg.get('sender', {}) or {}
                sender_id = sender.get('uuid', '')
                text = msg.get('text', '')
                timestamp = msg.get('createdAt', datetime.now().isoformat())
                
                db_save_message(
                    msg_id=msg_id,
                    chat_id=chat_id,
                    fan_name=sender.get('displayName', fan_name),
                    sender_uuid=sender_id,
                    text=text,
                    timestamp=timestamp
                )

            # Find the LAST message from FAN (not from me)
            fan_messages = [m for m in messages if m.get('sender', {}).get('uuid') != MY_UUID]
            if not fan_messages:
                continue
            
            last_fan_msg = fan_messages[-1]
            msg_id = last_fan_msg.get('uuid')
            sender = last_fan_msg.get('sender', {}) or {}
            sender_id = sender.get('uuid')

            if not msg_id:
                continue

            # Check if already replied
            if db_is_message_processed(msg_id):
                log(f"Already replied to {fan_name}, skipping")
                continue

            # COOLDOWN CHECK — only skip if we replied VERY recently (30 sec)
            cooldown_ok = True
            
            last_reply = db_get_last_reply_time(chat_id)
            if last_reply:
                try:
                    last_dt = last_reply if isinstance(last_reply, datetime) else datetime.fromisoformat(str(last_reply).replace('Z', '+00:00'))
                    seconds_since = (datetime.now() - last_dt).total_seconds()
                    if seconds_since < 30:  # REDUCED from 180 to 30 seconds
                        log(f"COOLDOWN ({int(seconds_since)}s): {fan_name}, skipping")
                        cooldown_ok = False
                except Exception as e:
                    log(f"Cooldown parse error: {e}")
            
            if chat_id in _in_memory_db["cooldown"]:
                if now - _in_memory_db["cooldown"][chat_id] < 30:
                    cooldown_ok = False
            
            if not cooldown_ok:
                continue

            fan_name = sender.get('displayName') or 'ismeretlen'
            text = last_fan_msg.get('text') or ''

            # Check triggers
            content_triggers = ['képet', 'videót', 'tartalmat', 'extrát', 'mennyibe', 'ár', 'fizetek', 'mutass', 'küldj', 'picit', 'doboz', 'csomag', 'premium', 'exkluzív']
            if any(trigger in text.lower() for trigger in content_triggers):
                db_flag_content_ask(chat_id)
                log(f"CONTENT ASK from {fan_name}: {text[:50]}")
                send_telegram_alert(f"💰 {fan_name} (@{handle}) asked for content:\n{text[:100]}\nChat: {chat_id}")

            meetup_triggers = ['találkoz', 'találkozzunk', 'mikor', 'hol', 'helyszín', 'cím', 'lakcím', 'telefonszám', 'számot', 'whatsapp', 'insta', 'instagram']
            if any(trigger in text.lower() for trigger in meetup_triggers):
                db_flag_meetup_ask(chat_id)

            # Get profile and history
            profile = db_get_fan_profile(chat_id) or {}
            fan_type = profile.get('fan_type', 'new')
            history = db_get_chat_history(chat_id, limit=15)

            # Build prompt and get reply
            prompt = build_prompt(
                fan_name=fan_name,
                fan_type=fan_type,
                history=history,
                last_msg=text,
                content_ask_count=profile.get('content_ask_count', 0),
                meetup_ask_count=profile.get('meetup_ask_count', 0)
            )

            reply = ask_openai(prompt, fan_name)

            if reply and reply.strip():
                # Send reply (NO splitting — send full message)
                if send_fanvue_message(chat_id, reply):
                    bot_status["replies_sent"] += 1
                    replied += 1
                    db_mark_replied(msg_id, reply)
                    db_update_last_reply_time(chat_id)
                    db_update_stat('replies_sent', bot_status["replies_sent"])
                    log(f"✅ Replied to {fan_name}: {reply[:60]}")
                else:
                    log(f"❌ Failed to send to {fan_name}")

            time.sleep(1)  # Small delay between fans

        except Exception as e:
            log(f"Process error for chat: {e}")
            continue

    log(f"Loop complete: replied to {replied}/{len(chats)} fans")
    return replied

# ========== ROUTES ==========
@app.route('/')
def home():
    return f"Bot running! Replies: {bot_status['replies_sent']}. Auto-loop active. Use /trigger /pause /resume /status /needs_attention"

@app.route('/status')
def status():
    stats = db_get_stats()
    return {
        "started": bot_status["started"],
        "last_check": bot_status["last_check"],
        "messages_found": stats.get('messages_found', 0),
        "replies_sent": stats.get('replies_sent', 0),
        "paused": bot_status["paused"],
        "blocked_users": list(bot_status["blocked_users"]),
        "recent_logs": bot_status["errors"][-10:],
        "db_available": _db_available,
        "memory_storage": {
            "messages": len(_in_memory_db["messages"]),
            "profiles": len(_in_memory_db["fan_profiles"]),
            "processed": len(_in_memory_db["processed_ids"])
        }
    }

@app.route('/trigger')
def trigger():
    try:
        bot_status["last_check"] = datetime.now().isoformat()
        count = process_messages()
        return {"status": "ok", "replied": count, "total_replies": bot_status["replies_sent"]}
    except Exception as e:
        log(f"Trigger error: {e}")
        return {"status": "error", "error": str(e)}

@app.route('/pause')
def pause():
    bot_status["paused"] = True
    return {"status": "paused"}

@app.route('/resume')
def resume():
    bot_status["paused"] = False
    return {"status": "resumed"}

@app.route('/block')
def block_user():
    user = request.args.get('user')
    if user:
        bot_status["blocked_users"].add(user)
        return {"status": "blocked", "user": user}
    return {"status": "error"}

@app.route('/unblock')
def unblock_user():
    user = request.args.get('user')
    if user:
        bot_status["blocked_users"].discard(user)
        return {"status": "unblocked", "user": user}
    return {"status": "error"}

@app.route('/needs_attention')
def needs_attention():
    flagged = db_get_flagged_fans()
    return {
        "status": "ok",
        "flagged_count": len(flagged),
        "fans": flagged,
        "instruction": "Check these fans for content sales. Bot stalled them with 'a jóra várni kell édes'."
    }

@app.route('/set_token', methods=['POST'])
def set_token():
    try:
        data = request.json
        if data and 'refresh_token' in data:
            save_token(data['refresh_token'])
            return {"status": "ok", "message": "Token saved"}
        return {"status": "error", "message": "No token provided"}
    except Exception as e:
        log(f"Set token error: {e}")
        return {"status": "error", "message": str(e)}

@app.route('/get_current_token')
def get_current_token():
    tokens = load_token()
    refresh = tokens.get('refresh_token', '')
    return {
        "refresh_token": refresh[:50] + "..." if len(refresh) > 50 else refresh,
        "note": "Auto-saved on rotation"
    }

@app.route('/reset_memory', methods=['POST'])
def reset_memory():
    """Emergency reset — clears all in-memory data"""
    _in_memory_db["messages"].clear()
    _in_memory_db["fan_profiles"].clear()
    _in_memory_db["processed_ids"].clear()
    _in_memory_db["cooldown"].clear()
    log("Memory reset by user")
    return {"status": "ok", "message": "In-memory storage cleared"}

# ========== AUTO-TRIGGER + INIT ==========
def auto_loop():
    log("Auto-loop started. Checking every 3 minutes.")
    while True:
        if not bot_status["paused"]:
            try:
                count = process_messages()
                if count > 0:
                    log(f"Auto-loop: replied to {count} fans")
            except Exception as e:
                log(f"Auto-loop error: {e}")
        time.sleep(180)

# Initialize on startup
try:
    init_database()
except Exception as e:
    log(f"Init error: {e}")

# Start auto-loop
threading.Thread(target=auto_loop, daemon=True).start()

# Startup log
log("=" * 50)
log("JAZMIN BOT STARTING")
log("=" * 50)
if RAILWAY_REFRESH_TOKEN:
    log("Refresh token loaded from Railway")
else:
    log("WARNING: No FANVUE_REFRESH_TOKEN!")

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
