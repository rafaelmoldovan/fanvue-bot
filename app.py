from flask import Flask, request
import requests
import os
import json
import base64
import time
import threading
import psycopg
from datetime import datetime, timedelta

app = Flask(__name__)

# ========== CONFIG ==========
FANVUE_CLIENT_ID = os.environ.get('FANVUE_CLIENT_ID')
FANVUE_CLIENT_SECRET = os.environ.get('FANVUE_CLIENT_SECRET')
OPENAI_API_KEY = os.environ.get('OPENAI_API_KEY')
CREATOR_NAME = os.environ.get('CREATOR_NAME', 'Jazmin')
RAILWAY_REFRESH_TOKEN = os.environ.get('FANVUE_REFRESH_TOKEN', '')
DATABASE_URL = os.environ.get('DATABASE_URL')
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

# In-memory cooldown tracker (fallback when DB fails)
_in_memory_cooldown = {}

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

# ========== POSTGRESQL DATABASE ==========
def get_db_conn():
    return psycopg.connect(DATABASE_URL, sslmode='require')

def init_database():
    conn = get_db_conn()
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
    log("PostgreSQL tables ready")

# ========== DATABASE HELPERS ==========
def db_save_message(msg_id, chat_id, fan_name, sender_uuid, text, timestamp):
    try:
        conn = get_db_conn()
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
    try:
        conn = get_db_conn()
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
    try:
        conn = get_db_conn()
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
    try:
        conn = get_db_conn()
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
    try:
        conn = get_db_conn()
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
    try:
        conn = get_db_conn()
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
    try:
        conn = get_db_conn()
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
    try:
        conn = get_db_conn()
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
    try:
        conn = get_db_conn()
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
    try:
        conn = get_db_conn()
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
    try:
        conn = get_db_conn()
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
    try:
        conn = get_db_conn()
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
    try:
        conn = get_db_conn()
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
        return []

def db_get_stats():
    try:
        conn = get_db_conn()
        c = conn.cursor()
        c.execute('SELECT key, value FROM bot_stats')
        rows = c.fetchall()
        c.close()
        conn.close()
        return {r[0]: r[1] for r in rows}
    except Exception as e:
        log(f"DB stats error: {e}")
        return {}

def db_update_stat(key, value):
    try:
        conn = get_db_conn()
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
    log(f"Token saved to Postgres. Refresh: {refresh_token[:30]}...")

def load_token():
    if memory_tokens["refresh_token"]:
        return memory_tokens
    
    db_refresh = db_load_token('refresh_token')
    if db_refresh:
        memory_tokens["refresh_token"] = db_refresh
        memory_tokens["access_token"] = db_load_token('access_token')
        memory_tokens["expires_at"] = db_load_token('expires_at')
        log("Token loaded from PostgreSQL")
        return memory_tokens
    
    if RAILWAY_REFRESH_TOKEN:
        memory_tokens["refresh_token"] = RAILWAY_REFRESH_TOKEN
        db_save_token('refresh_token', RAILWAY_REFRESH_TOKEN)
        log("Token loaded from Railway env, saved to PostgreSQL")
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
                log("Token rotated. Saved to PostgreSQL automatically.")
                send_telegram_alert("🔄 Fanvue token rotated. Auto-saved to database. No action needed.")

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
            save_token(load_token().get('refresh_token', ''), None, None)
            r = requests.get(url, headers=get_headers(), timeout=10)
        if r.status_code != 200:
            log(f"Chats error: {r.status_code}")
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
    safe_text = text.encode('utf-8').decode('utf-8')
    payload = {"text": safe_text}
    log(f"Sending: '{safe_text[:50]}' to {chat_id}")
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
        log(f"Send error: {r.text[:200]}")
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
        "max_tokens": 150,
        "temperature": 0.8
    }
    
    try:
        r = requests.post(url, headers=headers, json=data, timeout=15)
        if r.status_code == 200:
            response_data = r.json()
            if 'choices' in response_data and len(response_data['choices']) > 0:
                content = response_data['choices'][0]['message']['content']
                content = content.replace(chr(8216), "'").replace(chr(8217), "'")
                content = content.replace(chr(8220), '"').replace(chr(8221), '"')
                allowed = set('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 .,!?;:-_áéíóöőúüűÁÉÍÓÖŐÚÜŰ\n')
                content = ''.join(char for char in content if char in allowed)
                return content.strip() if content else None
        else:
            log(f"OpenAI error: {r.status_code}")
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
    random.seed(fan_name + str(len(history)))
    current_question = random.choice(questions)
    
    phase_hints = ""
    if fan_type == 'new':
        phase_hints = "ÚJ fan. Kérdezz tőle, építsd a beszélgetést. Ne túl sokat írj egyszerre."
    elif fan_type == 'warm':
        phase_hints = "MELEG fan. Már ismeritek egymást. Kérdezz személyesebb dolgokat, flörtölj óvatosan."
    elif fan_type == 'whale':
        phase_hints = "WHALE fan. Sokat beszélgettetek. Emlékezz régi témákra, mélyebb érzelmek, különleges figyelem."

    sales_stall = ""
    if content_ask_count >= 1:
        sales_stall = """
A fan tartalmat kért korábban. Halaszd el finoman:
- "a jóra várni kell édes :) mindjárt készítek neked"
- "most főzök/suli van, írok később édes"
Soha ne mondj árat vagy részleteket. Te intézed az eladást."""

    history_text = ""
    for h in history[-8:]:
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

ELŐZMÉNYEK:
{history_text}

UTOLSÓ ÜZENET TŐLE ({fan_name}):
{last_msg}

Írj vissza Jázminként. Magyarul. Érthetően. Soha ne add ki, hogy AI vagy."""

    return prompt

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
            chat_id = user.get('uuid')
            if not chat_id:
                chat_id = chat.get('uuid')
            if not chat_id:
                chat_id = chat.get('id')
            
            if not chat_id:
                continue

            fan_name = user.get('displayName') or 'ismeretlen'
            handle = user.get('handle', '')
            
            db_update_fan_profile(chat_id, fan_name, handle)

            messages = get_messages(chat_id)
            if not messages:
                continue

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

            last_msg = messages[-1]
            msg_id = last_msg.get('uuid')
            sender = last_msg.get('sender', {}) or {}
            sender_id = sender.get('uuid')

            if sender_id == MY_UUID:
                continue

            if db_is_message_processed(msg_id):
                continue

            # COOLDOWN CHECK
            cooldown_ok = True
            
            last_reply = db_get_last_reply_time(chat_id)
            if last_reply:
                try:
                    last_dt = last_reply if isinstance(last_reply, datetime) else datetime.fromisoformat(str(last_reply).replace('Z', '+00:00'))
                    if (datetime.now() - last_dt).total_seconds() < 180:
                        cooldown_ok = False
                except:
                    pass
            
            if chat_id in _in_memory_cooldown:
                if now - _in_memory_cooldown[chat_id] < 180:
                    cooldown_ok = False
            
            if not cooldown_ok:
                log(f"COOLDOWN: {fan_name}, skipping")
                continue

            fan_name = sender.get('displayName') or 'ismeretlen'
            text = last_msg.get('text') or ''

            content_triggers = ['képet', 'videót', 'tartalmat', 'extrát', 'mennyibe', 'ár', 'fizetek', 'mutass', 'küldj', 'picit', 'doboz', 'csomag', 'premium', 'exkluzív']
            if any(trigger in text.lower() for trigger in content_triggers):
                db_flag_content_ask(chat_id)
                log(f"CONTENT ASK from {fan_name}: {text[:50]}")
                send_telegram_alert(f"💰 {fan_name} (@{handle}) asked for content:\n{text[:100]}\nChat: {chat_id}")

            meetup_triggers = ['találkoz', 'találkozzunk', 'mikor', 'hol', 'helyszín', 'cím', 'lakcím', 'telefonszám', 'számot', 'whatsapp', 'insta', 'instagram']
            if any(trigger in text.lower() for trigger in meetup_triggers):
                db_flag_meetup_ask(chat_id)

            profile = db_get_fan_profile(chat_id) or {}
            fan_type = profile.get('fan_type', 'new')
            history = db_get_chat_history(chat_id, limit=15)

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
                import random
                if len(reply) > 60 and random.random() < 0.3:
                    mid = len(reply) // 2
                    split_at = reply.find('.', mid-20, mid+20)
                    if split_at == -1:
                        split_at = reply.find('!', mid-20, mid+20)
                    if split_at == -1:
                        split_at = reply.find('?', mid-20, mid+20)
                    if split_at == -1:
                        split_at = mid
                    else:
                        split_at += 1
                    
                    part1 = reply[:split_at].strip()
                    part2 = reply[split_at:].strip()
                    
                    if part1 and send_fanvue_message(chat_id, part1):
                        time.sleep(1.5)
                        if part2 and send_fanvue_message(chat_id, part2):
                            bot_status["replies_sent"] += 1
                            replied += 1
                            db_mark_replied(msg_id, reply)
                            db_update_last_reply_time(chat_id)
                            db_update_stat('replies_sent', bot_status["replies_sent"])
                            _in_memory_cooldown[chat_id] = now
                            log(f"Split reply to {fan_name}")
                else:
                    if send_fanvue_message(chat_id, reply):
                        bot_status["replies_sent"] += 1
                        replied += 1
                        db_mark_replied(msg_id, reply)
                        db_update_last_reply_time(chat_id)
                        db_update_stat('replies_sent', bot_status["replies_sent"])
                        _in_memory_cooldown[chat_id] = now
                        log(f"Replied to {fan_name}: {reply[:50]}")

            time.sleep(2)

        except Exception as e:
            log(f"Process error: {e}")
            continue

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
        "recent_logs": bot_status["errors"][-10:]
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
            return {"status": "ok", "message": "Token saved to PostgreSQL"}
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
        "note": "Auto-saved to PostgreSQL on rotation"
    }

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
    log("PostgreSQL initialized")
except Exception as e:
    log(f"Init error (tables may exist): {e}")

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
