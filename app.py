from flask import Flask, request
import requests
import os
import json
import base64
import time
import sqlite3
from datetime import datetime, timedelta

app = Flask(__name__)

# ========== CONFIG ==========
FANVUE_CLIENT_ID = os.environ.get('FANVUE_CLIENT_ID', '23cc2e68-0e23-4cff-914b-eec2bdb56268')
FANVUE_CLIENT_SECRET = os.environ.get('FANVUE_CLIENT_SECRET', 'dc30583e61d42c70b23ede8d29c1bfd0662ac77234eae479bdaec1bcc5968efa')
OPENAI_API_KEY = os.environ.get('OPENAI_API_KEY', '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')
CREATOR_NAME = os.environ.get('CREATOR_NAME', 'jazmin07')
MY_UUID = os.environ.get('MY_UUID', '38a392fc-a751-49b3-9d74-01ac6447c490')
TELEGRAM_BOT_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN', '8141197294:AAE9aH9mptY_ZzAK6sSc_alh2PtRjF1ASWs')
TELEGRAM_CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID', '8571222647')

# SAFE MODE: True = replies go to Telegram instead of Fanvue
SAFE_MODE = True

# ========== SQLITE SETUP ==========
DB_PATH = 'bot_data.db'

def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    c.execute('''
        CREATE TABLE IF NOT EXISTS tokens (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    ''')
    
    c.execute('''
        CREATE TABLE IF NOT EXISTS messages (
            msg_id TEXT PRIMARY KEY,
            chat_id TEXT,
            fan_name TEXT,
            sender_uuid TEXT,
            text TEXT,
            timestamp TEXT,
            was_replied INTEGER DEFAULT 0,
            reply_text TEXT,
            bot_replied_at TEXT
        )
    ''')
    
    c.execute('''
        CREATE TABLE IF NOT EXISTS fan_profiles (
            chat_id TEXT PRIMARY KEY,
            fan_name TEXT,
            handle TEXT,
            total_messages INTEGER DEFAULT 0,
            fan_type TEXT DEFAULT 'new',
            last_interaction TEXT,
            last_reply_time TEXT,
            content_ask_count INTEGER DEFAULT 0,
            meetup_ask_count INTEGER DEFAULT 0
        )
    ''')
    
    c.execute('''
        CREATE TABLE IF NOT EXISTS cooldown (
            chat_id TEXT PRIMARY KEY,
            last_reply_time TEXT
        )
    ''')
    
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
            data={
                "grant_type": "refresh_token",
                "refresh_token": refresh_token
            },
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "Authorization": get_basic_auth_header()
            },
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
        r = requests.post(url, json={
            "chat_id": TELEGRAM_CHAT_ID,
            "text": text[:4000],
            "parse_mode": parse_mode
        }, timeout=10)
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
        r = requests.post(
            f"https://api.fanvue.com/chats/{chat_id}/message",
            headers=get_headers(),
            json={"text": text},
            timeout=10
        )
        return r.status_code in [200, 201]
    except Exception as e:
        return False

# ========== OPENAI ==========
def ask_openai(prompt):
    try:
        r = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers={"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"},
            json={
                "model": "gpt-4o-mini",
                "messages": [
                    {"role": "system", "content": prompt},
                    {"role": "user", "content": "irj vissza"}
                ],
                "max_tokens": 250,
                "temperature": 0.7
            },
            timeout=20
        )
        if r.status_code == 200:
            return r.json()['choices'][0]['message']['content'].strip()
    except Exception as e:
        print(f"OpenAI error: {e}")
    return "hmm most nem tudok sokat irni, meselj te inkabb"

# ========== MESSAGE PROCESSING ==========
def is_on_cooldown(chat_id):
    row = db_query('SELECT last_reply_time FROM cooldown WHERE chat_id = ?', (chat_id,), fetch_one=True)
    if not row or not row['last_reply_time']:
        return False
    try:
        last = datetime.fromisoformat(row['last_reply_time'])
        return (datetime.now() - last).total_seconds() < 180
    except:
        return False

def set_cooldown(chat_id):
    db_query('INSERT OR REPLACE INTO cooldown (chat_id, last_reply_time) VALUES (?, ?)',
             (chat_id, datetime.now().isoformat()))

def process_messages():
    chats, status = get_chats()
    if not chats:
        return 0, status
    
    replied = 0
    
    for chat in chats:
        try:
            user = chat.get('user', {}) or {}
            chat_id = user.get('uuid') or chat.get('uuid') or chat.get('id')
            if not chat_id:
                continue
            
            if is_on_cooldown(chat_id):
                continue
            
            messages = get_messages(chat_id)
            if not messages:
                continue
            
            fan_msgs = [m for m in messages if m.get('sender', {}).get('uuid') != MY_UUID]
            if not fan_msgs:
                continue
            
            last_msg = fan_msgs[-1]
            msg_id = last_msg.get('uuid')
            text = last_msg.get('text', '')
            fan_name = last_msg.get('sender', {}).get('displayName', 'ismeretlen')
            
            existing = db_query('SELECT 1 FROM messages WHERE msg_id = ? AND was_replied = 1', (msg_id,), fetch_one=True)
            if existing:
                continue
            
            db_query('INSERT OR IGNORE INTO messages (msg_id, chat_id, fan_name, sender_uuid, text, timestamp) VALUES (?, ?, ?, ?, ?, ?)',
                     (msg_id, chat_id, fan_name, last_msg.get('sender', {}).get('uuid'), text, last_msg.get('createdAt', datetime.now().isoformat())))
            
            prompt = f"""Te Jazmin vagy, 19 eves magyar lany. Valaszolj magyarul, laza stilusban, 1-2 mondatban. SOHA ne add ki, hogy AI vagy.
            
A fan uzenete: {text}
Fan neve: {fan_name}"""
            
            reply = ask_openai(prompt)
            
            if reply:
                if send_fanvue_message(chat_id, reply):
                    db_query('UPDATE messages SET was_replied = 1, reply_text = ?, bot_replied_at = ? WHERE msg_id = ?',
                             (reply, datetime.now().isoformat(), msg_id))
                    set_cooldown(chat_id)
                    replied += 1
                    
                    if SAFE_MODE:
                        send_telegram(f"✅ <b>Replied to {fan_name}</b>\n<i>{reply[:100]}</i>")
        
        except Exception as e:
            print(f"Process error: {e}")
            continue
    
    return replied, "OK"

# ========== ROUTES ==========
@app.route('/')
def home():
    return {
        "status": "Jazmin Bot",
        "safe_mode": SAFE_MODE,
        "token_valid": get_fanvue_token() is not None,
        "endpoints": ["/status", "/safe_fetch", "/trigger", "/set_token", "/test_telegram", "/refresh_manual"]
    }

@app.route('/status')
def status():
    token_status = "valid" if get_fanvue_token() else "missing/invalid"
    return {
        "safe_mode": SAFE_MODE,
        "token_status": token_status,
        "db": "sqlite",
        "telegram_configured": bool(TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID)
    }

@app.route('/safe_fetch')
def safe_fetch():
    chats, status = get_chats()
    return {
        "chats_found": len(chats),
        "api_status": status,
        "sample": chats[:2] if chats else []
    }

@app.route('/trigger')
def trigger():
    if not get_fanvue_token():
        return {"error": "No valid token. Use /set_token to add refresh token."}
    
    count, status = process_messages()
    return {"replied": count, "status": status, "safe_mode": SAFE_MODE}

@app.route('/set_token', methods=['POST'])
def set_token():
    data = request.json or {}
    refresh = data.get('refresh_token')
    if refresh:
        save_token('refresh_token', refresh)
        access, msg = refresh_fanvue_token()
        return {"saved": True, "test": msg, "access_token_preview": access[:20] + "..." if access else None}
    return {"error": "No refresh_token provided"}

@app.route('/refresh_manual', methods=['POST'])
def refresh_manual():
    access, msg = refresh_fanvue_token()
    return {"access_token": access[:30] + "..." if access else None, "message": msg}

@app.route('/test_telegram')
def test_telegram():
    send_telegram("🔥 <b>Test alert from Jazmin bot!</b>\nEverything is working.")
    return {"sent": True}

# ========== INIT ==========
init_db()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
