from flask import Flask, request
import requests
import os
import json
import sqlite3
import base64
import time
from datetime import datetime, timedelta

app = Flask(__name__)

FANVUE_CLIENT_ID = os.environ.get('FANVUE_CLIENT_ID')
FANVUE_CLIENT_SECRET = os.environ.get('FANVUE_CLIENT_SECRET')
KIMI_API_KEY = os.environ.get('KIMI_API_KEY')
CREATOR_NAME = os.environ.get('CREATOR_NAME', 'Creator')

DB_PATH = '/tmp/fanvue_bot.db'

bot_status = {
    "started": datetime.now().isoformat(),
    "last_check": "never",
    "messages_found": 0,
    "replies_sent": 0,
    "errors": [],
    "paused": False,
    "blocked_users": set()
}

def log(msg):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{timestamp}] {msg}"
    print(line, flush=True)
    bot_status["errors"].append(line)
    if len(bot_status["errors"]) > 100:
        bot_status["errors"] = bot_status["errors"][-100:]

def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("CREATE TABLE IF NOT EXISTS tokens (id INTEGER PRIMARY KEY, refresh_token TEXT, access_token TEXT, expires_at TEXT)")
    conn.commit()
    conn.close()

def save_token(refresh_token, access_token=None, expires_at=None):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("DELETE FROM tokens")
    c.execute("INSERT INTO tokens (refresh_token, access_token, expires_at) VALUES (?, ?, ?)", (refresh_token, access_token, expires_at))
    conn.commit()
    conn.close()

def load_token():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT refresh_token, access_token, expires_at FROM tokens ORDER BY id DESC LIMIT 1")
    row = c.fetchone()
    conn.close()
    if row:
        return {"refresh_token": row[0], "access_token": row[1], "expires_at": row[2]}
    return {}

def get_basic_auth_header():
    credentials = f"{FANVUE_CLIENT_ID}:{FANVUE_CLIENT_SECRET}"
    encoded = base64.b64encode(credentials.encode('utf-8')).decode('utf-8')
    return f"Basic {encoded}"

def refresh_fanvue_token():
    tokens = load_token()
    refresh_token = tokens.get('refresh_token')

    if not refresh_token:
        log("No refresh token in database!")
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
                log("Token rotated and saved to database")

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

def get_chats():
    url = "https://api.fanvue.com/chats"
    try:
        r = requests.get(url, headers=get_headers(), timeout=10)
        if r.status_code == 401:
            save_token(load_token().get('refresh_token'), None, None)
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
            save_token(load_token().get('refresh_token'), None, None)
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
    payload = {"content": text}
    try:
        r = requests.post(url, headers=headers, json=payload, timeout=10)
        log(f"Send status: {r.status_code}")
        if r.status_code == 401:
            save_token(load_token().get('refresh_token'), None, None)
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

def ask_kimi(message, fan_name=""):
    url = "https://api.moonshot.ai/v1/chat/completions"
    headers = {
        "Authorization": "Bearer " + KIMI_API_KEY,
        "Content-Type": "application/json"
    }
    system = f"You are {CREATOR_NAME}. Reply in Hungarian. Keep under 30 words. Be sweet and casual."
    data = {
        "model": "kimi-latest",
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": message}
        ],
        "max_tokens": 100
    }
    try:
        r = requests.post(url, headers=headers, json=data, timeout=15)
        if r.status_code == 200:
            content = r.json()['choices'][0]['message']['content']
            return content.strip() if content else "Szia! 😊"
        else:
            log(f"Kimi error: {r.status_code}")
            return "Szia! 😊 Mi ujsag?"
    except Exception as e:
        log(f"Kimi error: {e}")
        return "Szia! 😊 Mi ujsag?"

processed_messages = set()

def process_messages():
    if bot_status["paused"]:
        return 0

    chats = get_chats()
    if not chats:
        return 0

    replied = 0
    my_uuid = '38a392fc-a751-49b3-9d74-01ac6447c490'

    for chat in chats[:10]:
        try:
            user = chat.get('user', {}) or {}
            chat_id = user.get('uuid')
            if not chat_id:
                continue

            messages = get_messages(chat_id)
            if not messages:
                continue

            last_msg = messages[-1]
            msg_id = last_msg.get('uuid')
            sender = last_msg.get('sender', {}) or {}

            if sender.get('uuid') != my_uuid and msg_id not in processed_messages:
                fan_name = sender.get('displayName') or 'babe'
                text = last_msg.get('text') or ''

                if fan_name in bot_status["blocked_users"]:
                    processed_messages.add(msg_id)
                    continue

                log(f"NEW MSG from {fan_name}: {text[:50]}")
                bot_status["messages_found"] += 1

                reply = ask_kimi(text, fan_name)

                if send_fanvue_message(chat_id, reply):
                    bot_status["replies_sent"] += 1
                    replied += 1
                    processed_messages.add(msg_id)
                    log(f"Replied: {reply[:50]}")

                time.sleep(2)

        except Exception as e:
            log(f"Process error: {e}")
            continue

    return replied

@app.route('/')
def home():
    return f"Bot running! Replies: {bot_status['replies_sent']}. Use /trigger /pause /resume /status"

@app.route('/status')
def status():
    return {
        "started": bot_status["started"],
        "last_check": bot_status["last_check"],
        "messages_found": bot_status["messages_found"],
        "replies_sent": bot_status["replies_sent"],
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

@app.route('/set_token', methods=['POST'])
def set_token():
    data = request.json
    if data and 'refresh_token' in data:
        save_token(data['refresh_token'])
        return {"status": "ok", "message": "Token saved"}
    return {"status": "error", "message": "No token provided"}

if __name__ == '__main__':
    init_db()
    log("=" * 50)
    log("API BOT STARTING")
    log("=" * 50)
    tokens = load_token()
    if not tokens.get('refresh_token'):
        log("WARNING: No refresh token. Use /set_token to set it.")
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
