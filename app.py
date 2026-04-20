from flask import Flask, request
import requests
import os
import json
import base64
import time
from datetime import datetime, timedelta

app = Flask(__name__)

FANVUE_CLIENT_ID = os.environ.get('FANVUE_CLIENT_ID')
FANVUE_CLIENT_SECRET = os.environ.get('FANVUE_CLIENT_SECRET')
OPENAI_API_KEY = os.environ.get('OPENAI_API_KEY')
CREATOR_NAME = os.environ.get('CREATOR_NAME', 'Creator')

# Token storage - use Railway variable as primary, fallback to memory
RAILWAY_REFRESH_TOKEN = os.environ.get('FANVUE_REFRESH_TOKEN', '')

bot_status = {
    "started": datetime.now().isoformat(),
    "last_check": "never",
    "messages_found": 0,
    "replies_sent": 0,
    "errors": [],
    "paused": False,
    "blocked_users": set()
}

# In-memory token storage (lost on restart, but Railway variable is primary)
memory_tokens = {
    "refresh_token": RAILWAY_REFRESH_TOKEN,
    "access_token": None,
    "expires_at": None
}

def log(msg):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{timestamp}] {msg}"
    print(line, flush=True)
    bot_status["errors"].append(line)
    if len(bot_status["errors"]) > 100:
        bot_status["errors"] = bot_status["errors"][-100:]

def save_token(refresh_token, access_token=None, expires_at=None):
    """Save token to memory (Railway variable is set manually)"""
    memory_tokens["refresh_token"] = refresh_token
    memory_tokens["access_token"] = access_token
    memory_tokens["expires_at"] = expires_at
    log(f"Token saved to memory. Refresh token: {refresh_token[:30]}...")

def load_token():
    """Load token from memory or Railway variable"""
    if memory_tokens["refresh_token"]:
        return memory_tokens
    # Fallback to Railway variable
    if RAILWAY_REFRESH_TOKEN:
        memory_tokens["refresh_token"] = RAILWAY_REFRESH_TOKEN
        return memory_tokens
    return {}

def get_basic_auth_header():
    credentials = f"{FANVUE_CLIENT_ID}:{FANVUE_CLIENT_SECRET}"
    encoded = base64.b64encode(credentials.encode('utf-8')).decode('utf-8')
    return f"Basic {encoded}"

def refresh_fanvue_token():
    tokens = load_token()
    refresh_token = tokens.get('refresh_token')

    if not refresh_token:
        log("No refresh token found! Set FANVUE_REFRESH_TOKEN in Railway or use /set_token")
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

            # Save to memory
            save_token(new_refresh or refresh_token, access_token, expires_at)

            if new_refresh and new_refresh != refresh_token:
                log("WARNING: Token was rotated. Update FANVUE_REFRESH_TOKEN in Railway with:")
                log(new_refresh)

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
    # Ensure text is properly encoded
    safe_text = text.encode('utf-8').decode('utf-8')
    payload = {"text": safe_text}
    log(f"Sending message: '{safe_text[:50]}' to chat: {chat_id}")
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

def ask_openai(message, fan_name=""):
    url = "https://api.openai.com/v1/chat/completions"
    headers = {
        "Authorization": "Bearer " + OPENAI_API_KEY,
        "Content-Type": "application/json"
    }
    system = f"You are {CREATOR_NAME}. Reply in Hungarian but use ONLY normal letters (a-z, no accents). Keep under 30 words. Be sweet and casual. Example: 'szia' not 'szia', 'egeszsegedre' not 'egészségedre'"
    data = {
        "model": "gpt-4o-mini",
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": message}
        ],
        "max_tokens": 100
    }
    try:
        r = requests.post(url, headers=headers, json=data, timeout=15)
        log(f"OpenAI status: {r.status_code}")
        if r.status_code == 200:
            response_data = r.json()
            log(f"OpenAI response keys: {list(response_data.keys())}")
            if 'choices' in response_data and len(response_data['choices']) > 0:
                content = response_data['choices'][0]['message']['content']
                # Replace smart quotes with normal quotes
                content = content.replace(chr(8216), "'").replace(chr(8217), "'").replace(chr(8220), '"').replace(chr(8221), '"')
                # Keep Hungarian characters and basic punctuation
                allowed = set('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 .,!?;:-_áéíóöőúüűÁÉÍÓÖŐÚÜŰ')
                content = ''.join(char for char in content if char in allowed)
                log(f"OpenAI content: '{content}'")
                return content.strip() if content else "Szia! 😊"
            else:
                log(f"OpenAI no choices: {response_data}")
                return "Szia! 😊 Mi ujsag?"
        else:
            log(f"OpenAI error: {r.status_code} - {r.text[:200]}")
            return "Szia! 😊 Mi ujsag?"
    except Exception as e:
        log(f"OpenAI error: {e}")
        return "Szia! 😊 Mi ujsag?"

processed_messages = set()

# ========== MISSING FUNCTION ADDED HERE ==========
def is_message_processed(msg_id):
    """Check if a message has already been replied to"""
    return msg_id in processed_messages
# =================================================

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
            # Also try other possible fields
            if not chat_id:
                chat_id = chat.get('uuid')
            if not chat_id:
                chat_id = chat.get('id')
            log(f"Chat ID: {chat_id}, User: {user}")
            if not chat_id:
                continue

            messages = get_messages(chat_id)
            if not messages:
                continue

            last_msg = messages[-1]
            msg_id = last_msg.get('uuid')
            sender = last_msg.get('sender', {}) or {}

            if sender.get('uuid') != my_uuid and not is_message_processed(msg_id):
                # Double-check to prevent race conditions
                if is_message_processed(msg_id):
                    log(f"Message {msg_id} was processed by another thread, skipping")
                    continue
                fan_name = sender.get('displayName') or 'babe'
                text = last_msg.get('text') or ''

                if fan_name in bot_status["blocked_users"]:
                    processed_messages.add(msg_id)
                    continue

                log(f"NEW MSG from {fan_name}: {text[:50]}")
                bot_status["messages_found"] += 1

                # Add context about the fan for more personalized replies
                context = f"Fan name: {fan_name}. Their message: {text}"
                reply = ask_openai(context, fan_name)

                if reply and reply.strip():
                    if send_fanvue_message(chat_id, reply):
                        bot_status["replies_sent"] += 1
                        replied += 1
                        processed_messages.add(msg_id)
                        log(f"Replied: {reply[:50]}")
                else:
                    log("Empty reply, skipping")

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
    try:
        data = request.json
        if data and 'refresh_token' in data:
            save_token(data['refresh_token'])
            return {"status": "ok", "message": "Token saved to memory. ALSO add to Railway variables for persistence!"}
        return {"status": "error", "message": "No token provided"}
    except Exception as e:
        log(f"Set token error: {e}")
        return {"status": "error", "message": str(e)}

@app.route('/get_current_token')
def get_current_token():
    """Show current token so you can update Railway variable"""
    tokens = load_token()
    refresh = tokens.get('refresh_token', '')
    return {
        "refresh_token": refresh[:50] + "..." if len(refresh) > 50 else refresh,
        "note": "If token was rotated, copy this and update FANVUE_REFRESH_TOKEN in Railway"
    }

# Initialize
log("=" * 50)
log("API BOT STARTING")
log("=" * 50)
if RAILWAY_REFRESH_TOKEN:
    log("Refresh token loaded from Railway variable")
else:
    log("WARNING: No FANVUE_REFRESH_TOKEN in Railway variables!")

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
