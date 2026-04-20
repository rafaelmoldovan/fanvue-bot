from flask import Flask, request
import requests
import os
import base64
from datetime import datetime, timedelta

app = Flask(__name__)

FANVUE_CLIENT_ID = os.environ.get('FANVUE_CLIENT_ID')
FANVUE_CLIENT_SECRET = os.environ.get('FANVUE_CLIENT_SECRET')
FANVUE_REFRESH_TOKEN = os.environ.get('FANVUE_REFRESH_TOKEN')
KIMI_API_KEY = os.environ.get('KIMI_API_KEY')
CREATOR_NAME = os.environ.get('CREATOR_NAME', 'Creator')

fanvue_access_token = None
token_expires_at = None

bot_status = {
    "started": datetime.now().isoformat(),
    "last_check": "never",
    "messages_found": 0,
    "errors": []
}

def log(msg):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{timestamp}] {msg}"
    print(line, flush=True)
    bot_status["errors"].append(line)
    if len(bot_status["errors"]) > 100:
        bot_status["errors"] = bot_status["errors"][-100:]

log("FANVUE_REFRESH_TOKEN: " + ("SET" if FANVUE_REFRESH_TOKEN else "EMPTY"))
log("KIMI_API_KEY: " + ("SET" if KIMI_API_KEY else "EMPTY"))

processed_messages = set()
pending_messages = {}

def get_basic_auth_header():
    credentials = f"{FANVUE_CLIENT_ID}:{FANVUE_CLIENT_SECRET}"
    encoded = base64.b64encode(credentials.encode('utf-8')).decode('utf-8')
    return f"Basic {encoded}"

def refresh_fanvue_token():
    global fanvue_access_token, token_expires_at

    url = "https://auth.fanvue.com/oauth2/token"

    payload = {
        "grant_type": "refresh_token",
        "refresh_token": FANVUE_REFRESH_TOKEN
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
            fanvue_access_token = data.get('access_token')
            new_refresh = data.get('refresh_token')
            expires_in = data.get('expires_in', 3600)
            token_expires_at = datetime.now() + timedelta(seconds=expires_in - 60)
            log("Got new Fanvue access token via refresh")
            if new_refresh and new_refresh != FANVUE_REFRESH_TOKEN:
                log("Refresh token was rotated - update your Railway variable!")
                log(f"New refresh token: {new_refresh[:50]}...")
            return fanvue_access_token
        else:
            log(f"Refresh error: {r.status_code} - {r.text[:200]}")
            return None
    except Exception as e:
        log(f"Refresh exception: {e}")
        return None

def get_fanvue_token():
    if fanvue_access_token and token_expires_at and datetime.now() < token_expires_at:
        return fanvue_access_token
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
            global fanvue_access_token
            fanvue_access_token = None
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
            global fanvue_access_token
            fanvue_access_token = None
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
    payload = {
        "content": text
    }
    try:
        r = requests.post(url, headers=headers, json=payload, timeout=10)
        log(f"Send status: {r.status_code}")
        if r.status_code == 401:
            global fanvue_access_token
            fanvue_access_token = None
            headers["Authorization"] = "Bearer " + (get_fanvue_token() or "")
            r = requests.post(url, headers=headers, json=payload, timeout=10)
            log(f"Retry status: {r.status_code}")
        if r.status_code == 200 or r.status_code == 201:
            return True
        log(f"Send error body: {r.text[:200]}")
        return False
    except Exception as e:
        log(f"Send error: {e}")
        return False

def ask_kimi(message, fan_name, chat_history="", fan_known_name=""):
    url = "https://api.moonshot.ai/v1/chat/completions"
    headers = {
        "Authorization": "Bearer " + KIMI_API_KEY,
        "Content-Type": "application/json"
    }
    name_to_use = fan_known_name if fan_known_name else ""
    system = "You are " + CREATOR_NAME + ", a friendly creator. Reply in Hungarian. Keep it short (max 30 words). Be sweet and casual. Fan name: " + name_to_use
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
        log(f"Kimi status: {r.status_code}")
        if r.status_code == 200:
            response_data = r.json()
            log(f"Kimi response: {str(response_data)[:200]}")
            content = response_data['choices'][0]['message']['content']
            if content and content.strip():
                return content.strip()
            else:
                log("Kimi returned empty content")
                return "Szia! 😊 Mi ujsag?"
        else:
            log(f"Kimi error: {r.status_code} - {r.text[:200]}")
            return "Szia! 😊 Mi ujsag?"
    except Exception as e:
        log(f"Kimi exception: {e}")
        return "Szia! 😊 Mi ujsag?"

def process_pending_messages():
    now = datetime.now()
    replied = 0
    items = list(pending_messages.items())[:3]
    for msg_id, pending in items:
        msg_time = pending["time"]
        if now - msg_time >= timedelta(minutes=2):
            msg_data = pending["data"]
            chat_id = msg_data["chat_id"]
            fan_name = msg_data["fan_name"]
            text = msg_data["text"]
            history = msg_data["history"]
            known_name = msg_data["known_name"]
            log(f"Replying to {fan_name}")
            reply = ask_kimi(text, fan_name, history, known_name)
            if send_fanvue_message(chat_id, reply):
                processed_messages.add(msg_id)
                del pending_messages[msg_id]
                replied += 1
                log(f"Replied: {reply[:50]}")
            else:
                log("Send failed, will retry")
                break
    return replied

def scan_for_new_messages():
    chats = get_chats()
    if not chats:
        return 0
    found = 0
    fan_known_names = {}
    my_uuid = '38a392fc-a751-49b3-9d74-01ac6447c490'
    for chat in chats[-10:]:
        user = chat.get('user', {}) or {}
        chat_id = user.get('uuid')
        if not chat_id:
            continue
        messages = get_messages(chat_id)
        for msg in messages:
            msg_id = msg.get('uuid')
            sender = msg.get('sender', {}) or {}
            fan_id = sender.get('uuid', '')
            is_fan = sender.get('uuid') != my_uuid
            is_new = msg_id not in processed_messages and msg_id not in pending_messages
            if is_fan and is_new:
                fan_name = sender.get('displayName') or 'babe'
                text = msg.get('text') or ''
                log(f"NEW MSG from {fan_name}: {text[:50]}")
                bot_status["messages_found"] += 1
                found += 1
                text_lower = text.lower()
                if "nevem" in text_lower or "hívnak" in text_lower:
                    import re
                    match = re.search(r'(?:nevem|hívnak|a nevem)\s+(\w+)', text_lower)
                    if match:
                        fan_known_names[fan_id] = match.group(1).capitalize()
                known_name = fan_known_names.get(fan_id, "")
                recent_msgs = messages[-5:] if len(messages) > 5 else messages
                history = ""
                for m in recent_msgs:
                    s = m.get('sender', {}) or {}
                    role = "Fan" if s.get('uuid') != my_uuid else "You"
                    m_text = s.get('text') or ''
                    history += role + ": " + m_text + "\n"
                pending_messages[msg_id] = {
                    "time": datetime.now(),
                    "data": {
                        "chat_id": chat_id,
                        "fan_name": fan_name,
                        "text": text,
                        "history": history,
                        "known_name": known_name
                    }
                }
                log(f"Added to pending")
    return found

def process_all_chats():
    log("Processing...")
    bot_status["last_check"] = datetime.now().isoformat()
    replied = process_pending_messages()
    found = scan_for_new_messages()
    return replied + found

@app.route('/')
def home():
    return f"Bot running! Pending: {len(pending_messages)}. Use /trigger /clear /status"

@app.route('/status')
def status():
    return {
        "started": bot_status["started"],
        "last_check": bot_status["last_check"],
        "messages_found": bot_status["messages_found"],
        "pending_replies": len(pending_messages),
        "recent_logs": bot_status["errors"][-10:]
    }

@app.route('/trigger')
def trigger_poll():
    try:
        count = process_all_chats()
        return {
            "status": "ok",
            "processed": count,
            "pending": len(pending_messages)
        }
    except Exception as e:
        log(f"Trigger error: {e}")
        return {"status": "error", "error": str(e)}

@app.route('/clear')
def clear_pending():
    pending_messages.clear()
    processed_messages.clear()
    return {"status": "ok", "message": "Cleared"}

@app.route('/webhook', methods=['POST'])
def webhook():
    return 'OK', 200

@app.route('/callback')
def callback():
    return "OAuth callback"

if __name__ == '__main__':
    log("=" * 50)
    log("BOT STARTING")
    log("=" * 50)
    pending_messages.clear()
    processed_messages.clear()
    log("Cleared old messages on startup")
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
