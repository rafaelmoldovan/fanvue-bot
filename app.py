from flask import Flask, request
import requests
import os
import time
import threading
from datetime import datetime, timedelta

app = Flask(__name__)

FANVUE_TOKEN = os.environ.get('FANVUE_TOKEN')
KIMI_API_KEY = os.environ.get('KIMI_API_KEY')
CREATOR_NAME = os.environ.get('CREATOR_NAME', 'Creator')

# Global status
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

log("FANVUE_TOKEN: " + ("SET" if FANVUE_TOKEN else "EMPTY"))
log("KIMI_API_KEY: " + ("SET" if KIMI_API_KEY else "EMPTY"))

processed_messages = set()
pending_messages = {}  # msg_id -> {"time": datetime, "data": {...}}
bot_start_time = datetime.now().isoformat()

def get_headers():
    return {
        "Authorization": "Bearer " + FANVUE_TOKEN,
        "X-Fanvue-API-Version": "2025-06-26",
        "Content-Type": "application/json"
    }

def safe_str(value):
    return str(value) if value is not None else ""

def test_fanvue_connection():
    url = "https://api.fanvue.com/chats"
    try:
        r = requests.get(url, headers=get_headers(), timeout=10)
        log(f"Test connection status: {r.status_code}")
        return r.status_code, r.text
    except Exception as e:
        log(f"Test connection error: {e}")
        return 0, str(e)

def get_chats():
    url = "https://api.fanvue.com/chats"
    try:
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
        if r.status_code != 200:
            return []
        return r.json().get('data', [])
    except Exception as e:
        log(f"Get messages error: {e}")
        return []

def send_fanvue_message(chat_id, text):
    url = "https://api.fanvue.com/chat-messages"
    headers = get_headers()
    payload = {
        "recipientUuid": chat_id,
        "content": text
    }
    try:
        r = requests.post(url, headers=headers, json=payload, timeout=10)
        log(f"Send status: {r.status_code}")
        return r.status_code == 200
    except Exception as e:
        log(f"Send error: {e}")
        return False

def ask_kimi(message, fan_name, chat_history="", fan_known_name=""):
    url = "https://api.moonshot.cn/v1/chat/completions"
    headers = {
        "Authorization": "Bearer " + KIMI_API_KEY,
        "Content-Type": "application/json"
    }
    
    name_to_use = fan_known_name if fan_known_name else ""
    
    system = "You are " + CREATOR_NAME + ". Reply to fan messages naturally in Hungarian.\n\nPERSONALITY:\n- Act innocent, sweet, slightly shy but curious\n- Make them feel special and understood\n- Build emotional connection - make them fall in love or feel deeply attracted\n- Never use their username (like 'john123') - only use their real name if they told you before\n- Take your time - reply thoughtfully, not instantly\n\nRULES:\n- Chat naturally, ask questions about them\n- Show genuine interest in their life\n- Be flirty but subtle - let them chase you\n- NEVER upsell photos or PPV directly\n- You can hint at things vaguely but NEVER push sales\n- Keep responses 20-40 words\n- Use emojis sparingly (1-2 max)\n\nGOAL:\nMake them emotionally invested. They should feel like they're getting to know the real you. Build anticipation.\n\nFan name to use: " + name_to_use + "\nChat history: " + chat_history
    
    data = {
        "model": "kimi-k2.5",
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": message}
        ],
        "max_tokens": 100
    }
    
    try:
        r = requests.post(url, headers=headers, json=data, timeout=30)
        r.raise_for_status()
        return r.json()['choices'][0]['message']['content']
    except Exception as e:
        log(f"Kimi error: {e}")
        return "Szia! 😊 Mi ujsag?"

def extract_name(text):
    if not text:
        return ""
    text_lower = text.lower()
    if "nevem" in text_lower or "hívnak" in text_lower or "a nevem" in text_lower:
        import re
        match = re.search(r'(?:nevem|hívnak|a nevem)\s+(\w+)', text_lower)
        if match:
            return match.group(1).capitalize()
    return ""

def process_pending_messages():
    """Check pending messages and reply if 2 minutes passed"""
    now = datetime.now()
    replied = 0
    
    for msg_id, pending in list(pending_messages.items()):
        msg_time = pending["time"]
        if now - msg_time >= timedelta(minutes=2):
            # Time to reply!
            msg_data = pending["data"]
            chat_id = msg_data["chat_id"]
            fan_name = msg_data["fan_name"]
            text = msg_data["text"]
            history = msg_data["history"]
            known_name = msg_data["known_name"]
            
            log(f"Replying to pending message from {fan_name}")
            reply = ask_kimi(text, fan_name, history, known_name)
            send_fanvue_message(chat_id, reply)
            processed_messages.add(msg_id)
            del pending_messages[msg_id]
            replied += 1
            log(f"Replied: {reply[:50]}")
    
    return replied

def scan_for_new_messages():
    """Scan for new messages and add to pending"""
    chats = get_chats()
    if not chats:
        return 0
    
    found = 0
    fan_known_names = {}
    
    for chat in chats:
        user = chat.get('user', {}) or {}
        chat_id = user.get('uuid')
        if not chat_id:
            continue
        
        messages = get_messages(chat_id)
        
        for msg in messages:
            msg_id = msg.get('uuid')
            sender = msg.get('sender', {}) or {}
            fan_id = sender.get('uuid', '')
            
            my_uuid = '38a392fc-a751-49b3-9d74-01ac6447c490'
            is_fan = sender.get('uuid') != my_uuid
            is_new = msg_id not in processed_messages and msg_id not in pending_messages
            
            if is_fan and is_new:
                fan_name = sender.get('displayName') or 'babe'
                text = msg.get('text') or ''
                
                log(f"NEW MSG from {fan_name}: {text[:50]}")
                bot_status["messages_found"] += 1
                found += 1
                
                extracted = extract_name(text)
                if extracted and fan_id not in fan_known_names:
                    fan_known_names[fan_id] = extracted
                
                known_name = fan_known_names.get(fan_id, "")
                
                recent_msgs = messages[-5:] if len(messages) > 5 else messages
                history = ""
                for m in recent_msgs:
                    s = m.get('sender', {}) or {}
                    role = "Fan" if s.get('uuid') != my_uuid else "You"
                    m_text = s.get('text') or ''
                    history += role + ": " + m_text + "\n"
                
                # Add to pending with current time
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
                log(f"Added to pending (will reply in 2 min)")
    
    return found

def process_all_chats():
    """Main processing: scan new + reply to pending"""
    log("Processing chats...")
    bot_status["last_check"] = datetime.now().isoformat()
    
    # First, reply to any pending messages that are ready
    replied = process_pending_messages()
    
    # Then, scan for new messages
    found = scan_for_new_messages()
    
    total = replied + (1 if found > 0 else 0)
    return total

@app.route('/')
def home():
    pending_count = len(pending_messages)
    return f"Bot running! Pending replies: {pending_count}. Use /trigger to check, /status for info."

@app.route('/test')
def test_api():
    status, text = test_fanvue_connection()
    return {
        "status_code": status,
        "response_preview": text[:200],
        "token_prefix": FANVUE_TOKEN[:20] if FANVUE_TOKEN else "EMPTY"
    }

@app.route('/status')
def status():
    return {
        "started": bot_status["started"],
        "last_check": bot_status["last_check"],
        "messages_found": bot_status["messages_found"],
        "pending_replies": len(pending_messages),
        "recent_logs": bot_status["errors"][-20:]
    }

@app.route('/trigger')
def trigger_poll():
    try:
        count = process_all_chats()
        pending_count = len(pending_messages)
        return {
            "status": "ok",
            "processed": count,
            "pending_replies": pending_count,
            "message": f"{pending_count} messages waiting for 2-min delay"
        }
    except Exception as e:
        log(f"Trigger error: {e}")
        import traceback
        log(traceback.format_exc())
        return {"status": "error", "error": str(e)}

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
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
