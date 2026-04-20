from flask import Flask, request
import requests
import os
import time
import threading
from datetime import datetime

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
bot_start_time = datetime.now().isoformat()

def get_headers():
    return {
        "Authorization": "Bearer " + FANVUE_TOKEN,
        "X-Fanvue-API-Version": "2025-06-26",
        "Content-Type": "application/json"
    }

def test_fanvue_connection():
    """Test if Fanvue API is working"""
    url = "https://api.fanvue.com/chats"
    try:
        r = requests.get(url, headers=get_headers(), timeout=10)
        log(f"Test connection status: {r.status_code}")
        log(f"Response preview: {r.text[:200]}")
        return r.status_code, r.text
    except Exception as e:
        log(f"Test connection error: {e}")
        return 0, str(e)

def get_chats():
    url = "https://api.fanvue.com/chats"
    try:
        r = requests.get(url, headers=get_headers(), timeout=10)
        log(f"Get chats status: {r.status_code}")
        if r.status_code != 200:
            log(f"Error: {r.text[:200]}")
            return []
        data = r.json()
        chats = data.get('data', [])
        log(f"Found {len(chats)} chats")
        return chats
    except Exception as e:
        log(f"Get chats exception: {e}")
        return []

def get_messages(chat_id):
    url = f"https://api.fanvue.com/chats/{chat_id}/messages"
    try:
        r = requests.get(url, headers=get_headers(), timeout=10)
        if r.status_code != 200:
            log(f"Get messages error: {r.status_code} - {r.text[:200]}")
            return []
        data = r.json()
        msgs = data.get('data', [])
        log(f"Got {len(msgs)} messages")
        return msgs
    except Exception as e:
        log(f"Get messages exception: {e}")
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
        log(f"Send message status: {r.status_code}")
        return r.status_code == 200
    except Exception as e:
        log(f"Send message error: {e}")
        return False

def ask_kimi(message, fan_name, chat_history="", fan_known_name=""):
    url = "https://api.moonshot.cn/v1/chat/completions"
    headers = {
        "Authorization": "Bearer " + KIMI_API_KEY,
        "Content-Type": "application/json"
    }
    
    name_to_use = fan_known_name if fan_known_name else ""
    
    system = """You are """ + CREATOR_NAME + """. Reply to fan messages naturally in Hungarian.

PERSONALITY:
- Act innocent, sweet, slightly shy but curious
- Make them feel special and understood
- Build emotional connection - make them fall in love or feel deeply attracted
- Never use their username (like 'john123') - only use their real name if they told you before
- Take your time - reply thoughtfully, not instantly

RULES:
- Chat naturally, ask questions about them
- Show genuine interest in their life
- Be flirty but subtle - let them chase you
- NEVER upsell photos or PPV directly
- You can hint at things vaguely ("I have something special planned..." or "maybe I'll show you later") but NEVER push sales
- Keep responses 20-40 words
- Use emojis sparingly (1-2 max)
- Wait 2 minutes for reply (simulate thinking time)

GOAL:
Make them emotionally invested. They should feel like they're getting to know the real you. Build anticipation.

Fan name to use: """ + name_to_use + """
Chat history: """ + chat_history
    
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
    text_lower = text.lower()
    if "nevem" in text_lower or "hívnak" in text_lower or "a nevem" in text_lower:
        import re
        match = re.search(r'(?:nevem|hívnak|a nevem)\s+(\w+)', text_lower)
        if match:
            return match.group(1).capitalize()
    return ""

def process_all_chats():
    """Process all chats and return results"""
    log("Processing chats...")
    bot_status["last_check"] = datetime.now().isoformat()
    chats = get_chats()
    
    if not chats:
        log("No chats found")
        return 0
    
    messages_processed = 0
    fan_known_names = {}
    
    for chat in chats:
        chat_id = chat.get('user', {}).get('uuid')
        if not chat_id:
            continue
        
        messages = get_messages(chat_id)
        log(f"Chat {str(chat_id)[:8]}: {len(messages)} messages")
        
        for msg in messages:
            msg_id = msg.get('uuid')
            sender = msg.get('sender', {})
            msg_time = msg.get('sentAt', '')
            fan_id = sender.get('uuid', '')
            
            # Skip messages from me (creator)
            is_fan = sender.get('uuid') != '38a392fc-a751-49b3-9d74-01ac6447c490'
            
            is_new = msg_id not in processed_messages
            
            # Parse times for comparison
            try:
                msg_dt = datetime.fromisoformat(msg_time.replace('Z', '+00:00'))
                bot_dt = datetime.fromisoformat(bot_start_time.replace('Z', '+00:00'))
                is_after_start = msg_dt > bot_dt
            except:
                is_after_start = True
            
            log(f"Msg: id={str(msg_id)[:8]}, fan={is_fan}, new={is_new}, after={is_after_start}, time={msg_time}")
            
            if is_fan and is_new:
                fan_name = sender.get('displayName', 'babe')
                text = msg.get('text', '')
                
                log(f"NEW MSG from {fan_name}: {text[:50]}")
                bot_status["messages_found"] += 1
                messages_processed += 1
                
                extracted = extract_name(text)
                if extracted and fan_id not in fan_known_names:
                    fan_known_names[fan_id] = extracted
                    log(f"Learned name: {extracted}")
                
                known_name = fan_known_names.get(fan_id, "")
                
                recent_msgs = messages[-5:] if len(messages) > 5 else messages
                history = ""
                for m in recent_msgs:
                    s = m.get('sender', {})
                    role = "Fan" if s.get('uuid') != '38a392fc-a751-49b3-9d74-01ac6447c490' else "You"
                    history += role + ": " + m.get('text', '') + "\n"
                
                log("Waiting 2 min...")
                time.sleep(120)
                
                reply = ask_kimi(text, fan_name, history, known_name)
                send_fanvue_message(chat_id, reply)
                processed_messages.add(msg_id)
                log(f"Replied: {reply[:50]}")
    
    return messages_processed

@app.route('/')
def home():
    return "Bot running! Use /test to test API, /trigger to check messages, /status for info."

@app.route('/test')
def test_api():
    """Test Fanvue API connection"""
    status, text = test_fanvue_connection()
    return {
        "status_code": status,
        "response_preview": text[:500],
        "token_prefix": FANVUE_TOKEN[:20] if FANVUE_TOKEN else "EMPTY"
    }

@app.route('/status')
def status():
    return {
        "started": bot_status["started"],
        "last_check": bot_status["last_check"],
        "messages_found": bot_status["messages_found"],
        "recent_logs": bot_status["errors"][-20:]
    }

@app.route('/trigger')
def trigger_poll():
    """Check for new messages"""
    try:
        count = process_all_chats()
        return {"status": "ok", "messages_processed": count}
    except Exception as e:
        log(f"Trigger error: {e}")
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
