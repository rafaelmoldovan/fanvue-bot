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
    "errors": [],
    "debug_info": {}
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

def test_fanvue_connection():
    """Test if Fanvue API is working"""
    url = "https://api.fanvue.com/v1/chats"
    headers = {"Authorization": "Bearer " + FANVUE_TOKEN}
    try:
        r = requests.get(url, headers=headers, timeout=10)
        log(f"Test connection status: {r.status_code}")
        log(f"Response preview: {r.text[:200]}")
        return r.status_code, r.text
    except Exception as e:
        log(f"Test connection error: {e}")
        return 0, str(e)

def get_chats():
    url = "https://api.fanvue.com/v1/chats"
    headers = {"Authorization": "Bearer " + FANVUE_TOKEN}
    try:
        r = requests.get(url, headers=headers, timeout=10)
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
    url = f"https://api.fanvue.com/v1/chats/{chat_id}/messages"
    headers = {"Authorization": "Bearer " + FANVUE_TOKEN}
    try:
        r = requests.get(url, headers=headers, timeout=10)
        if r.status_code != 200:
            log(f"Get messages error: {r.status_code}")
            return []
        return r.json().get('data', [])
    except Exception as e:
        log(f"Get messages exception: {e}")
        return []

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
        "recent_logs": bot_status["errors"][-20:],
        "debug": bot_status.get("debug_info", {})
    }

@app.route('/trigger')
def trigger_poll():
    """Check for new messages"""
    try:
        log("Trigger started")
        bot_status["last_check"] = datetime.now().isoformat()
        
        chats = get_chats()
        if not chats:
            log("No chats found")
            return {"status": "ok", "messages_processed": 0, "chats_found": 0}
        
        messages_processed = 0
        for chat in chats:
            chat_id = chat.get('id')
            if not chat_id:
                continue
            
            messages = get_messages(chat_id)
            log(f"Chat {str(chat_id)[:8]}: {len(messages)} messages")
            
            for msg in messages:
                msg_id = msg.get('id')
                sender = msg.get('sender', {})
                msg_time = msg.get('createdAt', '')
                msg_text = msg.get('text', '')
                
                log(f"Msg: id={str(msg_id)[:8]}, type={sender.get('type')}, time={msg_time}, text={msg_text[:30]}")
                
                is_fan = sender.get('type') == 'fan'
                is_new = msg_id not in processed_messages
                is_after_start = msg_time > bot_start_time
                
                log(f"Checks: fan={is_fan}, new={is_new}, after_start={is_after_start}")
                
                if is_fan and is_new and is_after_start:
                    log(f"NEW MESSAGE: {msg_text[:50]}")
                    bot_status["messages_found"] += 1
                    messages_processed += 1
                    
                    # Simple test reply
                    reply = "Szia! 😊 Koszi az uzenetet!"
                    processed_messages.add(msg_id)
                    log(f"Would reply: {reply}")
        
        return {
            "status": "ok",
            "messages_processed": messages_processed,
            "chats_found": len(chats),
            "bot_start_time": bot_start_time
        }
        
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
