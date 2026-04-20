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

print(f"FANVUE_TOKEN loaded: {FANVUE_TOKEN[:20]}..." if FANVUE_TOKEN else "FANVUE_TOKEN is EMPTY!")
print(f"KIMI_API_KEY loaded: {KIMI_API_KEY[:20]}..." if KIMI_API_KEY else "KIMI_API_KEY is EMPTY!")

processed_messages = set()
bot_start_time = datetime.now().isoformat()

def ask_kimi(message, fan_name, chat_history="", fan_known_name=""):
    url = "https://api.moonshot.cn/v1/chat/completions"
    headers = {
        "Authorization": "Bearer " + KIMI_API_KEY,
        "Content-Type": "application/json"
    }
    
    # Only use name if they told us their real name before
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
- Wait 2 minutes before replying (simulate thinking time)

GOAL:
Make them emotionally invested. They should feel like they're getting to know the real you. Build anticipation.

CONVERSATION STAGES:
- First few messages: Ask about them, be curious, warm
- After they open up: Be more personal, remember details
- When they're hooked: Slightly more flirty, mysterious hints

IMPORTANT: If they mention buying/tipping, thank them warmly but don't immediately push more. Let them want more.

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
        print(f"Kimi error: {e}")
        return "Szia! 😊 Mi ujsag?"

def send_fanvue(chat_id, text):
    url = "https://api.fanvue.com/v1/chats/" + chat_id + "/messages"
    headers = {
        "Authorization": "Bearer " + FANVUE_TOKEN,
        "Content-Type": "application/json"
    }
    try:
        r = requests.post(url, headers=headers, json={"text": text}, timeout=10)
        print(f"Sent to Fanvue: {r.status_code} - {r.text}")
    except Exception as e:
        print(f"Fanvue send error: {e}")

def get_chats():
    url = "https://api.fanvue.com/v1/chats"
    headers = {"Authorization": "Bearer " + FANVUE_TOKEN}
    try:
        print("Fetching chats...")
        r = requests.get(url, headers=headers, timeout=10)
        print(f"Chats response: {r.status_code}")
        if r.status_code != 200:
            print(f"Error body: {r.text}")
            return []
        data = r.json()
        print(f"Found {len(data.get('data', []))} chats")
        return data.get('data', [])
    except Exception as e:
        print(f"Get chats error: {e}")
        return []

def get_messages(chat_id):
    url = "https://api.fanvue.com/v1/chats/" + chat_id + "/messages"
    headers = {"Authorization": "Bearer " + FANVUE_TOKEN}
    try:
        r = requests.get(url, headers=headers, timeout=10)
        if r.status_code != 200:
            print(f"Messages error: {r.status_code} - {r.text}")
            return []
        return r.json().get('data', [])
    except Exception as e:
        print(f"Get messages error: {e}")
        return []

def extract_name(text):
    """Try to extract real name if fan introduces themselves"""
    # Simple extraction - can be improved later
    text_lower = text.lower()
    if "nevem" in text_lower or "hívnak" in text_lower or "a nevem" in text_lower:
        # Try to find name after these phrases
        import re
        match = re.search(r'(?:nevem|hívnak|a nevem)\s+(\w+)', text_lower)
        if match:
            return match.group(1).capitalize()
    return ""

def poll_for_messages():
    print("Starting poll thread...")
    fan_known_names = {}  # Store real names: {fan_id: name}
    
    while True:
        print(f"[{datetime.now()}] Checking for new messages...")
        chats = get_chats()
        
        if not chats:
            print("No chats found or error occurred")
        
        for chat in chats:
            chat_id = chat.get('id')
            if not chat_id:
                continue
            
            print(f"Checking chat: {chat_id}")
            messages = get_messages(chat_id)
            print(f"Found {len(messages)} messages in chat")
            
            for msg in messages:
                msg_id = msg.get('id')
                sender = msg.get('sender', {})
                msg_time = msg.get('createdAt', '')
                fan_id = sender.get('uuid', '')
                
                print(f"Message: id={msg_id}, type={sender.get('type')}, time={msg_time}")
                
                if (sender.get('type') == 'fan' and 
                    msg_id not in processed_messages and
                    msg_time > bot_start_time):
                    
                    fan_name = sender.get('displayName', 'babe')
                    text = msg.get('text', '')
                    
                    print(f"NEW MESSAGE from {fan_name}: {text}")
                    
                    # Check if they told us their real name
                    extracted = extract_name(text)
                    if extracted and fan_id not in fan_known_names:
                        fan_known_names[fan_id] = extracted
                        print(f"Learned real name: {extracted}")
                    
                    known_name = fan_known_names.get(fan_id, "")
                    
                    recent_msgs = messages[-5:] if len(messages) > 5 else messages
                    history = ""
                    for m in recent_msgs:
                        s = m.get('sender', {})
                        role = "Fan" if s.get('type') == 'fan' else "You"
                        history += role + ": " + m.get('text', '') + "\n"
                    
                    # Wait 2 minutes before replying
                    print("Waiting 2 minutes before replying...")
                    time.sleep(120)
                    
                    reply = ask_kimi(text, fan_name, history, known_name)
                    send_fanvue(chat_id, reply)
                    processed_messages.add(msg_id)
                    print(f"Replied: {reply}")
                else:
                    if msg_id in processed_messages:
                        print(f"Message {msg_id} already processed")
                    elif sender.get('type') != 'fan':
                        print(f"Message not from fan")
                    elif msg_time <= bot_start_time:
                        print(f"Message too old")
        
        print(f"Sleeping 2 minutes...")
        time.sleep(120)

@app.route('/')
def home():
    return "Fanvue Bot is running!"

@app.route('/webhook', methods=['POST'])
def webhook():
    return 'OK', 200

@app.route('/callback')
def callback():
    return "OAuth callback"

if __name__ == '__main__':
    print("=" * 50)
    print("FANVUE BOT STARTING")
    print("=" * 50)
    poll_thread = threading.Thread(target=poll_for_messages, daemon=True)
    poll_thread.start()
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
