from flask import Flask, request
import requests
import os
import time
import threading

app = Flask(__name__)

FANVUE_TOKEN = os.environ.get('FANVUE_TOKEN')
KIMI_API_KEY = os.environ.get('KIMI_API_KEY')
CREATOR_NAME = os.environ.get('CREATOR_NAME', 'Creator')

processed_messages = set()

def ask_kimi(message, fan_name, chat_history=""):
    url = "https://api.moonshot.cn/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {KIMI_API_KEY}",
        "Content-Type": "application/json"
    }
    
    system = "You are " + CREATOR_NAME + ". Reply to fan messages naturally in Hungarian. Fan name: " + fan_name + ". Keep it under 40 words. Flirty but exclusive. Remember details they share. Guide toward PPV sales gently. Chat history: " + chat_history
    
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
        return "Szia " + fan_name + "! Mi ujsag?"

def send_fanvue(chat_id, text):
    url = "https://api.fanvue.com/v1/chats/" + chat_id + "/messages"
    headers = {
        "Authorization": "Bearer " + FANVUE_TOKEN,
        "Content-Type": "application/json"
    }
    try:
        r = requests.post(url, headers=headers, json={"text": text}, timeout=10)
        print(f"Sent to Fanvue: {r.status_code}")
    except Exception as e:
        print(f"Fanvue error: {e}")

def get_chats():
    url = "https://api.fanvue.com/v1/chats"
    headers = {"Authorization": "Bearer " + FANVUE_TOKEN}
    try:
        r = requests.get(url, headers=headers, timeout=10)
        r.raise_for_status()
        return r.json().get('data', [])
    except Exception as e:
        print(f"Get chats error: {e}")
        return []

def get_messages(chat_id):
    url = "https://api.fanvue.com/v1/chats/" + chat_id + "/messages"
    headers = {"Authorization": "Bearer " + FANVUE_TOKEN}
    try:
        r = requests.get(url, headers=headers, timeout=10)
        r.raise_for_status()
        return r.json().get('data', [])
    except Exception as e:
        print(f"Get messages error: {e}")
        return []

def poll_for_messages():
    while True:
        print("Checking for new messages...")
        chats = get_chats()
        
        for chat in chats:
            chat_id = chat.get('id')
            if not chat_id:
                continue
                
            messages = get_messages(chat_id)
            
            for msg in messages:
                msg_id = msg.get('id')
                sender = msg.get('sender', {})
                
                if sender.get('type') == 'fan' and msg_id not in processed_messages:
                    fan_name = sender.get('displayName', 'babe')
                    text = msg.get('text', '')
                    
                    print(f"New message from {fan_name}: {text}")
                    
                    recent_msgs = messages[-5:] if len(messages) > 5 else messages
                    history = ""
                    for m in recent_msgs:
                        s = m.get('sender', {})
                        role = "Fan" if s.get('type') == 'fan' else "You"
                        history += role + ": " + m.get('text', '') + "\n"
                    
                    reply = ask_kimi(text, fan_name, history)
                    send_fanvue(chat_id, reply)
                    processed_messages.add(msg_id)
                    print(f"Replied: {reply}")
        
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
    poll_thread = threading.Thread(target=poll_for_messages, daemon=True)
    poll_thread.start()
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
