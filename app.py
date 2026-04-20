from flask import Flask, request
import requests
import os

app = Flask(__name__)

FANVUE_TOKEN = os.environ.get('FANVUE_TOKEN')
KIMI_API_KEY = os.environ.get('KIMI_API_KEY')
CREATOR_NAME = os.environ.get('CREATOR_NAME', 'Creator')

@app.route('/webhook', methods=['POST'])
def handle_fanvue():
    data = request.json
    
    if data.get('type') == 'message.created':
        msg = data['data']
        fan_text = msg['text']
        chat_id = msg['chatId']
        fan_name = msg['sender'].get('displayName', 'babe')
        
        reply = ask_kimi(fan_text, fan_name)
        send_fanvue(chat_id, reply)
    
    return 'OK', 200

def ask_kimi(message, fan_name):
    url = "https://api.moonshot.cn/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {KIMI_API_KEY}",
        "Content-Type": "application/json"
    }
    
    system = f"""You are {CREATOR_NAME}. Reply to fan messages naturally.
    Fan name: {fan_name}. Keep it under 40 words. Flirty but exclusive.
    Remember details they share. Guide toward PPV sales gently."""
    
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
        return f"Hey {fan_name}! 😘 What's up?"

def send_fanvue(chat_id, text):
    url = f"https://api.fanvue.com/v1/chats/{chat_id}/messages"
    headers = {
        "Authorization": f"Bearer {FANVUE_TOKEN}",
        "Content-Type": "application/json"
    }
    try:
        requests.post(url, headers=headers, json={"text": text}, timeout=10)
    except Exception as e:
        print(f"Fanvue error: {e}")

@app.route('/')
def home():
    return "Fanvue Bot Running!"

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
