from flask import Flask, request
import requests
import os
import time
import threading

app = Flask(__name__)

FANVUE_TOKEN = os.environ.get('FANVUE_TOKEN')
KIMI_API_KEY = os.environ.get('KIMI_API_KEY')
CREATOR_NAME = os.environ.get('CREATOR_NAME', 'Creator')

@app.route('/')
def home():
    return "Fanvue Bot is running!"

@app.route('/webhook', methods=['POST'])
def webhook():
    return 'OK', 200

@app.route('/callback')
def callback():
    code = request.args.get('code')
    if code:
        return f"Authorization code received: {code[:10]}... (check logs)"
    return "Callback received"

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
