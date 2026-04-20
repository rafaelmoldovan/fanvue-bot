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
    
    system = f"""You are {CREATOR_NAME}. Reply to fan messages naturally in Hungarian.
    Fan name: {fan_name}. Keep it under 40 words. Flirty but exclusive.
    Remember details they share. Guide toward PPV sales gently.
    
    Chat history: {chat_history}"""
    
    data = {
        "model": "kimi-k2.5",
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": message}
        ],
        "max_tokens": 100
