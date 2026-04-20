from flask import Flask, request
import os
import json
import time
import requests
from datetime import datetime, timedelta
from playwright.sync_api import sync_playwright

app = Flask(__name__)

# Config
FANVUE_EMAIL = os.environ.get('FANVUE_EMAIL')
FANVUE_PASSWORD = os.environ.get('FANVUE_PASSWORD')
KIMI_API_KEY = os.environ.get('KIMI_API_KEY')
CREATOR_NAME = os.environ.get('CREATOR_NAME', 'Creator')

bot_status = {
    "started": datetime.now().isoformat(),
    "last_check": "never",
    "messages_found": 0,
    "replies_sent": 0,
    "errors": [],
    "paused": False,
    "blocked_users": set()
}

def log(msg):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{timestamp}] {msg}"
    print(line, flush=True)
    bot_status["errors"].append(line)
    if len(bot_status["errors"]) > 100:
        bot_status["errors"] = bot_status["errors"][-100:]

log("BOT STARTING")
log(f"CREATOR_NAME: {CREATOR_NAME}")
log(f"FANVUE_EMAIL: {'SET' if FANVUE_EMAIL else 'EMPTY'}")
log(f"KIMI_API_KEY: {'SET' if KIMI_API_KEY else 'EMPTY'}")

class FanvueBot:
    def __init__(self):
        self.browser = None
        self.context = None
        self.page = None
        self.logged_in = False
        self.last_messages = {}

    def start(self):
        try:
            self.playwright = sync_playwright().start()
            self.browser = self.playwright.chromium.launch(
                headless=True,
                args=['--no-sandbox', '--disable-setuid-sandbox']
            )
            self.context = self.browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            )
            self.page = self.context.new_page()
            log("Browser started")
            return True
        except Exception as e:
            log(f"Browser start error: {e}")
            return False

    def login(self):
        try:
            log("Logging into Fanvue...")
            self.page.goto("https://fanvue.com/login", wait_until="networkidle")
            time.sleep(2)

            # Fill email
            self.page.fill('input[type="email"]', FANVUE_EMAIL)
            time.sleep(0.5)

            # Fill password
            self.page.fill('input[type="password"]', FANVUE_PASSWORD)
            time.sleep(0.5)

            # Click login
            self.page.click('button[type="submit"]')
            time.sleep(5)

            # Check if logged in
            current_url = self.page.url
            log(f"Current URL after login: {current_url}")

            if "login" not in current_url:
                log("Login successful!")
                self.logged_in = True
                return True
            else:
                log("Login failed - still on login page")
                # Take screenshot for debugging
                self.page.screenshot(path="/tmp/login_error.png")
                return False
        except Exception as e:
            log(f"Login error: {e}")
            return False

    def get_messages(self):
        try:
            self.page.goto("https://fanvue.com/messages", wait_until="networkidle")
            time.sleep(3)

            # Look for chat items - try different selectors
            selectors = [
                '[data-testid="chat-list-item"]',
                '.chat-list-item',
                '[class*="chat"]',
                'a[href*="/messages/"]'
            ]

            chats = []
            for selector in selectors:
                chats = self.page.query_selector_all(selector)
                if chats:
                    log(f"Found {len(chats)} chats with selector: {selector}")
                    break

            if not chats:
                log("No chats found")
                return []

            new_messages = []
            for i, chat in enumerate(chats[:10]):  # Check first 10 chats
                try:
                    # Get fan name
                    name_selectors = ['[data-testid="chat-name"]', '.chat-name', 'h3', 'h4', 'span']
                    fan_name = None
                    for sel in name_selectors:
                        elem = chat.query_selector(sel)
                        if elem:
                            fan_name = elem.inner_text()
                            break

                    if not fan_name:
                        fan_name = f"User_{i}"

                    # Check for unread indicator
                    has_unread = False
                    unread_selectors = ['[data-testid="unread-badge"]', '.unread', '[class*="unread"]']
                    for sel in unread_selectors:
                        if chat.query_selector(sel):
                            has_unread = True
                            break

                    # Click to open chat
                    chat.click()
                    time.sleep(2)

                    # Get messages in chat
                    msg_selectors = [
                        '[data-testid="message-bubble"]',
                        '.message-bubble',
                        '[class*="message"]'
                    ]

                    messages = []
                    for sel in msg_selectors:
                        messages = self.page.query_selector_all(sel)
                        if messages:
                            break

                    # Get last few messages
                    for msg in messages[-3:]:
                        try:
                            text = msg.inner_text()
                            # Check if it's from fan (not from me)
                            is_me = msg.get_attribute("data-from-me") or "me" in str(msg.get_attribute("class"))

                            if text and not is_me:
                                msg_id = f"{fan_name}_{hash(text)}"
                                if msg_id not in self.last_messages:
                                    self.last_messages[msg_id] = True
                                    new_messages.append({
                                        "fan_name": fan_name,
                                        "text": text,
                                        "chat_index": i
                                    })
                                    log(f"New message from {fan_name}: {text[:50]}")
                        except:
                            continue

                    # Go back
                    self.page.goto("https://fanvue.com/messages", wait_until="networkidle")
                    time.sleep(2)

                except Exception as e:
                    log(f"Chat parse error: {e}")
                    continue

            return new_messages

        except Exception as e:
            log(f"Get messages error: {e}")
            return []

    def send_reply(self, text):
        try:
            # Find input
            input_selectors = [
                '[data-testid="message-input"]',
                'textarea[placeholder*="message"]',
                'textarea[placeholder*="Message"]',
                'input[type="text"]'
            ]

            input_box = None
            for sel in input_selectors:
                input_box = self.page.query_selector(sel)
                if input_box:
                    break

            if input_box:
                input_box.fill(text)
                time.sleep(0.5)

                # Press Enter
                self.page.keyboard.press("Enter")
                time.sleep(1)

                log(f"Sent reply: {text[:50]}")
                return True
            else:
                log("Could not find message input")
                return False

        except Exception as e:
            log(f"Send error: {e}")
            return False

    def ask_kimi(self, message, fan_name):
        url = "https://api.moonshot.ai/v1/chat/completions"
        headers = {
            "Authorization": "Bearer " + KIMI_API_KEY,
            "Content-Type": "application/json"
        }

        system = f"You are {CREATOR_NAME}. Reply in Hungarian. Keep under 30 words. Be sweet, casual, slightly flirty. Fan name: {fan_name}"

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
                response = r.json()
                content = response['choices'][0]['message']['content']
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

    def process_messages(self):
        if bot_status["paused"]:
            log("Bot is paused")
            return 0

        if not self.logged_in:
            if not self.login():
                return 0

        new_msgs = self.get_messages()
        replied = 0

        for msg in new_msgs:
            fan_name = msg["fan_name"]
            text = msg["text"]

            if fan_name in bot_status["blocked_users"]:
                log(f"Skipping blocked user: {fan_name}")
                continue

            log(f"Processing message from {fan_name}: {text[:50]}")
            bot_status["messages_found"] += 1

            # Generate reply
            reply = self.ask_kimi(text, fan_name)

            # Click on chat again
            chats = self.page.query_selector_all('a[href*="/messages/"]')
            if msg["chat_index"] < len(chats):
                chats[msg["chat_index"]].click()
                time.sleep(2)

            if self.send_reply(reply):
                bot_status["replies_sent"] += 1
                replied += 1

            time.sleep(3)  # Delay between replies

        return replied

    def close(self):
        if self.browser:
            self.browser.close()
        if hasattr(self, 'playwright') and self.playwright:
            self.playwright.stop()

# Global bot instance
bot = FanvueBot()

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
        if not bot.browser:
            if not bot.start():
                return {"status": "error", "error": "Could not start browser"}
            bot.login()

        bot_status["last_check"] = datetime.now().isoformat()
        count = bot.process_messages()

        return {
            "status": "ok",
            "replied": count,
            "total_replies": bot_status["replies_sent"]
        }
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
    return {"status": "error", "message": "No user specified"}

@app.route('/unblock')
def unblock_user():
    user = request.args.get('user')
    if user:
        bot_status["blocked_users"].discard(user)
        return {"status": "unblocked", "user": user}
    return {"status": "error", "message": "No user specified"}

if __name__ == '__main__':
    log("=" * 50)
    log("PLAYWRIGHT BOT STARTING")
    log("=" * 50)
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
