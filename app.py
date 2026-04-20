from flask import Flask, request
import os
import time
import requests
from datetime import datetime
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
    "blocked_users": set(),
    "logged_in": False
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

class FanvueBot:
    def __init__(self):
        self.browser = None
        self.context = None
        self.page = None
        self.last_messages = {}

    def start(self):
        try:
            log("Starting browser...")
            self.playwright = sync_playwright().start()
            self.browser = self.playwright.chromium.launch(
                headless=True,
                args=[
                    '--no-sandbox',
                    '--disable-setuid-sandbox',
                    '--disable-dev-shm-usage',
                    '--disable-gpu',
                    '--no-first-run',
                    '--no-zygote',
                    '--single-process'
                ]
            )
            self.context = self.browser.new_context(
                viewport={'width': 1280, 'height': 720},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            )
            self.page = self.context.new_page()
            log("Browser started successfully")
            return True
        except Exception as e:
            log(f"Browser start error: {e}")
            return False

    def login(self):
        try:
            log("Logging into Fanvue...")
            self.page.goto("https://fanvue.com/login", wait_until="domcontentloaded")
            time.sleep(3)

            # Fill email
            email_input = self.page.query_selector('input[type="email"]')
            if not email_input:
                email_input = self.page.query_selector('input[name="email"]')
            if not email_input:
                email_input = self.page.query_selector('input[placeholder*="mail"]')

            if email_input:
                email_input.fill(FANVUE_EMAIL)
                log("Email filled")
            else:
                log("Email input not found")
                return False

            time.sleep(1)

            # Fill password
            pass_input = self.page.query_selector('input[type="password"]')
            if pass_input:
                pass_input.fill(FANVUE_PASSWORD)
                log("Password filled")
            else:
                log("Password input not found")
                return False

            time.sleep(1)

            # Click login button
            login_btn = self.page.query_selector('button[type="submit"]')
            if login_btn:
                login_btn.click()
                log("Login button clicked")
            else:
                log("Login button not found")
                return False

            time.sleep(5)

            # Check current URL
            current_url = self.page.url
            log(f"Current URL: {current_url}")

            if "login" not in current_url.lower():
                log("Login successful!")
                bot_status["logged_in"] = True
                return True
            else:
                log("Login failed - still on login page")
                # Take screenshot for debugging
                try:
                    self.page.screenshot(path="/tmp/login_error.png")
                    log("Screenshot saved to /tmp/login_error.png")
                except:
                    pass
                return False

        except Exception as e:
            log(f"Login error: {e}")
            return False

    def get_messages(self):
        try:
            log("Navigating to messages...")
            self.page.goto("https://fanvue.com/messages", wait_until="domcontentloaded")
            time.sleep(5)

            # Wait for chat list
            self.page.wait_for_selector('a[href*="/messages/"]', timeout=15000)

            # Get all chat links
            chat_links = self.page.query_selector_all('a[href*="/messages/"]')
            log(f"Found {len(chat_links)} chats")

            new_messages = []

            for i, link in enumerate(chat_links[:5]):  # Check first 5 chats
                try:
                    # Get fan name
                    name_elem = link.query_selector('h3, h4, span, p')
                    fan_name = name_elem.inner_text() if name_elem else f"User_{i}"

                    # Check for unread indicator (usually a dot or number)
                    has_unread = False

                    # Look for unread badge
                    unread_elem = link.query_selector('[class*="unread"], [class*="badge"], [class*="dot"]')
                    if unread_elem:
                        has_unread = True

                    # Or check if there's a time indicator that's recent
                    time_elem = link.query_selector('time')
                    if time_elem:
                        # If time says "now" or few minutes ago, likely unread
                        time_text = time_elem.inner_text()
                        if any(word in time_text.lower() for word in ['now', 'min', '1m', '2m', '3m', '4m', '5m']):
                            has_unread = True

                    # Click to open chat
                    link.click()
                    time.sleep(3)

                    # Get messages in chat
                    msg_bubbles = self.page.query_selector_all('[class*="message"]')

                    if msg_bubbles:
                        # Get last message
                        last_msg = msg_bubbles[-1]
                        text = last_msg.inner_text()

                        # Check if it's from fan (not from me)
                        # Usually fan messages are on left, mine on right
                        # Or check for specific class
                        msg_classes = last_msg.get_attribute("class") or ""
                        is_from_me = "sent" in msg_classes.lower() or "me" in msg_classes.lower() or "right" in msg_classes.lower()

                        if text and not is_from_me:
                            msg_id = f"{fan_name}_{hash(text)}"
                            if msg_id not in self.last_messages:
                                self.last_messages[msg_id] = True
                                new_messages.append({
                                    "fan_name": fan_name,
                                    "text": text
                                })
                                log(f"New message from {fan_name}: {text[:50]}")

                    # Go back to messages list
                    self.page.goto("https://fanvue.com/messages", wait_until="domcontentloaded")
                    time.sleep(3)

                except Exception as e:
                    log(f"Chat parse error: {e}")
                    continue

            return new_messages

        except Exception as e:
            log(f"Get messages error: {e}")
            return []

    def send_reply(self, text):
        try:
            # Find message input
            input_selectors = [
                'textarea[placeholder*="message"]',
                'textarea[placeholder*="Message"]',
                'input[placeholder*="message"]',
                'div[contenteditable="true"]',
                'textarea'
            ]

            input_box = None
            for sel in input_selectors:
                input_box = self.page.query_selector(sel)
                if input_box:
                    log(f"Found input with selector: {sel}")
                    break

            if input_box:
                input_box.fill(text)
                time.sleep(1)

                # Press Enter to send
                self.page.keyboard.press("Enter")
                time.sleep(2)

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

        if not bot_status["logged_in"]:
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

            # Send reply
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
        "logged_in": bot_status["logged_in"],
        "blocked_users": list(bot_status["blocked_users"]),
        "recent_logs": bot_status["errors"][-10:]
    }

@app.route('/trigger')
def trigger():
    try:
        if not bot.browser:
            if not bot.start():
                return {"status": "error", "error": "Could not start browser"}

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
