from flask import Flask, request
import os
import time
import requests
from datetime import datetime
from playwright.sync_api import sync_playwright

app = Flask(__name__)

FANVUE_EMAIL = os.environ.get("FANVUE_EMAIL")
FANVUE_PASSWORD = os.environ.get("FANVUE_PASSWORD")
KIMI_API_KEY = os.environ.get("KIMI_API_KEY")
CREATOR_NAME = os.environ.get("CREATOR_NAME", "Creator")

bot_status = {
    "started": datetime.now().isoformat(),
    "last_check": "never",
    "messages_found": 0,
    "replies_sent": 0,
    "errors": [],
    "paused": False,
    "logged_in": False
}

def log(msg):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{timestamp}] {msg}"
    print(line, flush=True)
    bot_status["errors"].append(line)
    if len(bot_status["errors"]) > 100:
        bot_status["errors"] = bot_status["errors"][-100:]

class FanvueBot:
    def __init__(self):
        self.browser = None
        self.page = None
        self.last_messages = {}

    def start(self):
        try:
            log("Starting browser...")
            self.playwright = sync_playwright().start()
            self.browser = self.playwright.chromium.launch(
                headless=True,
                args=[
                    "--no-sandbox",
                    "--disable-setuid-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-gpu",
                    "--no-first-run",
                    "--no-zygote",
                    "--single-process",
                    "--disable-blink-features=AutomationControlled"
                ]
            )
            self.context = self.browser.new_context(
                viewport={"width": 1280, "height": 720},
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
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
            self.page.goto("https://www.fanvue.com/signin", wait_until="networkidle")
            time.sleep(5)

            # Use JavaScript to find and fill inputs
            log("Filling login form...")

            result = self.page.evaluate("""
                () => {
                    const inputs = document.querySelectorAll('input');
                    let emailInput = null;
                    let passInput = null;

                    for (let input of inputs) {
                        const type = input.type || '';
                        const name = input.name || '';
                        const placeholder = input.placeholder || '';
                        const ariaLabel = input.getAttribute('aria-label') || '';

                        if (type === 'email' || name === 'email' || 
                            placeholder.toLowerCase().includes('email') ||
                            ariaLabel.toLowerCase().includes('email')) {
                            emailInput = input;
                        }
                        if (type === 'password' || name === 'password' || 
                            placeholder.toLowerCase().includes('password') ||
                            ariaLabel.toLowerCase().includes('password')) {
                            passInput = input;
                        }
                    }

                    return {
                        emailFound: !!emailInput,
                        passFound: !!passInput,
                        totalInputs: inputs.length
                    };
                }
            """)

            log(f"Inputs found: {result}")

            if not result.get('emailFound') or not result.get('passFound'):
                log("Could not find login inputs")
                return False

            # Fill email using JavaScript
            self.page.evaluate(f"""
                () => {{
                    const inputs = document.querySelectorAll('input');
                    let emailInput = null;
                    for (let input of inputs) {{
                        const placeholder = input.placeholder || '';
                        const ariaLabel = input.getAttribute('aria-label') || '';
                        if (input.type === 'email' || input.name === 'email' || 
                            placeholder.toLowerCase().includes('email') ||
                            ariaLabel.toLowerCase().includes('email')) {{
                            emailInput = input;
                        }}
                    }}
                    if (emailInput) {{
                        emailInput.value = '{FANVUE_EMAIL}';
                        emailInput.dispatchEvent(new Event('input', {{ bubbles: true }}));
                        emailInput.dispatchEvent(new Event('change', {{ bubbles: true }}));
                    }}
                }}
            """)

            time.sleep(1)

            # Fill password
            self.page.evaluate(f"""
                () => {{
                    const inputs = document.querySelectorAll('input');
                    let passInput = null;
                    for (let input of inputs) {{
                        const placeholder = input.placeholder || '';
                        const ariaLabel = input.getAttribute('aria-label') || '';
                        if (input.type === 'password' || input.name === 'password' ||
                            placeholder.toLowerCase().includes('password') ||
                            ariaLabel.toLowerCase().includes('password')) {{
                            passInput = input;
                        }}
                    }}
                    if (passInput) {{
                        passInput.value = '{FANVUE_PASSWORD}';
                        passInput.dispatchEvent(new Event('input', {{ bubbles: true }}));
                        passInput.dispatchEvent(new Event('change', {{ bubbles: true }}));
                    }}
                }}
            """)

            time.sleep(1)

            # Submit form
            self.page.evaluate("""
                () => {
                    const form = document.querySelector('form');
                    if (form) form.submit();
                }
            """)

            time.sleep(5)

            current_url = self.page.url
            log(f"URL after login: {current_url}")

            if "login" not in current_url.lower():
                log("Login successful!")
                bot_status["logged_in"] = True
                return True
            else:
                log("Login failed")
                return False

        except Exception as e:
            log(f"Login error: {e}")
            return False

    def get_messages(self):
        try:
            self.page.goto("https://www.fanvue.com/messages", wait_until="networkidle")
            time.sleep(5)

            chats = self.page.query_selector_all('a[href*="/messages/"]')
            log(f"Found {len(chats)} chats")

            new_messages = []

            for i, chat in enumerate(chats[:3]):
                try:
                    chat.click()
                    time.sleep(3)

                    # Get all message elements
                    msgs = self.page.query_selector_all('[class*="message"]')

                    if msgs:
                        last = msgs[-1]
                        text = last.inner_text()
                        msg_class = last.get_attribute("class") or ""
                        is_me = "sent" in msg_class.lower() or "right" in msg_class.lower()

                        if text and not is_me:
                            msg_id = f"msg_{i}_{hash(text)}"
                            if msg_id not in self.last_messages:
                                self.last_messages[msg_id] = True
                                new_messages.append({"index": i, "text": text})
                                log(f"New msg: {text[:50]}")

                    self.page.goto("https://www.fanvue.com/messages", wait_until="networkidle")
                    time.sleep(3)

                except Exception as e:
                    log(f"Chat error: {e}")
                    continue

            return new_messages

        except Exception as e:
            log(f"Get messages error: {e}")
            return []

    def send_reply(self, text):
        try:
            selectors = [
                'textarea',
                'div[contenteditable="true"]',
                'input[type="text"]'
            ]

            inp = None
            for sel in selectors:
                inp = self.page.query_selector(sel)
                if inp:
                    break

            if inp:
                inp.fill(text)
                time.sleep(1)
                self.page.keyboard.press("Enter")
                time.sleep(2)
                log(f"Sent: {text[:50]}")
                return True
            else:
                log("No input found")
                return False

        except Exception as e:
            log(f"Send error: {e}")
            return False

    def ask_kimi(self, message):
        url = "https://api.moonshot.ai/v1/chat/completions"
        headers = {
            "Authorization": "Bearer " + KIMI_API_KEY,
            "Content-Type": "application/json"
        }

        data = {
            "model": "kimi-latest",
            "messages": [
                {"role": "system", "content": f"You are {CREATOR_NAME}. Reply in Hungarian. Keep under 30 words. Be sweet and casual."},
                {"role": "user", "content": message}
            ],
            "max_tokens": 100
        }

        try:
            r = requests.post(url, headers=headers, json=data, timeout=15)
            if r.status_code == 200:
                content = r.json()["choices"][0]["message"]["content"]
                return content.strip() if content else "Szia! 😊"
            else:
                log(f"Kimi error: {r.status_code}")
                return "Szia! 😊 Mi ujsag?"
        except Exception as e:
            log(f"Kimi error: {e}")
            return "Szia! 😊 Mi ujsag?"

    def process_messages(self):
        if bot_status["paused"]:
            return 0

        if not bot_status["logged_in"]:
            if not self.login():
                return 0

        msgs = self.get_messages()
        replied = 0

        for msg in msgs:
            text = msg["text"]
            reply = self.ask_kimi(text)

            chats = self.page.query_selector_all('a[href*="/messages/"]')
            if msg["index"] < len(chats):
                chats[msg["index"]].click()
                time.sleep(2)
                if self.send_reply(reply):
                    bot_status["replies_sent"] += 1
                    replied += 1

            time.sleep(3)

        return replied

    def close(self):
        if self.browser:
            self.browser.close()
        if hasattr(self, 'playwright') and self.playwright:
            self.playwright.stop()

bot = FanvueBot()

@app.route("/")
def home():
    return f"Bot running! Replies: {bot_status['replies_sent']}. Use /trigger /pause /resume /status"

@app.route("/status")
def status():
    return {
        "started": bot_status["started"],
        "last_check": bot_status["last_check"],
        "messages_found": bot_status["messages_found"],
        "replies_sent": bot_status["replies_sent"],
        "paused": bot_status["paused"],
        "logged_in": bot_status["logged_in"],
        "recent_logs": bot_status["errors"][-10:]
    }

@app.route("/trigger")
def trigger():
    try:
        if not bot.browser:
            if not bot.start():
                return {"status": "error", "error": "Browser failed"}

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

@app.route("/pause")
def pause():
    bot_status["paused"] = True
    return {"status": "paused"}

@app.route("/resume")
def resume():
    bot_status["paused"] = False
    return {"status": "resumed"}

@app.route("/block")
def block_user():
    user = request.args.get("user")
    if user:
        bot_status["blocked_users"].add(user)
        return {"status": "blocked", "user": user}
    return {"status": "error"}

@app.route("/unblock")
def unblock_user():
    user = request.args.get("user")
    if user:
        bot_status["blocked_users"].discard(user)
        return {"status": "unblocked", "user": user}
    return {"status": "error"}

if __name__ == "__main__":
    log("=" * 50)
    log("BOT STARTING")
    log("=" * 50)
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))