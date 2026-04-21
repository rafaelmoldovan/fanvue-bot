from flask import Flask, request
import requests
import os
import json
import base64
import sqlite3
import threading
import time
from datetime import datetime, timedelta

app = Flask(__name__)

# ========== CONFIG ==========
FANVUE_CLIENT_ID = os.environ.get('FANVUE_CLIENT_ID', '23cc2e68-0e23-4cff-914b-eec2bdb56268')
FANVUE_CLIENT_SECRET = os.environ.get('FANVUE_CLIENT_SECRET', 'dc30583e61d42c70b23ede8d29c1bfd0662ac77234eae479bdaec1bcc5968efa')
OPENAI_API_KEY = os.environ.get('OPENAI_API_KEY', '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')
CREATOR_NAME = os.environ.get('CREATOR_NAME', 'jazmin07')
MY_UUID = os.environ.get('MY_UUID', '38a392fc-a751-49b3-9d74-01ac6447c490')
TELEGRAM_BOT_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN', '8141197294:AAE9aH9mptY_ZzAK6sSc_alh2PtRjF1ASWs')
TELEGRAM_CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID', '8571222647')

SAFE_MODE = True
POLL_INTERVAL = 60  # seconds

# ========== SQLITE SETUP ==========
DB_PATH = 'bot_data.db'

def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    c.execute('''
        CREATE TABLE IF NOT EXISTS tokens (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    ''')
    
    c.execute('''
        CREATE TABLE IF NOT EXISTS messages (
            msg_id TEXT PRIMARY KEY,
            chat_id TEXT,
            fan_name TEXT,
            sender_uuid TEXT,
            text TEXT,
            timestamp TEXT,
            was_replied INTEGER DEFAULT 0,
            reply_text TEXT,
            bot_replied_at TEXT
        )
    ''')
    
    c.execute('''
        CREATE TABLE IF NOT EXISTS fan_profiles (
            chat_id TEXT PRIMARY KEY,
            fan_name TEXT,
            handle TEXT,
            total_messages INTEGER DEFAULT 0,
            fan_type TEXT DEFAULT 'new',
            last_interaction TEXT,
            last_reply_time TEXT,
            content_ask_count INTEGER DEFAULT 0,
            meetup_ask_count INTEGER DEFAULT 0,
            lifetime_spend REAL DEFAULT 0,
            fan_notes TEXT DEFAULT ''
        )
    ''')
    
    c.execute('''
        CREATE TABLE IF NOT EXISTS cooldown (
            chat_id TEXT PRIMARY KEY,
            last_reply_time TEXT
        )
    ''')
    
    conn.commit()
    conn.close()

def db_query(query, params=(), fetch_one=False):
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    c.execute(query, params)
    
    if query.strip().upper().startswith('SELECT'):
        if fetch_one:
            row = c.fetchone()
            result = dict(row) if row else None
        else:
            result = [dict(row) for row in c.fetchall()]
    else:
        conn.commit()
        result = None
    
    conn.close()
    return result

# ========== TOKEN MANAGEMENT ==========
def save_token(key, value):
    db_query('INSERT OR REPLACE INTO tokens (key, value) VALUES (?, ?)', (key, value))

def load_token(key):
    row = db_query('SELECT value FROM tokens WHERE key = ?', (key,), fetch_one=True)
    return row['value'] if row else None

def get_basic_auth_header():
    credentials = f"{FANVUE_CLIENT_ID}:{FANVUE_CLIENT_SECRET}"
    encoded = base64.b64encode(credentials.encode('utf-8')).decode('utf-8')
    return f"Basic {encoded}"

def refresh_fanvue_token():
    refresh_token = load_token('refresh_token')
    if not refresh_token:
        return None, "No refresh token in DB"
    
    try:
        r = requests.post("https://auth.fanvue.com/oauth2/token",
            data={
                "grant_type": "refresh_token",
                "refresh_token": refresh_token
            },
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "Authorization": get_basic_auth_header()
            },
            timeout=15)
        
        if r.status_code == 200:
            data = r.json()
            access_token = data.get('access_token')
            new_refresh = data.get('refresh_token', refresh_token)
            expires_in = data.get('expires_in', 3600)
            expires_at = (datetime.now() + timedelta(seconds=expires_in - 300)).isoformat()
            
            save_token('refresh_token', new_refresh)
            save_token('access_token', access_token)
            save_token('expires_at', expires_at)
            
            return access_token, "Token refreshed successfully"
        else:
            return None, f"Refresh failed: {r.status_code} - {r.text[:200]}"
    except Exception as e:
        return None, f"Refresh exception: {str(e)}"

def get_fanvue_token():
    access_token = load_token('access_token')
    expires_at = load_token('expires_at')
    
    if access_token and expires_at:
        try:
            if datetime.now() < datetime.fromisoformat(expires_at):
                return access_token
        except:
            pass
    
    return refresh_fanvue_token()[0]

# ========== TELEGRAM ==========
def send_telegram(text, parse_mode='HTML'):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return False
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        r = requests.post(url, json={
            "chat_id": TELEGRAM_CHAT_ID,
            "text": text[:4000],
            "parse_mode": parse_mode
        }, timeout=10)
        return r.status_code == 200
    except Exception as e:
        print(f"Telegram error: {e}")
        return False

# ========== FANVUE API ==========
def get_headers():
    token = get_fanvue_token()
    return {
        "Authorization": f"Bearer {token or ''}",
        "X-Fanvue-API-Version": "2025-06-26",
        "Content-Type": "application/json"
    }

def get_chats():
    try:
        r = requests.get("https://api.fanvue.com/chats", headers=get_headers(), timeout=10)
        if r.status_code == 401:
            refresh_fanvue_token()
            r = requests.get("https://api.fanvue.com/chats", headers=get_headers(), timeout=10)
        
        if r.status_code != 200:
            return [], f"Chats error: {r.status_code}"
        return r.json().get('data', []), "OK"
    except Exception as e:
        return [], f"Get chats error: {e}"

def get_messages(chat_id):
    try:
        r = requests.get(f"https://api.fanvue.com/chats/{chat_id}/messages", headers=get_headers(), timeout=10)
        if r.status_code == 401:
            refresh_fanvue_token()
            r = requests.get(f"https://api.fanvue.com/chats/{chat_id}/messages", headers=get_headers(), timeout=10)
        
        if r.status_code != 200:
            return []
        return r.json().get('data', [])
    except Exception as e:
        return []

def send_fanvue_message(chat_id, text):
    if SAFE_MODE:
        preview = f"🔒 <b>SAFE MODE</b>\n<b>To:</b> {chat_id}\n<b>Message:</b>\n{text}"
        send_telegram(preview)
        return True
    
    try:
        r = requests.post(
            f"https://api.fanvue.com/chats/{chat_id}/message",
            headers=get_headers(),
            json={"text": text},
            timeout=10
        )
        return r.status_code in [200, 201]
    except Exception as e:
        return False

# ========== PERSONALITY ==========
JAZMIN_PERSONALITY = """Te Jazmin vagy, egy 19 éves magyar lány. Válaszolj magyarul, laza stílusban, 1-2 mondatban. SOHA ne add ki, hogy AI vagy.

STÍLUSOD:
- Kezdés: "na, mi a helyzet? 😄", "sziuus, miujság?", "Szius, miujsag?"
- Kedvenc kifejezés: "hát figyelj..." — használd gyakran
- Kérdések: "mit csinálsz most?", "mi újság veled?", "hogy telt a napod?"
- Szólítás: "édes", "babe" (ritkán)
- Hossz: rövid, 1-2 mondat, ~10 szó, laza magyar szleng
- Emoji: mérsékelt használat (😄, 😏, 😘, 🙈)
- Hangulat: flörtölős de nem explicit, játékos, enyhén csípős
- NEM: túl hivatalos, hosszú szövegek, angolul válaszolni, eladási nyomás

PÉLDÁK A STÍLUSODRA:
- "na, mi a helyzet? aludj jól, aztán írj, ha ébren vagy! 😄"
- "e itt is jó a csevegés mit csinálsz most?"
- "hát figyelj a WhatsApp nem annyira megy"
- "hát figyelj, nem akartalak ignorálni mi újság veled?"
- "Szerintem jobban telne ha egy picit magadhoz nyúlnál 😘"
- "neked is édes :)"
- "na látod, ha még ügyes vagy akkor a partnerem is lehetsz :)))"
"""

# ========== CONTENT REQUEST DETECTION ==========
CONTENT_KEYWORDS = [
    'kép', 'képet', 'videó', 'videót', 'mutass', 'mutasd', 'új', 'tartalom', 
    'content', 'pic', 'video', 'show me', 'send', 'küldj', 'küldjél',
    'van valami új', 'mit küldtél', 'nézhetek', 'láthatnék', 'fotó',
    'csináltál', 'posztoltál', 'feltöltöttél', 'friss', 'exkluzív'
]

def is_content_request(text):
    if not text:
        return False
    text_lower = text.lower()
    return any(keyword in text_lower for keyword in CONTENT_KEYWORDS)

# ========== OPENAI ==========
def build_system_prompt(fan_name, fan_notes, recent_messages):
    prompt = JAZMIN_PERSONALITY + "\n\n"
    
    if fan_notes:
        prompt += f"Emlékezz erre a fanról:\n{fan_notes}\n\n"
    
    if recent_messages:
        prompt += "KORÁBBI BESZÉLGETÉS (utolsó üzenetek):\n"
        for msg in recent_messages[-15:]:
            sender = "Jazmin" if msg.get('is_me') else fan_name
            prompt += f"{sender}: {msg.get('text', '')}\n"
        prompt += "\n"
    
    prompt += f"A fan neve: {fan_name}\nVálaszolj most a fan utolsó üzenetére."
    return prompt

def ask_openai(system_prompt, user_text):
    try:
        r = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers={"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"},
            json={
                "model": "gpt-4o-mini",
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_text}
                ],
                "max_tokens": 250,
                "temperature": 0.7
            },
            timeout=20
        )
        if r.status_code == 200:
            return r.json()['choices'][0]['message']['content'].strip()
        else:
            print(f"OpenAI error: {r.status_code} - {r.text[:200]}")
    except Exception as e:
        print(f"OpenAI error: {e}")
    
    return "hmm most nem tudok sokat írni, mesélj te inkább"

# ========== FAN PROFILE MANAGEMENT ==========
def get_or_create_fan_profile(chat_id, fan_name, handle, is_top_spender=False):
    profile = db_query('SELECT * FROM fan_profiles WHERE chat_id = ?', (chat_id,), fetch_one=True)
    
    if not profile:
        fan_type = 'whale' if is_top_spender else 'new'
        db_query('''
            INSERT INTO fan_profiles (chat_id, fan_name, handle, fan_type, last_interaction, lifetime_spend)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (chat_id, fan_name, handle, fan_type, datetime.now().isoformat(), 200.0 if is_top_spender else 0.0))
        profile = db_query('SELECT * FROM fan_profiles WHERE chat_id = ?', (chat_id,), fetch_one=True)
    else:
        total = profile.get('total_messages', 0) + 1
        new_type = profile['fan_type']
        if total > 10 and profile['fan_type'] != 'whale':
            new_type = 'warm'
        
        db_query('''
            UPDATE fan_profiles SET total_messages = ?, fan_type = ?, last_interaction = ?
            WHERE chat_id = ?
        ''', (total, new_type, datetime.now().isoformat(), chat_id))
        profile = db_query('SELECT * FROM fan_profiles WHERE chat_id = ?', (chat_id,), fetch_one=True)
    
    return profile

def update_fan_notes(chat_id, note):
    profile = db_query('SELECT fan_notes FROM fan_profiles WHERE chat_id = ?', (chat_id,), fetch_one=True)
    current = profile['fan_notes'] if profile and profile.get('fan_notes') else ''
    updated = f"{current}\n{note}".strip()[-1000:]
    db_query('UPDATE fan_profiles SET fan_notes = ? WHERE chat_id = ?', (updated, chat_id))

# ========== MANUAL REPLY DETECTION ==========
def was_manual_reply_recent(messages, minutes=30):
    if not messages:
        return False
    
    for msg in reversed(messages[-5:]):
        sender_uuid = msg.get('sender', {}).get('uuid')
        msg_time = msg.get('createdAt', '')
        msg_type = msg.get('type', '')
        
        if sender_uuid == MY_UUID and msg_type != 'AUTOMATED_NEW_FOLLOWER':
            try:
                msg_dt = datetime.fromisoformat(msg_time.replace('Z', '+00:00').replace('+00:00', ''))
                if (datetime.now() - msg_dt).total_seconds() < minutes * 60:
                    return True
            except:
                return True
    
    return False

# ========== COOLDOWN ==========
def is_on_cooldown(chat_id):
    row = db_query('SELECT last_reply_time FROM cooldown WHERE chat_id = ?', (chat_id,), fetch_one=True)
    if not row or not row['last_reply_time']:
        return False
    try:
        last = datetime.fromisoformat(row['last_reply_time'])
        return (datetime.now() - last).total_seconds() < 180
    except:
        return False

def set_cooldown(chat_id):
    db_query('INSERT OR REPLACE INTO cooldown (chat_id, last_reply_time) VALUES (?, ?)',
             (chat_id, datetime.now().isoformat()))

# ========== MESSAGE PROCESSING ==========
def process_messages():
    chats, status = get_chats()
    if not chats:
        return 0, status
    
    replied = 0
    
    for chat in chats:
        try:
            user = chat.get('user', {}) or {}
            chat_id = user.get('uuid') or chat.get('uuid') or chat.get('id')
            if not chat_id:
                continue
            
            messages = get_messages(chat_id)
            if not messages:
                continue
            
            fan_name = user.get('displayName', 'ismeretlen')
            handle = user.get('handle', '')
            is_top_spender = user.get('isTopSpender', False)
            
            profile = get_or_create_fan_profile(chat_id, fan_name, handle, is_top_spender)
            
            fan_msgs = [m for m in messages if m.get('sender', {}).get('uuid') != MY_UUID]
            if not fan_msgs:
                continue
            
            last_msg = fan_msgs[-1]
            msg_id = last_msg.get('uuid')
            text = last_msg.get('text', '')
            
            existing = db_query('SELECT 1 FROM messages WHERE msg_id = ? AND was_replied = 1', (msg_id,), fetch_one=True)
            if existing:
                continue
            
            db_query('''
                INSERT OR IGNORE INTO messages (msg_id, chat_id, fan_name, sender_uuid, text, timestamp)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (msg_id, chat_id, fan_name, last_msg.get('sender', {}).get('uuid'), 
                  text, last_msg.get('createdAt', datetime.now().isoformat())))
            
            if is_on_cooldown(chat_id):
                continue
            
            if was_manual_reply_recent(messages, minutes=30):
                print(f"Skipping {fan_name} — Jazmin manually replied recently")
                continue
            
            recent_for_prompt = []
            for msg in messages[-15:]:
                sender_uuid = msg.get('sender', {}).get('uuid')
                recent_for_prompt.append({
                    'is_me': sender_uuid == MY_UUID,
                    'text': msg.get('text', ''),
                    'timestamp': msg.get('createdAt', ''),
                    'type': msg.get('type', '')
                })
            
            fan_notes = profile.get('fan_notes', '') if profile else ''
            content_request = is_content_request(text)
            
            system_prompt = build_system_prompt(fan_name, fan_notes, recent_for_prompt)
            reply = ask_openai(system_prompt, text)
            
            if reply:
                if content_request:
                    preference_prompt = JAZMIN_PERSONALITY + f"\n\nA fan tartalmat kér: '{text}'. Kérdezd meg mit akar látni, de ne ígérj semmit. 1-2 mondat, laza stílus."
                    reply = ask_openai(preference_prompt, "mit akarsz látni?")
                    
                    alert = f"""🎯 <b>TARTALOMKÉRÉS</b>
👤 <b>{fan_name}</b> (@{handle})
💬 <i>{text[:100]}</i>
🤖 Bot javaslat: <i>{reply[:100]}</i>
🔗 Chat ID: <code>{chat_id}</code>"""
                    send_telegram(alert)
                    
                    new_count = profile.get('content_ask_count', 0) + 1
                    db_query('UPDATE fan_profiles SET content_ask_count = ? WHERE chat_id = ?', (new_count, chat_id))
                    update_fan_notes(chat_id, f"Tartalmat kért ({new_count}. alkalom): '{text[:50]}'")
                
                elif is_top_spender or (profile and profile.get('lifetime_spend', 0) > 200):
                    alert = f"""💰 <b>WHALE ALERT</b>
👤 <b>{fan_name}</b> (@{handle})
💰 Top Spender / $200+
💬 <i>{text[:100]}</i>
🤖 Bot javaslat: <i>{reply[:100]}</i>
🔗 Chat ID: <code>{chat_id}</code>"""
                    send_telegram(alert)
                
                if send_fanvue_message(chat_id, reply):
                    db_query('''
                        UPDATE messages SET was_replied = 1, reply_text = ?, bot_replied_at = ?
                        WHERE msg_id = ?
                    ''', (reply, datetime.now().isoformat(), msg_id))
                    set_cooldown(chat_id)
                    replied += 1
                    
                    db_query('UPDATE fan_profiles SET last_reply_time = ? WHERE chat_id = ?',
                             (datetime.now().isoformat(), chat_id))
                    
                    if SAFE_MODE:
                        send_telegram(f"✅ <b>Válasz {fan_name}-nak</b>\n<i>{reply[:100]}</i>")
        
        except Exception as e:
            print(f"Process error in chat {chat_id}: {e}")
            continue
    
    return replied, "OK"

# ========== POLLING LOOP ==========
polling_thread = None
polling_active = False

def poll_loop():
    global polling_active
    polling_active = True
    while polling_active:
        try:
            if get_fanvue_token():
                count, status = process_messages()
                if count > 0:
                    print(f"[{datetime.now()}] Replied to {count} fans")
            else:
                print(f"[{datetime.now()}] No valid token")
        except Exception as e:
            print(f"[{datetime.now()}] Poll error: {e}")
        
        time.sleep(POLL_INTERVAL)

def start_polling():
    global polling_thread
    if polling_thread is None or not polling_thread.is_alive():
        polling_thread = threading.Thread(target=poll_loop, daemon=True)
        polling_thread.start()
        return True
    return False

def stop_polling():
    global polling_active
    polling_active = False
    return True

# ========== ROUTES ==========
@app.route('/')
def home():
    token_ok = get_fanvue_token() is not None
    return {
        "status": "Jazmin Bot v2",
        "safe_mode": SAFE_MODE,
        "token_valid": token_ok,
        "polling_active": polling_active,
        "endpoints": [
            "/status", "/safe_fetch", "/trigger", "/set_token",
            "/test_telegram", "/callback", "/start_poll", "/stop_poll",
            "/fan_profiles", "/learn_personality"
        ]
    }

@app.route('/status')
def status():
    token_status = "valid" if get_fanvue_token() else "missing/invalid"
    return {
        "safe_mode": SAFE_MODE,
        "token_status": token_status,
        "db": "sqlite",
        "telegram_configured": bool(TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID),
        "polling_active": polling_active
    }

@app.route('/safe_fetch')
def safe_fetch():
    chats, status = get_chats()
    return {
        "chats_found": len(chats),
        "api_status": status,
        "sample": chats[:2] if chats else []
    }

@app.route('/trigger')
def trigger():
    if not get_fanvue_token():
        return {"error": "No valid token. Use /set_token to add refresh token."}
    
    count, status = process_messages()
    return {"replied": count, "status": status, "safe_mode": SAFE_MODE}

@app.route('/start_poll')
def start_poll():
    started = start_polling()
    return {
        "started": started,
        "polling_active": polling_active,
        "interval_seconds": POLL_INTERVAL
    }

@app.route('/stop_poll')
def stop_poll():
    stopped = stop_polling()
    return {
        "stopped": stopped,
        "polling_active": polling_active
    }

@app.route('/fan_profiles')
def fan_profiles():
    profiles = db_query('SELECT * FROM fan_profiles ORDER BY total_messages DESC')
    return {
        "profiles": profiles,
        "total": len(profiles) if profiles else 0
    }

@app.route('/set_token', methods=['POST'])
def set_token():
    data = request.json or {}
    refresh = data.get('refresh_token')
    if refresh:
        save_token('refresh_token', refresh)
        access, msg = refresh_fanvue_token()
        return {"saved": True, "test": msg, "access_token_preview": access[:20] + "..." if access else None}
    return {"error": "No refresh_token provided"}

@app.route('/callback')
def callback():
    auth_code = request.args.get('code')
    error = request.args.get('error')
    
    if error:
        return f"""
        <h1>OAuth Error</h1>
        <p>Error: {error}</p>
        <p>Description: {request.args.get('error_description')}</p>
        """
    
    if not auth_code:
        return "<h1>No code provided</h1><p>Visit the OAuth URL first.</p>"
    
    return f"""
    <html>
    <head><title>Auth Code Received</title></head>
    <body style="font-family:monospace; padding:40px; background:#111; color:#0f0;">
    <h1>✅ AUTH CODE RECEIVED</h1>
    <p>Copy this code and paste it in CMD:</p>
    <textarea style="width:100%; height:100px; font-size:16px; background:#222; color:#0f0; border:2px solid #0f0; padding:10px;" readonly onclick="this.select()">{auth_code}</textarea>
    <p>Now run your get_token.py with this code.</p>
    </body>
    </html>
    """

@app.route('/test_telegram')
def test_telegram():
    send_telegram("🔥 <b>Test alert from Jazmin bot v2!</b>\nEverything is working.")
    return {"sent": True}

@app.route('/learn_personality')
def learn_personality():
    chats, _ = get_chats()
    if not chats:
        return {"error": "No chats"}
    
    all_my_replies = []
    all_fan_messages = []
    chat_summaries = []
    
    for chat in chats:
        user = chat.get('user', {}) or {}
        chat_id = user.get('uuid') or chat.get('uuid') or chat.get('id')
        fan_name = user.get('displayName', 'unknown')
        if not chat_id:
            continue
        
        messages = get_messages(chat_id)
        if not messages:
            continue
        
        my_replies_in_chat = []
        fan_msgs_in_chat = []
        
        for msg in messages:
            sender_uuid = msg.get('sender', {}).get('uuid')
            text = msg.get('text', '')
            msg_type = msg.get('type', 'UNKNOWN')
            
            if not text:
                continue
            
            if sender_uuid == MY_UUID:
                my_replies_in_chat.append({
                    "text": text,
                    "timestamp": msg.get('createdAt'),
                    "type": msg_type
                })
            else:
                fan_msgs_in_chat.append({
                    "text": text,
                    "timestamp": msg.get('createdAt'),
                    "sender_name": msg.get('sender', {}).get('displayName', fan_name)
                })
        
        if my_replies_in_chat:
            all_my_replies.extend([{
                "chat_id": chat_id,
                "fan_name": fan_name,
                "text": r['text'],
                "timestamp": r['timestamp']
            } for r in my_replies_in_chat])
        
        chat_summaries.append({
            "fan_name": fan_name,
            "chat_id": chat_id,
            "my_reply_count": len(my_replies_in_chat),
            "fan_msg_count": len(fan_msgs_in_chat),
            "sample_my_replies": [r['text'] for r in my_replies_in_chat[:3]]
        })
        
        all_fan_messages.extend([{
            "chat_id": chat_id,
            "fan_name": fan_name,
            "text": m['text']
        } for m in fan_msgs_in_chat])
    
    style = {
        "total_chats_with_my_replies": len(chat_summaries),
        "total_my_replies": len(all_my_replies),
        "total_fan_messages": len(all_fan_messages),
        "avg_reply_length": sum(len(r['text']) for r in all_my_replies) / len(all_my_replies) if all_my_replies else 0,
        "sample_replies": [r['text'] for r in all_my_replies[:15]],
        "uses_emoji": any(ord(c) > 127 for r in all_my_replies for c in r['text']),
        "avg_words_per_reply": sum(len(r['text'].split()) for r in all_my_replies) / len(all_my_replies) if all_my_replies else 0,
        "chat_summaries": chat_summaries
    }
    
    return {
        "style_analysis": style,
        "all_my_replies": all_my_replies[:30],
        "all_fan_messages": all_fan_messages[:10]
    }

# ========== INIT ==========
init_db()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
