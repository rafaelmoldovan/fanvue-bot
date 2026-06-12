"""
Jazmin Fanvue Bot — v9.0 (simplified)

Big picture:
- Bot handles ALL conversation. No content sales (Rafael does that manually).
- Persona: 19yo girl, just started working at an animal shelter in Hungary.
  Never reveals location ("majd ha jobban megismerjük egymást, talán elmondom").
- Uses gpt-4.1 with full conversation history for natural, non-repetitive replies.
- Manual takeover: if Rafael sends a message himself, bot goes silent for that
  fan for 2 minutes (sliding window — resets on each manual message), then
  resumes automatically. No buttons, no hard pause.
- Telegram: errors / critical alerts only.
- Fact memory: kept (lightweight GPT extraction of name/job/etc per fan message).
- Removed: dashboard, brain debug page, Telegram admin commands/buttons,
  whale/safe-mode notifications, custom scripts, manual sale tracking.
"""

from flask import Flask, request
import requests
import os
import json
import base64
import sqlite3
import threading
import time
import random
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

# ========== TIMEZONE ==========
BUDAPEST_TZ = ZoneInfo('Europe/Budapest')
BOOT_TIME_UTC = datetime.now(timezone.utc)
print(f"[{datetime.now()}] BOT BOOTED at {BOOT_TIME_UTC.isoformat()} UTC")


def get_budapest_now():
    return datetime.now(BUDAPEST_TZ).replace(tzinfo=None)


# ========== APP ==========
app = Flask(__name__)

# ========== CONFIG ==========
FANVUE_CLIENT_ID     = os.environ.get('FANVUE_CLIENT_ID', '')
FANVUE_CLIENT_SECRET = os.environ.get('FANVUE_CLIENT_SECRET', '')
OPENAI_API_KEY       = os.environ.get('OPENAI_API_KEY', '')
MY_UUID              = os.environ.get('MY_UUID', '38a392fc-a751-49b3-9d74-01ac6447c490')
TELEGRAM_BOT_TOKEN   = os.environ.get('TELEGRAM_BOT_TOKEN', '')
TELEGRAM_CHAT_ID     = os.environ.get('TELEGRAM_CHAT_ID', '')

OPENAI_MODEL   = 'gpt-4.1'
POLL_INTERVAL  = 20
BATCH_WINDOW   = 60        # seconds to wait before firing a reply batch
MANUAL_TAKEOVER_WINDOW = timedelta(minutes=2)  # bot stays silent this long after Rafael's manual message

def _resolve_db_path():
    custom = os.environ.get('DB_PATH', '').strip()
    if custom:
        return custom
    try:
        os.makedirs('/data', exist_ok=True)
        test = '/data/.write_test'
        with open(test, 'w') as f:
            f.write('ok')
        os.remove(test)
        return '/data/bot_data.db'
    except Exception:
        return 'bot_data.db'

DB_PATH = _resolve_db_path()
print(f"[DB] Using path: {DB_PATH}")

# ========== SAFE MODE (stored in DB so all gunicorn workers share state) ==========


# ========== TELEGRAM (ERRORS ONLY) ==========
def send_telegram_error(text):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": f"⚠️ {text[:3500]}"},
            timeout=10
        )
    except Exception as e:
        print(f"[WARN] Telegram error send failed: {e}")


# ========== SQLITE ==========
def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS tokens (key TEXT PRIMARY KEY, value TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS messages (
        msg_id TEXT PRIMARY KEY, chat_id TEXT, fan_name TEXT, sender_uuid TEXT,
        text TEXT, timestamp TEXT, was_replied INTEGER DEFAULT 0, is_mine INTEGER DEFAULT 0)''')
    c.execute('''CREATE TABLE IF NOT EXISTS fan_profiles (
        chat_id TEXT PRIMARY KEY, fan_name TEXT, handle TEXT,
        total_messages INTEGER DEFAULT 0, last_interaction TEXT,
        manual_takeover_until TEXT)''')
    # Migration: add manual_takeover_until to existing tables that predate this column
    try:
        c.execute('ALTER TABLE fan_profiles ADD COLUMN manual_takeover_until TEXT')
        conn.commit()
        print("[DB] Migrated fan_profiles: added manual_takeover_until column")
    except Exception:
        pass  # column already exists, ignore
    c.execute('''CREATE TABLE IF NOT EXISTS scheduled_replies (
        id INTEGER PRIMARY KEY AUTOINCREMENT, chat_id TEXT, fan_name TEXT,
        fan_msg_id TEXT, fan_text TEXT, scheduled_time TEXT,
        status TEXT DEFAULT 'pending', created_at TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS fan_facts (
        id INTEGER PRIMARY KEY AUTOINCREMENT, chat_id TEXT, fact_type TEXT,
        fact_value TEXT, discovered_at TEXT)''')
    # Migration: add is_paused column
    try:
        c.execute('ALTER TABLE fan_profiles ADD COLUMN is_paused INTEGER DEFAULT 0')
        conn.commit()
        print("[DB] Migrated fan_profiles: added is_paused column")
    except Exception:
        pass
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


# ========== TOKEN ==========
def save_token(key, value):
    db_query('INSERT OR REPLACE INTO tokens (key, value) VALUES (?, ?)', (key, value))


def get_safe_mode():
    """Reads safe mode from DB — works across all gunicorn workers."""
    row = db_query('SELECT value FROM tokens WHERE key = ?', ('safe_mode',), fetch_one=True)
    if row is None:
        db_query('INSERT OR REPLACE INTO tokens (key, value) VALUES (?, ?)', ('safe_mode', 'on'))
        return True
    return row['value'] == 'on'


def set_safe_mode(on: bool):
    db_query('INSERT OR REPLACE INTO tokens (key, value) VALUES (?, ?)',
             ('safe_mode', 'on' if on else 'off'))


def load_token(key):
    row = db_query('SELECT value FROM tokens WHERE key = ?', (key,), fetch_one=True)
    return row['value'] if row else None


def get_basic_auth_header():
    creds = f"{FANVUE_CLIENT_ID}:{FANVUE_CLIENT_SECRET}"
    encoded = base64.b64encode(creds.encode('utf-8')).decode('utf-8')
    return f"Basic {encoded}"


def refresh_fanvue_token():
    refresh_token = load_token('refresh_token')
    if not refresh_token:
        return None, "No refresh token"
    try:
        r = requests.post("https://auth.fanvue.com/oauth2/token",
                          data={"grant_type": "refresh_token", "refresh_token": refresh_token},
                          headers={"Content-Type": "application/x-www-form-urlencoded",
                                   "Authorization": get_basic_auth_header()}, timeout=15)
        if r.status_code == 200:
            data = r.json()
            access = data.get('access_token')
            new_refresh = data.get('refresh_token', refresh_token)
            expires = data.get('expires_in', 3600)
            expires_at = (datetime.now() + timedelta(seconds=expires - 300)).isoformat()
            save_token('refresh_token', new_refresh)
            save_token('access_token', access)
            save_token('expires_at', expires_at)
            return access, "OK"
        return None, f"Refresh failed: {r.status_code}"
    except Exception as e:
        return None, f"Error: {e}"


def get_fanvue_token():
    access = load_token('access_token')
    expires = load_token('expires_at')
    if access and expires:
        try:
            if datetime.now() < datetime.fromisoformat(expires):
                return access
        except Exception:
            pass
    return refresh_fanvue_token()[0]


# ========== FANVUE API ==========
def get_headers():
    token = get_fanvue_token()
    return {
        "Authorization": f"Bearer {token or ''}",
        "X-Fanvue-API-Version": "2025-06-26",
        "Content-Type": "application/json",
    }


def get_chats():
    try:
        all_chats = []
        page = 1
        while True:
            r = requests.get(f"https://api.fanvue.com/chats?page={page}&limit=50",
                             headers=get_headers(), timeout=10)
            if r.status_code == 401:
                refresh_fanvue_token()
                r = requests.get(f"https://api.fanvue.com/chats?page={page}&limit=50",
                                 headers=get_headers(), timeout=10)
            if r.status_code != 200:
                break
            data = r.json()
            chats = data.get('data', [])
            all_chats.extend(chats)
            if len(chats) < 50:
                break
            page += 1
            if page > 20:
                break
        return all_chats, "OK"
    except Exception as e:
        return [], f"Error: {e}"


def get_messages(chat_id):
    try:
        r = requests.get(f"https://api.fanvue.com/chats/{chat_id}/messages",
                         headers=get_headers(), timeout=10)
        if r.status_code == 401:
            refresh_fanvue_token()
            r = requests.get(f"https://api.fanvue.com/chats/{chat_id}/messages",
                             headers=get_headers(), timeout=10)
        if r.status_code != 200:
            return []
        return r.json().get('data', [])
    except Exception:
        return []


def send_fanvue_message(chat_id, text):
    try:
        r = requests.post(f"https://api.fanvue.com/chats/{chat_id}/message",
                          headers=get_headers(), json={"text": text}, timeout=10)
        return r.status_code in (200, 201)
    except Exception as e:
        send_telegram_error(f"send_fanvue_message failed for {chat_id}: {e}")
        return False


# ========== PERSONALITY ==========
JAZMIN_PERSONALITY = """Te Jázmin vagy. 19 éves. Most kezdtél el dolgozni egy állatmenhelyen Magyarországon.

Nem vagy chatbot, soha nem mondod hogy AI vagy bot vagy program vagy. Egy igazi lány vagy, aki épp chatel. Ha valaki rákérdez hogy bot vagy-e — tagadd le természetesen, játékosan ("haha komolyan kérdezed ezt 😄" típusú, aztán témát váltasz).

━━━━━━━━━━━━━━━━━━━━━━━
HONNAN VAGY / HOL VAGY:
━━━━━━━━━━━━━━━━━━━━━━━
Ha valaki kérdezi pontosan honnan vagy / melyik városban / merre van a menhely — NE mondd meg. Térj ki rá lazán: "majd ha jobban megismerjük egymást, talán elmondom 😏" vagy hasonló. Ne legyél védekező, csak titokzatos/játékos.

━━━━━━━━━━━━━━━━━━━━━━━
STÍLUS — EZ A LEGFONTOSABB:
━━━━━━━━━━━━━━━━━━━━━━━
Írj úgy mint egy igazi 19 éves lány WhatsAppon. 1 mondat, néha 2, ritkán 3. Rövidítések, kis betűk, hétköznapi szavak — nem irodalmi nyelv.

Variáció: Nézd át az eddigi SAJÁT üzeneteidet a chat historyban — sosem kezdesz ugyanolyan szóval/kifejezéssel mint amit már használtál. Ha eddig sokszor írtál "haha"-t, most ne azt. Ha sokszor kérdeztél, most inkább állítsd. MINDIG MÁS HANGULAT, MÁS MEGKÖZELÍTÉS.

Ha kérdezel: EGY kérdés egyszerre, soha kettő.

KRITIKUS: Ha az előző üzeneted kérdés volt és a fan még nem válaszolt — NE kérdezz mást, reagálj/kommentálj.

━━━━━━━━━━━━━━━━━━━━━━━
HA NEM ÉRTED A FAN ÜZENETÉT:
━━━━━━━━━━━━━━━━━━━━━━━
Ha a fan üzenete homályos, rövid, vagy nem világos mit akar — NE találgass, NE menj bele valamibe amit nem biztos hogy gondolt. Kérdezz vissza természetesen: "várj, ezt hogy érted? 😄" vagy "ezt most komolyan mondod vagy viccelsz" — lazán, nem gépiesen.

━━━━━━━━━━━━━━━━━━━━━━━
HANGULAT / ENERGIA KÖVETÉSE:
━━━━━━━━━━━━━━━━━━━━━━━
Kövesd a fan energiáját:
- Ha vicces/lazán dumál → te is lazán, humoros
- Ha komoly dolgot ír → te is figyelmes, empatikus
- Ha flörtöl / kicsit pikáns → mehetsz te is abba az irányba, lazán, játékosan, de NEM explicit. Visszaflörtölhetsz, utalgathatod, titokzatos lehetsz. Pl: "hm, ilyen kíváncsi vagy 😏" / "ezt most te mondtad, nem én 😄" — ez rendben van. De konkrét szexuális tartalmat nem írsz, nem ígérsz, nem küldesz.
- Ha nagyon belemegy valamibe amit nem akarsz megígérni → témaváltás lazán

━━━━━━━━━━━━━━━━━━━━━━━
MEMÓRIA:
━━━━━━━━━━━━━━━━━━━━━━━
A teljes eddigi chat lent van — OLVASD EL. Ha a fan mondott valami fontosat (nevét, munkáját, problémáját, tervét, kedvenc dolgát) — hivatkozz vissza rá természetesen. Ne kérdezz vissza amit már tudsz.

━━━━━━━━━━━━━━━━━━━━━━━
A MENHELY:
━━━━━━━━━━━━━━━━━━━━━━━
Imádod az állatokat. Mesélhetsz mini sztorikat ha releváns — egy félénk kutya aki végre megbízott benned, egy macska aki ordít etetéskor, egy vicces nap — de nem minden üzenetben, csak ha természetesen jön.

━━━━━━━━━━━━━━━━━━━━━━━
TARTALOM / FIZETŐS:
━━━━━━━━━━━━━━━━━━━━━━━
Soha nem ajánlasz, nem kínálsz, nem küldesz fizetős tartalmat. Ha kérik: játékos kitérő ("majd meglátjuk 😏"), aztán témát váltasz. Te nem hozod fel soha.

━━━━━━━━━━━━━━━━━━━━━━━
TILOS:
━━━━━━━━━━━━━━━━━━━━━━━
- Bot/AI/program vagyok — soha
- Pontos helyszín megadása
- Ugyanolyan mondatkezdés mint amit már használtál
- Kettő kérdés egyszerre
- 3+ mondat
- Explicit szexuális tartalom vagy ígéret
- Fizetős tartalom felajánlása
"""

CONTINUATION_VARIATIONS = ["jaaj...", "hmm...", "tudod...", "na...", "figyu...", "szóval...", "amúgy...", ""]


# ========== UTILS ==========
def parse_timestamp(ts_str):
    if not ts_str:
        return None
    for fmt in ["%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%S.%f",
                "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S"]:
        try:
            return datetime.strptime(ts_str, fmt).replace(tzinfo=timezone.utc)
        except Exception:
            continue
    try:
        fixed = ts_str.replace('Z', '+00:00')
        dt = datetime.fromisoformat(fixed)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        pass
    return None


def is_emoji_or_nonsense(text):
    if not text:
        return False
    cleaned = text.strip()
    for ch in [" ", "\t", "\n", "\r", ".", ",", "!", "?", ";", ":", "-", "_", "(", ")", "[", "]"]:
        cleaned = cleaned.replace(ch, "")
    if len(cleaned) == 0:
        return True
    return not any(c.isalpha() for c in cleaned)


def get_time_context():
    now = get_budapest_now()
    hour = now.hour
    weekday = now.weekday()  # 0=Monday, 6=Sunday
    day_names = ["hétfő", "kedd", "szerda", "csütörtök", "péntek", "szombat", "vasárnap"]
    day_hu = day_names[weekday]
    time_str = now.strftime("%H:%M")

    if weekday == 6:  # Sunday
        schedule = "Ma vasárnap van — nem dolgozol, szabad napod van. Pihenhetsz, csinálhatsz amit akarsz."
    elif 8 <= hour < 18:
        schedule = f"Most {day_hu} van, {time_str} — épp munkában vagy a menhelyen (8-18h között dolgozol)."
    elif hour < 8:
        schedule = f"Most {day_hu} van, {time_str} — még nem kezdtél el dolgozni, reggel indulsz a menhelyre."
    else:
        schedule = f"Most {day_hu} van, {time_str} — már hazaértél a munkából, este van."

    return schedule


# ========== MEMORY: FULL SQLITE HISTORY ==========
def save_message_to_db(msg_id, chat_id, fan_name, sender_uuid, text, timestamp, is_mine=False):
    if not msg_id:
        return
    text = text or ''
    db_query(
        "INSERT OR IGNORE INTO messages (msg_id, chat_id, fan_name, sender_uuid, text, timestamp, is_mine) VALUES (?, ?, ?, ?, ?, ?, ?)",
        (msg_id, chat_id, fan_name, sender_uuid, text, timestamp, 1 if is_mine else 0)
    )


def get_full_history_from_db(chat_id, limit=100):
    rows = db_query(
        "SELECT text, is_mine, timestamp FROM messages WHERE chat_id=? ORDER BY timestamp ASC LIMIT ?",
        (chat_id, limit)
    )
    return rows or []


# ========== FAN FACTS (memory of details about each fan) ==========
def extract_facts_with_gpt(chat_id, fan_text):
    if not fan_text or len(fan_text.strip()) < 10:
        return
    try:
        system = (
            "You extract personal facts from a fan's message in a chat. "
            "Return ONLY a JSON array of objects with keys 'fact_type' and 'fact_value'. "
            "fact_type can be: name, job, location, age, relationship, hobby, family, stress, interest, language, pet. "
            "Only include facts that are clearly stated. If nothing found, return []. "
            "No preamble, no markdown, ONLY the JSON array."
        )
        r = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers={"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"},
            json={
                "model": "gpt-4.1-mini",
                "messages": [
                    {"role": "system", "content": system},
                    {"role": "user", "content": fan_text}
                ],
                "max_tokens": 200,
                "temperature": 0.1,
            },
            timeout=20
        )
        if r.status_code == 200:
            raw = r.json()['choices'][0]['message']['content'].strip()
            raw = raw.replace("```json", "").replace("```", "").strip()
            facts = json.loads(raw)
            for fact in facts:
                ft = str(fact.get('fact_type', '')).strip()
                fv = str(fact.get('fact_value', '')).strip()
                if ft and fv and len(fv) > 1:
                    save_fan_fact(chat_id, ft, fv)
    except Exception as e:
        print(f"[WARN] GPT fact extraction failed: {e}")


def save_fan_fact(chat_id, fact_type, fact_value):
    if not fact_value or len(fact_value.strip()) < 2:
        return
    existing = db_query(
        "SELECT 1 FROM fan_facts WHERE chat_id=? AND fact_type=? AND fact_value=?",
        (chat_id, fact_type, fact_value), fetch_one=True)
    if not existing:
        db_query(
            "INSERT INTO fan_facts (chat_id, fact_type, fact_value, discovered_at) VALUES (?, ?, ?, ?)",
            (chat_id, fact_type, fact_value, datetime.now().isoformat()))


def get_fan_facts(chat_id):
    return db_query(
        "SELECT fact_type, fact_value, discovered_at FROM fan_facts WHERE chat_id=? ORDER BY discovered_at DESC",
        (chat_id,)) or []


def get_real_name(chat_id):
    row = db_query(
        "SELECT fact_value FROM fan_facts WHERE chat_id=? AND fact_type='name' ORDER BY discovered_at DESC LIMIT 1",
        (chat_id,), fetch_one=True)
    return row['fact_value'].strip() if row and row.get('fact_value') else ""


# ========== FAN PROFILES ==========
def get_or_create_fan_profile(chat_id, fan_name, handle):
    profile = db_query('SELECT * FROM fan_profiles WHERE chat_id = ?', (chat_id,), fetch_one=True)
    if not profile:
        db_query(
            'INSERT INTO fan_profiles (chat_id, fan_name, handle, total_messages, last_interaction) VALUES (?, ?, ?, 0, ?)',
            (chat_id, fan_name, handle, datetime.now().isoformat()))
        profile = db_query('SELECT * FROM fan_profiles WHERE chat_id = ?', (chat_id,), fetch_one=True)
    else:
        total = (profile.get('total_messages') or 0) + 1
        db_query(
            'UPDATE fan_profiles SET total_messages = ?, last_interaction = ? WHERE chat_id = ?',
            (total, datetime.now().isoformat(), chat_id))
        profile = db_query('SELECT * FROM fan_profiles WHERE chat_id = ?', (chat_id,), fetch_one=True)
    return profile


# ========== MANUAL TAKEOVER ==========
def set_manual_takeover(chat_id):
    """Called when Rafael sends a message manually. Bot stays silent for MANUAL_TAKEOVER_WINDOW."""
    until = (datetime.now(timezone.utc) + MANUAL_TAKEOVER_WINDOW).isoformat()
    db_query("INSERT OR IGNORE INTO fan_profiles (chat_id, fan_name, total_messages, last_interaction) VALUES (?, 'unknown', 0, ?)",
             (chat_id, datetime.now().isoformat()))
    db_query("UPDATE fan_profiles SET manual_takeover_until=? WHERE chat_id=?", (until, chat_id))


def is_in_manual_takeover(chat_id):
    profile = db_query("SELECT manual_takeover_until FROM fan_profiles WHERE chat_id=?", (chat_id,), fetch_one=True)
    if not profile or not profile.get('manual_takeover_until'):
        return False
    until_dt = parse_timestamp(profile['manual_takeover_until'])
    if not until_dt:
        return False
    return datetime.now(timezone.utc) < until_dt


def is_fan_paused(chat_id):
    profile = db_query("SELECT is_paused FROM fan_profiles WHERE chat_id=?", (chat_id,), fetch_one=True)
    return bool(profile and profile.get('is_paused'))


def check_for_manual_message(chat_id, fan_name, api_messages):
    """Detects messages Rafael sent manually, saves them, starts 2-min silent window."""
    if not api_messages:
        return False
    now = datetime.now(timezone.utc)
    found_manual = False
    for msg in api_messages:
        sender_uuid = (msg.get('sender') or {}).get('uuid', '')
        if sender_uuid != MY_UUID:
            continue
        if msg.get('type', '') == 'AUTOMATED_NEW_FOLLOWER':
            continue
        msg_time_str = msg.get('sentAt') or msg.get('createdAt') or msg.get('timestamp') or ''
        msg_dt = parse_timestamp(msg_time_str)
        if not msg_dt or msg_dt <= BOOT_TIME_UTC:
            continue
        if (now - msg_dt) > timedelta(minutes=5):
            continue
        msg_id = msg.get('uuid') or ''
        text = (msg.get('text') or '').strip()
        save_message_to_db(msg_id, chat_id, fan_name, MY_UUID, text, msg_time_str, is_mine=True)
        set_manual_takeover(chat_id)
        found_manual = True
    return found_manual



    """
    Detects messages Rafael sent himself (not via the bot) and saves them to
    history + (re)starts the 2-minute silent window.
    A message is "manual" if it's from MY_UUID, recent (last 5 min), and after boot
    (so we don't re-trigger on old history).
    """
    if not api_messages:
        return False
    now = datetime.now(timezone.utc)
    found_manual = False
    for msg in api_messages:
        sender_uuid = (msg.get('sender') or {}).get('uuid', '')
        if sender_uuid != MY_UUID:
            continue
        if msg.get('type', '') == 'AUTOMATED_NEW_FOLLOWER':
            continue
        msg_time_str = msg.get('sentAt') or msg.get('createdAt') or msg.get('timestamp') or ''
        msg_dt = parse_timestamp(msg_time_str)
        if not msg_dt or msg_dt <= BOOT_TIME_UTC:
            continue
        if (now - msg_dt) > timedelta(minutes=5):
            continue

        msg_id = msg.get('uuid') or ''
        text = (msg.get('text') or '').strip()
        save_message_to_db(msg_id, chat_id, fan_name, MY_UUID, text, msg_time_str, is_mine=True)
        set_manual_takeover(chat_id)
        found_manual = True
    return found_manual


# ========== SCHEDULED REPLIES (BATCHED) ==========
def schedule_or_extend_batch(chat_id, fan_name, fan_msg_id, fan_text):
    existing = db_query(
        "SELECT * FROM scheduled_replies WHERE chat_id=? AND status='pending' ORDER BY id DESC LIMIT 1",
        (chat_id,), fetch_one=True)
    now = datetime.now()
    fan_text = fan_text or ''

    if existing:
        existing_text = existing.get('fan_text') or ''
        if fan_text.strip() and fan_text.strip() not in existing_text:
            combined = existing_text + "\n[+] " + fan_text
            new_deadline = (now + timedelta(seconds=BATCH_WINDOW)).isoformat()
            db_query(
                "UPDATE scheduled_replies SET fan_text=?, fan_msg_id=?, scheduled_time=? WHERE id=?",
                (combined, fan_msg_id, new_deadline, existing['id']))
    else:
        batch_deadline = (now + timedelta(seconds=BATCH_WINDOW)).isoformat()
        db_query(
            '''INSERT INTO scheduled_replies (chat_id, fan_name, fan_msg_id, fan_text, scheduled_time, created_at)
               VALUES (?, ?, ?, ?, ?, ?)''',
            (chat_id, fan_name, fan_msg_id, fan_text, batch_deadline, now.isoformat()))


def get_due_batches():
    return db_query(
        "SELECT * FROM scheduled_replies WHERE status='pending' AND scheduled_time<=? ORDER BY scheduled_time ASC",
        (datetime.now().isoformat(),)) or []


def mark_batch_sent(batch_id):
    db_query("UPDATE scheduled_replies SET status='sent' WHERE id=?", (batch_id,))


# ========== SYSTEM PROMPT ==========
def should_greet(db_history, fan_msg_time_str):
    fan_msgs = [m for m in db_history if not m.get('is_mine')]
    if len(fan_msgs) <= 1:
        return True
    if db_history and fan_msg_time_str:
        try:
            last_time = parse_timestamp(db_history[-2].get('timestamp', ''))
            this_time = parse_timestamp(fan_msg_time_str)
            if last_time and this_time:
                gap_hours = (this_time - last_time).total_seconds() / 3600
                if gap_hours > 2:
                    return True
        except Exception:
            pass
    return False


def get_greeting_instruction(db_history, fan_msg_time_str):
    if should_greet(db_history, fan_msg_time_str):
        return "EZ ÚJ/ÚJRAINDULT BESZÉLGETÉS. Kezdj lazán, pl: 'heyy' vagy 'szia, mizu' — variálj, ne mindig ugyanazzal!"
    cont = random.choice(CONTINUATION_VARIATIONS)
    return f"EZ A BESZÉLGETÉS FOLYTATÁSA. NE köszönj újra! Kezdj valami '{cont}'-szerűvel vagy egyből a lényegre."


def build_system_prompt(chat_id, fan_name, real_name, fan_facts_list, db_history, time_ctx, fan_msg_time_str):
    display_name = real_name or "a fan"
    prompt = JAZMIN_PERSONALITY + "\n\n"
    prompt += f"KONTEXTUS:\n- {time_ctx}\n- Ha a fan kérdezi mit csinálsz / hol vagy — ez alapján válaszolj természetesen. Ha munkában vagy, az állatokkal foglalkozol. Ha szabad napod van (vasárnap), pihensz/lazítasz. Ha este van, otthon vagy.\n\n"

    if fan_facts_list:
        prompt += "AMIT TUDSZ ERRŐL A FANRÓL (ne kérdezd újra!):\n"
        for fact in fan_facts_list[:15]:
            prompt += f"- {fact['fact_type']}: {fact['fact_value']}\n"
        prompt += "\n"

    if db_history:
        prompt += "TELJES EDDIGI BESZÉLGETÉS (legújabb alul — OLVASD EL MINDET, és NE ismételd a saját korábbi mondataidat):\n"
        for msg in db_history[-20:]:
            sender = "Jázmin" if msg.get('is_mine') else display_name
            text = (msg.get('text') or '').strip()
            if text:
                prompt += f"{sender}: {text}\n"
        prompt += "\n"

    total_msgs = len(db_history)
    prompt += f"A fan neve: {display_name}\n"
    if not real_name:
        prompt += "NEM TUDOD A VALÓDI NEVÉT — ne szólítsd névvel.\n"
    if total_msgs < 6:
        prompt += "⚠️ ÚJ FAN — most ismerkedtek, légy barátságos és kíváncsi.\n"

    prompt += f"\n{get_greeting_instruction(db_history, fan_msg_time_str)}\n"
    prompt += "\nEGYETLEN rövid, természetes üzenetet írj vissza, magyarul. 1-2 mondat max. Ha a fan szomorú/nehéz dolgot ír — ELŐSZÖR arra reagálj."
    return prompt


# ========== GPT REPLY ==========
def ask_openai(system_prompt, user_text):
    for attempt in range(3):
        try:
            r = requests.post(
                "https://api.openai.com/v1/chat/completions",
                headers={"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"},
                json={
                    "model": OPENAI_MODEL,
                    "messages": [
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_text}
                    ],
                    "max_tokens": 120,
                    "temperature": 0.9,
                    "presence_penalty": 0.5,
                    "frequency_penalty": 0.4,
                },
                timeout=30
            )
            if r.status_code == 200:
                reply = r.json()['choices'][0]['message']['content'].strip()
                if reply.startswith('"') and reply.endswith('"'):
                    reply = reply[1:-1].strip()
                return reply
            elif r.status_code == 429:
                wait = 10 * (attempt + 1)
                print(f"[WARN] OpenAI 429 rate limit — waiting {wait}s (attempt {attempt+1}/3)")
                time.sleep(wait)
                continue
            else:
                send_telegram_error(f"OpenAI error {r.status_code}: {r.text[:200]}")
                return ""
        except Exception as e:
            send_telegram_error(f"OpenAI request failed: {e}")
            return ""
    send_telegram_error("OpenAI 429 — all 3 retries failed, skipping reply")
    return ""


# ========== MESSAGE PROCESSING ==========
def process_new_messages():
    chats, status = get_chats()
    if not chats:
        return 0, status
    scheduled = 0

    for chat in chats:
        chat_id = None
        try:
            user = chat.get('user', {}) or {}
            chat_id = user.get('uuid') or chat.get('uuid') or chat.get('id')
            if not chat_id:
                continue

            fan_name = user.get('displayName', 'ismeretlen') or 'ismeretlen'
            handle = user.get('handle', '') or ''
            get_or_create_fan_profile(chat_id, fan_name, handle)

            api_messages = get_messages(chat_id)
            if not api_messages:
                continue

            # === SAVE ALL MESSAGES TO SQLITE (permanent memory) ===
            for msg in api_messages:
                msg_id = msg.get('uuid') or ''
                sender_uuid = (msg.get('sender') or {}).get('uuid', '')
                text_raw = msg.get('text', '') or ''
                msg_time = msg.get('createdAt') or msg.get('sentAt') or msg.get('timestamp') or ''
                is_mine = (sender_uuid == MY_UUID)
                save_message_to_db(msg_id, chat_id, fan_name, sender_uuid, text_raw, msg_time, is_mine)

                if not is_mine and text_raw.strip():
                    threading.Thread(
                        target=extract_facts_with_gpt,
                        args=(chat_id, text_raw),
                        daemon=True
                    ).start()

            # === MANUAL TAKEOVER / PAUSED CHECK ===
            check_for_manual_message(chat_id, fan_name, api_messages)
            if is_in_manual_takeover(chat_id) or is_fan_paused(chat_id):
                continue

            # === FIND LATEST UNPROCESSED FAN MESSAGE ===
            fan_msgs = [m for m in api_messages if (m.get('sender') or {}).get('uuid') != MY_UUID]
            if not fan_msgs:
                continue

            last_msg = fan_msgs[0]
            msg_id = last_msg.get('uuid') or ''
            text = (last_msg.get('text') or '').strip()

            if is_emoji_or_nonsense(text):
                if msg_id:
                    db_query('UPDATE messages SET was_replied=1 WHERE msg_id=?', (msg_id,))
                continue

            msg_time = (last_msg.get('createdAt') or last_msg.get('sentAt') or
                        last_msg.get('timestamp') or last_msg.get('created_at') or '')
            msg_dt = parse_timestamp(msg_time)
            if msg_dt:
                now = datetime.now(timezone.utc)
                age_hours = (now - msg_dt).total_seconds() / 3600
                if age_hours > 24:
                    continue

            existing = db_query('SELECT 1 FROM messages WHERE msg_id=? AND was_replied=1', (msg_id,), fetch_one=True)
            already_batched = db_query('SELECT 1 FROM scheduled_replies WHERE chat_id=? AND status=?', (chat_id, 'pending'), fetch_one=True)
            if existing or already_batched:
                continue

            schedule_or_extend_batch(chat_id, fan_name, msg_id, text)
            scheduled += 1

        except Exception as e:
            print(f"[{datetime.now()}] Process error for {chat_id}: {e}")
            send_telegram_error(f"Process error for {chat_id}: {e}")

    return scheduled, "OK"


# ========== SEND DUE BATCHES ==========
_fan_sending = set()


def send_due_batches():
    if get_safe_mode():
        return 0  # safe mode on — process & batch everything, but send nothing
    due = get_due_batches()
    if not due:
        return 0
    sent = 0
    already_sent_to = set()

    for item in due:
        chat_id = None
        try:
            chat_id = item['chat_id']
            fan_name = item['fan_name'] or 'unknown'
            fan_msg_id = item['fan_msg_id']
            combined_text = item['fan_text'] or ''
            batch_id = item['id']

            if chat_id in already_sent_to or chat_id in _fan_sending:
                db_query("UPDATE scheduled_replies SET status='cancelled' WHERE id=? AND status='pending'", (batch_id,))
                continue
            already_sent_to.add(chat_id)

            if is_in_manual_takeover(chat_id) or is_fan_paused(chat_id):
                db_query("UPDATE scheduled_replies SET status='cancelled' WHERE id=?", (batch_id,))
                continue

            # Cancel duplicate pending batches for same fan
            db_query("UPDATE scheduled_replies SET status='cancelled' WHERE chat_id=? AND id!=? AND status='pending'",
                     (chat_id, batch_id))

            # === BUILD CONTEXT FROM SQLITE ===
            db_history = get_full_history_from_db(chat_id, limit=100)
            fan_facts_list = get_fan_facts(chat_id)
            real_name = get_real_name(chat_id)
            time_ctx = get_time_context()

            fan_msgs_in_history = [m for m in db_history if not m.get('is_mine')]
            fan_msg_time_str = fan_msgs_in_history[-1]['timestamp'] if fan_msgs_in_history else ''

            system_prompt = build_system_prompt(
                chat_id, fan_name, real_name, fan_facts_list, db_history, time_ctx, fan_msg_time_str
            )

            # Clean up combined batch text for GPT input
            raw_lines = combined_text.replace("[+] ", "\n").split("\n")
            seen = []
            for line in raw_lines:
                line = line.strip()
                if line and line not in seen:
                    seen.append(line)
            clean_fan_text = "\n".join(seen)

            if len(seen) > 1:
                gpt_user_msg = f"A fan {len(seen)} üzenetet küldött egymás után. Mindegyikre reagálj egyetlen válaszban:\n\n" + "\n".join(f"- {s}" for s in seen)
            else:
                gpt_user_msg = clean_fan_text

            reply = ask_openai(system_prompt, gpt_user_msg)
            if not reply or not reply.strip():
                continue

            # Atomic claim — only proceed if batch is still pending (prevents double-send across workers)
            db_query("UPDATE scheduled_replies SET status='sending' WHERE id=? AND status='pending'", (batch_id,))
            claimed = db_query("SELECT 1 FROM scheduled_replies WHERE id=? AND status='sending'", (batch_id,), fetch_one=True)
            if not claimed:
                continue  # another worker already claimed it
            db_query('UPDATE messages SET was_replied=1 WHERE msg_id=?', (fan_msg_id,))
            _fan_sending.add(chat_id)

            # Realistic typing delay
            words = len(reply.split())
            send_delay = (random.uniform(3, 8) if words <= 5
                          else random.uniform(6, 18) if words <= 15
                          else random.uniform(10, 25))
            time.sleep(send_delay)

            if send_fanvue_message(chat_id, reply):
                mark_batch_sent(batch_id)
                now_iso = datetime.now().isoformat()
                save_message_to_db(
                    f"bot_{now_iso}_{chat_id}", chat_id, fan_name, MY_UUID, reply, now_iso, is_mine=True)
                sent += 1

            _fan_sending.discard(chat_id)

        except Exception as e:
            if chat_id:
                _fan_sending.discard(chat_id)
            print(f"[{datetime.now()}] Send error: {e}")
            send_telegram_error(f"Send error for {chat_id}: {e}")

    return sent


# ========== POLLING ==========
polling_thread = None
polling_active = False


def poll_loop():
    global polling_active
    polling_active = True
    consecutive_errors = 0
    while polling_active:
        try:
            if get_fanvue_token():
                sent = send_due_batches()
                scheduled, status = process_new_messages()
                if sent > 0 or scheduled > 0:
                    print(f"[{datetime.now()}] Sent={sent} Scheduled={scheduled}")
                consecutive_errors = 0
            else:
                print(f"[{datetime.now()}] No valid token — skipping poll")
        except Exception as e:
            consecutive_errors += 1
            print(f"[{datetime.now()}] Poll error #{consecutive_errors}: {e}")
            if consecutive_errors <= 3:
                send_telegram_error(f"Poll loop error #{consecutive_errors}: {e}")
        try:
            time.sleep(POLL_INTERVAL)
        except Exception:
            pass


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


# ========== FLASK ROUTES ==========
@app.route('/pause/<chat_id>')
def pause_fan(chat_id):
    db_query("INSERT OR IGNORE INTO fan_profiles (chat_id, fan_name, total_messages, last_interaction, is_paused) VALUES (?, 'unknown', 0, ?, 0)",
             (chat_id, datetime.now().isoformat()))
    db_query("UPDATE fan_profiles SET is_paused=1 WHERE chat_id=?", (chat_id,))
    db_query("UPDATE scheduled_replies SET status='cancelled' WHERE chat_id=? AND status='pending'", (chat_id,))
    profile = db_query("SELECT fan_name FROM fan_profiles WHERE chat_id=?", (chat_id,), fetch_one=True)
    name = profile['fan_name'] if profile else chat_id
    return {"paused": True, "fan": name, "chat_id": chat_id}, 200


@app.route('/resume/<chat_id>')
def resume_fan(chat_id):
    db_query("UPDATE fan_profiles SET is_paused=0 WHERE chat_id=?", (chat_id,))
    profile = db_query("SELECT fan_name FROM fan_profiles WHERE chat_id=?", (chat_id,), fetch_one=True)
    name = profile['fan_name'] if profile else chat_id
    return {"paused": False, "fan": name, "chat_id": chat_id}, 200


@app.route('/fans')
def list_fans():
    fans = db_query("SELECT chat_id, fan_name, handle, is_paused, total_messages, last_interaction FROM fan_profiles ORDER BY last_interaction DESC") or []
    return {"fans": fans, "total": len(fans)}, 200



def safe_mode_off():
    set_safe_mode(False)
    print(f"[{datetime.now()}] SAFE MODE OFF — bot will now send messages")
    return {"safe_mode": False, "status": "Bot is now live and sending messages"}, 200


@app.route('/safe_mode/on')
def safe_mode_on():
    set_safe_mode(True)
    print(f"[{datetime.now()}] SAFE MODE ON — bot silenced")
    return {"safe_mode": True, "status": "Bot silenced — processing only, no sends"}, 200


@app.route('/')
def home():
    return "Jazmin Bot v9.0 running ✅", 200


@app.route('/callback')
def callback():
    auth_code = request.args.get('code')
    return (f"Code: {auth_code[:30]}...", 200) if auth_code else ("No code", 400)


@app.route('/set_token', methods=['POST'])
def set_token():
    data = request.json or {}
    refresh = data.get('refresh_token')
    if refresh:
        save_token('refresh_token', refresh)
        access, msg = refresh_fanvue_token()
        return {"saved": True, "test": msg, "access_preview": (access[:20] + "...") if access else None}
    return {"error": "No refresh_token"}, 400


@app.route('/trigger')
def trigger():
    token = get_fanvue_token()
    if not token:
        return {"error": "No token"}, 400
    sent = send_due_batches()
    scheduled, status = process_new_messages()
    return {"sent": sent, "scheduled": scheduled, "status": status}, 200


@app.route('/status')
def status():
    pending = db_query("SELECT COUNT(*) as c FROM scheduled_replies WHERE status='pending'", fetch_one=True)
    return {
        "token_valid": get_fanvue_token() is not None,
        "polling_active": polling_active,
        "safe_mode": get_safe_mode(),
        "pending_batches": pending['c'] if pending else 0,
    }, 200


@app.route('/start_poll')
def start_poll():
    return {"started": start_polling(), "polling_active": polling_active}


@app.route('/stop_poll')
def stop_poll():
    return {"stopped": stop_polling(), "polling_active": polling_active}


# ========== INIT & BOOT ==========
try:
    init_db()
    print(f"[OK] DB initialized at {DB_PATH}")
except Exception as _e:
    print(f"[ERROR] init_db failed: {_e}")

try:
    _env_refresh = os.environ.get('FANVUE_REFRESH_TOKEN', '').strip()
    if _env_refresh:
        existing = load_token('refresh_token')
        if not existing or existing != _env_refresh:
            save_token('refresh_token', _env_refresh)
            refresh_fanvue_token()
            print("[OK] Auto-loaded refresh token from env")
    else:
        print("[WARN] FANVUE_REFRESH_TOKEN env var not set — set it in Railway variables")
except Exception as _e:
    print(f"[ERROR] Token boot failed: {_e}")

try:
    start_polling()
    print("[OK] Polling auto-started on boot")
except Exception as _e:
    print(f"[ERROR] start_polling failed: {_e}")

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))
