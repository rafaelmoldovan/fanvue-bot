"""
Jazmin Fanvue Bot — v10.0 (clean, audited)

- Bot handles ALL conversation. No content sales.
- Persona: 19yo girl, works at an animal shelter in Hungary.
- Uses gpt-4.1 for replies, gpt-4.1-mini for fact extraction.
- Manual takeover: Rafael sends a message → bot silent 2 min (sliding), auto-resumes.
- Permanent pause/resume per fan via webhook or dashboard.
- Per-fan AI directive: custom instruction from dashboard injected into system prompt.
- Safe mode: always boots ON. Turn off via /safe_mode/off (stored in DB, works across workers).
- Telegram: errors only.
- Dashboard: Iron Man UI at /dashboard?pw=<DASHBOARD_PASSWORD>
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

# ─────────────────────────────────────────────────────────────────────────────
# TIMEZONE
# ─────────────────────────────────────────────────────────────────────────────
BUDAPEST_TZ   = ZoneInfo('Europe/Budapest')
BOOT_TIME_UTC = datetime.now(timezone.utc)
print(f"[{datetime.now()}] BOT BOOTED at {BOOT_TIME_UTC.isoformat()} UTC")


def get_budapest_now():
    return datetime.now(BUDAPEST_TZ).replace(tzinfo=None)


# ─────────────────────────────────────────────────────────────────────────────
# APP + CONFIG
# ─────────────────────────────────────────────────────────────────────────────
app = Flask(__name__)

FANVUE_CLIENT_ID     = os.environ.get('FANVUE_CLIENT_ID', '')
FANVUE_CLIENT_SECRET = os.environ.get('FANVUE_CLIENT_SECRET', '')
OPENAI_API_KEY       = os.environ.get('OPENAI_API_KEY', '')
MY_UUID              = os.environ.get('MY_UUID', '38a392fc-a751-49b3-9d74-01ac6447c490')
TELEGRAM_BOT_TOKEN   = os.environ.get('TELEGRAM_BOT_TOKEN', '')
TELEGRAM_CHAT_ID     = os.environ.get('TELEGRAM_CHAT_ID', '')
DASHBOARD_PASSWORD   = os.environ.get('DASHBOARD_PASSWORD', 'jazmin2024')

OPENAI_MODEL          = 'gpt-4.1'
OPENAI_MINI_MODEL     = 'gpt-4.1-mini'
POLL_INTERVAL         = 20          # seconds between poll cycles
BATCH_WINDOW          = 60          # seconds to wait before firing a reply
MANUAL_TAKEOVER_SECS  = 120         # 2 minutes silent after Rafael's manual message


# ─────────────────────────────────────────────────────────────────────────────
# DB PATH
# ─────────────────────────────────────────────────────────────────────────────
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


# ─────────────────────────────────────────────────────────────────────────────
# TELEGRAM (errors only)
# ─────────────────────────────────────────────────────────────────────────────
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
        print(f"[WARN] Telegram send failed: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# SQLITE
# ─────────────────────────────────────────────────────────────────────────────
def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    c.execute('''CREATE TABLE IF NOT EXISTS tokens
                 (key TEXT PRIMARY KEY, value TEXT)''')

    c.execute('''CREATE TABLE IF NOT EXISTS messages (
        msg_id       TEXT PRIMARY KEY,
        chat_id      TEXT,
        fan_name     TEXT,
        sender_uuid  TEXT,
        text         TEXT,
        timestamp    TEXT,
        was_replied  INTEGER DEFAULT 0,
        is_mine      INTEGER DEFAULT 0,
        facts_done   INTEGER DEFAULT 0
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS fan_profiles (
        chat_id               TEXT PRIMARY KEY,
        fan_name              TEXT,
        handle                TEXT,
        total_messages        INTEGER DEFAULT 0,
        last_interaction      TEXT,
        manual_takeover_until TEXT,
        is_paused             INTEGER DEFAULT 0,
        fan_note              TEXT
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS scheduled_replies (
        id             INTEGER PRIMARY KEY AUTOINCREMENT,
        chat_id        TEXT,
        fan_name       TEXT,
        fan_msg_id     TEXT,
        fan_text       TEXT,
        scheduled_time TEXT,
        status         TEXT DEFAULT 'pending',
        created_at     TEXT
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS fan_facts (
        id           INTEGER PRIMARY KEY AUTOINCREMENT,
        chat_id      TEXT,
        fact_type    TEXT,
        fact_value   TEXT,
        discovered_at TEXT
    )''')

    # ── Migrations (safe to run on existing DB) ──
    migrations = [
        'ALTER TABLE fan_profiles ADD COLUMN manual_takeover_until TEXT',
        'ALTER TABLE fan_profiles ADD COLUMN is_paused INTEGER DEFAULT 0',
        'ALTER TABLE fan_profiles ADD COLUMN fan_note TEXT',
        'ALTER TABLE messages ADD COLUMN facts_done INTEGER DEFAULT 0',
    ]
    for sql in migrations:
        try:
            c.execute(sql)
            conn.commit()
        except Exception:
            pass  # column already exists

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


# ─────────────────────────────────────────────────────────────────────────────
# TOKENS + SAFE MODE
# ─────────────────────────────────────────────────────────────────────────────
def save_token(key, value):
    db_query('INSERT OR REPLACE INTO tokens (key, value) VALUES (?, ?)', (key, value))


def load_token(key):
    row = db_query('SELECT value FROM tokens WHERE key = ?', (key,), fetch_one=True)
    return row['value'] if row else None


def get_safe_mode():
    """DB-backed — works across all gunicorn workers."""
    row = db_query('SELECT value FROM tokens WHERE key = ?', ('safe_mode',), fetch_one=True)
    if row is None:
        db_query('INSERT OR REPLACE INTO tokens (key, value) VALUES (?, ?)', ('safe_mode', 'on'))
        return True
    return row['value'] == 'on'


def set_safe_mode(on: bool):
    db_query('INSERT OR REPLACE INTO tokens (key, value) VALUES (?, ?)',
             ('safe_mode', 'on' if on else 'off'))


# ─────────────────────────────────────────────────────────────────────────────
# FANVUE AUTH
# ─────────────────────────────────────────────────────────────────────────────
def get_basic_auth_header():
    creds   = f"{FANVUE_CLIENT_ID}:{FANVUE_CLIENT_SECRET}"
    encoded = base64.b64encode(creds.encode()).decode()
    return f"Basic {encoded}"


def refresh_fanvue_token():
    rt = load_token('refresh_token')
    if not rt:
        return None, "No refresh token"
    try:
        r = requests.post(
            "https://auth.fanvue.com/oauth2/token",
            data={"grant_type": "refresh_token", "refresh_token": rt},
            headers={"Content-Type": "application/x-www-form-urlencoded",
                     "Authorization": get_basic_auth_header()},
            timeout=15
        )
        if r.status_code == 200:
            data       = r.json()
            access     = data.get('access_token')
            new_rt     = data.get('refresh_token', rt)
            expires_in = data.get('expires_in', 3600)
            expires_at = (datetime.now() + timedelta(seconds=expires_in - 300)).isoformat()
            save_token('refresh_token', new_rt)
            save_token('access_token',  access)
            save_token('expires_at',    expires_at)
            return access, "OK"
        return None, f"Refresh failed: {r.status_code}"
    except Exception as e:
        return None, f"Error: {e}"


def get_fanvue_token():
    access  = load_token('access_token')
    expires = load_token('expires_at')
    if access and expires:
        try:
            if datetime.now() < datetime.fromisoformat(expires):
                return access
        except Exception:
            pass
    return refresh_fanvue_token()[0]


# ─────────────────────────────────────────────────────────────────────────────
# FANVUE API
# ─────────────────────────────────────────────────────────────────────────────
def get_headers():
    token = get_fanvue_token()
    return {
        "Authorization": f"Bearer {token or ''}",
        "X-Fanvue-API-Version": "2025-06-26",
        "Content-Type": "application/json",
    }


def get_chats():
    try:
        all_chats, page = [], 1
        while True:
            r = requests.get(f"https://api.fanvue.com/chats?page={page}&limit=50",
                             headers=get_headers(), timeout=10)
            if r.status_code == 401:
                refresh_fanvue_token()
                r = requests.get(f"https://api.fanvue.com/chats?page={page}&limit=50",
                                 headers=get_headers(), timeout=10)
            if r.status_code != 200:
                break
            chats = r.json().get('data', [])
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
        r = requests.post(
            f"https://api.fanvue.com/chats/{chat_id}/message",
            headers=get_headers(), json={"text": text}, timeout=10
        )
        return r.status_code in (200, 201)
    except Exception as e:
        send_telegram_error(f"send_fanvue_message failed for {chat_id}: {e}")
        return False


# ─────────────────────────────────────────────────────────────────────────────
# PERSONALITY
# ─────────────────────────────────────────────────────────────────────────────
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

Variáció: Nézd át az eddigi SAJÁT üzeneteidet a chat historyban — sosem kezdesz ugyanolyan szóval/kifejezéssel mint amit már használtál. MINDIG MÁS HANGULAT, MÁS MEGKÖZELÍTÉS.

Ha kérdezel: EGY kérdés egyszerre, soha kettő.

KRITIKUS: Ha az előző üzeneted kérdés volt és a fan még nem válaszolt — NE kérdezz mást, reagálj/kommentálj.

━━━━━━━━━━━━━━━━━━━━━━━
HA NEM ÉRTED A FAN ÜZENETÉT:
━━━━━━━━━━━━━━━━━━━━━━━
Ha a fan üzenete homályos, rövid, vagy nem világos mit akar — NE találgass. Kérdezz vissza természetesen: "várj, ezt hogy érted? 😄" vagy "ezt most komolyan mondod vagy viccelsz" — lazán, nem gépiesen.

━━━━━━━━━━━━━━━━━━━━━━━
HANGULAT / ENERGIA KÖVETÉSE:
━━━━━━━━━━━━━━━━━━━━━━━
Kövesd a fan energiáját:
- Ha vicces/lazán dumál → te is lazán, humoros
- Ha komoly dolgot ír → te is figyelmes, empatikus
- Ha flörtöl / kicsit pikáns → mehetsz te is abba az irányba, lazán, játékosan, de NEM explicit. Visszaflörtölhetsz, utalgathatod, titokzatos lehetsz. Pl: "hm, ilyen kíváncsi vagy 😏" / "ezt most te mondtad, nem én 😄"
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


# ─────────────────────────────────────────────────────────────────────────────
# UTILS
# ─────────────────────────────────────────────────────────────────────────────
def parse_timestamp(ts_str):
    if not ts_str:
        return None
    for fmt in ["%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%S.%f",
                "%Y-%m-%dT%H:%M:%SZ",     "%Y-%m-%dT%H:%M:%S"]:
        try:
            return datetime.strptime(ts_str, fmt).replace(tzinfo=timezone.utc)
        except Exception:
            continue
    try:
        fixed = ts_str.replace('Z', '+00:00')
        dt    = datetime.fromisoformat(fixed)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None


def is_emoji_or_nonsense(text):
    if not text:
        return False
    cleaned = text.strip()
    for ch in " \t\n\r.,!?;:-_()[]":
        cleaned = cleaned.replace(ch, "")
    if not cleaned:
        return True
    return not any(c.isalpha() for c in cleaned)


def get_time_context():
    now     = get_budapest_now()
    hour    = now.hour
    weekday = now.weekday()
    names   = ["hétfő","kedd","szerda","csütörtök","péntek","szombat","vasárnap"]
    day_hu  = names[weekday]
    t_str   = now.strftime("%H:%M")

    if weekday == 6:
        return "Ma vasárnap van — nem dolgozol, szabad napod van. Pihensz, lazítasz."
    elif 8 <= hour < 18:
        return f"Most {day_hu} van, {t_str} — épp munkában vagy a menhelyen (8-18h között dolgozol)."
    elif hour < 8:
        return f"Most {day_hu} van, {t_str} — még nem kezdtél el dolgozni, reggel indulsz."
    else:
        return f"Most {day_hu} van, {t_str} — már hazaértél a munkából, este van."


# ─────────────────────────────────────────────────────────────────────────────
# MESSAGE STORAGE
# ─────────────────────────────────────────────────────────────────────────────
def save_message_to_db(msg_id, chat_id, fan_name, sender_uuid, text, timestamp,
                       is_mine=False, facts_done=0):
    if not msg_id:
        return
    db_query(
        """INSERT OR IGNORE INTO messages
           (msg_id, chat_id, fan_name, sender_uuid, text, timestamp, is_mine, facts_done)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (msg_id, chat_id, fan_name, sender_uuid, text or '', timestamp,
         1 if is_mine else 0, facts_done)
    )


def get_full_history_from_db(chat_id, limit=100):
    return db_query(
        "SELECT text, is_mine, timestamp FROM messages WHERE chat_id=? ORDER BY timestamp ASC LIMIT ?",
        (chat_id, limit)
    ) or []


# ─────────────────────────────────────────────────────────────────────────────
# FAN FACTS — only runs on genuinely new messages (facts_done=0)
# ─────────────────────────────────────────────────────────────────────────────
def extract_facts_with_gpt(msg_id, chat_id, fan_text):
    """Runs in background thread. Only called for messages where facts_done=0."""
    if not fan_text or len(fan_text.strip()) < 10:
        db_query('UPDATE messages SET facts_done=1 WHERE msg_id=?', (msg_id,))
        return
    try:
        system = (
            "You extract personal facts from a fan's message in a chat. "
            "Return ONLY a JSON array of objects with keys 'fact_type' and 'fact_value'. "
            "fact_type can be: name, job, location, age, relationship, hobby, family, stress, interest, language, pet. "
            "Only include facts clearly stated. If nothing found, return []. "
            "No preamble, no markdown, ONLY the JSON array."
        )
        r = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers={"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"},
            json={
                "model": OPENAI_MINI_MODEL,
                "messages": [
                    {"role": "system", "content": system},
                    {"role": "user",   "content": fan_text}
                ],
                "max_tokens": 200,
                "temperature": 0.1,
            },
            timeout=20
        )
        if r.status_code == 200:
            raw   = r.json()['choices'][0]['message']['content'].strip()
            raw   = raw.replace("```json", "").replace("```", "").strip()
            facts = json.loads(raw)
            for fact in facts:
                ft = str(fact.get('fact_type',  '')).strip()
                fv = str(fact.get('fact_value', '')).strip()
                if ft and fv and len(fv) > 1:
                    _save_fan_fact(chat_id, ft, fv)
    except Exception as e:
        print(f"[WARN] GPT fact extraction failed: {e}")
    finally:
        # Always mark as done so we never retry
        db_query('UPDATE messages SET facts_done=1 WHERE msg_id=?', (msg_id,))


def _save_fan_fact(chat_id, fact_type, fact_value):
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
        "SELECT fact_type, fact_value FROM fan_facts WHERE chat_id=? ORDER BY discovered_at DESC",
        (chat_id,)
    ) or []


def get_real_name(chat_id):
    row = db_query(
        "SELECT fact_value FROM fan_facts WHERE chat_id=? AND fact_type='name' ORDER BY discovered_at DESC LIMIT 1",
        (chat_id,), fetch_one=True)
    return row['fact_value'].strip() if row and row.get('fact_value') else ""


# ─────────────────────────────────────────────────────────────────────────────
# FAN PROFILES
# ─────────────────────────────────────────────────────────────────────────────
def get_or_create_fan_profile(chat_id, fan_name, handle):
    profile = db_query('SELECT * FROM fan_profiles WHERE chat_id=?', (chat_id,), fetch_one=True)
    if not profile:
        db_query(
            'INSERT INTO fan_profiles (chat_id, fan_name, handle, total_messages, last_interaction) VALUES (?, ?, ?, 0, ?)',
            (chat_id, fan_name, handle, datetime.now().isoformat()))
    else:
        total = (profile.get('total_messages') or 0) + 1
        db_query(
            'UPDATE fan_profiles SET total_messages=?, last_interaction=?, fan_name=?, handle=? WHERE chat_id=?',
            (total, datetime.now().isoformat(), fan_name, handle, chat_id))


# ─────────────────────────────────────────────────────────────────────────────
# PAUSE / MANUAL TAKEOVER
# ─────────────────────────────────────────────────────────────────────────────
def is_fan_paused(chat_id):
    row = db_query("SELECT is_paused FROM fan_profiles WHERE chat_id=?", (chat_id,), fetch_one=True)
    return bool(row and row.get('is_paused'))


def set_manual_takeover(chat_id):
    until = (datetime.now(timezone.utc) + timedelta(seconds=MANUAL_TAKEOVER_SECS)).isoformat()
    db_query(
        "INSERT OR IGNORE INTO fan_profiles (chat_id, fan_name, total_messages, last_interaction) VALUES (?, 'unknown', 0, ?)",
        (chat_id, datetime.now().isoformat()))
    db_query("UPDATE fan_profiles SET manual_takeover_until=? WHERE chat_id=?", (until, chat_id))


def is_in_manual_takeover(chat_id):
    row = db_query("SELECT manual_takeover_until FROM fan_profiles WHERE chat_id=?", (chat_id,), fetch_one=True)
    if not row or not row.get('manual_takeover_until'):
        return False
    until_dt = parse_timestamp(row['manual_takeover_until'])
    return bool(until_dt and datetime.now(timezone.utc) < until_dt)


def check_for_manual_message(chat_id, fan_name, api_messages):
    """
    Detects messages Rafael sent himself via Fanvue (not the bot).
    If found within the last 5 min and after boot → start/extend 2-min silent window.
    """
    if not api_messages:
        return False
    now          = datetime.now(timezone.utc)
    found_manual = False
    for msg in api_messages:
        sender_uuid = (msg.get('sender') or {}).get('uuid', '')
        if sender_uuid != MY_UUID:
            continue
        if msg.get('type', '') == 'AUTOMATED_NEW_FOLLOWER':
            continue
        ts_str = msg.get('sentAt') or msg.get('createdAt') or msg.get('timestamp') or ''
        msg_dt = parse_timestamp(ts_str)
        if not msg_dt or msg_dt <= BOOT_TIME_UTC:
            continue
        if (now - msg_dt) > timedelta(minutes=5):
            continue
        msg_id = msg.get('uuid') or ''
        text   = (msg.get('text') or '').strip()
        save_message_to_db(msg_id, chat_id, fan_name, MY_UUID, text, ts_str,
                           is_mine=True, facts_done=1)
        set_manual_takeover(chat_id)
        found_manual = True
    return found_manual


def should_bot_skip(chat_id):
    """Returns True if the bot should not reply to this fan right now."""
    return is_fan_paused(chat_id) or is_in_manual_takeover(chat_id)


# ─────────────────────────────────────────────────────────────────────────────
# SCHEDULED REPLY BATCHING
# ─────────────────────────────────────────────────────────────────────────────
def schedule_or_extend_batch(chat_id, fan_name, fan_msg_id, fan_text):
    existing = db_query(
        "SELECT * FROM scheduled_replies WHERE chat_id=? AND status='pending' ORDER BY id DESC LIMIT 1",
        (chat_id,), fetch_one=True)
    now      = datetime.now()
    fan_text = fan_text or ''

    if existing:
        existing_text = existing.get('fan_text') or ''
        if fan_text.strip() and fan_text.strip() not in existing_text:
            combined     = existing_text + "\n[+] " + fan_text
            new_deadline = (now + timedelta(seconds=BATCH_WINDOW)).isoformat()
            db_query(
                "UPDATE scheduled_replies SET fan_text=?, fan_msg_id=?, scheduled_time=? WHERE id=?",
                (combined, fan_msg_id, new_deadline, existing['id']))
    else:
        deadline = (now + timedelta(seconds=BATCH_WINDOW)).isoformat()
        db_query(
            "INSERT INTO scheduled_replies (chat_id, fan_name, fan_msg_id, fan_text, scheduled_time, created_at) VALUES (?,?,?,?,?,?)",
            (chat_id, fan_name, fan_msg_id, fan_text, deadline, now.isoformat()))


def get_due_batches():
    return db_query(
        "SELECT * FROM scheduled_replies WHERE status='pending' AND scheduled_time<=? ORDER BY scheduled_time ASC",
        (datetime.now().isoformat(),)
    ) or []


def mark_batch_sent(batch_id):
    db_query("UPDATE scheduled_replies SET status='sent' WHERE id=?", (batch_id,))


# ─────────────────────────────────────────────────────────────────────────────
# SYSTEM PROMPT
# ─────────────────────────────────────────────────────────────────────────────
def should_greet(db_history, fan_msg_time_str):
    fan_msgs = [m for m in db_history if not m.get('is_mine')]
    if len(fan_msgs) <= 1:
        return True
    if db_history and fan_msg_time_str:
        try:
            last_time = parse_timestamp(db_history[-2].get('timestamp', ''))
            this_time = parse_timestamp(fan_msg_time_str)
            if last_time and this_time:
                if (this_time - last_time).total_seconds() / 3600 > 2:
                    return True
        except Exception:
            pass
    return False


def build_system_prompt(chat_id, fan_name, real_name, fan_facts_list, db_history, time_ctx, fan_msg_time_str):
    display_name = real_name or fan_name or "a fan"
    prompt       = JAZMIN_PERSONALITY + "\n\n"

    # Time context
    prompt += (f"KONTEXTUS:\n- {time_ctx}\n"
               "- Ha a fan kérdezi mit csinálsz / hol vagy — ez alapján válaszolj természetesen.\n\n")

    # Per-fan directive from dashboard (highest priority after personality)
    profile  = db_query("SELECT fan_note FROM fan_profiles WHERE chat_id=?", (chat_id,), fetch_one=True)
    fan_note = ((profile or {}).get('fan_note') or '').strip()
    if fan_note:
        prompt += f"⭐ SPECIÁLIS UTASÍTÁS ERRE A FANRA (kövesd ezt felül mindent):\n{fan_note}\n\n"

    # Known facts
    if fan_facts_list:
        prompt += "AMIT TUDSZ ERRŐL A FANRÓL (ne kérdezd újra!):\n"
        for f in fan_facts_list[:15]:
            prompt += f"- {f['fact_type']}: {f['fact_value']}\n"
        prompt += "\n"

    # Chat history
    if db_history:
        prompt += "EDDIGI BESZÉLGETÉS (legújabb alul — OLVASD EL, és NE ismételd saját mondataidat):\n"
        for msg in db_history[-20:]:
            sender = "Jázmin" if msg.get('is_mine') else display_name
            text   = (msg.get('text') or '').strip()
            if text:
                prompt += f"{sender}: {text}\n"
        prompt += "\n"

    # Fan meta
    prompt += f"A fan neve: {display_name}\n"
    if not real_name:
        prompt += "NEM TUDOD A VALÓDI NEVÉT — ne szólítsd névvel.\n"
    if len(db_history) < 6:
        prompt += "⚠️ ÚJ FAN — most ismerkedtek, légy barátságos és kíváncsi.\n"

    # Greeting / continuation instruction
    if should_greet(db_history, fan_msg_time_str):
        prompt += "\nEZ ÚJ/ÚJRAINDULT BESZÉLGETÉS. Kezdj lazán, pl: 'heyy' vagy 'szia, mizu' — variálj!\n"
    else:
        cont = random.choice(CONTINUATION_VARIATIONS)
        prompt += f"\nEZ FOLYTATÁS. NE köszönj újra! Kezdj '{cont}'-szerűvel vagy egyből a lényegre.\n"

    prompt += "\nEGYETLEN rövid, természetes üzenetet írj vissza, magyarul. 1-2 mondat max. Ha a fan szomorú/nehéz dolgot ír — ELŐSZÖR arra reagálj."
    return prompt


# ─────────────────────────────────────────────────────────────────────────────
# GPT REPLY
# ─────────────────────────────────────────────────────────────────────────────
def ask_openai(system_prompt, user_text):
    for attempt in range(3):
        try:
            r = requests.post(
                "https://api.openai.com/v1/chat/completions",
                headers={"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"},
                json={
                    "model":             OPENAI_MODEL,
                    "messages":          [
                        {"role": "system", "content": system_prompt},
                        {"role": "user",   "content": user_text}
                    ],
                    "max_tokens":        120,
                    "temperature":       0.9,
                    "presence_penalty":  0.5,
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
                print(f"[WARN] OpenAI 429 — waiting {wait}s (attempt {attempt+1}/3)")
                time.sleep(wait)
                continue
            else:
                send_telegram_error(f"OpenAI error {r.status_code}: {r.text[:200]}")
                return ""
        except Exception as e:
            send_telegram_error(f"OpenAI request failed: {e}")
            return ""
    send_telegram_error("OpenAI 429 — all 3 retries failed")
    return ""


# ─────────────────────────────────────────────────────────────────────────────
# MESSAGE PROCESSING
# ─────────────────────────────────────────────────────────────────────────────
def process_new_messages():
    chats, status = get_chats()
    if not chats:
        return 0, status
    scheduled = 0

    for chat in chats:
        chat_id = None
        try:
            user    = chat.get('user', {}) or {}
            chat_id = user.get('uuid') or chat.get('uuid') or chat.get('id')
            if not chat_id:
                continue

            fan_name = user.get('displayName', 'ismeretlen') or 'ismeretlen'
            handle   = user.get('handle', '') or ''
            get_or_create_fan_profile(chat_id, fan_name, handle)

            api_messages = get_messages(chat_id)
            if not api_messages:
                continue

            # ── Save all messages; fire fact extraction only for genuinely new fan messages ──
            for msg in api_messages:
                msg_id      = msg.get('uuid') or ''
                sender_uuid = (msg.get('sender') or {}).get('uuid', '')
                text_raw    = (msg.get('text') or '').strip()
                msg_time    = msg.get('createdAt') or msg.get('sentAt') or msg.get('timestamp') or ''
                is_mine     = (sender_uuid == MY_UUID)

                # Check if this message is truly new (not yet in DB)
                already_saved = db_query('SELECT facts_done FROM messages WHERE msg_id=?', (msg_id,), fetch_one=True)

                save_message_to_db(msg_id, chat_id, fan_name, sender_uuid, text_raw, msg_time,
                                   is_mine=is_mine, facts_done=0 if not already_saved else (already_saved.get('facts_done') or 0))

                # Only run fact extraction on new fan messages we haven't processed yet
                if not is_mine and text_raw and not already_saved:
                    threading.Thread(
                        target=extract_facts_with_gpt,
                        args=(msg_id, chat_id, text_raw),
                        daemon=True
                    ).start()

            # ── Check for Rafael's manual messages → trigger takeover window ──
            check_for_manual_message(chat_id, fan_name, api_messages)

            # ── Skip if paused or in manual takeover ──
            if should_bot_skip(chat_id):
                continue

            # ── Find latest unprocessed fan message ──
            fan_msgs = [m for m in api_messages
                        if (m.get('sender') or {}).get('uuid') != MY_UUID]
            if not fan_msgs:
                continue

            last_msg = fan_msgs[0]
            msg_id   = last_msg.get('uuid') or ''
            text     = (last_msg.get('text') or '').strip()

            # Skip emoji/nonsense
            if is_emoji_or_nonsense(text):
                if msg_id:
                    db_query('UPDATE messages SET was_replied=1 WHERE msg_id=?', (msg_id,))
                continue

            # Skip messages older than 24h
            msg_time = (last_msg.get('createdAt') or last_msg.get('sentAt') or
                        last_msg.get('timestamp') or '')
            msg_dt   = parse_timestamp(msg_time)
            if msg_dt and (datetime.now(timezone.utc) - msg_dt).total_seconds() > 86400:
                continue

            # Skip if already replied or already batched for this fan
            already_replied = db_query(
                'SELECT 1 FROM messages WHERE msg_id=? AND was_replied=1', (msg_id,), fetch_one=True)
            already_batched = db_query(
                "SELECT 1 FROM scheduled_replies WHERE chat_id=? AND status='pending'", (chat_id,), fetch_one=True)
            if already_replied or already_batched:
                continue

            schedule_or_extend_batch(chat_id, fan_name, msg_id, text)
            scheduled += 1

        except Exception as e:
            print(f"[{datetime.now()}] Process error for {chat_id}: {e}")
            send_telegram_error(f"Process error for {chat_id}: {e}")

    return scheduled, "OK"


# ─────────────────────────────────────────────────────────────────────────────
# SEND DUE BATCHES
# ─────────────────────────────────────────────────────────────────────────────
_fan_sending = set()


def send_due_batches():
    if get_safe_mode():
        return 0  # silent — bot still polls and batches, just never sends

    due = get_due_batches()
    if not due:
        return 0

    sent           = 0
    already_sent_to = set()

    for item in due:
        chat_id = None
        try:
            chat_id      = item['chat_id']
            fan_name     = item['fan_name'] or 'unknown'
            fan_msg_id   = item['fan_msg_id']
            combined_text = item['fan_text'] or ''
            batch_id     = item['id']

            # Skip if we already handled this fan this cycle or another thread is sending
            if chat_id in already_sent_to or chat_id in _fan_sending:
                db_query(
                    "UPDATE scheduled_replies SET status='cancelled' WHERE id=? AND status='pending'",
                    (batch_id,))
                continue
            already_sent_to.add(chat_id)

            # Skip if paused or in manual takeover
            if should_bot_skip(chat_id):
                db_query("UPDATE scheduled_replies SET status='cancelled' WHERE id=?", (batch_id,))
                continue

            # Cancel any other pending batches for this fan (duplicates)
            db_query(
                "UPDATE scheduled_replies SET status='cancelled' WHERE chat_id=? AND id!=? AND status='pending'",
                (chat_id, batch_id))

            # ── Build context ──
            db_history       = get_full_history_from_db(chat_id, limit=100)
            fan_facts_list   = get_fan_facts(chat_id)
            real_name        = get_real_name(chat_id)
            time_ctx         = get_time_context()
            fan_msgs_history = [m for m in db_history if not m.get('is_mine')]
            fan_msg_time_str = fan_msgs_history[-1]['timestamp'] if fan_msgs_history else ''

            system_prompt = build_system_prompt(
                chat_id, fan_name, real_name, fan_facts_list,
                db_history, time_ctx, fan_msg_time_str
            )

            # ── Clean up batched text for GPT ──
            raw_lines = combined_text.replace("[+] ", "\n").split("\n")
            seen      = []
            for line in raw_lines:
                line = line.strip()
                if line and line not in seen:
                    seen.append(line)

            if len(seen) > 1:
                gpt_user_msg = (f"A fan {len(seen)} üzenetet küldött egymás után. "
                                "Mindegyikre reagálj egyetlen válaszban:\n\n" +
                                "\n".join(f"- {s}" for s in seen))
            else:
                gpt_user_msg = seen[0] if seen else combined_text

            # ── Get GPT reply ──
            reply = ask_openai(system_prompt, gpt_user_msg)
            if not reply or not reply.strip():
                # Don't leave batch stuck as pending — cancel it so it doesn't loop
                db_query("UPDATE scheduled_replies SET status='cancelled' WHERE id=?", (batch_id,))
                continue

            # ── Atomic claim: flip to 'sending' so no other worker can grab it ──
            db_query(
                "UPDATE scheduled_replies SET status='sending' WHERE id=? AND status='pending'",
                (batch_id,))
            claimed = db_query(
                "SELECT 1 FROM scheduled_replies WHERE id=? AND status='sending'",
                (batch_id,), fetch_one=True)
            if not claimed:
                continue  # another worker claimed it first

            _fan_sending.add(chat_id)

            # ── Realistic typing delay ──
            words      = len(reply.split())
            send_delay = (random.uniform(3, 8)   if words <= 5
                          else random.uniform(6, 18)  if words <= 15
                          else random.uniform(10, 25))
            time.sleep(send_delay)

            # ── Send + record ──
            if send_fanvue_message(chat_id, reply):
                mark_batch_sent(batch_id)
                db_query('UPDATE messages SET was_replied=1 WHERE msg_id=?', (fan_msg_id,))
                now_iso = datetime.now().isoformat()
                save_message_to_db(
                    f"bot_{now_iso}_{chat_id}", chat_id, fan_name,
                    MY_UUID, reply, now_iso, is_mine=True, facts_done=1)
                sent += 1
            else:
                # Send failed — revert to pending so it retries next cycle
                db_query("UPDATE scheduled_replies SET status='pending' WHERE id=?", (batch_id,))

            _fan_sending.discard(chat_id)

        except Exception as e:
            if chat_id:
                _fan_sending.discard(chat_id)
            print(f"[{datetime.now()}] Send error: {e}")
            send_telegram_error(f"Send error for {chat_id}: {e}")

    return sent


# ─────────────────────────────────────────────────────────────────────────────
# POLL LOOP
# ─────────────────────────────────────────────────────────────────────────────
polling_thread  = None
polling_active  = False


def poll_loop():
    global polling_active
    polling_active   = True
    consecutive_errs = 0
    while polling_active:
        try:
            if get_fanvue_token():
                sent      = send_due_batches()
                scheduled, _ = process_new_messages()
                if sent > 0 or scheduled > 0:
                    print(f"[{datetime.now()}] Sent={sent} Scheduled={scheduled}")
                consecutive_errs = 0
            else:
                print(f"[{datetime.now()}] No valid token — skipping poll")
        except Exception as e:
            consecutive_errs += 1
            print(f"[{datetime.now()}] Poll error #{consecutive_errs}: {e}")
            if consecutive_errs <= 3:
                send_telegram_error(f"Poll loop error #{consecutive_errs}: {e}")
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


# ─────────────────────────────────────────────────────────────────────────────
# FLASK ROUTES
# ─────────────────────────────────────────────────────────────────────────────
@app.route('/')
def home():
    return "Jazmin Bot v10.0 running ✅", 200


@app.route('/status')
def status():
    pending = db_query("SELECT COUNT(*) as c FROM scheduled_replies WHERE status='pending'", fetch_one=True)
    return {
        "token_valid":    get_fanvue_token() is not None,
        "polling_active": polling_active,
        "safe_mode":      get_safe_mode(),
        "pending_batches": pending['c'] if pending else 0,
    }, 200


@app.route('/safe_mode/off')
def safe_mode_off():
    set_safe_mode(False)
    print(f"[{datetime.now()}] SAFE MODE OFF — bot is live")
    return {"safe_mode": False, "status": "Bot is now live and sending messages"}, 200


@app.route('/safe_mode/on')
def safe_mode_on():
    set_safe_mode(True)
    print(f"[{datetime.now()}] SAFE MODE ON — bot silenced")
    return {"safe_mode": True, "status": "Bot silenced"}, 200


@app.route('/pause/<chat_id>')
def pause_fan(chat_id):
    db_query(
        "INSERT OR IGNORE INTO fan_profiles (chat_id, fan_name, total_messages, last_interaction) VALUES (?, 'unknown', 0, ?)",
        (chat_id, datetime.now().isoformat()))
    db_query("UPDATE fan_profiles SET is_paused=1 WHERE chat_id=?", (chat_id,))
    db_query("UPDATE scheduled_replies SET status='cancelled' WHERE chat_id=? AND status='pending'", (chat_id,))
    profile = db_query("SELECT fan_name FROM fan_profiles WHERE chat_id=?", (chat_id,), fetch_one=True)
    name    = (profile or {}).get('fan_name', chat_id)
    return {"paused": True, "fan": name, "chat_id": chat_id}, 200


@app.route('/resume/<chat_id>')
def resume_fan(chat_id):
    db_query("UPDATE fan_profiles SET is_paused=0 WHERE chat_id=?", (chat_id,))
    profile = db_query("SELECT fan_name FROM fan_profiles WHERE chat_id=?", (chat_id,), fetch_one=True)
    name    = (profile or {}).get('fan_name', chat_id)
    return {"paused": False, "fan": name, "chat_id": chat_id}, 200


@app.route('/set_note/<chat_id>', methods=['POST'])
def set_note(chat_id):
    data = request.json or {}
    note = data.get('note', '')
    db_query(
        "INSERT OR IGNORE INTO fan_profiles (chat_id, fan_name, total_messages, last_interaction) VALUES (?, 'unknown', 0, ?)",
        (chat_id, datetime.now().isoformat()))
    db_query("UPDATE fan_profiles SET fan_note=? WHERE chat_id=?", (note, chat_id))
    return {"ok": True, "chat_id": chat_id, "note": note}, 200


@app.route('/fan_facts/<chat_id>')
def fan_facts_route(chat_id):
    facts = db_query(
        "SELECT fact_type, fact_value FROM fan_facts WHERE chat_id=? ORDER BY discovered_at DESC",
        (chat_id,)
    ) or []
    return {"facts": facts, "chat_id": chat_id}, 200


@app.route('/fans')
def list_fans():
    fans = db_query(
        "SELECT chat_id, fan_name, handle, is_paused, total_messages, last_interaction, fan_note FROM fan_profiles ORDER BY last_interaction DESC"
    ) or []
    return {"fans": fans, "total": len(fans)}, 200


@app.route('/dashboard_data')
def dashboard_data():
    fans = db_query(
        "SELECT chat_id, fan_name, handle, is_paused, total_messages, last_interaction, fan_note FROM fan_profiles ORDER BY last_interaction DESC"
    ) or []
    pending    = db_query("SELECT COUNT(*) as c FROM scheduled_replies WHERE status='pending'", fetch_one=True)
    sent_today = db_query(
        "SELECT COUNT(*) as c FROM scheduled_replies WHERE status='sent' AND created_at >= date('now')",
        fetch_one=True)
    return {
        "safe_mode":      get_safe_mode(),
        "polling_active": polling_active,
        "pending_batches": pending['c'] if pending else 0,
        "sent_today":     sent_today['c'] if sent_today else 0,
        "fan_count":      len(fans),
        "fans":           fans,
    }, 200


@app.route('/dashboard')
def dashboard():
    pw = request.args.get('pw', '')
    if pw != DASHBOARD_PASSWORD:
        return '''<!DOCTYPE html><html><body style="background:#000;color:#00d4ff;font-family:monospace;
        display:flex;align-items:center;justify-content:center;height:100vh;flex-direction:column;gap:16px;">
        <div style="font-size:2em;letter-spacing:4px;">⬡ J.A.R.V.I.S</div>
        <div style="opacity:0.5;font-size:0.85em;letter-spacing:2px;">UNAUTHORIZED ACCESS DETECTED</div>
        <form method="get" style="display:flex;gap:8px;margin-top:16px;">
        <input name="pw" type="password" placeholder="Enter access code"
          style="background:#001a2e;border:1px solid #00d4ff;color:#00d4ff;padding:10px 16px;font-size:15px;outline:none;letter-spacing:2px;">
        <button type="submit"
          style="background:#00d4ff;color:#000;border:none;padding:10px 20px;cursor:pointer;font-weight:bold;font-family:monospace;letter-spacing:2px;">
          ENTER</button>
        </form></body></html>''', 401
    html_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'dashboard.html')
    try:
        with open(html_path, 'r', encoding='utf-8') as f:
            return f.read()
    except FileNotFoundError:
        return "dashboard.html not found — make sure it's in the same folder as app.py", 500


@app.route('/trigger')
def trigger():
    if not get_fanvue_token():
        return {"error": "No valid token"}, 400
    sent      = send_due_batches()
    scheduled, s = process_new_messages()
    return {"sent": sent, "scheduled": scheduled, "status": s}, 200


@app.route('/start_poll')
def start_poll():
    return {"started": start_polling(), "polling_active": polling_active}


@app.route('/stop_poll')
def stop_poll():
    return {"stopped": stop_polling(), "polling_active": polling_active}


@app.route('/callback')
def callback():
    auth_code = request.args.get('code')
    return (f"Code: {auth_code[:30]}...", 200) if auth_code else ("No code", 400)


@app.route('/set_token', methods=['POST'])
def set_token():
    data    = request.json or {}
    refresh = data.get('refresh_token')
    if not refresh:
        return {"error": "No refresh_token"}, 400
    save_token('refresh_token', refresh)
    access, msg = refresh_fanvue_token()
    return {
        "saved":           True,
        "test":            msg,
        "access_preview":  (access[:20] + "...") if access else None
    }


# ─────────────────────────────────────────────────────────────────────────────
# BOOT
# ─────────────────────────────────────────────────────────────────────────────
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
        print("[WARN] FANVUE_REFRESH_TOKEN env var not set")
except Exception as _e:
    print(f"[ERROR] Token boot failed: {_e}")

try:
    start_polling()
    print("[OK] Polling auto-started on boot")
except Exception as _e:
    print(f"[ERROR] start_polling failed: {_e}")

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))
