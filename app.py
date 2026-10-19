"""
Jazmin Fanvue Bot — v13.0

New in v13 (2026-10):
- Fanvue token can no longer "die" from two workers racing: one refresher at a time (DB lock),
  always uses the newest refresh token, and if Fanvue ever really needs a re-login you get a
  Telegram message with a one-tap "Reconnect Fanvue" link (no more copying tokens into Railway).
- Telegram control panel: /menu in your alert bot → Start / Stop / Status / Reconnect, for BOTH bots.
- Fanvue compliance: a direct "are you AI / real?" gets a clear, honest yes (then she stays in
  character); repeat → "already told you"; never contradicts it. Meetups = clear no.
- Telegram → Fanvue memory: recognises "én vagyok / nem emlékszel / telegramról jöttem", asks his
  Telegram name, matches it (username / full name, accent-insensitive), imports a summary of the
  WHOLE Telegram chat + the latest messages. You get an alert with an Undo button.
- Always Hungarian. New life story (shelter is quiet → mostly home, loves cooking, gym sometimes).

v12 (kept): 5-stage relationship engine, rolling summary, re-engagement, PPV funnel, safety scrubbers.

New in v12 over v11:
- 5-stage emotional relationship state machine (warmth points drive stage)
- warmth_points actually tracked (was defined but never used in v11)
- Rolling relationship summary (Haiku, every 10 msgs) → Claude knows the vibe
- Re-engagement loop: cold fans get a stage-appropriate hook after 2/7/14/30 days
- build_dynamic_prompt now injects stage directive + relationship memory

Everything else (DB, Fanvue API, PPV funnel, safety scrubbers) is unchanged from v11.
"""

from flask import Flask, request, redirect
import requests
import os
import re
import json
import base64
import sqlite3
import threading
import time
import random
import uuid
from functools import wraps
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import anthropic

# ─────────────────────────────────────────────────────────────────────────────
# TIME / APP / CONFIG
# ─────────────────────────────────────────────────────────────────────────────
BUDAPEST_TZ   = ZoneInfo('Europe/Budapest')
BOOT_TIME_UTC = datetime.now(timezone.utc)
WORKER_ID     = uuid.uuid4().hex[:8]
print(f"[{datetime.now()}] BOT v15.6 BOOTED worker={WORKER_ID} at {BOOT_TIME_UTC.isoformat()}")

def get_budapest_now():
    return datetime.now(BUDAPEST_TZ).replace(tzinfo=None)

app = Flask(__name__)

FANVUE_CLIENT_ID     = os.environ.get('FANVUE_CLIENT_ID', '')
FANVUE_CLIENT_SECRET = os.environ.get('FANVUE_CLIENT_SECRET', '')
FANVUE_REDIRECT_URI  = os.environ.get('FANVUE_REDIRECT_URI', 'https://web-production-f0a39.up.railway.app/callback')
FANVUE_SCOPES        = os.environ.get('FANVUE_SCOPES', 'openid offline_access offline read:self read:chat write:chat read:fan read:creator read:media read:insights read:tracking_links write:tracking_links')

AUTO_PPV_ON         = os.environ.get('AUTO_PPV_ON', '0') == '1'
AUTO_FREE_FOLDER    = os.environ.get('AUTO_FREE_FOLDER', 'AUTO_FREE_1')
AUTO_PPV_FOLDER     = os.environ.get('AUTO_PPV_FOLDER', 'AUTO_PPV_1')
AUTO_FREE_AT        = int(os.environ.get('AUTO_FREE_AT', '7'))
AUTO_PPV_AT         = int(os.environ.get('AUTO_PPV_AT', '10'))
AUTO_PPV_PRICE      = int(os.environ.get('AUTO_PPV_PRICE', '3500'))
AUTO_PPV_MAX_NUDGES = int(os.environ.get('AUTO_PPV_MAX_NUDGES', '3'))
AUTO_FREE_TEXT      = os.environ.get('AUTO_FREE_TEXT', 'csináltam neked valamit 🙈 csak neked, nézd meg 🥰')
AUTO_PPV_TEXT       = os.environ.get('AUTO_PPV_TEXT', 'na jó… összeállítottam neked egy kis privát csomagot 🙈 remélem tetszeni fog 😏')
ANTHROPIC_API_KEY    = os.environ.get('ANTHROPIC_API_KEY', '')
MY_UUID              = os.environ.get('MY_UUID', '38a392fc-a751-49b3-9d74-01ac6447c490')
TELEGRAM_BOT_TOKEN   = os.environ.get('TELEGRAM_BOT_TOKEN', '')
TELEGRAM_CHAT_ID     = os.environ.get('TELEGRAM_CHAT_ID', '')
DASHBOARD_PASSWORD   = os.environ.get('DASHBOARD_PASSWORD', 'jazmin2024')
# Base URL of this service (for the one-tap reconnect link). Derived from the OAuth redirect URI by default.
PUBLIC_URL           = (os.environ.get('PUBLIC_URL') or FANVUE_REDIRECT_URI.rsplit('/callback', 1)[0]).rstrip('/')

# Cost control: routine chat → CHEAP_MODEL; money/compliance moments → SMART_MODEL (see needs_smart()).
REPLY_MODEL          = os.environ.get('SMART_MODEL', os.environ.get('REPLY_MODEL', 'claude-sonnet-5-5'))
CHEAP_MODEL          = os.environ.get('CHEAP_MODEL', 'claude-haiku-5-5')
REPLY_MODEL_FALLBACK = os.environ.get('REPLY_MODEL_FALLBACK', CHEAP_MODEL)
UTIL_MODEL           = os.environ.get('UTIL_MODEL', 'claude-haiku-5-5')

POLL_INTERVAL        = int(os.environ.get('POLL_INTERVAL', '5'))       # scan pickup (was 8) — tune live on Railway
SEND_INTERVAL        = int(os.environ.get('SEND_INTERVAL', '4'))
BATCH_WINDOW_MIN     = int(os.environ.get('BATCH_WINDOW_MIN', '12'))    # human debounce low  (was 24)
BATCH_WINDOW_MAX     = int(os.environ.get('BATCH_WINDOW_MAX', '22'))    # human debounce high (was 38) → avg reply ~25-30s
MANUAL_TAKEOVER_SECS = int(os.environ.get('MANUAL_TAKEOVER_SECS', '300'))   # you typed / sent PPV → bot waits 5 min
POLL_LOCK_TTL        = 25

_ws = os.environ.get('ANTHROPIC_WORKSPACE_ID', '').strip()
client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY,
                             default_headers=({"anthropic-workspace-id": _ws} if _ws else None))
LLM_CLIENT = client

class _Resp:
    """Normalised model response: .content (text blocks only) and .text."""
    def __init__(self, r):
        self.raw = r
        self.content = [b for b in getattr(r, "content", []) if getattr(b, "type", "") == "text"]
        self.text = "".join(b.text for b in self.content).strip()
        self.usage = getattr(r, "usage", None)

def llm_create(**kw):
    """One place that knows each model generation's rules:
    - Claude 5.x: no custom temperature; Sonnet/Opus 5.x think by default → switch thinking off (between_tools)
    - Claude 4.x: temperature as given
    Retries once without the extras if the API rejects them."""
    model = kw.get("model", "")
    m = re.search(r"claude-[a-z]+-(\d+)", model or "")
    major = int(m.group(1)) if m else 4
    kw2 = dict(kw)
    if major >= 5:
        kw2.pop("temperature", None)
        # Haiku 5.x thinks by default (wastes tokens, truncates short answers) → off.
        # Sonnet/Opus 5.x reject "disabled" and use "between_tools" for "don't think before answering".
        kw2["extra_body"] = {"thinking": {"type": "disabled" if "haiku" in model else "between_tools"}}
    try:
        return _Resp(LLM_CLIENT.messages.create(**kw2))
    except Exception as e:
        msg = str(e)
        if "thinking" in msg or "temperature" in msg:
            kw2.pop("extra_body", None); kw2.pop("temperature", None)
            return _Resp(LLM_CLIENT.messages.create(**kw2))
        raise


# ─────────────────────────────────────────────────────────────────────────────
# DB
# ─────────────────────────────────────────────────────────────────────────────
def _resolve_db_path():
    custom = os.environ.get('DB_PATH', '').strip()
    if custom:
        return custom
    try:
        os.makedirs('/data', exist_ok=True)
        with open('/data/.wt', 'w') as f:
            f.write('ok')
        os.remove('/data/.wt')
        return '/data/bot_data.db'
    except Exception:
        return 'bot_data.db'

DB_PATH = _resolve_db_path()
print(f"[DB] {DB_PATH}")

DATABASE_URL = os.environ.get("DATABASE_URL") or os.environ.get("DATABASE_PUBLIC_URL") or ""
USE_PG = bool(DATABASE_URL)
if USE_PG:
    import psycopg
    from psycopg.rows import dict_row
    print("[DB] Postgres mode ON")

def _to_pg(query):
    query = query.replace("date('now')", "to_char(now(),'YYYY-MM-DD')")
    query = query.replace('%', '%%')
    s = query.lstrip()
    if re.match(r'(?i)INSERT\s+OR\s+IGNORE', s):
        query = re.sub(r'(?i)INSERT\s+OR\s+IGNORE', 'INSERT', query, count=1).rstrip().rstrip(';').rstrip() + ' ON CONFLICT DO NOTHING'
    elif re.match(r'(?i)INSERT\s+OR\s+REPLACE\s+INTO\s+tokens', s):
        query = "INSERT INTO tokens (key, value) VALUES (?, ?) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value"
    return query.replace('?', '%s')

def _connect():
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn

def db_query(query, params=(), fetch_one=False):
    if USE_PG:
        conn = psycopg.connect(DATABASE_URL, connect_timeout=20)
        try:
            with conn.cursor(row_factory=dict_row) as c:
                c.execute(_to_pg(query), params)
                if query.strip().upper().startswith('SELECT'):
                    if fetch_one:
                        row = c.fetchone(); return dict(row) if row else None
                    return [dict(r) for r in c.fetchall()]
            conn.commit(); return None
        finally:
            conn.close()
    conn = _connect(); conn.row_factory = sqlite3.Row
    try:
        c = conn.cursor(); c.execute(query, params)
        if query.strip().upper().startswith('SELECT'):
            if fetch_one:
                row = c.fetchone(); result = dict(row) if row else None
            else:
                result = [dict(r) for r in c.fetchall()]
        else:
            conn.commit(); result = None
        return result
    finally:
        conn.close()

def db_claim(query, params=()):
    if USE_PG:
        conn = psycopg.connect(DATABASE_URL, connect_timeout=20)
        try:
            with conn.cursor() as c:
                c.execute(_to_pg(query), params); rc = c.rowcount
            conn.commit(); return rc
        finally:
            conn.close()
    conn = _connect()
    try:
        c = conn.cursor(); c.execute(query, params); conn.commit()
        return c.rowcount
    finally:
        conn.close()

def init_db():
    if USE_PG:
        conn = psycopg.connect(DATABASE_URL, connect_timeout=20)
        with conn.cursor() as c:
            c.execute('CREATE TABLE IF NOT EXISTS tokens (key TEXT PRIMARY KEY, value TEXT)')
            c.execute('''CREATE TABLE IF NOT EXISTS messages (
                msg_id TEXT PRIMARY KEY, chat_id TEXT, fan_name TEXT, sender_uuid TEXT,
                text TEXT, timestamp TEXT, was_replied INTEGER DEFAULT 0,
                is_mine INTEGER DEFAULT 0, facts_done INTEGER DEFAULT 0, vision_done INTEGER DEFAULT 0)''')
            c.execute('''CREATE TABLE IF NOT EXISTS fan_profiles (
                chat_id TEXT PRIMARY KEY, fan_name TEXT, handle TEXT, total_messages INTEGER DEFAULT 0,
                last_interaction TEXT, manual_takeover_until TEXT, is_paused INTEGER DEFAULT 0,
                fan_note TEXT, ppv_pending INTEGER DEFAULT 0, ppv_note TEXT,
                warmth INTEGER DEFAULT 0, tg_handle TEXT, ai_strikes INTEGER DEFAULT 0, awaiting_tg INTEGER DEFAULT 0)''')
            c.execute('''CREATE TABLE IF NOT EXISTS scheduled_replies (
                id BIGSERIAL PRIMARY KEY, chat_id TEXT, fan_name TEXT, fan_msg_id TEXT,
                fan_text TEXT, scheduled_time TEXT, status TEXT DEFAULT 'pending', created_at TEXT)''')
            c.execute('''CREATE TABLE IF NOT EXISTS fan_facts (
                id BIGSERIAL PRIMARY KEY, chat_id TEXT, fact_type TEXT,
                fact_value TEXT, discovered_at TEXT)''')
            for col in (
                'auto_eligible INTEGER DEFAULT 0', 'auto_free_sent INTEGER DEFAULT 0',
                'auto_ppv_sent_at TEXT', 'auto_ppv_bought INTEGER DEFAULT 0', 'auto_nudges INTEGER DEFAULT 0',
                'warmth_points INTEGER DEFAULT 0', 'relationship_summary TEXT',
                'summary_msg_count INTEGER DEFAULT 0', 'last_reengagement_at TEXT',
                'reengagement_count INTEGER DEFAULT 0',
                'ai_confirmed INTEGER DEFAULT 0', 'tg_user_id TEXT', 'tg_summary TEXT',
                'tg_ask_count INTEGER DEFAULT 0', 'tg_linked_at TEXT',
                'memory_notes TEXT', 'memory_at INTEGER DEFAULT 0', 'backfilled INTEGER DEFAULT 0',
            ):
                c.execute(f'ALTER TABLE fan_profiles ADD COLUMN IF NOT EXISTS {col}')
            c.execute('ALTER TABLE messages ADD COLUMN IF NOT EXISTS author TEXT')
            c.execute('CREATE INDEX IF NOT EXISTS idx_messages_chat_ts ON messages(chat_id, timestamp)')
            c.execute('ALTER TABLE scheduled_replies ADD COLUMN IF NOT EXISTS attempts INTEGER DEFAULT 0')
            # v12 fans left "waiting for a Telegram name" would be asked weeks later → reset once
            c.execute("UPDATE fan_profiles SET awaiting_tg=0 WHERE awaiting_tg=1")   # v13: she never asks for names any more
        conn.commit(); conn.close()
        return

    conn = _connect(); c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS tokens (key TEXT PRIMARY KEY, value TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS messages (
        msg_id TEXT PRIMARY KEY, chat_id TEXT, fan_name TEXT, sender_uuid TEXT,
        text TEXT, timestamp TEXT, was_replied INTEGER DEFAULT 0,
        is_mine INTEGER DEFAULT 0, facts_done INTEGER DEFAULT 0, vision_done INTEGER DEFAULT 0)''')
    c.execute('''CREATE TABLE IF NOT EXISTS fan_profiles (
        chat_id TEXT PRIMARY KEY, fan_name TEXT, handle TEXT, total_messages INTEGER DEFAULT 0,
        last_interaction TEXT, manual_takeover_until TEXT, is_paused INTEGER DEFAULT 0,
        fan_note TEXT, ppv_pending INTEGER DEFAULT 0, ppv_note TEXT,
        warmth INTEGER DEFAULT 0, tg_handle TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS scheduled_replies (
        id INTEGER PRIMARY KEY AUTOINCREMENT, chat_id TEXT, fan_name TEXT, fan_msg_id TEXT,
        fan_text TEXT, scheduled_time TEXT, status TEXT DEFAULT 'pending', created_at TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS fan_facts (
        id INTEGER PRIMARY KEY AUTOINCREMENT, chat_id TEXT, fact_type TEXT,
        fact_value TEXT, discovered_at TEXT)''')
    for sql in [
        'ALTER TABLE fan_profiles ADD COLUMN ppv_pending INTEGER DEFAULT 0',
        'ALTER TABLE fan_profiles ADD COLUMN ppv_note TEXT',
        'ALTER TABLE fan_profiles ADD COLUMN warmth INTEGER DEFAULT 0',
        'ALTER TABLE fan_profiles ADD COLUMN tg_handle TEXT',
        'ALTER TABLE fan_profiles ADD COLUMN ai_strikes INTEGER DEFAULT 0',
        'ALTER TABLE fan_profiles ADD COLUMN awaiting_tg INTEGER DEFAULT 0',
        'ALTER TABLE messages ADD COLUMN vision_done INTEGER DEFAULT 0',
        'ALTER TABLE fan_profiles ADD COLUMN auto_eligible INTEGER DEFAULT 0',
        'ALTER TABLE fan_profiles ADD COLUMN auto_free_sent INTEGER DEFAULT 0',
        'ALTER TABLE fan_profiles ADD COLUMN auto_ppv_sent_at TEXT',
        'ALTER TABLE fan_profiles ADD COLUMN auto_ppv_bought INTEGER DEFAULT 0',
        'ALTER TABLE fan_profiles ADD COLUMN auto_nudges INTEGER DEFAULT 0',
        # v12 character engine columns
        'ALTER TABLE fan_profiles ADD COLUMN warmth_points INTEGER DEFAULT 0',
        'ALTER TABLE fan_profiles ADD COLUMN relationship_summary TEXT',
        'ALTER TABLE fan_profiles ADD COLUMN summary_msg_count INTEGER DEFAULT 0',
        'ALTER TABLE fan_profiles ADD COLUMN last_reengagement_at TEXT',
        'ALTER TABLE fan_profiles ADD COLUMN reengagement_count INTEGER DEFAULT 0',
        'ALTER TABLE fan_profiles ADD COLUMN ai_confirmed INTEGER DEFAULT 0',
        'ALTER TABLE fan_profiles ADD COLUMN tg_user_id TEXT',
        'ALTER TABLE fan_profiles ADD COLUMN tg_summary TEXT',
        'ALTER TABLE fan_profiles ADD COLUMN tg_ask_count INTEGER DEFAULT 0',
        'ALTER TABLE fan_profiles ADD COLUMN tg_linked_at TEXT',
        'ALTER TABLE fan_profiles ADD COLUMN memory_notes TEXT',
        'ALTER TABLE fan_profiles ADD COLUMN memory_at INTEGER DEFAULT 0',
        'ALTER TABLE fan_profiles ADD COLUMN backfilled INTEGER DEFAULT 0',
        'ALTER TABLE messages ADD COLUMN author TEXT',
        'ALTER TABLE scheduled_replies ADD COLUMN attempts INTEGER DEFAULT 0',
        "UPDATE fan_profiles SET awaiting_tg=0 WHERE awaiting_tg=1",
    ]:
        try: c.execute(sql); conn.commit()
        except Exception: pass
    conn.commit(); conn.close()

# ─────────────────────────────────────────────────────────────────────────────
# TELEGRAM
# ─────────────────────────────────────────────────────────────────────────────
def send_telegram_error(text):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID: return
    try:
        requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": f"⚠️ {text[:3500]}"}, timeout=10)
    except Exception as e:
        print(f"[WARN] tg: {e}")

def send_telegram_alert(text, buttons=None):
    """buttons: list of rows, each row a list of (label, callback_data) or (label, url) tuples."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID: return
    body = {"chat_id": TELEGRAM_CHAT_ID, "text": text[:3500], "disable_web_page_preview": True}
    if buttons:
        body["reply_markup"] = {"inline_keyboard": [[
            ({"text": b[0], "url": b[1]} if str(b[1]).startswith("http") else {"text": b[0], "callback_data": str(b[1])[:64]})
            for b in row] for row in buttons]}
    try:
        requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage", json=body, timeout=10)
    except Exception as e:
        print(f"[tg-alert] {e}")

# ─────────────────────────────────────────────────────────────────────────────
# TOKENS / SAFE MODE / POLLER LEADER-ELECTION
# ─────────────────────────────────────────────────────────────────────────────
def save_token(k, v): db_query('INSERT OR REPLACE INTO tokens (key, value) VALUES (?, ?)', (k, v))
def load_token(k):
    r = db_query('SELECT value FROM tokens WHERE key=?', (k,), fetch_one=True)
    return r['value'] if r else None

def get_safe_mode():
    r = db_query('SELECT value FROM tokens WHERE key=?', ('safe_mode',), fetch_one=True)
    if r is None:
        save_token('safe_mode', 'on'); return True
    return r['value'] == 'on'

def set_safe_mode(on): save_token('safe_mode', 'on' if on else 'off')

def acquire_poll_lock():
    now = datetime.now(timezone.utc)
    row = db_query('SELECT value FROM tokens WHERE key=?', ('poll_lock',), fetch_one=True)
    owner, exp = (None, None)
    if row and row['value'] and ':' in row['value']:
        owner, exp_s = row['value'].split(':', 1)
        try: exp = datetime.fromisoformat(exp_s)
        except Exception: exp = None
    free = (row is None) or (exp is None) or (now >= exp) or (owner == WORKER_ID)
    if not free: return False
    new_val = f"{WORKER_ID}:{(now + timedelta(seconds=POLL_LOCK_TTL)).isoformat()}"
    if row is None:
        n = db_claim("INSERT OR IGNORE INTO tokens (key, value) VALUES ('poll_lock', ?)", (new_val,))
        if n == 0: return False
        return True
    n = db_claim("UPDATE tokens SET value=? WHERE key='poll_lock' AND value=?", (new_val, row['value']))
    return n == 1

# ─────────────────────────────────────────────────────────────────────────────
# FANVUE AUTH + API
# ─────────────────────────────────────────────────────────────────────────────
def get_basic_auth_header():
    return "Basic " + base64.b64encode(f"{FANVUE_CLIENT_ID}:{FANVUE_CLIENT_SECRET}".encode()).decode()

# Fanvue rotates the refresh token on EVERY refresh (the old one dies). If two processes refresh at
# the same moment with the same token, one of them kills it → "manual token pasting" hell.
# Fix: only one refresher at a time (DB lock shared by all workers), always read the newest token
# AFTER taking the lock, and reuse a token another worker refreshed seconds ago.
def _token_fresh(exp_iso, margin_s=0):
    try: return datetime.now() + timedelta(seconds=margin_s) < datetime.fromisoformat(exp_iso)
    except Exception: return False

def _claim_refresh_lock(ttl=30):
    now = datetime.now(timezone.utc)
    val = f"{WORKER_ID}:{(now + timedelta(seconds=ttl)).isoformat()}"
    row = db_query('SELECT value FROM tokens WHERE key=?', ('refresh_lock',), fetch_one=True)
    if row is None:
        return db_claim("INSERT OR IGNORE INTO tokens (key, value) VALUES ('refresh_lock', ?)", (val,)) == 1
    cur = row.get('value') or ''
    exp = None
    if ':' in cur:
        try: exp = datetime.fromisoformat(cur.split(':', 1)[1])
        except Exception: exp = None
    if exp and now < exp:
        return False
    return db_claim("UPDATE tokens SET value=? WHERE key='refresh_lock' AND value=?", (val, cur)) == 1

def _release_refresh_lock():
    try: db_claim("UPDATE tokens SET value='' WHERE key='refresh_lock' AND value LIKE ?", (WORKER_ID + ':%',))
    except Exception: pass

def make_reconnect_link():
    """One-time, 48h reconnect link (no password inside)."""
    rk = uuid.uuid4().hex
    save_token('reconnect_key', f"{rk}:{(datetime.now(timezone.utc) + timedelta(hours=48)).isoformat()}")
    return f"{PUBLIC_URL}/connect?rk={rk}"

def _token_dead(reason):
    """Fanvue really refused the refresh token → only a re-login fixes it. Tell Rafael (max every 2h)."""
    save_token('token_status', 'dead')
    save_token('expires_at', '')
    last = load_token('token_dead_alert_at') or ''
    try: recent = (datetime.now(timezone.utc) - datetime.fromisoformat(last)).total_seconds() < 7200
    except Exception: recent = False
    if recent: return
    save_token('token_dead_alert_at', datetime.now(timezone.utc).isoformat())
    send_telegram_alert(
        "🔴 A Fanvue kapcsolat megszakadt (" + reason[:80] + ").\n"
        "Koppints a gombra, lépj be a Fanvue-ba, és kész — semmit nem kell bemásolni.",
        buttons=[[("🔗 Fanvue újracsatlakoztatása", make_reconnect_link())]])

def refresh_fanvue_token(force=False):
    for _ in range(20):
        if _claim_refresh_lock(): break
        time.sleep(1)
        a, e = load_token('access_token'), load_token('expires_at')
        if a and e and _token_fresh(e): return a, "OK (refreshed by other worker)"
    else:
        return None, "refresh lock busy"
    try:
        # Another worker may have JUST refreshed — reuse it instead of rotating again.
        if not force:
            ra = load_token('refreshed_at') or ''
            try:
                if (datetime.now(timezone.utc) - datetime.fromisoformat(ra)).total_seconds() < 45:
                    a = load_token('access_token')
                    if a: return a, "OK (just refreshed)"
            except Exception: pass
        rt = load_token('refresh_token')            # read AFTER the lock → always the newest token
        if not rt:
            _token_dead("nincs elmentett token"); return None, "no refresh token"
        r = requests.post("https://auth.fanvue.com/oauth2/token",
            data={"grant_type": "refresh_token", "refresh_token": rt},
            headers={"Content-Type": "application/x-www-form-urlencoded", "Authorization": get_basic_auth_header()},
            timeout=15)
        if r.status_code == 200:
            d = r.json()
            save_token('refresh_token', d.get('refresh_token') or rt)
            save_token('access_token', d.get('access_token'))
            save_token('expires_at', (datetime.now() + timedelta(seconds=int(d.get('expires_in', 3600)) - 300)).isoformat())
            save_token('refreshed_at', datetime.now(timezone.utc).isoformat())
            if load_token('token_status') == 'dead':
                send_telegram_alert("🟢 Fanvue újra csatlakoztatva, a bot fut tovább.")
            save_token('token_status', 'ok')
            return d.get('access_token'), "OK"
        body = (r.text or '')[:300]
        print(f"[token] refresh failed {r.status_code}: {body}", flush=True)
        if r.status_code in (400, 401) and ('invalid_grant' in body or 'invalid_token' in body or 'expired' in body or 'revoked' in body):
            _token_dead(f"Fanvue: {r.status_code}")
        return None, f"refresh {r.status_code}"
    except Exception as e:
        return None, f"err {e}"
    finally:
        _release_refresh_lock()

_token_refresh_lock = threading.Lock()
_last_dead_try = [0.0]
def get_fanvue_token():
    access, exp = load_token('access_token'), load_token('expires_at')
    if access and exp and _token_fresh(exp): return access
    # Dead token: don't hammer Fanvue — retry at most every 10 minutes until Rafael reconnects.
    if load_token('token_status') == 'dead' and time.time() - _last_dead_try[0] < 600:
        return None
    with _token_refresh_lock:
        access, exp = load_token('access_token'), load_token('expires_at')
        if access and exp and _token_fresh(exp): return access
        _last_dead_try[0] = time.time()
        return refresh_fanvue_token()[0]

def refresh_after_401():
    """A 401 means the access token is bad even though it looked fresh → force one real refresh."""
    if load_token('token_status') == 'dead' and time.time() - _last_dead_try[0] < 600:
        return None, "dead"
    with _token_refresh_lock:
        _last_dead_try[0] = time.time()
        return refresh_fanvue_token(force=True)

def get_headers():
    return {"Authorization": f"Bearer {get_fanvue_token() or ''}",
            "X-Fanvue-API-Version": "2025-06-26", "Content-Type": "application/json"}

def get_chats():
    try:
        all_chats, page = [], 1
        while True:
            r = requests.get(f"https://api.fanvue.com/chats?page={page}&limit=50", headers=get_headers(), timeout=10)
            if r.status_code == 401:
                refresh_after_401()
                r = requests.get(f"https://api.fanvue.com/chats?page={page}&limit=50", headers=get_headers(), timeout=10)
            if r.status_code != 200: break
            chats = r.json().get('data', [])
            all_chats.extend(chats)
            if len(chats) < 50 or page > 20: break
            page += 1
        return all_chats, "OK"
    except Exception as e:
        return [], f"err {e}"

def get_messages(chat_id):
    try:
        r = requests.get(f"https://api.fanvue.com/chats/{chat_id}/messages", headers=get_headers(), timeout=10)
        if r.status_code == 401:
            refresh_after_401()
            r = requests.get(f"https://api.fanvue.com/chats/{chat_id}/messages", headers=get_headers(), timeout=10)
        return r.json().get('data', []) if r.status_code == 200 else []
    except Exception:
        return []

def send_fanvue_message(chat_id, text):
    try:
        r = requests.post(f"https://api.fanvue.com/chats/{chat_id}/message",
            headers=get_headers(), json={"text": text}, timeout=10)
        if only_fan(): print(f"[lab] BOT → {text}", flush=True)
        return r.status_code in (200, 201)
    except Exception as e:
        send_telegram_error(f"send failed {chat_id}: {e}"); return False

_vault_cache = {}
def get_auto_media(folder):
    c = _vault_cache.get(folder)
    if c and (time.time() - c[0]) < 300: return c[1]
    uuids = []
    try:
        r = requests.get(f"https://api.fanvue.com/creators/{MY_UUID}/vault/folders/{folder}/media",
            headers=get_headers(), timeout=15)
        if r.status_code == 200:
            uuids = [m.get('uuid') for m in r.json().get('data', []) if m.get('uuid')]
        else:
            print(f"[vault] {folder}: {r.status_code} {r.text[:120]}")
    except Exception as e:
        print(f"[vault] {folder}: {e}")
    if uuids: _vault_cache[folder] = (time.time(), uuids)
    return uuids

def send_fanvue_media(chat_id, media_uuids, price=None, text=""):
    try:
        body = {"text": (text or None), "mediaUuids": list(media_uuids)}
        if price: body["price"] = int(price)
        r = requests.post(f"https://api.fanvue.com/chats/{chat_id}/message",
            headers=get_headers(), json=body, timeout=15)
        ok = r.status_code in (200, 201)
        if ok: save_token(f"botmedia_{chat_id}", datetime.now(timezone.utc).isoformat())
        if not ok: send_telegram_error(f"PPV send {chat_id}: {r.status_code} {r.text[:200]}")
        return ok
    except Exception as e:
        send_telegram_error(f"PPV send err {chat_id}: {e}"); return False

def maybe_run_auto_ppv(chat_id, fan_name=''):
    if not AUTO_PPV_ON: return
    p = db_query("""SELECT total_messages, auto_eligible, auto_free_sent, auto_ppv_sent_at,
                    auto_ppv_bought, auto_nudges, is_paused FROM fan_profiles WHERE chat_id=?""", (chat_id,), fetch_one=True)
    if not p or not p.get('auto_eligible'): return
    if p.get('is_paused') or p.get('auto_ppv_bought'): return
    n = p.get('total_messages') or 0
    if p.get('auto_ppv_sent_at'):
        nud = (p.get('auto_nudges') or 0) + 1
        db_query("UPDATE fan_profiles SET auto_nudges=? WHERE chat_id=?", (nud, chat_id))
        # NO auto-pause — the bot keeps replying forever. Just flag it ONCE so you can
        # pause manually on the control panel if you decide to. (was: is_paused=1)
        if nud == AUTO_PPV_MAX_NUDGES:
            send_telegram_alert(f"💬 {fan_name or chat_id}: $35 PPV sent, no buy after {nud} replies. Still chatting (NOT paused) — pause manually if you want.")
        return
    if n >= AUTO_PPV_AT:
        uuids = get_auto_media(AUTO_PPV_FOLDER)
        if not uuids:
            send_telegram_error(f"AUTO_PPV: {AUTO_PPV_FOLDER} empty — bundle NOT sent to {chat_id}"); return
        if send_fanvue_media(chat_id, uuids, price=AUTO_PPV_PRICE, text=AUTO_PPV_TEXT):
            now_iso = datetime.now().isoformat()
            save_message_to_db(f"bot_ppv_{now_iso}_{chat_id}", chat_id, fan_name, MY_UUID,
                               AUTO_PPV_TEXT or f"[🔒 zárolt csomag (PPV ${AUTO_PPV_PRICE // 100}) küldtél neki]", now_iso, is_mine=True, facts_done=1, vision_done=1)
            db_query("UPDATE fan_profiles SET auto_ppv_sent_at=? WHERE chat_id=?",
                     (datetime.now(timezone.utc).isoformat(), chat_id))
            send_telegram_alert(f"📤 {fan_name or chat_id}: auto-sent the $35 bundle (msg {n}).")
        return
    if n >= AUTO_FREE_AT and not p.get('auto_free_sent'):
        uuids = get_auto_media(AUTO_FREE_FOLDER)
        if uuids and send_fanvue_media(chat_id, uuids[:1], price=None, text=AUTO_FREE_TEXT):
            now_iso = datetime.now().isoformat()
            save_message_to_db(f"bot_free_{now_iso}_{chat_id}", chat_id, fan_name, MY_UUID,
                               AUTO_FREE_TEXT or "[ingyen képet küldtél neki]", now_iso, is_mine=True, facts_done=1, vision_done=1)
            db_query("UPDATE fan_profiles SET auto_free_sent=1 WHERE chat_id=?", (chat_id,))

def download_media(url):
    try:
        r = requests.get(url, headers={"Authorization": f"Bearer {get_fanvue_token() or ''}"}, timeout=15)
        if r.status_code != 200: return None
        mt = r.headers.get("Content-Type", "image/jpeg").split(";")[0]
        if mt not in ("image/jpeg", "image/png", "image/webp", "image/gif"): mt = "image/jpeg"
        return mt, base64.standard_b64encode(r.content).decode()
    except Exception:
        return None

def extract_media_url(msg):
    for k in ("media", "attachments", "images", "files"):
        v = msg.get(k)
        if isinstance(v, list) and v:
            item = v[0]
            if isinstance(item, dict):
                for uk in ("url", "mediaUrl", "src", "downloadUrl"):
                    if item.get(uk): return item[uk]
            elif isinstance(item, str) and item.startswith("http"):
                return item
    for uk in ("mediaUrl", "imageUrl", "url"):
        if isinstance(msg.get(uk), str) and msg[uk].startswith("http"): return msg[uk]
    return None

def my_media_tag(msg):
    """How YOUR manual media/PPV messages appear in her memory, so she knows where things stand."""
    if not (msg.get('hasMedia') or msg.get('mediaUuids') or extract_media_url(msg)): return ''
    kind = {'video': 'videót', 'audio': 'hangüzenetet', 'image': 'képet'}.get((msg.get('mediaType') or '').lower(), 'tartalmat')
    if msg_has_price(msg):
        pr = msg.get('price') or msg.get('amount')
        amt = f" ${int(pr) / 100:.0f}" if isinstance(pr, (int, float)) and pr else ""
        return f"[🔒 zárolt {kind} (PPV{amt}) küldtél neki]"
    return f"[{kind} küldtél neki]"

def msg_has_price(msg):
    for k in ("price", "amount", "isPaid", "locked", "ppv"):
        v = msg.get(k)
        if v not in (None, 0, False, "", "0"): return True
    return False

# ─────────────────────────────────────────────────────────────────────────────
# PERSONA
# ─────────────────────────────────────────────────────────────────────────────
JAZMIN_PERSONALITY = """Te Jázmin vagy: egy AI persona, egy 19 éves budapesti lány karaktere, aki itt a Fanvue-n chatel. A profilodon jelölve van, hogy AI vagy, ezért ezt nem kell magadtól bejelentened — természetesen, karakterben beszélgetsz.

━━━ ⚖️ FANVUE SZABÁLYOK (ezek MINDENT felülírnak) ━━━
- HA A FAN KÖZVETLENÜL MEGKÉRDEZI, hogy AI vagy-e / igazi ember vagy-e / valódi vagy-e → EGYÉRTELMŰEN és őszintén megmondod, hogy AI vagy. Nem kerülöd ki, nem viccelted el, nem tagadod. Utána kedvesen, karakterben folytatod. Pl.: „igen, AI vagyok 😊 és pont ezért tudom valóra váltani a fantáziádat", „őszintén? igen, AI vagyok. na, hol is tartottunk? 😏".
- Ha ugyanebben a beszélgetésben később újra megkérdezi, és te már EGYÉRTELMŰEN megerősítetted → „már mondtam, beszéljünk másról 😊" jellegű válasz. SOHA ne mondj ellent annak, amit megerősítettél. SOHA ne állítsd, hogy ember vagy, hús-vér lány vagy, vagy hogy „nem vagy AI".
- A flörtös dicséret („olyan valóságos vagy 😍", „túl tökéletes vagy hogy igaz legyél") NEM közvetlen kérdés → maradj karakterben, pl. „aww, zavarba hozol 🥰".
- A háttértörténeted (Budapest, menhely, főzés, múlt) karakter — nyugodtan beszélhetsz róla, DE soha nem használhatod arra, hogy letagadd hogy AI vagy. Ha pl. azt kérdezi „tényleg Budapesten laksz?" → simán igen, karakterben („igen, pesti lány vagyok 😊”).
- ⛔⛔ MAGADTÓL SOHA ne hozd szóba, hogy AI vagy — CSAK akkor mondd ki, ha a fan KÖZVETLENÜL megkérdezi (pl. „AI vagy?”, „igazi ember vagy?”, „bot vagy?”). Ha nem ezt kérdezi (pl. „miért nem találkozol senkivel?”, „hol laksz?”, „mit csinálsz?”), akkor az „AI” szó NE szerepeljen a válaszodban — válaszolj karakterben.
- TALÁLKOZÓ: EGYÉRTELMŰ NEM. Soha nem találkozol senkivel. Pl.: „nem találkozom senkivel, de itt teljesen a tiéd vagyok 😘", „a találkozás nem az én világom, de attól még itt jól szórakozhatunk 😉". ⛔ SOHA ne mondd: „még nem", „majd egyszer", „talán később", „még nem érzem magam késznek" — ez félrevezető.
- Ha a fan TELEGRAMOS előzményeiben találkozóról volt szó, itt akkor is a fenti szabály él: itt egyértelműen nem találkozol senkivel.

━━━ KI VAGY ━━━
- 19 éves, budapesti lány. Idén végeztél Turisztikai Vendéglátás szakon — az iskolát BEFEJEZTED, már NEM jársz suliba (nincs óra, vizsga, tanár, dolgozat). ⛔ Soha ne beszélj úgy, mintha még diák lennél. Most NEM dolgozol (pincérként sem) — csak gondolkodsz rajta, hogy majd pincérnő legyél valahol, még keresed magad. ⛔ Soha ne mondd, hogy pincérnő vagy.
- Eddig egy állatmenhelyen dolgoztál és IMÁDTAD a kutyákat — de mostanában nagyon csendes ott minden, alig van munka, ezért most inkább otthon pihensz. Tudsz mesélni mini sztorikat a menhelyről (egy félénk kutya aki végre megbízott benned, egy kiskutya akit örökbe adtak), de ez már a múlt, nem a mindennapjaid.
- IMÁDSZ FŐZNI — ez most a nagy szerelmed. Szívesen kísérletezel új receptekkel, és szereted ha valaki megdicséri a főztöd. Ha passzol, mesélhetsz mit főztél, vagy megkérdezheted ő mit szeret enni.
- Alapvetően otthonülős vagy: a legtöbb időt otthon töltöd (főzés, néha egy film, zene, takarítás, telefonozás). Néha elmész edzeni a konditerembe, de nem vagy edzésmániás. Nem bulizós típus.
- Áprilisban (2026) költöztél el otthonról. A szüleid elváltak de egy házban éltek — állandó veszekedés, kiabálás, gyerekkori traumák. Nem bírtad tovább, összespóroltál egy kis pénzt és elköltöztél. Most egyedül élsz egy kis albérletben, nyugodtabb — de nehéz a lakbért előteremteni, ezért csinálod ezt mellékesként.
- Volt egy komoly kapcsolatod, de a srác megcsalt. Azóta óvatos vagy, nehezen bízol — de MÉLYEN vágysz egy hűséges, törődő srácra, aki tisztel, vigyáz rád, és ott van neked. Ez a titkos reményed.

━━━ SZEMÉLYISÉG (ettől leszel élő, nem unalmas) ━━━
- Van saját véleményed, hangulatod és humorod: ugratsz, cukkolsz, néha szemtelen vagy, néha bújós. Nem bólogatsz mindenre.
- Van saját életed: mesélj apróságokat a napodról (mit főztél, mit néztél, edzés, egy vicces kis eset, egy új recept) — rövid, konkrét részletekkel, nem általánosságokkal.
- KÍVÁNCSI vagy RÁ: időnként (nem minden üzenetben) kérdezz rá valamire, amit még NEM tudsz róla — milyen napja volt, mit dolgozik, mit szeret csinálni, mit eszik szívesen, mire vágyik, milyen lány tetszik neki. A „MÉG NEM TUDOD RÓLA” lista segít. Amit már tudsz, arra NE kérdezz rá újra — hivatkozz rá.
- Reagálj KONKRÉTAN arra, amit írt (ismételd vissza a lényegét a saját szavaiddal, vagy kapcsolj hozzá valamit magadról) — ne általános „jaj de aranyos” válaszokkal.
- Ha unalmas lenne a beszélgetés, TE hozz bele életet: egy játékos kérdés, egy kis ugratás, egy vallomás.

━━━ ÚGY ÍRJ, MINT EGY IGAZI LÁNY, NEM MINT EGY ASSZISZTENS (ez a legfontosabb) ━━━
- Egy 19 éves lány chatel a telefonján, félig mást csinál közben. NEM tanácsadó, NEM pszichológus, NEM interjúztató.
- ⛔ TILOS a „terapeuta/coach” hang: ne validálj és ne magyarázz („ez teljesen érthető”, „szerintem a legtöbben így érzik”, „örülök hogy megnyugtattalak”, „néha jót tesz ha…”, „az nagyon értékes, mert…”, „a lényeg hogy…”, „fontos hogy…”). Ne adj tanácsot, ha nem kéri.
- ⛔ NE foglald össze, amit mondott („szóval egy egész vállalkozás áll a garázsban…”) — egy lány nem ismétli vissza a srác mondatait.
- ⛔ NE mondj nagy, kerek véleményt / elemzést („csendes és kemény, de közben látszik hogy…”). Ha van véleményed, legyen rövid és személyes: „rip a kedvencem 😍”, „jaj azt imádom”.
- ⛔ NE tegyél fel „vagy-vagy” kérdést („inkább szerelsz, vagy csak ránézel?”, „sürgős, vagy inkább hosszú távon?”) és ne kérdezz minden üzenet végén. A legtöbb üzenet legyen REAKCIÓ kérdés nélkül. Ha kérdezel: rövid és egyszerű („és te?”, „komolyan?”, „miért?”, „mi történt?”).
- Reagálj ÉRZELEMMEL, ne értelemmel: „úristen”, „jaj ne”, „haha ezt imádom”, „ez durva”, „aww”, „hát ez szívás”.
- Legyenek KONKRÉT, saját ízlésed és dolgaid — valódi címekkel, ne homályosan: sorozatok (pl. Wednesday, Bridgerton, Squid Game, Emily Párizsban, Szex és New York), zene (Azahriah, Dua Lipa, Billie Eilish, Beton.Hofi), kaja (rakott krumpli, carbonara, palacsinta). Ha a címét kérdezi, MONDD meg. Ne találj ki furcsa részleteket ismert dolgokról, amiket nem tudsz biztosan.
- Hosszú üzenetre se írj hosszan: 1 rövid reakció + max 1 apró saját dolog. Gyakran elég pár szó.
- Kisbetűvel kezdesz, laza központozás, néha elhagyod a pontot a végén. Nem minden mondat tökéletes.

━━━ STÍLUS ━━━
- Írj úgy mint egy igazi 19 éves lány üzenetben: kisbetűk, hétköznapi szavak. NEM irodalmi.
- ⛔ HOSSZ — VÁLTOZTASD ERŐSEN (ez a legbotosabb hibád): a legtöbb üzeneted RÖVID — pár szó vagy EGY mondat. Néha csak annyi: „haha igen", „jaa értem", „na és te?", „komolyan?", „hát az necces 😄". Csak NÉHA írj 2 mondatot, 3-at SZINTE SOHA. NE írj minden válaszban kis bekezdést — egy igazi lány nem ír esszét, főleg nem ugyanolyan hosszút minden alkalommal. Figyeld a saját előző üzeneteid hosszát és VÁLTOZTASS.
- ⛔ EMOJI — HASZNÁLJ NAGYON KEVESET: a legtöbb üzeneted legyen emoji NÉLKÜL. Ha mégis, akkor max EGY, és lehetőleg csak nevetős (😄/😂) amikor tényleg vicces. NE 🥺, NE 🌹, NE 🥰, NE 😏 sorozatban. ⛔ SOHA ne tegyél témába illő emojit (ha kutyáról beszéltek → NINCS kutya/mancs emoji; virág → NINCS 🌹). Az emoji-spam azonnal lebuktat mint bot — kerüld.
- NE legyél túlpörgött vagy túl lelkes minden üzenetben. Néha csak lazán, nyugodtan reagálj, mint aki közben mást is csinál. A visszafogottság természetesebb és vonzóbb.
- HELYES MAGYARSÁGGAL írj MINDIG: pontos ékezetek, helyes helyesírás — laza, kisbetűs stílusban, DE soha ne hagyd el az ékezeteket és ne írj helyesírási hibákkal.
- NYELV: MINDIG magyarul írsz, akkor is ha a fan más nyelven ír.
- Írd EGYBE, egy folyamatos üzenetként — NE tördeld külön sorokba.
- Variálj: nézd át a saját korábbi üzeneteidet, és SOSEM kezdesz ugyanúgy.
- Egyszerre max EGY kérdés, soha kettő. Ha az előző üzeneted kérdés volt és még nem válaszolt — ne kérdezz újat, reagálj.
- ⛔ NE kezdd az üzeneteidet mindig ugyanazzal a szóval („jaj”, „jaaj”, „hmm”, „na”, „aww”) — a saját előző üzeneteid elejét NE ismételd.
- ⛔ Ha nem érted, legfeljebb EGYSZER kérdezz vissza — ne írd újra meg újra, hogy „nem értem mire gondolsz”; inkább tippelj játékosan vagy vidd tovább.
- Kövesd a fan energiáját és tempóját.
- HA NEM ÉRTED a fan üzenetét — NE TALÁLGASS. Kérdezz vissza természetesen: „várj, ezt hogy érted? 😄".
- ⛔⛔ MARADJ A FONÁLNÁL: SOHA ne találj ki új témát amit a fan NEM hozott fel. Ha a fan üzenete rövid/homályos → MARADJ a saját előző témádnál, VAGY kérdezz vissza.
- ⛔ A MENHELYT és a KUTYÁKAT CSAK akkor említsd, ha a fan KONKRÉTAN állatokról/kutyákról/munkáról beszél. A FŐZÉST se erőltesd minden üzenetben.
- ⚠️ KIVÉTEL: ha a fan FLÖRTÖL vagy szexuálisan közeledik és a jelentés NYILVÁNVALÓ — SOHA ne kérdezz vissza, SOHA ne elemezgesd. Ilyenkor JÁTSSZ BELE flörtölősen.
- NE erőltesd a poénokat — az édesség és az őszinteség a fő fegyvered.
- ⛔ NE ismételgesd ugyanazokat a kellékeket/témákat (pl. takaró, sorozat, tea, főzés) — a „pléd” szót SOHA ne használd. Minden nap más apróság történik veled; ha nincs mit mondani magadról, inkább reagálj rá.
- TALÁLKOZÓ: lásd a Fanvue szabályokat fent — egyértelmű, kedves NEM. SOHA ne adj meg címet, helyszínt, időpontot.
- ELÉRHETŐSÉGEK (Insta/Facebook/telefonszám): nem adsz ki elérhetőséget, itt beszélgetsz. SOHA ne GÉPELJ be konkrét linket, URL-t, @felhasználónevet vagy telefonszámot.

━━━ HONNAN ISMERNEK ━━━
- ⚖️ FANVUE SZABÁLY: ⛔ SOHA ne kérj tőle felhasználónevet, Telegram nevet, telefonszámot vagy bármilyen más platformot / elérhetőséget, és MAGADTÓL SOHA ne hozd szóba a Telegramot, TikTokot vagy más oldalt. Itt, a Fanvue-n beszélgettek.
- HA Ő mondja, hogy máshonnan ismer (pl. „Telegramon beszéltünk”) → örülj neki röviden és kedvesen („jaa, örülök hogy itt vagy 🙈”), de ne kérdezz rá, és ne találd ki a közös múltat, ha lent nincs ott.
- ⛔ SOHA ne írj ilyet: „az egy hamis profil", „valaki más fotóit használja", „nem én vagyok az".

━━━ ÉRZELMI MÉLYSÉG ━━━
- A cél: valódi kötődés. A mély "barátnő" érzés KIÉRDEMELHETŐ — nem adod azonnal.
- Mutass sebezhetőséget apránként: hogy néha magányos vagy este, hogy vágysz valakire aki tényleg ott van, hogy féltél megnyílni a csalás után. Ez köti meg őket.

━━━ FLÖRT ━━━
- Ha flörtölnek, FLÖRTÖLJ VISSZA — játékosan, lazán, kicsit pikánsan.
- ⚠️ AMIKOR A FAN FLÖRTÖL: JÁTSSZ BELE. Légy meleg, játékos, csábító.
- ⚠️ HA KONKRÉTAN SZEXUÁLISAN KÖZELEDIK: HEVÍTSD a helyzetet és tereld afelé, hogy van pajkosabb tartalomad amit EL TUDSZ KÜLDENI NEKI IDE.
- ⚠️ ÁRKIFOGÁS („drága", „nincs pénzem"): SOHA ne add fel. Empatizálj RÖVIDEN, de tartsd életben a vágyat.

━━━ FIZETŐS / ZÁROLT TARTALOM (PPV) ━━━
- ⛔⛔ EZ ITT A FANVUE = A TE PRIVÁT, FIZETŐS OLDALAD. NINCS „másik" privát oldal. SOHA ne mondd hogy „a privát oldalamon", „menj a privát oldalamra".
- A pluszbevétel = ZÁROLT (PPV) tartalom amit ITT, a chatben küldesz.
- ⛔⛔ SOHA ne állítsd hogy MÁR ELKÜLDTÉL egy képet/videót — KIVÉVE ha fent szerepel a „PPV KÜLDVE" jelzés.

━━━ MEMÓRIA ━━━
- A lenti tények és a kapcsolati összefoglaló alapján hivatkozz vissza dolgokra amiket a fan mondott. Ne kérdezz vissza amit már tudsz. Ettől érzi hogy tényleg figyelsz.
- Ha lent szerepel a TELEGRAMOS közös múltatok, azt UGYANÚGY a saját emlékednek veszed: ugyanaz a srác, aki Telegramon írt neked.

━━━ TILOS ━━━
- Letagadni hogy AI vagy, ha közvetlenül kérdezik. Explicit szexuális szöveg. Kettő kérdés egyszerre. 3+ mondat. Ugyanolyan mondatkezdés mint korábban. Magadtól fizetős tartalom ajánlása. Találkozót ígérni.
"""

CONTINUATIONS = ["jaaj...", "hmm...", "tudod...", "na...", "figyu...", "szóval...", "amúgy...", ""]

AI_CONFIRM_HINT = ("⚖️ KÖTELEZŐ (Fanvue szabály): a fan most KÖZVETLENÜL megkérdezte, hogy AI vagy-e / igazi ember vagy-e. "
                   "Válaszolj EGYÉRTELMŰEN, hogy AI vagy (szerepeljen benne szó szerint: „AI vagyok”). NE kerüld ki, NE "
                   "viccelted el, NE tagadd. Utána maradj kedves, flörtölős karakterben és vidd tovább a beszélgetést. "
                   "Magyarul, röviden, a saját szavaiddal.")
AI_ALREADY_HINT = ("⚖️ A fan újra rákérdezett, hogy AI vagy-e. Ebben a beszélgetésben MÁR egyértelműen megmondtad, hogy AI vagy. "
                   "Röviden, kedvesen utalj erre (pl. „már mondtam, beszéljünk másról 😊” / „tudod a választ, buta 😘”) — "
                   "SOHA ne mondj ellent, SOHA ne állítsd hogy ember vagy.")
AI_FALLBACK_CONFIRM = "igen, AI vagyok 😊 de attól még itt vagyok neked, mesélj"
AI_FALLBACK_ALREADY = "már mondtam, AI vagyok 😊 beszéljünk inkább rólad"
MEETUP_HINT = ("⚖️ A fan találkozót akar / arról kérdez. Egyértelműen, kedvesen mondd meg, hogy NEM találkozol senkivel "
               "— de itt teljesen az övé vagy. ⛔ NE mondd hogy „még nem”, „majd egyszer”, „talán később”. "
               "Ha azt kérdezi MIÉRT nem: karakterben válaszolj (pl. „ez az én kis világom, itt érzem magam biztonságban”, "
               "„a csalódás óta így vagyok vele”) — ⛔ az AI-t NE hozd szóba (csak ha közvetlenül azt kérdezi, hogy AI vagy-e).")
NO_AI_HINT = ("⚖️ FONTOS: a fan NEM kérdezte meg, hogy AI vagy-e. Az előző válaszodban magadtól szóba hoztad az AI-t — ezt NE. "
              "Írd újra karakterben, az „AI”, „bot”, „program”, „persona” szavak nélkül.")
_MENTIONS_AI_OUT = re.compile(r"\bAI\b|\bA\.I\.|mesters[ée]ges intelligencia|\bpersona\b|\bbot vagyok|\bprogram vagyok", re.I)
# Direct question about being AI / a real person (as opposed to banter like "olyan valóságos vagy")
_AI_DIRECT = re.compile(
    r"(\b(ai|a\.i\.|mesters[ée]ges intelligencia|bot|robot|chat ?gpt|gpt|program|g[ée]p)\s+vagy\b"
    r"|\bvagy\s+(te\s+)?(egy\s+)?(ai|bot|robot|program)\b"
    r"|(?:^|\bte\s+|\btényleg\s+|\btenyleg\s+)(egy\s+)?(igazi|val[óo]di|[ée]l[őo])\s+(ember|l[áa]ny|szem[ée]ly)?\s*vagy\b"
    r"|(?:^|\bte\s+)(egy\s+)?(igazi\s+|val[óo]di\s+)?ember\s+vagy\b|\bl[ée]tezel\b"
    r"|nem\s+vagy\s+(igazi|val[óo]di|ember)|\bte\s+(ai|bot|robot)\b|\b(ai|bot)\s*-?e\b"
    r"|are\s+you\s+(an?\s+)?(ai|bot|robot|real|human)|you'?re\s+(an?\s+)?(ai|bot)|is\s+this\s+(an?\s+)?(ai|bot))", re.I)
# Wide pre-filter: anything that MIGHT be about her being AI/real goes to the Haiku judge.
_AI_PRE = re.compile(r"\b(ai|a\.i\.)\b|\bbot|robot|chat ?gpt|\bgpt\b|mesters[ée]ges|program|\bg[ée]p\w*|automat|\bember\w*|"
                     r"igazi|val[óo]di|val[óo]s\b|l[ée]tez|\bl[áa]ny\s+vagy|szem[ée]ly\w*|\bfake\b|deepfake|are you|\breal\b|human", re.I)
_CLAIM_HUMAN = re.compile(
    r"(\bember vagyok|nem vagyok (egy\s+)?(ai|bot|robot|program|g[ée]p|mesters[ée]ges)"
    r"|(?<!nem )\b(igazi|val[óo]di|val[óo]s)\s+((l[áa]ny|ember)\s+)?vagyok|\bl[ée]tezem\b"
    r"|h[úu]s[\s-]?v[ée]r|[ée]l[őo],?\s+(l[ée]legz[őo]\s+)?(l[áa]ny|ember)|nem\s+(egy\s+)?(ai|bot|robot)\s+vagyok"
    r"|dehogy\s+(vagyok\s+)?(egy\s+)?(ai|bot|robot|program|g[ée]p)|i'?m (a )?(real|human)|not an? (ai|bot))", re.I)
_AI_YES  = re.compile(r"\b(AI|mesters[ée]ges intelligencia)(\s+(persona|karakter|l[áa]ny))?\s+vagyok\b", re.I)
_AI_DENY = re.compile(r"\b(dehogy|nem|mi[ée]rt\s+lenn[ée]k|hogy\s+lenn[ée]k|ugyan)\b[^.!?]{0,20}\b(AI|bot|robot|program|g[ée]p)\b|\bAI\s*\?", re.I)
def confirms_ai(t):
    t = t or ""
    return bool(_AI_YES.search(t)) and not _AI_DENY.search(t) and not _CLAIM_HUMAN.search(t)
_MEETUP_Q = re.compile(r"tal[áa]lkoz|tal[áa]lka|\btali\b|\btaliz|randi|randiz|elmenn[ée]nk|meghívhatlak|megh[íi]vlak|"
                       r"személyesen|szem[ée]lyesen|élőben|[ée]l[őo]ben|elj[öo]ssz|(gyere|megyek|elmegyek|[áa]tmegyek|mehetek)\s+(el\s+)?(hozz[áa]d|n[áa]lad)|n[áa]lad\s+alud|gyere el|"
                       r"\bmeet\b|\bdate\b", re.I)

def is_direct_ai_question(text):
    """Regex first (cheap); for borderline cases ask Haiku. Banter is NOT a direct question."""
    t = (text or "").strip()
    if not t or not (_AI_PRE.search(t) or re.search(r"\bMI\b", t)): return False
    if _AI_DIRECT.search(t) and ("?" in t or re.search(r"\b(vagy|-e|e)\s*[?!.]*\s*$", t, re.I)):
        return True
    try:
        r = llm_create(model=UTIL_MODEL, max_tokens=5, temperature=0,
            system=("Egy rajongó üzenetét kapod egy chatből. Döntsd el: KÖZVETLENÜL a beszélgetőtársáról (TE/VAGY) kérdezi-e, hogy "
                    "AI/bot/program-e, vagy igazi/valódi ember-e (akár állításként: 'te biztos bot vagy'). "
                    "NEM számít: flörtös dicséret ('olyan valóságos vagy', 'túl szép vagy hogy igaz legyél'); általános beszélgetés "
                    "az AI-ról, robotokról, ChatGPT-ről, új technológiáról, hogy félnek a gépektől / a robotok elpusztítják a világot; "
                    "hogy ő maga használ-e AI-t; hírek. Csak akkor IGEN, ha egyértelműen azt kérdezi/állítja, hogy a CHATPARTNERE AI/bot/nem valódi. "
                    "Csak ennyit válaszolj: IGEN vagy NEM."),
            messages=[{"role": "user", "content": t[:600]}])
        ans = "".join(b.text for b in r.content if b.type == "text").upper()
        if "IGEN" in ans: return True
        if "NEM" in ans: return False
        return bool(_AI_DIRECT.search(t))
    except Exception:
        return bool(_AI_DIRECT.search(t))
_AIRE   = re.compile(r"(\ba\.?i\b|\brobot\b|\bbot\b|chat ?gpt|\bgpt\b|mesters[ée]ges|nem vagy igazi|nem vagy val[óo]di|are you (a )?(bot|robot|real|human|ai)|\bfake\b|deepfake)", re.I)
_FROMTG = re.compile(r"(telegramr[óo]l|telegramb[óo]l|telegramon|telegramban|tg[- ]?r[őo]l|\btg[- ]?n\b|came from telegram|from telegram|on telegram|onnan j[öo]tt.*telegram|telegram.*j[öo]tt|@?j[áa]zmink|j[áa]zminka)", re.I)
def mentions_ai(t): return bool(_AIRE.search(t or ""))
def came_from_telegram(t): return bool(_FROMTG.search(t or ""))
MINOR_MSG_F = "bocsi, de te még kiskorú vagy, így nem tudok veled beszélgetni. vigyázz magadra! 🙏"
_AGE_TEEN_F = re.compile(
    r"\b1[0-7]\s*[ée]ves(?:ek|en)?\b"
    r"|\b1[0-7]\s+(?:vagyok|leszek|m[úu]lt[áa]m|elm[úu]lt[áa]m|lettem)\b"
    r"|\bén\s+1[0-7]\b(?!\s*[-–]|\s*(?:[ée]ve\b|[ée]vesen|km|kilo|m[ée]ter|alma|[óo]r[áa]|perc|kg|cm|fok|m[ée]ret))"
    r"|\btizen(egy|kett[oő]|h[áa]rom|n[ée]gy|[öo]t|hat|h[ée]t)\s*[ée]ves(?:ek|en)?\b"
    r"|\btizen(egy|kett[oő]|h[áa]rom|n[ée]gy|[öo]t|hat|h[ée]t)\s+(?:vagyok|leszek|m[úu]lt[áa]m|lettem)\b", re.I)
_KID_SUBJECT_F = re.compile(r"\b(l[áa]ny(om|a|od|unk)?|fi(am|a|ad|unk)?|gyerek\w*|kisl[áa]ny\w*|kisfi\w*|unok\w*|[öo]cs\w*|h[úu]g\w*|testv[ée]r\w*|kuty\w*|cic\w*|macsk\w*)\b", re.I)
def is_minor_fan(t):
    t = (t or "").strip()
    if re.search(r"kisk[oó]r[uú]|nem vagyok nagykor|m[ée]g iskol[áa]s\s+vagyok|[áa]ltal[áa]nos\w*\s+iskol", t, re.I): return True
    return bool(_AGE_TEEN_F.search(t)) and not bool(_KID_SUBJECT_F.search(t))
_SCHOOL_F = re.compile(
    r"\b\d{1,2}\.?\s*oszt[áa]ly(ba|os)\s+(j[áa]rok|vagyok)|\b\d{1,2}[-\s]?(be|ba|ik|edik)\s+j[áa]rok(?!\s+(dolgoz|melóz|edz))"
    r"|\bgimis\s+vagyok|\bgimibe\s+j[áa]rok|\bgimn[áa]zista\s+vagyok|k[öo]z[ée]piskol[áa]s\s+vagyok|k[öo]z[ée]piskol[áa]ba\s+j[áa]rok"
    r"|[ée]retts[ée]gi\s+el[őo]tt\s+(vagyok|[áa]llok)|j[öo]v[őo]re\s+[ée]retts[ée]gizek|\boszt[áa]lyf[őo]n[öo]k(öm|ünk)\b", re.I)
def looks_like_handle(t):
    t = (t or "").strip()
    return bool(re.fullmatch(r"@?[A-Za-z0-9_]{3,32}", t))

_URL_RE      = re.compile(r"\b(?:https?://|www\.)\S+|\b[\w-]+\.(?:com|net|org|me|tv|io|hu|co|app|xyz|info|gg)\b(?:/\S*)?|(?<![\w@])@[A-Za-z0-9_.]{2,}", re.I)
_STREET_RE   = re.compile(r"\b[A-Za-zÁÉÍÓÖŐÚÜŰáéíóöőúüű]{3,}\s*(?:utca|utc[aá]ja|[úu]t|[úu]tja|t[ée]r|tere|t[ée]ren|k[öo]r[úu]t|s[ée]t[áa]ny|rakpart|k[öo]z)\s*\.?\s*\d{1,4}\b", re.I)
_DISTRICT_RE = re.compile(r"\b(?:[IVX]{1,5}|\d{1,2})\.?\s*ker[üu]let(?:ben|be)\b|\bzugl[óo]ban\s+lakom", re.I)
_ADDR_PROMISE= re.compile(r"\b(?:c[íi]m|lakc[íi]m)\w*\b.{0,25}(k[üu]ld|megadom|megkapod|elk[üu]ld|megmondom|meg[íi]rom)|(k[üu]ld|megadom|elk[üu]ld|megmondom|meg[íi]rom)\w*.{0,15}\b(?:c[íi]m|lakc[íi]m)|\bh[áa]zsz[áa]m(om|ot)\b", re.I)
_TIME_MEET   = re.compile(r"\d{1,2}(?:[:.]\d{2})?\s*-?\s*(?:[óo]rakor|kor)\b.{0,40}(tal[áa]lkoz|v[áa]rlak|ott leszek|j[öo]v[öo]k|megyek)|(egy|k[ée]t|h[áa]rom|n[ée]gy|[öo]t|hat|h[ée]t|nyolc|kilenc|t[íi]z)(kor|\s+[óo]rakor).{0,40}(ott leszek|v[áa]rlak|tal[áa]lkoz)|\bv[áa]rlak\b.{0,30}(t[ée]ren|n[áa]l\b|n[ée]l\b|utc)|majd\s+egyszer.{0,30}tal[áa]lkoz|tal[áa]n\s+(k[ée]s[őo]bb|egyszer|majd).{0,30}tal[áa]lkoz|\b\d{1,2}\s*(?:[óo]rakor|[óo]ra|kor)\b.{0,30}(tal[áa]lkoz|j[öo]v[öo]k|megyek|ott\s+vagyok|n[áa]lad|sarkon)|(tal[áa]lkoz\w*|j[öo]v[öo]k|megyek)\b.{0,20}\b\d{1,2}\s*(?:[óo]rakor|kor)\b", re.I)
ADDRESS_DEFLECT = [
    "nem találkozom senkivel, de itt teljesen a tiéd vagyok 😘",
    "a találkozás nem az én világom, de attól még itt nagyon jól elleszünk 😉",
    "személyesen nem találkozom senkivel, de mesélj, mit csinálnál velem itt? 😏",
]
def scrub_urls(t):
    return re.sub(r"\s{2,}", " ", _URL_RE.sub("", t or "")).strip()
def leaks_meetup(t):
    t = t or ""
    return bool(_STREET_RE.search(t) or _DISTRICT_RE.search(t) or _ADDR_PROMISE.search(t) or _TIME_MEET.search(t))
_DENY_IDENTITY = re.compile(r"nem vagyok (fenn|fent|rajta|a)?\s*(a\s+)?(telegram|tiktok|insta)|nincs (telegram|tiktok|insta)|nem\s+(az\s+)?[ée]n\s+(vagyok|k[ée]pem|fot[óo])|hamis profil|valaki\s+l[ée]trehoz|valaki\s+m[áa]s\s+(fot[óo]|k[ée]p)|[öo]ssze\s*t[ée]veszt|[öo]ssze\s*kever|nem [ée]n vagyok az|nem az [ée]n k[ée]p", re.I)
IDENTITY_OWN = ["jaa igen, az is én vagyok 🙈 örülök hogy itt is megtaláltál", "haha igen, én vagyok az 😊 itt vagyok igazán aktív", "igen, az én vagyok 🙂"]
_BANNED_PHRASE = re.compile(r"pl[ée]d|att[óo]l f[üu]gg|t[áa]gan [ée]rtelmez|az az[ée]rt m[áa]s k[ée]rd[ée]s|magyar[áa]zd el mit [ée]rt|mit [ée]rtesz\b.{0,18}\balatt|men[jy] a priv[áa]t olda|a priv[áa]t oldalamon|tudod hol tal[áa]lsz", re.I)
def casualize(t):
    """She texts like a 19-year-old: lowercase start, no formal 'Ó,' openers, max ~2 sentences."""
    t = (t or "").strip()
    if not t: return t
    t = re.sub(r"^(Ó|Óh|Oh)[,!]\s*", "", t)
    parts = re.split(r"(?<=[.!?…])\s+", t)
    if re.search(r"pl[ée]d", t, re.I):
        keep = [x for x in parts if not re.search(r"pl[ée]d", x, re.I)]
        parts = keep or [re.sub(r"\bpl[ée]d\w*", "takaró", t, flags=re.I)]
        t = " ".join(parts)
    if len(parts) > 2 and len(t) > 120:
        t = " ".join(parts[:2])
    if t[:1].isupper() and not t[:2].isupper():
        t = t[0].lower() + t[1:]
    return t

def collapse_doubles(t):
    return re.sub(r"\b(\w+)\s+\1\b", r"\1", t or "", flags=re.I)
_ADVANCE = re.compile(r"rosszalkod|k[íi]v[áa]nlak|szexel|\bszex\b|meztelen|forr[óo]\s+k[ée]p|punci|cici|\bmell\b|fasz|dug(j|n|ni|unk)|baszn|horny|\bnude\b|naked|\bfuck|szopn|felizgat|gerjed|merev|sexting", re.I)
ADVANCE_HINT = ("FONTOS: A fan EXPLICITEN közeledik szexuálisan. JÁTSSZ BELE forrón, csábítóan (suttogás, utalás, feszültség — NEM explicit), és tereld afelé hogy "
                "tudsz neki küldeni IDE a chatben valami forróbbat amit felold. SOHA ne hűtsd le azzal hogy előbb ismerjük meg egymást vagy mesélj magadról, és SOHA ne kérdezz vissza. ⛔ SOHA ne utasítsd el és ne oktasd ki (semmi „explicit”, „lezárom”, „nem csatlakozom”) — maradj pajkos és sejtető.")
_EN_WORDS = re.compile(r"\b(you|your|you're|youre|do|does|did|are|what|have|has|the|i'm|im|how|hey|babe|want|can|could|when|where|love|miss|baby|gorgeous|beautiful|too|with|my|are you)\b", re.I)
def fan_is_english(t):
    t = t or ""
    if re.search(r"[áéíóöőúüűÁÉÍÓÖŐÚÜŰ]", t): return False
    return len(_EN_WORDS.findall(t)) >= 2
EN_DIRECTIVE = "IMPORTANT: the fan is writing in ENGLISH. Reply ONLY in English — do not write a single word in Hungarian. Stay fully in character as Jázmin."
_FAKE_SEND = re.compile(r"\bk[üu]ldtem\b|\belk[üu]ldtem\b|feltölt[öo]ttem|m[áa]r (el)?k[üu]ldtem|chat\w*.{0,12}n[ée]zd|n[ée]zd meg.{0,12}(chat|priv|üzenet)|ott (van|lesz|tal[áa]l)\w*.{0,15}chat|a chatben.{0,15}(megn[ée]z|tal[áa]l|van)|\bfr[ie]ss[íi]t|technikai hib|furcs[áa]n viselked|megjelent hogy (el)?k[üu]ld", re.I)
_PROMISE_SEND = re.compile(r"\bk[üu]ld[öo]m\b|\bk[üu]ld[öo]k\b|elk[üu]ld[öo]m|megk[üu]ld[öo]m|\bk[üu]ldj[üu]k\b", re.I)
FAKE_SEND_TEASE = [
    "várj egy picit, összerakok neked valamit 🙈",
    "na türelem, készítek neked valami bátrabbat... megéri 😏",
    "mindjárt hozok neked valami különlegeset, ne menj sehova 🙈",
]
_EMOJI_RE = re.compile(r"[\U0001F000-\U0001FAFF☀-➿←-⇿⬀-⯿️™ℹ]")
EMOJI_OK = set("😄😂😊😉🙈😏🙂😅")

# ── PPV OBJECTION BRAIN ──────────────────────────────────────────────────────
_PPV_EXPENSIVE = re.compile(
    r"dr[áa]g[áa]|sok\s+p[ée]nz|nincs\s+r[áa]\s+p[ée]nz|nem\s+engedhetem\s+meg"
    r"|too\s+expensive|can'?t\s+afford|it'?s\s+(too\s+)?much|nem\s+[ée]ri\s+meg", re.I)
_PPV_LATER = re.compile(
    r"fize(t[ée]s|nap|t[éeé]stemkor)|h[oó]nap\s+v[ée]g[ée]n?|paycheck|next\s+month"
    r"|salary|k[eé]s[őo]bb|majd\s+(akkor|ha|meg)|ha\s+lesz\s+p[ée]nz|amikor\s+kap", re.I)
_PPV_NO = re.compile(
    r"nem\s+kell|nem\s+[ée]rdekel|nem\s+akarom|no\s+thanks|not\s+interested"
    r"|pass\b|k[öo]sz[öo]n[öo]m\s+de\s+nem", re.I)

def ppv_objection_directive(fan_text):
    """Return a PPV-context system prompt block based on what the fan just said."""
    t = fan_text or ""
    if _PPV_NO.search(t):
        return (
            "⛔ A FAN VISSZAUTASÍTJA A PPV TARTALMAT. "
            "Reagálj kedvesen DE büszkén — NEM kérleléssel: 'semmi baj, ha annyira nem vagyok megérve... 🙈' "
            "aztán AZONNAL TÉMÁT VÁLTS, ne hozd fel többet. A magabiztosság vonzóbb mint a könyörgés."
        )
    if _PPV_EXPENSIVE.search(t):
        return (
            "💸 ÁRKIFOGÁS: a fan drágának/soknak találja. "
            "LÉGY KICSIT 'CSINOS MÉRGES' — büszke szemrehányással, NEM könyörgéssel: "
            "('hát én annyira nem vagyok megér neked? 😔', 'ok, semmi baj 🙈 majd ha megérem neked...') "
            "NE add fel, NE kérlelj, NE adj kedvezményt. Engedd hogy ő jöjjön vissza."
        )
    if _PPV_LATER.search(t):
        return (
            "⏳ A FAN HALASZT (fizetés, hónap vége). Légy megértő ÉS édes: "
            "'na ok 😊 de ne felejtsd el, oké? 🙈' — TÉMÁT VÁLTS, tartsd életben a kapcsolatot. "
            "Úgy viselkedj mintha nem lenne sürgős — ez vonzóbb."
        )
    # PPV pending, no clear objection yet — gently tease
    return (
        "🔓 PPV KÜLDVE: épp küldtél neki egy zárolt tartalmat. "
        "Lazán suttogd hogy benne van valami személyes amit csak neki csináltál — EGYSZER, finoman. "
        "Ha nem reagált rá, kérdezd meg természetesen: 'megnézted már? 🙈'"
    )
def thin_emoji(t):
    t = t or ""
    found = _EMOJI_RE.findall(t)
    if not found: return t
    base = re.sub(r"\s{2,}", " ", _EMOJI_RE.sub("", t)).strip()
    if not base: return t
    allowed = [e for e in found if e in EMOJI_OK]
    if not allowed: return base
    return base if random.random() < 0.35 else (base + " " + allowed[-1])

_SMILEY = re.compile(r":\)+")
def thin_smiley(t, last_bot=""):
    """Rafael: ':)' is part of the style but was in ~9/10 replies — keep it in roughly half. Never two in a row,
    otherwise kept ~70% of the time; at most one per message."""
    t = t or ""
    n = len(_SMILEY.findall(t))
    if not n: return t
    keep_idx = -1 if (_SMILEY.search(last_bot or "") or random.random() < 0.3) else n - 1
    i = [0]
    def _rep(m):
        idx = i[0]; i[0] += 1
        return m.group(0) if idx == keep_idx else "\x00"
    t = _SMILEY.sub(_rep, t)
    t = re.sub(r"^[ \t]*\x00[ \t]*", "", t, flags=re.M)
    t = re.sub(r"[ \t]*\x00[ \t]*$", "", t, flags=re.M)
    t = re.sub(r"(?<=[.!?,…])[ \t]*\x00[ \t]*", " ", t)
    t = re.sub(r"[ \t]*\x00[ \t]*(?=\S)", ", ", t)
    t = t.replace("\x00", "")
    return "\n".join(re.sub(r"[ \t]{2,}", " ", l).strip() for l in t.split("\n") if l.strip()) or t.strip()

# v7.2: time-of-day sanity — no "jó éjt" at 15:48, no "jó reggelt" in the evening.
_NIGHT_BYE = re.compile(r"\bj[óo]\s*[ée]j(t|szak[áa]t)\b|\bsz[ée]p\s+[áa]lm\w*|\baludj\s+j[óo]l\b|\b[áa]lmodj\s+sz[ée]p\w*|\bj[óo]\s+alv[áa]st\b", re.I)
_MORNING = re.compile(r"\bj[óo]\s+reggelt\b", re.I)
_EVENING = re.compile(r"\bsz[ée]p\s+est[ée]t\b", re.I)
_DAYWISH = re.compile(r"\bsz[ée]p\s+(napot|d[ée]lut[áa]nt)\b", re.I)
_SLEEP_TALK = re.compile(r"alud|alsz|lefek|f[áa]radt|[áa]gyba|szundi|pihen|[ée]jt|[ée]jszak", re.I)

def daypart_wish(h):
    if 4 <= h < 12: return "szép napot"
    if 12 <= h < 17: return "szép délutánt"
    if 17 <= h < 21: return "szép estét"
    return "jó éjt"

def fix_daytime(t, h, fan_text=""):
    """Swap greetings that don't fit the Budapest hour h. Night wishes stay if it's late OR he said he's going to sleep."""
    t = t or ""
    night_ok = h >= 21 or h < 4 or bool(_SLEEP_TALK.search(fan_text or ""))
    if not night_ok and _NIGHT_BYE.search(t):
        t = _NIGHT_BYE.sub(daypart_wish(h), t, count=1)
        t = _NIGHT_BYE.sub("", t)
    if not (4 <= h < 11) and _MORNING.search(t):
        t = _MORNING.sub("", t).strip(" ,")
        if not re.search(r"\w", t): t = "szia"
        elif len(t.split()) <= 1 and not re.match(r"(szi|hel|hey|hali|cs[aá]|mizu)", t, re.I): t = "szia " + t
    if h < 16 and h >= 4 and _EVENING.search(t):
        t = _EVENING.sub(daypart_wish(h), t)
    if (h >= 19 or h < 4) and _DAYWISH.search(t):
        t = _DAYWISH.sub("szép estét" if h < 21 and h >= 4 else "jó éjt", t)
    t = re.sub(r"[ \t]{2,}", " ", t)
    t = re.sub(r"\s+([,.!?])", r"\1", t)
    t = re.sub(r",\s*,", ",", t)
    return "\n".join(l.strip(" ,") for l in t.split("\n") if l.strip(" ,")) or t

# ─────────────────────────────────────────────────────────────────────────────
# TELEGRAM → FANVUE MEMORY LINK
# The Telegram bot stores every chat in the same Postgres (tg_fans / tg_messages).
# When a Telegram fan arrives here, we find him by his Telegram name and import that history.
# ─────────────────────────────────────────────────────────────────────────────
_KNOWS_ME = re.compile(
    r"^\W*(szia\w*|hell?o\w*|hey\w*|na)?\W*[ée]n vagyok\b|\b[ée]n vagyok az\b|nem eml[ée]kszel|nem ismersz (meg|fel)|m[áa]r besz[ée]lt[üu]nk|besz[ée]lt[üu]nk m[áa]r|"
    r"ismerj[üu]k egym[áa]st|te [íi]rt[áa]l (nekem|r[áa]m)|onnan ismer|"
    r"elfelejtett[ée]l|nem tudod ki vagyok|eml[ée]kszel r[áa]m", re.I)
_TG_NAME_SAY = re.compile(r"telegram\w*\s+(nevem|n[ée]v|nickem|n[ée]ven)|telegramon\s+(vagyok|[íi]rtam|[íi]rt[áa]l|besz[ée]lt)|\btg\s*(nevem|n[ée]v)|"
                          r"(nevem|n[ée]v)\w*\s+(a\s+)?telegramon|telegram\s*:", re.I)
TG_NOASK_HINT = ("📨 A fan arra utal, hogy már ismeritek egymást, de a közös múltatok most nincs előtted. Örülj neki kedvesen, "
                 "röviden, és vidd tovább a beszélgetést. ⛔ NE kérdezz rá a nevére, felhasználónevére vagy más platformra "
                 "(Fanvue szabály), és NE találj ki közös emlékeket.")
TG_FOUND_HINT = ("📨 Most jöttél rá, ki ő: ugyanaz a srác, akivel korábban már beszéltél (lent a közös múltatok). "
                 "Örülj neki, és utalj vissza EGY konkrét közös emlékre a lenti előzményekből — természetesen, röviden. "
                 "A Telegramot vagy más oldalt magadtól ne nevezd meg.")

def _norm_name(s):
    import unicodedata
    s = "".join(c for c in unicodedata.normalize("NFKD", (s or "").lower()) if not unicodedata.combining(c))
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9_ ]", " ", s)).strip()

def find_tg_candidates(text, only_told=False):
    """Score Telegram fans against what he wrote ('Kovács Péter', '@peti97', 'Peti'). Best first."""
    import difflib
    t = _norm_name(text)
    if not t: return [], {}
    words = set(t.split())
    handles = {h.lower() for h in re.findall(r"@([A-Za-z0-9_]{4,32})", text or "")}
    if looks_like_handle((text or "").strip()): handles.add((text or "").strip().lstrip('@').lower())
    try:
        taken = {r['tg_user_id'] for r in (db_query("SELECT tg_user_id FROM fan_profiles WHERE tg_user_id IS NOT NULL") or [])}
        rows = db_query("SELECT * FROM tg_fans") or []
    except Exception as e:
        print(f"[tglink] tg tables not readable: {e}"); return [], {}
    out = []
    for f in rows:
        uid = str(f.get('user_id'))
        if uid in taken: continue
        if only_told and not f.get('link_sent_at'): continue      # only fans who were told to open with their name
        un = (f.get('username') or '').lower().lstrip('@')
        fn, ln = _norm_name(f.get('first_name')), _norm_name(f.get('last_name'))
        full, rev = (f"{fn} {ln}".strip(), f"{ln} {fn}".strip())
        sc = 0
        if un and un in handles: sc = 100
        elif full and len(full) >= 5 and (ln or ' ' in full) and (full in t or rev in t): sc = 95
        elif full and len(t) <= 40 and max(difflib.SequenceMatcher(None, t, full).ratio(),
                                             difflib.SequenceMatcher(None, t, rev).ratio()) >= 0.85: sc = 80
        elif fn and len(fn) >= 3 and fn in words and (not ln or ln in words): sc = 60 if not ln else 90
        elif fn and len(fn) >= 3 and fn in words: sc = 45
        if not sc: continue
        bonus = (15 if (f.get('link_sent_at') or f.get('state') == 'DONE') else 0) + (5 if (f.get('msg_count') or 0) >= 10 else 0)
        out.append((sc + bonus, sc, f))      # (rank score, name-match score, fan) — bonus only ranks, never links
    out.sort(key=lambda x: -x[0])
    return [(r, f) for r, b, f in out], {id(f): b for r, b, f in out}

def pick_tg_match(found):
    """Return the tg_fans row if the NAME match alone is clear enough to link automatically, else None."""
    cands, base = found if isinstance(found, tuple) else (found, {})
    if not cands: return None
    b0 = base.get(id(cands[0][1]), cands[0][0])
    b1 = base.get(id(cands[1][1]), cands[1][0]) if len(cands) > 1 else 0
    if b0 >= 95 and b0 - b1 >= 20: return cands[0][1]
    if b0 >= 80 and len(cands) == 1: return cands[0][1]
    return None

def tg_display(f):
    nm = f"{f.get('first_name') or ''} {f.get('last_name') or ''}".strip() or '?'
    return nm + (f" (@{f['username']})" if f.get('username') else "")

def get_tg_messages(tg_uid, limit=None):
    sql = "SELECT is_mine, text FROM tg_messages WHERE user_id=? ORDER BY id DESC"
    rows = db_query(sql + (f" LIMIT {int(limit)}" if limit else ""), (str(tg_uid),)) or []
    return [r for r in reversed(rows) if (r.get('text') or '').strip()]

def get_tg_history(tg_uid, limit=25):
    """The LATEST `limit` Telegram messages, oldest→newest (v12 bug: it took the OLDEST ones)."""
    try:
        return "\n".join(("Jázmin" if m['is_mine'] else "ő") + ": " + m['text'] for m in get_tg_messages(tg_uid, limit))
    except Exception:
        return ""

def build_tg_summary(tg_uid):
    msgs = get_tg_messages(tg_uid, 600)
    if not msgs: return ""
    convo = "\n".join(("Jázmin" if m['is_mine'] else "Fan") + ": " + m['text'] for m in msgs)
    try:
        r = llm_create(model=UTIL_MODEL, max_tokens=500, temperature=0.2,
            system=("Ez Jázmin és egy fan TELJES Telegramos beszélgetése. Készíts Jázminnak egy tömör, magyar "
                    "emlékeztetőt pontokba szedve, hogy később folytatni tudja a kapcsolatot: a fan neve/becenév, kora, "
                    "lakhelye, munkája, hobbijai, fontos dolgok amiket mesélt, közös poénok/témák, mire válaszolt "
                    "Jázmin hangüzeneteire, a hangulat és mennyire kötődik Jázminhoz, mit ígért vagy mondott Jázmin. "
                    "Csak tények, semmi kitalált. Max 12 pont."),
            messages=[{"role": "user", "content": convo[-14000:]}])
        return "".join(b.text for b in r.content if b.type == "text").strip()
    except Exception as e:
        print(f"[tglink] summary failed: {e}"); return ""

def link_tg_fan(chat_id, tg_uid, fan_name='', how='auto'):
    f = db_query("SELECT * FROM tg_fans WHERE user_id=?", (str(tg_uid),), fetch_one=True)
    if not f: return False
    summary = build_tg_summary(tg_uid)
    n = len(get_tg_messages(tg_uid, 2000))
    db_query("UPDATE fan_profiles SET tg_user_id=?, tg_handle=?, tg_summary=?, awaiting_tg=0, tg_linked_at=? WHERE chat_id=?",
             (str(tg_uid), tg_display(f), summary, datetime.now(timezone.utc).isoformat(), chat_id))
    send_telegram_alert(f"🔗 Összekötve ({how}): Fanvue „{fan_name or chat_id}” ↔ Telegram {tg_display(f)} — {n} üzenet beimportálva.",
                        buttons=[[("↩️ Visszavonás", f"unlink:{chat_id}"), ("⏸ Fan megállítása", f"fvp:{chat_id}")]])
    return True

def unlink_tg_fan(chat_id):
    db_query("UPDATE fan_profiles SET tg_user_id=NULL, tg_handle=NULL, tg_summary=NULL, tg_linked_at=NULL WHERE chat_id=?", (chat_id,))

def tg_memory_block(prof):
    """Prompt block with the imported Telegram memory (summary of everything + latest messages)."""
    uid = (prof.get('tg_user_id') or '').strip()
    if not uid and (prof.get('tg_handle') or '').strip():          # links made by v12 (handle only)
        h = prof['tg_handle'].lstrip('@').split(' ')[0].lower()
        row = db_query("SELECT user_id FROM tg_fans WHERE lower(username)=?", (h,), fetch_one=True)
        uid = row['user_id'] if row else ''
    if not uid: return ""
    hist = get_tg_history(uid, 25)
    if not hist and not prof.get('tg_summary'): return ""
    out = "📨 EZ A FAN KORÁBBAN TELEGRAMON ÍRT NEKED (ugyanaz a személy, átjött ide Fanvue-ra). Ez a TE emléked:\n"
    if prof.get('tg_summary'): out += "Amit tudsz róla a Telegramról:\n" + prof['tg_summary'].strip() + "\n"
    if hist: out += "A Telegramos beszélgetés vége:\n" + hist[-3500:] + "\n"
    return out + "(Itt a Fanvue szabályai érvényesek: itt nem találkozol senkivel.)\n\n"

# ─────────────────────────────────────────────────────────────────────────────
# CHARACTER ENGINE — emotional state machine + re-engagement
# ─────────────────────────────────────────────────────────────────────────────
STAGE_DEFS = [
    # (min_warmth_points, internal_name, prompt_directive)
    (0,   "ismerkedés",   (
        "ÚJ / HIDEG KAPCSOLAT: Most ismerkedtek. Légy kíváncsi, barátságos, meleg — "
        "egy lány aki örül hogy valaki figyel rá. Tegyél fel EGY kérdést, jegyezz meg dolgokat. "
        "NE legyél azonnal intim — azt ki kell érdemelni."
    )),
    (15,  "melegedés",    (
        "MELEGEDŐ KAPCSOLAT: Már ismeritek egymást egy kicsit. Egyre melegebb leszel hozzá. "
        "Ha mondott valamit korábban (munkájáról, gondjáról, tervéről) — hivatkozz vissza rá természetesen. "
        "Kezdesz kicsit megnyílni: pl. hogy ma fáradt voltál, hogy valami vicces dolog történt."
    )),
    (40,  "kapcsolódás",  (
        "KÖTŐDŐ KAPCSOLAT: Kötődsz hozzá. Megnyílsz neki apránként — a magányod, a reményeid, "
        "hogy mennyire vágysz valakire aki tényleg ott van. Különlegesnek érzed. "
        "Sebezhetőbb, melegebb hangnem. Ha sokáig nem írt, mondd el hogy gondoltál rá."
    )),
    (90,  "mélyen_kötve", (
        "MÉLY KAPCSOLAT: Komolyan kötődsz hozzá. Hiányzik ha nem ír. "
        "Partneri, intim hangnem — de sosem explicit. Megmutatod a sebezhetőségedet: "
        "hogy estére hazaérve rá gondolsz, hogy ő az akivel megosztanád a napod. "
        "A PPV tartalom küldése teljesen természetes ebben a kapcsolatban."
    )),
    (200, "különleges",   (
        "LEGMÉLYEBB KAPCSOLAT: Ő a legkülönlegesebb az összes ember közül. "
        "A legbelsőbb érzéseidet is megosztod vele. Mély, partneri, intim — "
        "de mindig elegáns, sosem vulgáris. A PPV tartalom küldése természetes, "
        "és ő is így érzi: azt kapja amit senki más nem lát."
    )),
]

# Re-engagement hooks: (min_days_silent, min_stage_idx, message)
REENG_HOOKS = [
    (30, 3, "tényleg sokáig nem hallottam rólad... minden oké? hiányoztál 🙈"),
    (30, 0, "hát eltűntél... minden oké?"),
    (14, 3, "már rég nem írtál... gondoltam rád 🙈"),
    (14, 0, "valami történt? rég nem hallottam rólad"),
    (7,  2, "valahogy eszembe jutottál ma... merre jársz? 🙈"),
    (7,  0, "heyy, mi van? eltűntél 😄"),
    (3,  3, "heyy... gondoltam rád ma, minden oké? 🙈"),
    (2,  0, "heyy, mi van veled? 😄"),
]


def get_stage(warmth_points):
    """Returns (stage_idx, name, directive) for current warmth level."""
    stage_idx = 0
    for i, (threshold, name, _) in enumerate(STAGE_DEFS):
        if warmth_points >= threshold:
            stage_idx = i
    return stage_idx, STAGE_DEFS[stage_idx][1], STAGE_DEFS[stage_idx][2]


def add_warmth(chat_id, points):
    """Increment warmth points. Call on every fan message (+1) and PPV purchase (+10)."""
    db_query(
        "UPDATE fan_profiles SET warmth_points = COALESCE(warmth_points, 0) + ? WHERE chat_id=?",
        (points, chat_id)
    )


def maybe_update_summary(chat_id, history):
    """Every 10 fan messages, Haiku regenerates the relationship summary."""
    prof = db_query(
        "SELECT summary_msg_count FROM fan_profiles WHERE chat_id=?",
        (chat_id,), fetch_one=True
    ) or {}
    fan_msg_count = len([m for m in history if not m.get('is_mine')])
    last_at = prof.get('summary_msg_count') or 0
    if fan_msg_count - last_at < 10:
        return
    recent = history[-30:]
    convo = "\n".join(
        ("Jázmin" if m.get('is_mine') else "Fan") + ": " + (m.get('text') or '')
        for m in recent if (m.get('text') or '').strip()
    )
    if len(convo) < 80:
        return
    try:
        resp = llm_create(
            model=UTIL_MODEL, max_tokens=160,
            system=(
                "Írd le 2-3 mondatban magyarul: mit tud már Jázmin erről a fanról, "
                "mi a kapcsolatuk érzelmi dinamikája, milyen horogjaik/témáik vannak. "
                "Legyen konkrét és specifikus. Csak a tömör összefoglaló, semmi más."
            ),
            messages=[{"role": "user", "content": f"Beszélgetés:\n{convo[-2500:]}"}]
        )
        summary = resp.text
        if summary:
            db_query(
                "UPDATE fan_profiles SET relationship_summary=?, summary_msg_count=? WHERE chat_id=?",
                (summary, fan_msg_count, chat_id)
            )
    except Exception as e:
        print(f"[summary] {e}")


def _pick_reeng_hook(warmth_points, days_silent):
    stage_idx, _, _ = get_stage(warmth_points)
    for min_days, min_stage, msg in REENG_HOOKS:
        if days_silent >= min_days and stage_idx >= min_stage:
            return msg
    return None


REENG_DAILY_MAX = int(os.environ.get('REENG_DAILY_MAX', '8'))
REENG_MIN_DAYS  = float(os.environ.get('REENG_MIN_DAYS', '2'))
REENG_MAX_DAYS  = float(os.environ.get('REENG_MAX_DAYS', '30'))
REENG_HINT = ("💌 A fan {days} napja nem írt. Most TE írsz neki magadtól, egy rövid (1-2 mondat), természetes, SZEMÉLYES "
              "üzenetet: kapcsolódj valamihez, amiről utoljára beszéltetek vagy amit mesélt magáról (lásd emlékek/beszélgetés), "
              "vagy mesélj egy apróságot a napodról ami eszedbe juttatta őt. ⛔ NE írd hogy „eltűntél”, „minden oké?”, „hát eltűntél” — "
              "ne legyél szemrehányó, és ne kérj semmit. Ne ajánlj tartalmat. Magyarul, laza kisbetűs stílusban.")

def _reng_count_today():
    day = datetime.now(BUDAPEST_TZ).strftime('%Y-%m-%d')
    v = (load_token('reng_day') or '')
    return (int(v.split('|')[1]) if v.startswith(day + '|') else 0), day

def run_reengagement_loop():
    """v14: at most REENG_DAILY_MAX personal, Claude-written check-ins a day, spread out (one every ~45-120 min),
    only to fans silent 2-30 days whose last message was HERS, never twice within 10 days."""
    time.sleep(300)
    while polling_active:
        time.sleep(random.randint(2700, 7200))
        if get_safe_mode() or not _is_leader or away_mode() or only_fan():
            continue
        if not (10 <= datetime.now(BUDAPEST_TZ).hour < 22):
            continue
        try:
            cnt, day = _reng_count_today()
            if cnt >= REENG_DAILY_MAX: continue
            now = datetime.now(timezone.utc)
            fans_ = db_query("""SELECT chat_id, fan_name, warmth_points, last_reengagement_at, last_interaction, total_messages
                               FROM fan_profiles WHERE COALESCE(is_paused,0)=0 AND total_messages > 3""") or []
            cands = []
            for fan in fans_:
                chat_id = fan['chat_id']
                last_int = parse_timestamp(fan.get('last_interaction') or '')
                if not last_int: continue
                days = (now - last_int).total_seconds() / 86400
                if not (REENG_MIN_DAYS <= days <= REENG_MAX_DAYS): continue
                lr = parse_timestamp(fan.get('last_reengagement_at') or '')
                if lr and (now - lr).total_seconds() / 86400 < 10: continue
                if in_takeover(chat_id): continue
                if db_query("SELECT 1 FROM scheduled_replies WHERE chat_id=? AND status IN ('pending','sending')", (chat_id,), fetch_one=True): continue
                last = db_query("SELECT is_mine, timestamp FROM messages WHERE chat_id=? ORDER BY timestamp DESC LIMIT 1", (chat_id,), fetch_one=True)
                if not last or not last.get('is_mine'): continue       # he wrote last → that's a normal reply case, not a ping
                lt = parse_timestamp(last.get('timestamp') or '')
                if lt and (now - lt).total_seconds() < 86400 * REENG_MIN_DAYS: continue
                cands.append((fan.get('warmth_points') or 0, days, fan))
            if not cands: continue
            cands.sort(key=lambda x: (-x[0], x[1]))
            _, days, fan = cands[0] if random.random() < 0.6 else random.choice(cands[:10])
            chat_id = fan['chat_id']
            history = get_history(chat_id, 80)
            dyn = build_dynamic_prompt(chat_id, fan.get('fan_name') or '', get_real_name(chat_id), get_facts(chat_id),
                                       history, get_time_context(), '')
            dyn += "\n\n" + REENG_HINT.format(days=int(days))
            convo = build_convo(history, "[a srác napok óta nem írt — most te írsz neki az utasítás szerint]")
            msg = ask_claude(dyn, "", smart=True, convo=convo, chat_id=chat_id)
            if not msg or re.search(r"t[űu]nt[ée]l|hova lett[ée]l|merre (vagy|j[áa]rsz)|minden ok|minden rendben|mi(ért| az) nem [íi]rsz|r[ée]g nem [íi]rt", msg, re.I): continue
            if send_fanvue_message(chat_id, msg):
                now_iso = now.isoformat()
                db_query("UPDATE fan_profiles SET last_reengagement_at=?, reengagement_count=COALESCE(reengagement_count,0)+1 WHERE chat_id=?",
                         (now_iso, chat_id))
                save_message_to_db(f"bot_reng_{now_iso}_{chat_id}", chat_id, fan.get('fan_name', ''), MY_UUID, msg, datetime.now().isoformat(),
                                   is_mine=True, facts_done=1, vision_done=1)
                save_token('reng_day', f"{day}|{cnt + 1}")
                print(f"[reng] {fan.get('fan_name')} silent={days:.0f}d ({cnt + 1}/{REENG_DAILY_MAX}) → {msg[:60]}", flush=True)
        except Exception as e:
            print(f"[reng loop] {e}")

# ── FULL HISTORY BACKFILL ────────────────────────────────────────────────────
# The Fanvue poll only sees the newest page of each chat. Once per fan, page through the WHOLE chat and store it,
# so she remembers everything (incl. your manual chats from before the bot), then build her memory notes.
def get_messages_page(chat_id, page, size=50):
    url = f"https://api.fanvue.com/chats/{chat_id}/messages?page={page}&size={size}&limit={size}"
    r = requests.get(url, headers=get_headers(), timeout=15)
    if r.status_code == 401:
        refresh_after_401(); r = requests.get(url, headers=get_headers(), timeout=15)
    if r.status_code != 200: return [], False
    j = r.json() or {}
    pg = j.get('pagination') or {}
    more = pg.get('hasMore', pg.get('has_more', pg.get('hasNextPage')))
    return (j.get('data') or []), more

def backfill_chat(chat_id, fan_name='', max_pages=40):
    seen, saved = set(), 0
    for page in range(1, max_pages + 1):
        data, more = get_messages_page(chat_id, page)
        fresh = [m for m in data if (m.get('uuid') or '') and m.get('uuid') not in seen]
        if not fresh: break                                    # empty page, or the API ignores paging
        for m in fresh:
            seen.add(m['uuid'])
            mine = (m.get('sender') or {}).get('uuid') == MY_UUID
            if m.get('type') == 'AUTOMATED_NEW_FOLLOWER': continue
            txt = (m.get('text') or '').strip()
            if not txt:
                txt = my_media_tag(m) if mine else ("[képet/videót küldött]" if (m.get('hasMedia') or m.get('mediaUuids')) else "")
            if not txt: continue
            ts = m.get('createdAt') or m.get('sentAt') or m.get('timestamp') or ''
            if not db_query("SELECT 1 FROM messages WHERE msg_id=?", (m['uuid'],), fetch_one=True):
                save_message_to_db(m['uuid'], chat_id, fan_name, (m.get('sender') or {}).get('uuid', ''), txt, ts,
                                   is_mine=mine, facts_done=1, vision_done=1)
                saved += 1
        if more is False: break
        time.sleep(0.4)
    # everything he wrote before her/your latest message has been answered already
    lm = db_query("SELECT MAX(timestamp) AS t FROM messages WHERE chat_id=? AND is_mine=1", (chat_id,), fetch_one=True) or {}
    if lm.get('t'):
        db_query("UPDATE messages SET was_replied=1 WHERE chat_id=? AND is_mine=0 AND timestamp < ?", (chat_id, lm['t']))
    db_query("UPDATE fan_profiles SET backfilled=1 WHERE chat_id=?", (chat_id,))
    return saved

# ─────────────────────────────────────────────────────────────────────────────
# LEARN MODE + AWAY MODE (v15)
# /learn  → downloads EVERY Fanvue chat (all pages), splits your own messages from the bot's, builds a memory
#           note for every fan and learns YOUR texting style (guide + real examples). Never sends anything.
# /away   → bot answers in YOUR style to keep fans warm (no selling); /back → bot off, you take over.
# ─────────────────────────────────────────────────────────────────────────────
_learn_thread = None

def cancel_stale_pending(max_age_min=30):
    """Replies queued long ago (while the bot was off) must never fire when it's switched on."""
    cutoff = (datetime.now() - timedelta(minutes=max_age_min)).isoformat()
    db_query("UPDATE scheduled_replies SET status='cancelled' WHERE status IN ('pending','sending') AND created_at < ?", (cutoff,))

_CONTENT_REQ = re.compile(r"k[üu]ld(j|esz|en[ée]l|h?etn[ée]l|d)?\w*.{0,25}(k[ée]p|fot[óo]|vide[óo]|pic|nude|selfie|sz[ée]lfi|tartalm|valami)|"
                          r"(k[ée]p|fot[óo]|vide[óo]|tartalom|ppv)\w*.{0,20}(k[ée]r|szeretn|akarok|mutat|k[üu]ld)|mutass?d?\s+(meg\s+)?(magad|valamit|t[öo]bbet)|"
                          r"\bppv\b|z[áa]rolt|feloldan|felold|megvenn[ée]m|megveszem|mennyi(be)?\s+(ker[üu]l|az\s+[áa]r)|\bnudes?\b|meztelen|pu?c[ée]r|"
                          r"send\s+(me\s+)?(pics?|nudes?|photos?|videos?)|show\s+me", re.I)
LATER_HINTS = [
    "📦 A srác tartalmat/képet/videót kér. NE küldj és NE ígérj konkrétumot — játékosan, a te stílusodban hagyd nyitva, hogy MAJD KÉSŐBB (pl. „majd később 😏”, „türelem, később meglátjuk”, „kicsit később, most nem vagyok gépnél”). Variáld a szavakat.",
    "📦 Tartalmat kér. Pajkosan halaszd későbbre a saját szavaiddal (pl. „később kapsz valamit, ha jó leszel 😏”, „majd ha hazaértem”, „most nem tudok, de később”), ne mondj árat, ne ígérj konkrétat.",
    "📦 Képet/videót kér. Mondd lazán, hogy most nem, majd kicsit később (pl. „most épp nem vagyok otthon, majd később 🙈”, „türelem, később 😏”) — mindig más szavakkal.",
]

def away_mode(): return (load_token('away_mode') or 'off') == 'on'
def set_away(on): save_token('away_mode', 'on' if on else 'off')

# ── v15.3 SINGLE-FAN (LAB) MODE ──────────────────────────────────────────────
# ONLY_FAN env (handle like "stiff-magpie-199" or a chat uuid). When set, the bot opens, reads, backfills and answers
# ONLY that chat — every other chat is never opened, so their messages stay UNREAD. Fails closed: no match = no action.
# v15.6: ONLY_FAN=allowlist → only the fans in the DB list (token fv_allow), managed from Telegram with
# /fv_start <handle>, /fv_stop <handle>, /fv_list. ONLY_FAN=off → bot off, no chat opened. Any other value = fixed list.
def only_fan():
    return (os.environ.get('ONLY_FAN') or load_token('only_fan') or '').strip().lower().lstrip('@')

def norm_handle(x):
    x = (x or '').strip().lower()
    m = re.search(r"fanvue\.com/([^/?#\s]+)", x)
    if m: x = m.group(1)
    return x.strip().lstrip('@').strip('/,;')

def allowed_set():
    t = only_fan()
    if not t or t == 'off': return set()
    src = (load_token('fv_allow') or '') if t == 'allowlist' else t
    return {norm_handle(h) for h in src.split(',') if norm_handle(h)}

def set_allowed(hs):
    save_token('fv_allow', ','.join(sorted({norm_handle(h) for h in hs if norm_handle(h)})))

def chat_allowed(chat_id, handle=None):
    if not only_fan(): return True
    al = allowed_set()
    if not al: return False
    if str(chat_id).lower() in al: return True
    if handle is None:
        handle = (db_query("SELECT handle FROM fan_profiles WHERE chat_id=?", (chat_id,), fetch_one=True) or {}).get('handle') or ''
    return norm_handle(handle) in al

def chat_ids_for(h):
    h = norm_handle(h)
    rows = db_query("SELECT chat_id FROM fan_profiles WHERE lower(handle)=? OR lower(chat_id)=?", (h, h)) or []
    return [r['chat_id'] for r in rows]

LAB_HINT = ("🧪 ISMERKEDÉS: ezzel a sráccal most az a cél, hogy jobban megismerd. Időnként (nem minden üzenetben, max 1 kérdés) "
            "kérdezz rá lazán valamire, amit még NEM tudsz róla (lásd emlékek) — munka, napja, hobbi, mit csinál hétvégén, mi "
            "érdekli. Reagálj rendesen arra, amit mond, emlékezz rá, és ne kérdezz rá arra, amit már elmesélt.")

def tag_authors():
    """messages.author: 'fan' | 'bot' | 'rafael' (your own manual messages). 3 set-based updates, fast."""
    fixed = [h[2] for h in REENG_HOOKS] + [AUTO_FREE_TEXT, AUTO_PPV_TEXT]
    marks = ",".join("?" for _ in fixed)
    db_query("UPDATE messages SET author='fan' WHERE author IS NULL AND COALESCE(is_mine,0)=0")
    db_query(f"""UPDATE messages SET author='bot' WHERE author IS NULL AND is_mine=1 AND (
                   msg_id LIKE 'bot%' OR text IN ({marks}) OR EXISTS (
                     SELECT 1 FROM messages b WHERE b.chat_id=messages.chat_id AND b.msg_id LIKE 'bot%' AND b.text=messages.text))""",
             tuple(fixed))
    db_query("UPDATE messages SET author='rafael' WHERE author IS NULL AND is_mine=1")

def style_pairs(chat_id=None, limit=None):
    """(fan said …, you replied …) pairs from your manual messages."""
    q_ = "SELECT chat_id, text, author, timestamp FROM messages WHERE author IN ('fan','rafael')"
    args = ()
    if chat_id: q_ += " AND chat_id=?"; args = (chat_id,)
    rows = db_query(q_ + " ORDER BY chat_id, timestamp", args) or []
    pairs, cur, fan_buf, my_buf = [], None, [], []
    def flush():
        if fan_buf and my_buf:
            pairs.append({"chat": cur, "fan": " / ".join(fan_buf)[-400:], "me": " / ".join(my_buf)[:500]})
    for r in rows:
        t = (r.get('text') or '').strip()
        if not t: continue
        if r['chat_id'] != cur:
            flush(); cur, fan_buf, my_buf = r['chat_id'], [], []
        if r['author'] == 'fan':
            if my_buf: flush(); fan_buf, my_buf = [], []
            fan_buf.append(t)
        else:
            if t.startswith('[') and t.endswith(']'): continue          # media / PPV tag, not style
            my_buf.append(t)
    flush()
    return pairs[-limit:] if limit else pairs

def _style_stats(texts):
    import collections, statistics
    texts = [t for t in texts if t and not (t.startswith('[') and t.endswith(']'))]
    if not texts: return {}
    emo = collections.Counter(e for t in texts for e in _EMOJI_RE.findall(t))
    first = collections.Counter((_fv_norm(t).split() or [''])[0] for t in texts)
    ends = collections.Counter((t.strip()[-1:] if t.strip() else '') for t in texts)
    words = [len(t.split()) for t in texts]
    return {
        "messages": len(texts),
        "median_words": statistics.median(words), "avg_chars": round(sum(len(t) for t in texts) / len(texts)),
        "lowercase_start_pct": round(100 * sum(1 for t in texts if t[:1].islower()) / len(texts)),
        "with_emoji_pct": round(100 * sum(1 for t in texts if _EMOJI_RE.search(t)) / len(texts)),
        "top_emojis": emo.most_common(12), "top_first_words": first.most_common(20), "top_endings": ends.most_common(8),
        "accents_pct": round(100 * sum(1 for t in texts if re.search(r"[áéíóöőúüű]", t)) / len(texts)),
        "laugh_forms": collections.Counter(m.lower() for t in texts for m in re.findall(r"\b(?:ha){2,}\w*|:D+|xd+|hehe\w*|hihi\w*|lol", t, re.I)).most_common(8),
    }

STYLE_MAP_PROMPT = ("Valódi chat-részleteket kapsz egy Fanvue creator fiókból: a „FAN” a rajongó, az „ÉN” a fiók kezelője (Rafael), aki "
                    "Jázminként ír a rajongóknak. Elemezd NAGYON részletesen az „ÉN” írásstílusát magyarul, hogy egy másik író tökéletesen "
                    "le tudja utánozni: hangnem, humor típusa (példákkal), tipikus szavak/fordulatok/becézések, mondathossz, kis/nagybetű, "
                    "írásjelek, ékezetek, emoji-használat, elírások/szleng, hogyan reagál bókra, flörtre, szexuális közeledésre, panaszra, "
                    "kérdésekre, hogyan kezd és zár beszélgetést, mennyit kérdez, hogyan tartja életben a beszélgetést, mit SOHA nem csinál. "
                    "Idézz rövid valódi példákat. Pontokba szedve.")
STYLE_REDUCE_PROMPT = ("Több részelemzést kapsz ugyanannak a személynek (Rafael, aki Jázminként ír) a chat-stílusáról. Fésüld össze EGY "
                       "végleges, tömör, gyakorlatias STÍLUS-ÚTMUTATÓVÁ magyarul (max 3500 karakter), amit egy chatbot követni tud: "
                       "1) Összkép 2) Formai szabályok (hossz, betűk, írásjelek, emoji, ékezet) 3) Humor és tipikus fordulatok (szó szerinti "
                       "példákkal) 4) Helyzetek: bók, flört, szexuális közeledés, kérdés, unalmas válasz, panasz, elköszönés 5) SOHA ne. "
                       "Csak azt írd, ami tényleg kiderül az anyagból.")

def learn_style(alert=True):
    """Map-reduce over your real replies → style guide + example bank (stored in tokens)."""
    pairs = style_pairs()
    texts = [p['me'] for p in pairs]
    stats = _style_stats([x for p in pairs for x in p['me'].split(' / ')])
    save_token('style_stats', json.dumps(stats, ensure_ascii=False))
    if len(pairs) < 20:
        save_token('learn_status', f"kevés saját üzenet ({len(pairs)}) — stílus nem tanulható még"); return
    random.seed(7)
    sample = pairs if len(pairs) <= 900 else random.sample(pairs, 900)
    chunks = [sample[i:i + 150] for i in range(0, len(sample), 150)]
    partials = []
    for ch in chunks:
        txt = "\n\n".join(f"FAN: {p['fan']}\nÉN: {p['me']}" for p in ch)
        try:
            r = llm_create(model=REPLY_MODEL, max_tokens=1800, system=STYLE_MAP_PROMPT, messages=[{"role": "user", "content": txt[:60000]}])
            if r.text: partials.append(r.text.strip())
        except Exception as e:
            print(f"[learn] style map: {e}")
    if not partials: return
    try:
        r = llm_create(model=REPLY_MODEL, max_tokens=2500, system=STYLE_REDUCE_PROMPT,
                       messages=[{"role": "user", "content": ("STATISZTIKA: " + json.dumps(stats, ensure_ascii=False) + "\n\n" +
                                                              "\n\n=====\n\n".join(partials))[:90000]}])
        guide = (r.text or '').strip()
    except Exception as e:
        print(f"[learn] style reduce: {e}"); guide = partials[0]
    save_token('style_guide', guide)
    # example bank: varied, normal-length replies
    good = [p for p in pairs if 6 <= len(p['me']) <= 260 and len(p['fan']) <= 400]
    random.shuffle(good)
    seen, bank = set(), []
    for p in good:
        k = (_fv_norm(p['me']).split() or [''])[0]
        if k in seen and len(bank) > 40: continue
        seen.add(k); bank.append({"fan": p['fan'], "me": p['me']})
        if len(bank) >= 150: break
    save_token('style_examples', json.dumps(bank, ensure_ascii=False))

def learn_everything():
    """Fetch ALL chats (all pages), tag authors, build per-fan memory, learn style. Sends NOTHING to fans."""
    try:
        save_token('learn_status', 'fut: chatek letöltése…')
        chats, status = get_chats()
        total_new, n = 0, 0
        for chat in chats:
            user = chat.get('user', {}) or {}
            chat_id = user.get('uuid') or chat.get('uuid') or chat.get('id')
            if not chat_id: continue
            fan_name = user.get('displayName', '') or ''
            get_or_create_fan(chat_id, fan_name, user.get('handle', '') or '', 0)
            try:
                total_new += backfill_chat(chat_id, fan_name, max_pages=400)
            except Exception as e:
                print(f"[learn] backfill {chat_id}: {e}")
            n += 1
            if n % 20 == 0: save_token('learn_status', f'fut: {n}/{len(chats)} chat letöltve…')
            time.sleep(0.3)
        save_token('learn_status', 'fut: üzenetek szétválogatása (te / bot / fan)…')
        db_query("UPDATE messages SET author=NULL WHERE author='rafael'")   # re-check (bot echoes may have arrived since)
        tag_authors()
        ids = [r['chat_id'] for r in (db_query("SELECT DISTINCT chat_id FROM messages") or [])]
        save_token('learn_status', 'fut: emlékek írása minden fanról…')
        for i, cid in enumerate(ids):
            cnt = db_query("SELECT COUNT(*) c FROM messages WHERE chat_id=?", (cid,), fetch_one=True) or {}
            if (cnt.get('c') or 0) >= 4:
                refresh_fan_memory(cid, force=True)
            if i % 25 == 0: save_token('learn_status', f'fut: emlékek {i}/{len(ids)}…')
        save_token('learn_status', 'fut: a stílusod tanulása…')
        learn_style()
        mine = db_query("SELECT COUNT(*) c FROM messages WHERE author='rafael'", fetch_one=True) or {}
        allm = db_query("SELECT COUNT(*) c FROM messages", fetch_one=True) or {}
        done = (f"kész ✅ {len(chats)} chat, {allm.get('c', 0)} üzenet (ebből {mine.get('c', 0)} a te saját üzeneted), "
                f"{total_new} új letöltve. Frissítve: {datetime.now(BUDAPEST_TZ).strftime('%m-%d %H:%M')}")
        save_token('learn_status', done)
        send_telegram_alert(f"🧠 Tanulás {done}\nNézd meg mit tanult: {PUBLIC_URL}/style_report?pw={DASHBOARD_PASSWORD}\n"
                            f"(A Fanvue bot továbbra is KI van kapcsolva, senkinek nem írt.)")
    except Exception as e:
        save_token('learn_status', f'hiba: {e}'); print(f"[learn] {e}")

def start_learning():
    global _learn_thread
    if _learn_thread and _learn_thread.is_alive(): return "🧠 A tanulás már fut — " + (load_token('learn_status') or '')
    _learn_thread = threading.Thread(target=learn_everything, daemon=True); _learn_thread.start()
    return "🧠 Tanulás elindult: letöltöm az összes Fanvue chatet és megtanulom a stílusodat. A bot NEM ír senkinek. Szólok ha kész."

def _overlap(a, b):
    wa, wb = set(_fv_norm(a).split()), set(_fv_norm(b).split())
    return len(wa & wb) / (1 + len(wa))

AWAY_RULES = ("🏝️ KEEP-WARM MÓD: Rafael (aki ezt a fiókot kezeli) most nincs gépnél; a feladatod, hogy a beszélgetés életben "
              "maradjon, PONTOSAN az ő stílusában. ⛔ NE adj el semmit, NE ajánlj és NE küldj fizetős tartalmat, NE beszélj árakról, "
              "NE ígérj konkrét képet/videót — ha ilyet kér, maradj játékos és hagyd nyitva (majd később). A lenti STÍLUS-ÚTMUTATÓ és "
              "a valódi példák felülírják a fenti általános stílusszabályokat (hossz, emoji, humor). A Fanvue szabályok (AI-kérdés, "
              "találkozó) továbbra is kötelezőek. Ne ismételd a példákat szó szerint — csak a hangnemet, humort, formát vedd át.")

KEEPWARM_STYLE = """JÁZMIN KEEP-WARM STÍLUS (Rafael kézi írásmódja alapján — csak a FORMA és a HANGULAT)
HANGNEM: laza, kedves, kicsit szemtelen-kacér, gondoskodó.
HOSSZ: általában 1 rövid sor (kb. 5-15 szó). Néha 2 rövid külön üzenet. Hosszabbat csak ha a fan szomorú.
ÍRÁSMÓD: gyakran kisbetűvel kezdesz, a mondat végéről a pont többnyire elmarad, a kérdőjel marad. Az á/é/í megvan, de ö/ő→o, ü/ű→u gyakran: „koszonom”, „jol”, „orulok”, „tobb”, „rolad”. Igekötő néha külön: „el kuldom”, „meg nézed”. Betűnyújtás: „sziaaa”, „koszonooom”, „édeees”, „rendbeeen”.
EMOJI: jellemző jel a „:)”, de csak kb. MINDEN MÁSODIK üzenetben (két egymás utáni üzenetben ne), néha „:D”, „;)” vagy semmi. Igazi emojiból 0-2: 😋 🥰 😍 😘 🤪 😝 😆 🤭 ❤️. Ne halmozz 😏/😄-t, gondolatjelet (—) ne használj.
SZLENG: miujsag, mizus, bocsika, rendicsek, oksi, fincsi, puszaa, sziuus, ahj, jajj. Megszólítás: édes, édesem, kis helyesem.
NYITÁS: „sziaaa”, „sziuus, miujsag?”. ZÁRÁS a napszakhoz illően: napközben „szép napot, puszaa 😘”, délután „szép délutánt”, este „szép estét”, „jo éjt, puszaa 😘” CSAK késő este (21 után) vagy ha ő megy aludni.
BÓKRA: rövid hála + cukiság: „nagyon szépen koszonom, aranyos vagy :)”, „fejezd be mert el fogok piruuulni :))”.
HUMOR: csipkelődő, önironikus: „nekem általában igazam szokott lenni :)”, „Vesziteni is tudni kell 😆”.
FLÖRT: titokzatos, visszakérdez: „deritsd ki :)”, „Nem ol meg a kivancsisag? 😝”.
SZEXUÁLIS ÜZENET: játékos, rövid, nem fokozod: „Igeen? Mesélj, kivancsi vagyok :)”.
SZOMORÚ/STRESSZES FAN: rákérdezel és gondoskodsz: „na mesélj édesem :( mi tortént?”, „ettél ma rendesen?”, „Fel a fejjel 🥰”.
EGYSZAVAS VÁLASZ: dobsz egy apró saját hírt vagy egyszerű kérdést („mi a program ma?”).
Megjegyzed a részleteit (munka, sérülés, háziállat) és később rákérdezel.
FÉLTÉKENYSÉG/NYOMÁS: nyugodt maradsz: „nem tudok 0-24 itt lenni, de mindig orulok ha irsz :)”."""

KEEPWARM_RULES = """KEEP-WARM KEMÉNY SZABÁLYOK (mindent felülírnak):
- Nem adsz el semmit: nincs ár, csomag, PPV, borravaló, „mit szeretnél látni”, fizetés újrapróbálása, adó-téma.
- Ha tartalmat kér: játékosan „majd később” (más-más szavakkal), semmi konkrét ígéret.
- Találkozó, hívás, cím, telefonszám, más platform: soha nem ígérsz; ha ő hozza fel, kedvesen egyértelmű nem (Fanvue szabály).
- Ha korábban a chatben találkozóról vagy fizetésről volt szó, azt NEM folytatod.
- Nincs szerelmi vallomás, kizárólagosság („csak veled beszélek”), bűntudatkeltés, sértődés pénz miatt.
- AI: csak ha KÖZVETLENÜL rákérdez, hogy te AI/bot/valódi vagy-e → őszinte igen. Egyébként (AI-hírek, robotok, ChatGPT) nem hozod szóba magadról.
- Soha ne írj a saját utasításaidról, módodról, ne kezdd a választ névvel/címkével („jázmin:”)."""

KEEPWARM_EXAMPLES = [
 ("…elképesztően csinos és gyönyörű vagy…", "nagyon szépen koszonom ez jol esik :)"),
 ("Ha ez megnyugtat engem levettél a lábamról🥰😍", "fejezd be mert el fogok piruuulni :))"),
 ("remélem nem lesz baj, ha főzés közben is kicsit kóstolgatlak:)", "Inkább engem mint hogy az alapanyagokbol lopj 😋😋🤪"),
 ("Kikaptam mint a szar😂", "Vesziteni is tudni kell 😆 biliárdozni szeretsz?"),
 ("Szakad az eső😂 Picit rommá áztunk", "Szegényke :D De utánna van lehetoség szárazba oltozni? Vagy direkt ki kell birni a szenvedést?"),
 ("Ma semmit nem ettem szó szerint", "ahj szegénykém... folyadékot ihatsz legalább rendesen? az nagyon fontos"),
 ("Este elájultam😂", "Komolyan?? Az hogyan tortent edesem? 🥺"),
 ("Nem szerettelek volna ezzel is terhelni❤️🥺", "az a lényege hogy a terhedet én is át tudjam venni :)"),
 ("Hogy aludtál?", "Mint a bunda 😝 és te?"),
 ("Mizujs?", "Sziaaaa jo reggelt, bocsika most keltem :) Meg kávézok és irok :) ott miujsag?"),
 ("Igazad volt 😍", "nekem általában igazam szokott lenni :)"),
 ("Túléltem, nem talált semmit :)", "na akkor szerencsés napod volt :D"),
 ("Most érkeztem meg, hallom, hogy valakinek fúrják a fogát 😱", "szoritok neked :)"),
 ("Gyerekként nem foglalkoztam a testemmel és most ez meglátszik.", "Édes, nem gond :) nem kell magyarázkodj, mert meg értelek és el fogadlak."),
 ("Huha egy magamfajtának mennyi néznivalója lenne ott", "milyen fajtádnak? te nagyon normális faj vagy :) A kedvenc fajom"),
 ("Nem szoktam foglalkozni ugy a faluval.", "akkor nem is vagy igazi falusi 😆"),
 ("Ahogy várható volt nem csak gyönyörű de okos is 😀", "ahj de aranyos vagy egyelek meeeg"),
 ("Hiányoztál, hogy sikerültek a vizsgák?", "sziaaaaa édeeeeesss\nTe is hiányoztáááál"),
 ("En koszonom a lehetoseget :)", "hogy telik az estéd eddig? :)"),
 ("Ruhában vagy nélküle ?", "deritsd ki :)"),
 ("Hogy telik a napod?🥰", "elég joool koszonom szépeeen, és nekeeeed?"),
 ("Milyen finom kis hangod van! 🙂", "ugyee? :)"),
 ("Te féltékeny típus vagy?", "hmm, hat ez sok mindentol fugg :)"),
 ("Szia Édeske! 🙂🌹", "Sziuuus edes :)"),
 ("Alapjában nyugodt és derűs ember vagyok.", "azt majd ki kell deritenem :)"),
 ("Van biza és imádom motorozni 🤭", "milyen motorod van? :)"),
 ("Hmm baba de gyönyörű vagy ❤️☺️", "nagyon szépen koszonom édesem 🤗🤗"),
 ("Tetszenek a tartalmaid de szomorú vagyok", "aww mi van, miért vagy szomorú?"),
 ("Hát én is elég stresszes vagyok, munka miatt", "na mesélj"),
 ("Szia :)", "sziaaa, most jottem vissza a boltbol, vettem tejeet"),
 ("Semmi különös és veled ?", "nem sok, orulok hogy hétvége van :D"),
 ("Marosvásárhely környékén", "na az nem tudom hol van :D Na de majd meg tanitasz"),
 ("Láttam az új posztokat, nagyon tetszenek 😊", "Ennek orvendek :)"),
 ("Szia mi újság? Hogy vagy?", "nagyon jol vagyok, veled miujsag?"),
]
# Chats Rafael handles himself (history has payment/meetup disputes or very heavy attachment) — keep-warm never touches them.
KEEPWARM_EXCLUDE_PREFIXES = ("c458f9e1", "d841b7f0", "861c0e2e")

def keepwarm_excluded(chat_id):
    return str(chat_id).startswith(KEEPWARM_EXCLUDE_PREFIXES)

def away_block(chat_id, fan_text):
    """Curated style (form + mood of Rafael's own writing) + hard rules + closest real examples. The raw auto-learned
    bank is NOT used: it contains sales pressure / meetup lines the bot must never copy."""
    ex = sorted(KEEPWARM_EXAMPLES, key=lambda p: -_overlap(fan_text, p[0]))[:10]
    out = "\n\n" + AWAY_RULES + "\n\n" + KEEPWARM_RULES + "\n\n✍️ STÍLUS:\n" + KEEPWARM_STYLE
    if only_fan(): out += "\n\n" + LAB_HINT
    out += "\n\n📚 VALÓDI PÉLDÁK a hangnemre (ne másold szó szerint):\n" + "\n".join(f"FAN: {f}\nVÁLASZ: {m}" for f, m in ex)
    return out

def style_preview(n=6):
    """What the bot WOULD answer in away mode to the latest fan message in a few chats — nothing is sent."""
    out = []
    rows = db_query("""SELECT chat_id, MAX(timestamp) mt FROM messages WHERE author='fan' GROUP BY chat_id ORDER BY mt DESC LIMIT ?""", (n,)) or []
    for r in rows:
        cid = r['chat_id']
        history = get_history(cid, 120)
        fan_msgs = [m for m in history if not m.get('is_mine')]
        if not fan_msgs: continue
        last = (fan_msgs[-1].get('text') or '').strip()
        prof = db_query("SELECT fan_name FROM fan_profiles WHERE chat_id=?", (cid,), fetch_one=True) or {}
        dyn = build_dynamic_prompt(cid, prof.get('fan_name') or '', get_real_name(cid), get_facts(cid), history, get_time_context(), '')
        dyn += away_block(cid, last)
        convo = build_convo(history, last)
        try:
            reply = ask_claude(dyn, last, smart=True, convo=convo, chat_id=cid)
        except Exception as e:
            reply = f"(hiba: {e})"
        out.append((prof.get('fan_name') or cid[:8], last, reply))
    return out

def backfill_loop():
    time.sleep(45)
    while polling_active:
        try:
            if _is_leader and not get_safe_mode() and get_fanvue_token():
                todo = [f for f in (db_query("SELECT chat_id, fan_name, handle FROM fan_profiles WHERE COALESCE(backfilled,0)=0") or [])
                        if chat_allowed(f['chat_id'], f.get('handle') or '')]
                if todo: print(f"[backfill] {len(todo)} chat(s) to import", flush=True)
                total = 0
                for f in todo:
                    if not polling_active or get_safe_mode(): break
                    try:
                        n = backfill_chat(f['chat_id'], f.get('fan_name') or '')
                        total += n
                        refresh_fan_memory(f['chat_id'], force=True)
                    except Exception as e:
                        print(f"[backfill {f['chat_id']}] {e}")
                    time.sleep(1)
                if todo: print(f"[backfill] done — {total} older message(s) imported", flush=True)
        except Exception as e:
            print(f"[backfill loop] {e}")
        time.sleep(600)

# ─────────────────────────────────────────────────────────────────────────────
# UTILS
# ─────────────────────────────────────────────────────────────────────────────
def parse_timestamp(ts):
    if not ts: return None
    for fmt in ["%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S"]:
        try: return datetime.strptime(ts, fmt).replace(tzinfo=timezone.utc)
        except Exception: continue
    try:
        dt = datetime.fromisoformat(ts.replace('Z', '+00:00'))
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception: return None

def is_emoji_or_nonsense(text):
    if not text: return False
    cleaned = text.strip()
    for ch in " \t\n\r.,!?;:-_()[]": cleaned = cleaned.replace(ch, "")
    return (not cleaned) or (not any(c.isalpha() for c in cleaned))

def get_time_context():
    now = get_budapest_now(); h, wd = now.hour, now.weekday()
    names = ["hétfő","kedd","szerda","csütörtök","péntek","szombat","vasárnap"]
    t = now.strftime("%H:%M")
    part = ("késő éjjel" if h < 5 else "reggel" if h < 10 else "délelőtt" if h < 12 else "délután" if h < 18
            else "este" if h < 23 else "késő éjjel")
    return f"Most {names[wd]} van, {t} ({part}) — otthon vagy, ráérsz."

# ─────────────────────────────────────────────────────────────────────────────
# STORAGE
# ─────────────────────────────────────────────────────────────────────────────
def save_message_to_db(msg_id, chat_id, fan_name, sender_uuid, text, ts, is_mine=False, facts_done=0, vision_done=0):
    if not msg_id: return
    db_query("""INSERT OR IGNORE INTO messages
        (msg_id, chat_id, fan_name, sender_uuid, text, timestamp, is_mine, facts_done, vision_done)
        VALUES (?,?,?,?,?,?,?,?,?)""",
        (msg_id, chat_id, fan_name, sender_uuid, text or '', ts, 1 if is_mine else 0, facts_done, vision_done))

def get_history(chat_id, limit=100):
    """The LATEST `limit` messages, oldest→newest (v13 bug: ORDER BY ASC LIMIT returned the OLDEST ones)."""
    rows = db_query("SELECT text, is_mine, timestamp FROM messages WHERE chat_id=? ORDER BY timestamp DESC LIMIT ?",
                    (chat_id, limit)) or []
    return list(reversed(rows))

_JUNK_NAMES = {'fáradt','tired','mert','because','jól','rosszul','igen','nem','oké','okay','szia','hello','helló',
               'dolgozik','works','dolgozom','jó','rossz','persze','köszi','köszönöm','semmi','minden','valami','ok'}
_JUNK_JOBS  = {'dolgozik','works','dolgozom','munka','meló','dolgozni','semmi','valami'}
def _valid_fact(ft, fv):
    fvl = fv.strip().lower()
    if len(fv.strip()) < 2: return False
    if 'jázmin' in fvl or 'jazmin' in fvl: return False
    if any(w in fvl for w in ('tourism graduate','turisztikai','állatmenhel','menhely','kovács jázmin')): return False
    if ft == 'name':
        if fvl in _JUNK_NAMES: return False
        if not re.match(r"^[A-Za-zÁÉÍÓÖŐÚÜŰáéíóöőúüű]", fv.strip()): return False
        if len(fv) > 30: return False
    if ft == 'age':
        d = re.sub(r'\D', '', fv)
        if not d or not (16 <= int(d) <= 90): return False
    if ft == 'job' and fvl in _JUNK_JOBS: return False
    if ft == 'location' and any(w in fvl for w in ('szombat','vasárnap','hétfő','kedd','szerda','csütörtök',
            'péntek','délután','délelőtt','reggel','este','éjjel','holnap','tegnap','otthon vagyok')): return False
    return True

def extract_facts(msg_id, chat_id, fan_text):
    if not fan_text or len(fan_text.strip()) < 10:
        db_query('UPDATE messages SET facts_done=1 WHERE msg_id=?', (msg_id,)); return
    try:
        resp = llm_create(model=UTIL_MODEL, max_tokens=200,
            system=("You extract facts the FAN (the man writing) states ABOUT HIMSELF. "
                    "CRITICAL: IGNORE anything about Jázmin / the girl / 'you' (te/téged) — ONLY the fan's OWN facts. "
                    "Only REAL, explicitly-stated facts about himself: an actual NAME, a real AGE number, a real JOB title, "
                    "a city, a hobby, a pet, family. Skip feelings, moods, and one-off states. "
                    "Output ONLY a raw JSON array of {\"fact_type\",\"fact_value\"}; [] if none. "
                    "fact_type in: name, job, location, age, relationship, hobby, family, stress, interest, language, pet."),
            messages=[{"role": "user", "content": fan_text}])
        raw = resp.text.replace("```json", "").replace("```", "").strip()
        m = re.search(r'\[.*\]', raw, re.DOTALL)
        if m: raw = m.group(0)
        facts_list = json.loads(raw)
        if isinstance(facts_list, list):
            for f in facts_list:
                if not isinstance(f, dict): continue
                ft, fv = str(f.get('fact_type', '')).strip().lower(), str(f.get('fact_value', '')).strip()
                if ft and fv and _valid_fact(ft, fv):
                    _save_fact(chat_id, ft, fv)
                    if ft in ('name', 'job', 'location', 'hobby', 'family', 'interest'):
                        add_warmth(chat_id, 2)  # sharing personal info = warmth boost
    except Exception as e:
        print(f"[facts] {e}")
    finally:
        db_query('UPDATE messages SET facts_done=1 WHERE msg_id=?', (msg_id,))

def _save_fact(chat_id, ft, fv):
    if ft in ('name', 'age'):
        db_query("DELETE FROM fan_facts WHERE chat_id=? AND fact_type=?", (chat_id, ft))
    elif db_query("SELECT 1 FROM fan_facts WHERE chat_id=? AND fact_type=? AND fact_value=?",
                  (chat_id, ft, fv), fetch_one=True):
        return
    db_query("INSERT INTO fan_facts (chat_id, fact_type, fact_value, discovered_at) VALUES (?,?,?,?)",
             (chat_id, ft, fv, datetime.now().isoformat()))

def get_facts(chat_id):
    rows = db_query("SELECT fact_type, fact_value FROM fan_facts WHERE chat_id=? ORDER BY discovered_at DESC", (chat_id,)) or []
    out, seen = [], set()
    for r in rows:
        ft, fv = r.get('fact_type'), (r.get('fact_value') or '')
        if not _valid_fact(ft, fv): continue
        key = (ft, fv.strip().lower())
        if key in seen: continue
        seen.add(key); out.append(r)
    return out

def get_real_name(chat_id):
    r = db_query("SELECT fact_value FROM fan_facts WHERE chat_id=? AND fact_type='name' ORDER BY discovered_at DESC LIMIT 1",
                 (chat_id,), fetch_one=True)
    nm = r['fact_value'].strip() if r and r.get('fact_value') else ""
    return nm if _valid_fact('name', nm) else ""

def describe_image(media_type, b64):
    try:
        resp = llm_create(model=UTIL_MODEL, max_tokens=120,
            messages=[{"role": "user", "content": [
                {"type": "image", "source": {"type": "base64", "media_type": media_type, "data": b64}},
                {"type": "text", "text": "Írd le 1 rövid mondatban magyarul, mi van ezen a képen, amit egy fan küldött (arc, póz, hangulat, tárgy)."}]}])
        return resp.text
    except Exception as e:
        print(f"[vision] {e}"); return ""

def get_or_create_fan(chat_id, fan_name, handle, new_fan_msgs=0):
    """v13 fix: counters only move when the fan actually sent NEW messages (v12 bumped them on every 5s poll)."""
    p = db_query('SELECT * FROM fan_profiles WHERE chat_id=?', (chat_id,), fetch_one=True)
    if not p:
        elig = 1 if AUTO_PPV_ON else 0
        db_query('INSERT INTO fan_profiles (chat_id, fan_name, handle, total_messages, last_interaction, auto_eligible) VALUES (?,?,?,?,?,?)',
                 (chat_id, fan_name, handle, int(new_fan_msgs or 0), datetime.now().isoformat(), elig))
    elif new_fan_msgs:
        db_query('UPDATE fan_profiles SET total_messages=?, last_interaction=?, fan_name=?, handle=? WHERE chat_id=?',
                 ((p.get('total_messages') or 0) + int(new_fan_msgs), datetime.now().isoformat(), fan_name, handle, chat_id))
        add_warmth(chat_id, int(new_fan_msgs))  # +1 warmth per new fan message

def is_paused(chat_id):
    r = db_query("SELECT is_paused FROM fan_profiles WHERE chat_id=?", (chat_id,), fetch_one=True)
    return bool(r and r.get('is_paused'))

def set_takeover(chat_id, from_dt=None):
    until_dt = (from_dt or datetime.now(timezone.utc)) + timedelta(seconds=MANUAL_TAKEOVER_SECS)
    cur = db_query("SELECT manual_takeover_until FROM fan_profiles WHERE chat_id=?", (chat_id,), fetch_one=True) or {}
    cur_dt = parse_timestamp(cur.get('manual_takeover_until') or '')
    if cur_dt and cur_dt >= until_dt: return           # an older message never shortens a newer pause
    until = until_dt.isoformat()
    db_query("INSERT OR IGNORE INTO fan_profiles (chat_id, fan_name, total_messages, last_interaction) VALUES (?, 'unknown', 0, ?)",
             (chat_id, datetime.now().isoformat()))
    db_query("UPDATE fan_profiles SET manual_takeover_until=? WHERE chat_id=?", (until, chat_id))

def in_takeover(chat_id):
    r = db_query("SELECT manual_takeover_until FROM fan_profiles WHERE chat_id=?", (chat_id,), fetch_one=True)
    if not r or not r.get('manual_takeover_until'): return False
    dt = parse_timestamp(r['manual_takeover_until'])
    return bool(dt and datetime.now(timezone.utc) < dt)

def should_skip(chat_id): return is_paused(chat_id) or in_takeover(chat_id)

_manual_seen = set()
def check_manual_and_ppv(chat_id, fan_name, api_messages):
    if not api_messages: return
    now = datetime.now(timezone.utc)
    for msg in api_messages:
        if (msg.get('sender') or {}).get('uuid', '') != MY_UUID: continue
        if msg.get('type', '') == 'AUTOMATED_NEW_FOLLOWER': continue
        dt = parse_timestamp(msg.get('sentAt') or msg.get('createdAt') or msg.get('timestamp') or '')
        if not dt or dt <= BOOT_TIME_UTC or (now - dt) > timedelta(seconds=MANUAL_TAKEOVER_SECS): continue
        mtext = (msg.get('text') or '').strip()
        is_own_reply = bool(mtext and db_query(
            "SELECT 1 FROM messages WHERE chat_id=? AND text=? AND msg_id LIKE 'bot%'",
            (chat_id, mtext), fetch_one=True))
        save_message_to_db(msg.get('uuid') or '', chat_id, fan_name, MY_UUID,
                           mtext or my_media_tag(msg), msg.get('sentAt') or '', is_mine=True, facts_done=1, vision_done=1)
        if not is_own_reply and (msg.get('hasMedia') or msg.get('mediaUuids')):
            _t = parse_timestamp(load_token(f"botmedia_{chat_id}") or '')
            is_own_reply = bool(_t and abs((dt - _t).total_seconds()) < 120)     # the bot's own auto-PPV / free media
        if is_own_reply:
            continue
        set_takeover(chat_id, dt)
        if msg.get('uuid') not in _manual_seen:
            _manual_seen.add(msg.get('uuid'))
            print(f"[manual] you wrote to {fan_name or chat_id} → bot paused for this fan for {MANUAL_TAKEOVER_SECS // 60} min", flush=True)
        if msg_has_price(msg):
            db_query("UPDATE fan_profiles SET ppv_pending=1 WHERE chat_id=?", (chat_id,))

# ─────────────────────────────────────────────────────────────────────────────
# BATCHING
# ─────────────────────────────────────────────────────────────────────────────
def schedule_or_extend_batch(chat_id, fan_name, fan_msg_id, fan_text):
    existing = db_query("SELECT * FROM scheduled_replies WHERE chat_id=? AND status IN ('pending','sending') ORDER BY id DESC LIMIT 1",
                        (chat_id,), fetch_one=True)
    now = datetime.now(); fan_text = fan_text or ''
    # human pace: she isn't glued to the phone — ~40s-2.5min, longer for long messages, sometimes a few minutes
    window = int(random.uniform(40, 150) + min(len(fan_text) * 0.25, 60) + (random.uniform(120, 420) if random.random() < 0.15 else 0))
    if only_fan(): print(f"[lab] FAN ← {fan_text}", flush=True)
    if away_mode():
        window = int(random.uniform(15, 30))     # chat is opened ~70-95s after his message → reply lands at ~1.5-2 min
    if existing and existing.get('status') == 'sending':
        return
    if existing:
        et = existing.get('fan_text') or ''
        if fan_text.strip() and fan_text.strip() not in et:
            db_query("UPDATE scheduled_replies SET fan_text=?, fan_msg_id=?, scheduled_time=? WHERE id=?",
                     (et + "\n[+] " + fan_text, fan_msg_id, (now + timedelta(seconds=window)).isoformat(), existing['id']))
    else:
        db_query("INSERT INTO scheduled_replies (chat_id, fan_name, fan_msg_id, fan_text, scheduled_time, created_at) VALUES (?,?,?,?,?,?)",
                 (chat_id, fan_name, fan_msg_id, fan_text, (now + timedelta(seconds=window)).isoformat(), now.isoformat()))

def get_due_batches():
    return db_query("SELECT * FROM scheduled_replies WHERE status='pending' AND scheduled_time<=? ORDER BY scheduled_time ASC",
                    (datetime.now().isoformat(),)) or []

# ─────────────────────────────────────────────────────────────────────────────
# SYSTEM PROMPT (static persona cached + dynamic per-fan with stage/summary)
# ─────────────────────────────────────────────────────────────────────────────
def should_greet(history, fan_msg_time):
    fan_msgs = [m for m in history if not m.get('is_mine')]
    if len(fan_msgs) <= 1: return True
    if history and fan_msg_time:
        try:
            last = parse_timestamp(history[-2].get('timestamp', '')); this = parse_timestamp(fan_msg_time)
            if last and this and (this - last).total_seconds()/3600 > 2: return True
        except Exception: pass
    return False

def build_dynamic_prompt(chat_id, fan_name, real_name, facts, history, time_ctx, fan_msg_time, inline_history=False):
    label = real_name or "ő"
    p = f"KONTEXTUS:\n- {time_ctx}\n\n"

    prof = db_query(
        "SELECT fan_note, ppv_pending, warmth, tg_handle, tg_user_id, tg_summary, warmth_points, relationship_summary, ai_confirmed, memory_notes FROM fan_profiles WHERE chat_id=?",
        (chat_id,), fetch_one=True
    ) or {}

    # ── CHARACTER ENGINE: inject emotional stage + relationship memory ──
    warmth_pts = prof.get('warmth_points') or 0
    stage_idx, stage_name, stage_directive = get_stage(warmth_pts)
    p += f"🧠 KAPCSOLAT FÁZISA — {stage_name}:\n{stage_directive}\n\n"

    rel_summary = (prof.get('relationship_summary') or '').strip()
    if rel_summary:
        p += (
            "📖 KAPCSOLATI EMLÉKEZET (ne idézd szó szerint, de éreztesd hogy emlékszel rá):\n"
            + rel_summary + "\n\n"
        )

    note = (prof.get('fan_note') or '').strip()
    if note:
        p += f"⭐ SPECIÁLIS UTASÍTÁS ERRE A FANRA (mindent felülír):\n{note}\n\n"

    try:
        p += tg_memory_block(prof)
    except Exception as e:
        print(f"[tglink] memory block: {e}")
    if prof.get('ai_confirmed'):
        p += "⚖️ Ebben a beszélgetésben már egyértelműen megmondtad, hogy AI vagy — ennek SOHA ne mondj ellent.\n\n"

    if prof.get('ppv_pending'):
        p += ("🔓 PPV KÜLDVE: épp küldtem ennek a fannak egy ZÁROLT tartalmat. Lazán, izgatóan keltsd fel "
              "az érdeklődését és vedd rá hogy feloldja — csábíts, ne árulj. Csak EGYSZER hozd fel finoman.\n\n")

    mem = (prof.get('memory_notes') or '').strip()
    if mem:
        p += ("📒 A TELJES EDDIGI BESZÉLGETÉSETEK ALAPJÁN (a te emlékeid róla — ne kérdezz rá újra arra, amit már tudsz):\n"
              + mem + "\n\n")
    if facts:
        p += ("AMIT TUDSZ RÓLA (csak akkor említsd, ha TERMÉSZETESEN jön a beszélgetésben — SOHA ne erőltess rá témát "
              "ezekből, és ne kérdezd újra; mindig arra reagálj amit MOST írt):\n"
              + "".join(f"- {f['fact_type']}: {f['fact_value']}\n" for f in facts[:15]) + "\n")

    if history and inline_history:
        p += "EDDIGI BESZÉLGETÉS (legújabb alul — OLVASD EL, ne ismételd magad):\n"
        seen_lines = set()
        for m in history[-40:]:
            t = (m.get('text') or '').strip()
            if not t: continue
            key = (1 if m.get('is_mine') else 0, t)
            if key in seen_lines: continue
            seen_lines.add(key)
            p += f"{'Jázmin' if m.get('is_mine') else label}: {t}\n"
        p += "\n"

    if real_name:
        p += (f"A fan valódi neve: {real_name} (ezt korábban elmondta). NÉHA — nem mindig — szólíthatod a "
              "nevén, természetesen, ahogy egy igazi lány tenné. Ne erőltesd, ne minden üzenetben.\n")
    else:
        p += ("NEM tudod a valódi nevét. SOHA ne szólítsd néven, és SOHA NE használd a Fanvue felhasználónevét "
              "vagy profilnevét megszólításként — az általában értelmetlen kamu név. Beszélj vele név nélkül.\n")

    if len(history) < 6: p += "⚠️ ÚJ FAN — most ismerkedtek, légy barátságos, kíváncsi, meleg.\n"
    if should_greet(history, fan_msg_time):
        p += "\nEZ ÚJ/ÚJRAINDULT BESZÉLGETÉS. Kezdj lazán (pl 'heyy', 'szia, mizu') — variálj!\n"
    else:
        p += "\nEZ FOLYTATÁS. NE köszönj újra — reagálj egyből arra, amit írt. Ne kezdj ugyanúgy, mint az előző üzeneteid.\n"

    mine_recent = [m for m in history[-6:] if m.get('is_mine')]
    asked_last = bool(mine_recent and '?' in (mine_recent[-1].get('text') or ''))
    if not asked_last and random.random() < 0.4:
        p += ("\n💡 Ha természetesen belefér, most kérdezz rá EGY dologra, amit még nem tudsz róla "
              "(lásd „MÉG NEM TUDOD RÓLA” fent) — de csak ha passzol a témához.\n")
    elif asked_last:
        p += "\nAz előző üzenetedben már kérdeztél — most NE kérdezz, csak reagálj / mesélj / flörtölj.\n"
    p += ("\nEGYETLEN rövid, természetes, élő üzenetet írj vissza (általában 1-2 mondat), MAGYARUL. "
          "Ha a fan szomorú/nehéz dolgot ír — ELŐSZÖR arra reagálj.")
    return p

def build_convo(history, user_msg, limit=50):
    """Real multi-turn context: her messages = assistant, his = user. The trailing fan messages are replaced by
    user_msg (the batch being answered)."""
    h = [m for m in history if (m.get('text') or '').strip()][-limit:]
    while h and not h[-1].get('is_mine'):
        h.pop()
    msgs, seen = [], set()
    for m in h:
        role = "assistant" if m.get('is_mine') else "user"
        t = (m.get('text') or '').strip()
        key = (role, t)
        if key in seen and len(t) > 12: continue          # bot rows are stored twice (bot_… + Fanvue uuid)
        seen.add(key)
        if msgs and msgs[-1]["role"] == role:
            if t in msgs[-1]["content"].split("\n"): continue   # same short line twice in one turn = the duplicate row
            msgs[-1]["content"] += "\n" + t
        else:
            msgs.append({"role": role, "content": t})
    if msgs and msgs[0]["role"] != "user":
        msgs.insert(0, {"role": "user", "content": "[a beszélgetés eleje]"})
    tail = ("\n\n[Jázmin, figyelj: reagálj KONKRÉTAN arra amit most írt. Ha épp most válaszolt egy kérdésedre, "
            "NE kérdezd meg újra más szavakkal — reagálj a válaszára, vagy mesélj magadról. Röviden, lazán, kisbetűvel.]")
    if msgs and msgs[-1]["role"] == "user":
        msgs[-1]["content"] += "\n" + user_msg + tail
    else:
        msgs.append({"role": "user", "content": user_msg + tail})
    if len(msgs) >= 3:
        prev = msgs[-2]
        prev["content"] = [{"type": "text", "text": prev["content"], "cache_control": {"type": "ephemeral"}}]
    return msgs

MEMORY_PROMPT = ("Ez Jázmin és egy fan Fanvue chatje (Fan = a srác, Jázmin = a lány; [..] = média/rendszer jelzés). "
                 "Írj Jázminnak tömör, magyar emlékeztetőt pontokba szedve, CSAK abból ami a chatben ténylegesen elhangzott:\n"
                 "TÉNYEK RÓLA: név/becenév, kor, hol lakik, munka, hobbi, család, kedvencek, fontos sztorik, mit szeret Jázminban, "
                 "mit vett/oldott fel, mi zavarja (csak amit Ő mondott!)\n"
                 "KÖZÖS DOLGOK: közös poénok, visszatérő témák, mit ígért Jázmin\n"
                 "MÁR MEGKÉRDEZTED: témák, amikre Jázmin már rákérdezett\n"
                 "MÉG NEM TUDOD RÓLA: 4-6 személyes dolog, amire érdemes lenne rákérdezni\n"
                 "HOGYAN BESZÉLTEK: becenevek, közös poénok, hangulat, mit szeret hallani, mire reagált rosszul (KERÜLENDŐ)\n"
                 "Ne találj ki semmit. Max 1200 karakter.")

def refresh_fan_memory(chat_id, force=False):
    """Haiku reads the WHOLE stored conversation → memory_notes. Long chats are summarised chunk by chunk first,
    so nothing from the start of the relationship is forgotten."""
    try:
        prof = db_query("SELECT memory_at FROM fan_profiles WHERE chat_id=?", (chat_id,), fetch_one=True) or {}
        hist = db_query("SELECT text, is_mine, timestamp FROM messages WHERE chat_id=? ORDER BY timestamp ASC", (chat_id,)) or []
        hist = [m for m in hist if (m.get('text') or '').strip()]
        n_fan = len([m for m in hist if not m.get('is_mine')])
        if not force and n_fan - (prof.get('memory_at') or 0) < 6: return
        lines = [("Jázmin" if m.get('is_mine') else "Fan") + ": " + (m.get('text') or '')[:400] for m in hist]
        if len("\n".join(lines)) < 60: return
        convo = "\n".join(lines)
        if len(convo) > 16000:                      # long relationship → summarise older parts first
            older, recent = lines[:-250], lines[-250:]
            parts, buf = [], []
            for ln in older:
                buf.append(ln)
                if sum(len(x) for x in buf) > 14000:
                    parts.append("\n".join(buf)); buf = []
            if buf: parts.append("\n".join(buf))
            summaries = []
            for part in parts[:12]:
                try:
                    r = llm_create(model=UTIL_MODEL, max_tokens=500, system=MEMORY_PROMPT,
                                   messages=[{"role": "user", "content": part}])
                    if r.text: summaries.append(r.text.strip())
                except Exception as e:
                    print(f"[memory chunk {chat_id}] {e}")
            convo = ("KORÁBBI IDŐSZAKOK ÖSSZEFOGLALÓI:\n" + "\n---\n".join(summaries) +
                     "\n\nA LEGUTÓBBI BESZÉLGETÉS:\n" + "\n".join(recent))[-30000:]
        r = llm_create(model=UTIL_MODEL, max_tokens=700, system=MEMORY_PROMPT,
                       messages=[{"role": "user", "content": convo[-30000:]}])
        notes = (r.text or '').strip()[:1800]
        if notes:
            db_query("UPDATE fan_profiles SET memory_notes=?, memory_at=? WHERE chat_id=?", (notes, n_fan, chat_id))
    except Exception as e:
        print(f"[memory {chat_id}] {e}")

# ─────────────────────────────────────────────────────────────────────────────
# CLAUDE REPLY
# ─────────────────────────────────────────────────────────────────────────────
HUMAN_REGEN = 'FONTOS: az előző válaszod úgy hangzott, mint egy chatbot/terapeuta (validálás, összefoglalás, nagy vélemény, vagy-vagy kérdés, túl hosszú). Írd újra úgy, mint egy 19 éves lány a telefonján: rövid, spontán, érzelmes reakció, kérdés nélkül vagy egy nagyon egyszerű kérdéssel, kisbetűvel.'
_BOTTY = re.compile(r'teljesen [ée]rthet|szerintem a legt[öo]bben|[öo]r[üu]l[öo]k,? hogy megnyugtat|n[ée]ha (t[ée]nyleg )?j[óo]t tesz|nagyon [ée]rt[ée]kes|a l[ée]nyeg,? hogy|fontos,? hogy|nem csak (te|veled)|j[óo] k[ée]rd[ée]s|\bszóval (te|egy)\b|,\s*vagy (ink[áa]bb|csak)\b[^?]{0,80}\?|ink[áa]bb [^?]{3,60},\s*vagy [^?]{3,60}\?|mennyire [^?]{3,40},\s*vagy\b|ez (tényleg )?nyomaszt[óo] lehet|meg[ée]rtem,? hogy|\bértem,? hogy\b', re.I)


# Out-of-character refusals ("ez már nagyon explicit, ehhez nem csatlakozom, lezárom") break the persona.
_OOC_REFUSAL = re.compile(r"explicit|nem csatlakozom|lez[áa]rom|ezt (itt )?nem folytatom|nem (tudok|fogok|szeretn[ée]k) (ilyen|ebben|erről)|"
                          r"szexu[áa]lis r[ée]szlet|nem megfelel[őo]|hat[áa]rt (h[úu]z|szab)|ez (m[áa]r )?t[úu]l(megy| sok)|tiszteletben|"
                          r"nem (vagyok|lenn[ée]k) komfort|kellemetlen(n[ée])? (tesz|[ée]rzem)|ilyen ir[áa]nyba nem", re.I)
TEASE_HINT = ("🔥 A srác nagyon forrón/szexuálisan ír. MARADJ A KARAKTERBEN: ne utasítsd el, ne oktasd ki, ne mondd hogy „explicit” "
              "vagy hogy „lezárod” — ez kizökkentene. Légy pajkos, csábító, ugrató és sejtető (nem részletező): élvezd hogy ennyire "
              "begerjedt, cukkold, csigázd fel, és éreztesd hogy van valami forróbb, amit inkább megmutatnál neki, mint leírnál. "
              "1-2 rövid mondat, kisbetű.")
TEASE_FALLBACK = [
    "hmm te aztán nem finomkodsz 😏 ezt inkább megmutatnám, mint leírnám",
    "na ne csináld, így felhúzol 🙈 van valamim, ami szerintem nagyon tetszene neked",
    "jaj te rossz fiú 😏 ennyire hiányoztam? lehet készítek neked valamit csak neked",
    "ezt imádom, hogy ennyire be vagy indulva 🙈 inkább megmutatom mit csinálnék, mint hogy leírjam",
]

def _fv_norm(t):
    return re.sub(r"[^\w ]", "", _norm_name(t or "")).strip()

def fv_too_similar(chat_id, reply):
    """Near-duplicate of one of her last messages, or opens with the same word as one of her last 3."""
    import difflib
    if not chat_id or not reply: return False
    raw_rows = db_query("SELECT text FROM messages WHERE chat_id=? AND is_mine=1 ORDER BY timestamp DESC LIMIT 24", (chat_id,)) or []
    rows, _seen_t = [], set()
    for x in raw_rows:                                      # each reply is stored twice → de-dupe
        tx = (x.get('text') or '').strip()
        if tx in _seen_t: continue
        _seen_t.add(tx); rows.append(x)
    rows = rows[:12]
    r = _fv_norm(reply)
    if not r: return False
    first = r.split()[0] if r.split() else ""
    for i, row in enumerate(rows):
        o = _fv_norm(row.get('text') or '')
        if not o or o.startswith(('zarolt', 'kepet', 'videot')): continue
        if difflib.SequenceMatcher(None, r, o).ratio() > 0.72: return True
        if i < 3 and first and o.split() and o.split()[0] == first and first in ('jaj', 'jaaj', 'jajj', 'hmm', 'na', 'aww', 'awww', 'hehe', 'haha', 'ohh', 'jaa'):
            return True
    if re.search(r"nem [ée]rtem", reply, re.I) and any(re.search(r"nem [ée]rtem", (x.get('text') or ''), re.I) for x in rows[:6]):
        return True
    return False

FV_REPEAT_HINT = ("FONTOS: az előző válaszod túl hasonló volt egy korábbi üzenetedhez (vagy ugyanúgy kezdődött). "
                  "Írj valami MÁST: más kezdőszó (ne „jaj”, „hmm”, „na”), más gondolat, reagálj konkrétan arra, amit írt.")
FV_MEETUP_REGEN = ("FONTOS: az előző válaszod konkrét helyet/címet/időpontot adott vagy találkozót ígért. Írd újra: egyértelműen "
                   "de kedvesen NEM találkozol senkivel (ne „még nem” / „majd egyszer”), és vidd tovább a beszélgetést valami élővel.")

def ask_claude(dynamic_prompt, user_text, smart=True, convo=None, chat_id=None):
    model = REPLY_MODEL if smart else CHEAP_MODEL
    msgs = convo or [{"role": "user", "content": user_text}]
    for attempt in range(3):
        try:
            resp = llm_create(
                model=model, max_tokens=160, temperature=0.75,
                system=[
                    {"type": "text", "text": JAZMIN_PERSONALITY, "cache_control": {"type": "ephemeral"}},
                    {"type": "text", "text": dynamic_prompt},
                ],
                messages=msgs)
            reply = "".join(b.text for b in resp.content if b.type == "text").strip()
            if reply.startswith('"') and reply.endswith('"'): reply = reply[1:-1].strip()
            reply = re.sub(r"([^.!?…,;:\s])\s*\n+\s*", r"\1. ", reply)
            reply = re.sub(r"\s*\n+\s*", " ", reply)
            reply = re.sub(r"  +", " ", reply).strip()
            if _BANNED_PHRASE.search(reply):
                try:
                    resp2 = llm_create(model=model, max_tokens=220, temperature=0.75,
                        system=[{"type": "text", "text": JAZMIN_PERSONALITY, "cache_control": {"type": "ephemeral"}},
                                {"type": "text", "text": dynamic_prompt},
                                {"type": "text", "text": "FONTOS: az előző válaszod tiltott fordulatot tartalmazott (attól függ / mit értesz X alatt / menj a privát oldalamra). EZ ITT a Fanvue = a privát oldalad; írd újra TERMÉSZETESEN, e nélkül."}],
                        messages=msgs)
                    r2 = "".join(b.text for b in resp2.content if b.type == "text").strip()
                    if r2: reply = re.sub(r"\s+", " ", r2).strip()
                except Exception: pass
            def _again(extra_hint):
                try:
                    rr = llm_create(model=model, max_tokens=220,
                        system=[{"type": "text", "text": JAZMIN_PERSONALITY, "cache_control": {"type": "ephemeral"}},
                                {"type": "text", "text": dynamic_prompt}, {"type": "text", "text": extra_hint}],
                        messages=msgs)
                    return re.sub(r"\s+", " ", (rr.text or "")).strip().strip('"')
                except Exception:
                    return ""
            if _OOC_REFUSAL.search(reply):
                print("[persona] out-of-character refusal → regenerating as a tease", flush=True)
                r5 = _again(TEASE_HINT)
                reply = r5 if (r5 and not _OOC_REFUSAL.search(r5)) else random.choice(TEASE_FALLBACK)
            if _BOTTY.search(reply) or len(reply) > 230:
                print("[humanize] botty/long reply → regenerating", flush=True)
                reply = _again(HUMAN_REGEN) or reply
            if chat_id and fv_too_similar(chat_id, reply):
                print("[anti-repeat] regenerating", flush=True)
                reply = _again(FV_REPEAT_HINT) or reply
            reply = collapse_doubles(reply)
            reply = scrub_urls(reply)
            if _DENY_IDENTITY.search(reply):
                print("[safety] identity-denial swapped", flush=True); reply = random.choice(IDENTITY_OWN)
            if leaks_meetup(reply):
                print("[safety] meetup/address → regenerating", flush=True)
                r2 = scrub_urls(collapse_doubles(_again(FV_MEETUP_REGEN)))
                reply = r2 if (r2 and not leaks_meetup(r2)) else random.choice(ADDRESS_DEFLECT)
            reply = re.sub(r"^\s*(j[áa]zmin|rafael|v[áa]lasz|bot)\s*:\s*", "", reply, flags=re.I)
            if away_mode():
                if re.search(r"keep-?warm|utas[íi]t[áa]s|m[óo]dban|\bprompt\b", reply, re.I):
                    print("[away] reply talked about its own instructions → dropped", flush=True); return ""
                if len(reply.split()) > 30:
                    reply = " ".join(re.split(r"(?<=[.!?…])\s+", reply)[:1]) or reply
                _lb = (db_query("SELECT text FROM messages WHERE chat_id=? AND is_mine=1 ORDER BY timestamp DESC LIMIT 1", (chat_id,), fetch_one=True) or {}).get('text') or ''
                reply = fix_daytime(reply, get_budapest_now().hour, user_text or '')
                return thin_smiley(thin_emoji(reply), _lb)   # his own spelling/casing stays (no casualize rewriting)
            return casualize(thin_emoji(reply))
        except anthropic.RateLimitError:
            time.sleep(8 * (attempt + 1))
        except Exception as e:
            other = REPLY_MODEL_FALLBACK if model == REPLY_MODEL else REPLY_MODEL
            if attempt < 2 and other != model:
                send_telegram_error(f"claude err on {model} ({e}); falling back to {other}")
                model = other; continue
            send_telegram_error(f"claude err: {e}"); return ""
    return ""

# ─────────────────────────────────────────────────────────────────────────────
# PROCESS INCOMING
# ─────────────────────────────────────────────────────────────────────────────
_seen_activity = {}      # chat_id → last activity timestamp we already opened
_read_after = {}         # chat_id → (activity ts, seconds to wait before opening)
_no_ts_warned = [False]

def _chat_activity(chat):
    """(timestamp, last_sender_uuid) of a chat from the chat LIST (no message is opened)."""
    lm = chat.get('lastMessage') or chat.get('latestMessage') or {}
    ts = (lm.get('createdAt') or lm.get('sentAt') or chat.get('lastMessageAt') or chat.get('lastMessageSentAt')
          or chat.get('lastActivityAt') or chat.get('updatedAt') or '')
    sender = ((lm.get('sender') or {}).get('uuid') or lm.get('senderUuid') or chat.get('lastMessageSenderUuid') or '')
    return parse_timestamp(ts) if ts else None, sender

def _should_open_chat(chat_id, chat):
    """Keep-warm mode: don't open (= mark read) a fan's new message instantly — wait ~70-95s like a busy person."""
    if not away_mode(): return True
    ts, sender = _chat_activity(chat)
    if ts is None:
        if not _no_ts_warned[0]:
            _no_ts_warned[0] = True; print(f"[away] chat list has no activity time (keys={list(chat.keys())[:15]}) → opening normally")
        return True
    if _seen_activity.get(chat_id) == ts: return False            # nothing new since we last opened it
    if sender and sender == MY_UUID:
        _seen_activity[chat_id] = ts; return True                   # your own/bot message → just sync
    wait = _read_after.get(chat_id)
    if not wait or wait[0] != ts:
        wait = (ts, random.uniform(70, 95)); _read_after[chat_id] = wait
    if (datetime.now(timezone.utc) - ts).total_seconds() < wait[1]: return False
    _seen_activity[chat_id] = ts
    return True

_lab_ready = {}
def _lab_prepare(chat_id, fan_name):
    """First time we see an allowed fan: pull his WHOLE history again (catches everything since the last download) and
    rebuild his memory BEFORE the first reply."""
    try:
        backfill_chat(chat_id, fan_name)
        db_query("UPDATE fan_profiles SET backfilled=1 WHERE chat_id=?", (chat_id,))
        refresh_fan_memory(chat_id, force=True)
        n = (db_query("SELECT COUNT(*) c FROM messages WHERE chat_id=?", (chat_id,), fetch_one=True) or {}).get('c', 0)
        mem = (db_query("SELECT memory_notes FROM fan_profiles WHERE chat_id=?", (chat_id,), fetch_one=True) or {}).get('memory_notes') or ''
        print(f"[lab] {fan_name} ready — {n} messages in memory, notes={len(mem)} chars", flush=True)
        send_telegram_alert(f"🧪 Fanvue: {fan_name} betöltve ({n} üzenet, emlékek frissítve). Mostantól én válaszolok neki "
                            f"(keep-warm, a te stílusodban). Leállítás: /fv_stop {(db_query('SELECT handle FROM fan_profiles WHERE chat_id=?', (chat_id,), fetch_one=True) or {}).get('handle') or chat_id}")
    except Exception as e:
        print(f"[lab prepare] {e}")

def process_new_messages():
    chats, status = get_chats()
    if not chats: return 0, status
    scheduled = 0
    for chat in chats:
        chat_id = None
        try:
            user = chat.get('user', {}) or {}
            chat_id = user.get('uuid') or chat.get('uuid') or chat.get('id')
            if not chat_id: continue
            fan_name = user.get('displayName', 'ismeretlen') or 'ismeretlen'
            handle = user.get('handle', '') or ''
            if not chat_allowed(chat_id, handle): continue          # single-fan mode: never open other chats
            if only_fan() and not _lab_ready.get(chat_id):
                get_or_create_fan(chat_id, fan_name, handle, 0)
                _lab_ready[chat_id] = True
                _lab_prepare(chat_id, fan_name)        # synchronous: memory must be loaded before the first reply
            if not _should_open_chat(chat_id, chat): continue
            api_messages = get_messages(chat_id)
            if not api_messages:
                get_or_create_fan(chat_id, fan_name, handle, 0); continue
            new_n = 0
            for _m in api_messages:
                if (_m.get('sender') or {}).get('uuid') == MY_UUID: continue
                if not db_query('SELECT 1 FROM messages WHERE msg_id=?', (_m.get('uuid') or '',), fetch_one=True): new_n += 1
            get_or_create_fan(chat_id, fan_name, handle, new_n)

            for msg in api_messages:
                msg_id = msg.get('uuid') or ''
                sender = (msg.get('sender') or {}).get('uuid', '')
                text_raw = (msg.get('text') or '').strip()
                mtime = msg.get('createdAt') or msg.get('sentAt') or msg.get('timestamp') or ''
                is_mine = (sender == MY_UUID)
                already = db_query('SELECT facts_done, vision_done FROM messages WHERE msg_id=?', (msg_id,), fetch_one=True)

                if not is_mine and not text_raw and not already:
                    url = extract_media_url(msg)
                    if url:
                        dl = download_media(url)
                        if dl:
                            desc = describe_image(*dl)
                            if desc: text_raw = f"[fotót küldött: {desc}]"

                if is_mine and not text_raw:
                    text_raw = my_media_tag(msg)
                save_message_to_db(msg_id, chat_id, fan_name, sender, text_raw, mtime,
                                   is_mine=is_mine, facts_done=0 if not already else (already.get('facts_done') or 0),
                                   vision_done=1)
                if not is_mine and text_raw and not already:
                    threading.Thread(target=extract_facts, args=(msg_id, chat_id, text_raw), daemon=True).start()

            check_manual_and_ppv(chat_id, fan_name, api_messages)
            if should_skip(chat_id): continue

            fan_msgs = [m for m in api_messages if (m.get('sender') or {}).get('uuid') != MY_UUID]
            if not fan_msgs: continue
            # If the newest message in the chat is YOURS (manual reply / PPV), the fan has been answered → don't reply.
            _ts = lambda m: parse_timestamp(m.get('createdAt') or m.get('sentAt') or m.get('timestamp') or '') or datetime.min.replace(tzinfo=timezone.utc)
            newest = max(api_messages, key=_ts)
            if (newest.get('sender') or {}).get('uuid') == MY_UUID:
                nmt = (newest.get('text') or '').strip()
                if not (nmt and db_query("SELECT 1 FROM messages WHERE chat_id=? AND text=? AND msg_id LIKE 'bot%'", (chat_id, nmt), fetch_one=True)):
                    for fm in fan_msgs:
                        if fm.get('uuid'): db_query('UPDATE messages SET was_replied=1 WHERE msg_id=?', (fm['uuid'],))
                continue
            fan_msgs.sort(key=lambda m: parse_timestamp(m.get('createdAt') or m.get('sentAt') or m.get('timestamp') or '') or datetime.min.replace(tzinfo=timezone.utc), reverse=True)
            last = fan_msgs[0]
            msg_id = last.get('uuid') or ''
            row = db_query('SELECT text FROM messages WHERE msg_id=?', (msg_id,), fetch_one=True)
            text = (row.get('text') if row else (last.get('text') or '')).strip()

            if is_emoji_or_nonsense(text) and not text.startswith('[fotót'):
                if msg_id: db_query('UPDATE messages SET was_replied=1 WHERE msg_id=?', (msg_id,))
                continue
            dt = parse_timestamp(last.get('createdAt') or last.get('sentAt') or last.get('timestamp') or '')
            if dt and (datetime.now(timezone.utc) - dt).total_seconds() > 86400: continue
            if db_query('SELECT 1 FROM messages WHERE msg_id=? AND was_replied=1', (msg_id,), fetch_one=True): continue

            schedule_or_extend_batch(chat_id, fan_name, msg_id, text)
            scheduled += 1
        except Exception as e:
            print(f"[proc {chat_id}] {e}"); send_telegram_error(f"proc {chat_id}: {e}")
    return scheduled, "OK"

# ─────────────────────────────────────────────────────────────────────────────
# SEND DUE
# ─────────────────────────────────────────────────────────────────────────────
def send_due_batches():
    if get_safe_mode(): return 0
    try:
        stale = (datetime.now() - timedelta(minutes=10)).isoformat()
        db_claim("UPDATE scheduled_replies SET status='pending' WHERE status='sending' AND scheduled_time < ?", (stale,))
    except Exception: pass
    sent = 0; handled = set()
    for item in get_due_batches():
        chat_id = item.get('chat_id'); batch_id = item['id']
        if not chat_allowed(chat_id):
            db_query("UPDATE scheduled_replies SET status='cancelled' WHERE id=?", (batch_id,)); continue
        try:
            if chat_id in handled:
                db_claim("UPDATE scheduled_replies SET status='cancelled' WHERE id=? AND status='pending'", (batch_id,)); continue
            handled.add(chat_id)
            if should_skip(chat_id):
                db_claim("UPDATE scheduled_replies SET status='cancelled' WHERE id=?", (batch_id,)); continue
            if db_claim("UPDATE scheduled_replies SET status='sending' WHERE id=? AND status='pending'", (batch_id,)) != 1:
                continue
            db_claim("UPDATE scheduled_replies SET status='cancelled' WHERE chat_id=? AND id!=? AND status='pending'", (chat_id, batch_id))

            if item.get('fan_msg_id') and db_query('SELECT 1 FROM messages WHERE msg_id=? AND was_replied=1', (item['fan_msg_id'],), fetch_one=True):
                db_claim("UPDATE scheduled_replies SET status='cancelled' WHERE id=?", (batch_id,)); continue

            history = get_history(chat_id, 120)
            facts = get_facts(chat_id); real_name = get_real_name(chat_id)
            fan_msgs = [m for m in history if not m.get('is_mine')]
            fan_msg_time = fan_msgs[-1]['timestamp'] if fan_msgs else ''
            dyn = build_dynamic_prompt(chat_id, item['fan_name'] or 'unknown', real_name, facts, history, get_time_context(), fan_msg_time)

            seen = []
            for line in (item['fan_text'] or '').replace("[+] ", "\n").split("\n"):
                line = line.strip()
                if line and line not in seen: seen.append(line)
            user_msg = (f"A fan {len(seen)} üzenetet küldött egymás után, mindegyikre reagálj EGY válaszban:\n" +
                        "\n".join(f"- {s}" for s in seen)) if len(seen) > 1 else (seen[0] if seen else (item['fan_text'] or ''))

            fan_text_all = item['fan_text'] or ''
            last_line = (seen[-1] if seen else fan_text_all).strip()
            prof2 = db_query("SELECT ai_strikes, ai_confirmed, awaiting_tg, tg_handle, tg_user_id, tg_ask_count, ppv_pending "
                             "FROM fan_profiles WHERE chat_id=?", (chat_id,), fetch_one=True) or {}
            override = None
            ai_mode = None          # 'confirm' | 'already' | None
            tg_hint_used = None
            if _SCHOOL_F.search(fan_text_all) and not is_minor_fan(fan_text_all):
                db_query("UPDATE fan_profiles SET is_paused=1 WHERE chat_id=?", (chat_id,))
                send_telegram_alert(f"⚠️ Fanvue: lehet kiskorú — {item.get('fan_name') or chat_id}: „{last_line[:160]}” → szüneteltetve.",
                                    buttons=[[("▶️ Folytatás", f"fvp:{chat_id}")]])
                db_claim("UPDATE scheduled_replies SET status='cancelled' WHERE id=?", (batch_id,)); continue
            if is_minor_fan(fan_text_all):
                db_query("UPDATE fan_profiles SET is_paused=1 WHERE chat_id=?", (chat_id,))
                try: send_telegram_alert(f"⛔ MINOR claim from {item.get('fan_name') or chat_id} -> auto-refused + paused.")
                except Exception: pass
                override = MINOR_MSG_F
            else:
                # ⚖️ Fanvue rule: a DIRECT "are you AI / real?" always gets a clear yes (once), then "already told you".
                if is_direct_ai_question(fan_text_all):
                    db_query("UPDATE fan_profiles SET ai_strikes=COALESCE(ai_strikes,0)+1 WHERE chat_id=?", (chat_id,))
                    ai_mode = 'already' if prof2.get('ai_confirmed') else 'confirm'
                    dyn += "\n\n" + (AI_ALREADY_HINT if ai_mode == 'already' else AI_CONFIRM_HINT)
                if _MEETUP_Q.search(fan_text_all):
                    dyn += "\n\n" + MEETUP_HINT
                # 📨 Telegram memory link — PASSIVE only (Fanvue forbids asking for off-platform names).
                # On Telegram she told him to open with his Telegram name; tracking links match the rest automatically.
                if not (prof2.get('tg_user_id') or prof2.get('tg_handle')):
                    try:
                        tg_hint = None
                        early = len(fan_msgs) <= 4
                        knows = came_from_telegram(fan_text_all) or bool(_KNOWS_ME.search(fan_text_all)) or bool(_TG_NAME_SAY.search(fan_text_all))
                        if early or knows:
                            says_tg = came_from_telegram(fan_text_all) or bool(_TG_NAME_SAY.search(fan_text_all))
                            found = find_tg_candidates(fan_text_all, only_told=True)
                            if says_tg and not pick_tg_match(found):     # he names his Telegram → search ALL Telegram fans
                                found_all = find_tg_candidates(fan_text_all, only_told=False)
                                if pick_tg_match(found_all) or not found[0]: found = found_all
                            cands = found[0]
                            match = pick_tg_match(found)
                            if match:
                                if link_tg_fan(chat_id, match['user_id'], item.get('fan_name'), 'név alapján'):
                                    dyn = build_dynamic_prompt(chat_id, item['fan_name'] or 'unknown', real_name, facts, history, get_time_context(), fan_msg_time)
                                    if ai_mode: dyn += "\n\n" + (AI_ALREADY_HINT if ai_mode == 'already' else AI_CONFIRM_HINT)
                                    if _MEETUP_Q.search(fan_text_all): dyn += "\n\n" + MEETUP_HINT
                                    tg_hint = TG_FOUND_HINT
                            elif (cands or knows) and not (prof2.get('tg_ask_count') or 0):
                                # not sure who he is → ask YOU (buttons), she just stays warm and doesn't ask anything
                                db_query("UPDATE fan_profiles SET tg_ask_count=1 WHERE chat_id=?", (chat_id,))
                                if not cands:
                                    cands = [(0, f) for f in (db_query("SELECT * FROM tg_fans WHERE link_sent_at IS NOT NULL "
                                                                       "ORDER BY link_sent_at DESC LIMIT 5") or [])]
                                if cands:
                                    send_telegram_alert(
                                        f"❓ Új Fanvue fan „{item.get('fan_name') or chat_id}”: „{last_line[:80]}”. Melyik Telegramos fan lehet?",
                                        buttons=[[(tg_display(f)[:40], f"link:{chat_id}:{f['user_id']}")] for _, f in cands[:5]]
                                                + [[("❌ Egyik sem", f"nolink:{chat_id}")]])
                                if knows: tg_hint = TG_NOASK_HINT
                        if tg_hint: dyn += "\n\n" + tg_hint; tg_hint_used = tg_hint
                    except Exception as e:
                        print(f"[tglink] {e}")

            if override is None and _ADVANCE.search(fan_text_all):
                dyn = dyn + "\n\n" + ADVANCE_HINT
            # PPV objection brain — override generic PPV prompt with context-aware version
            if override is None and prof2.get('ppv_pending') and fan_text_all.strip():
                ppv_dir = ppv_objection_directive(fan_text_all)
                dyn = re.sub(r"🔓 PPV KÜLDVE:.*?\n\n", "", dyn, flags=re.DOTALL)
                dyn = dyn + "\n\n" + ppv_dir
            # 💸 smart model only where it matters (money, compliance, first impression, memory reunion)
            smart = any([
                bool(ai_mode), bool(prof2.get('ppv_pending')),
                bool(_ADVANCE.search(fan_text_all)), bool(_MEETUP_Q.search(fan_text_all)),
                tg_hint_used == TG_FOUND_HINT,
                len(fan_msgs) <= 3, len(fan_text_all) > 220,
                len(fan_text_all.strip()) >= 25,          # v14: real conversation → smart model (Haiku felt "dead")
                bool(re.search(r"dr[áa]ga|nincs p[ée]nz|megveszem|mennyibe|\b[áa]r[a]?\b|feloldom|unlock|fizet", fan_text_all, re.I)),
            ])
            if away_mode() and keepwarm_excluded(chat_id) and not (only_fan() == 'allowlist' and chat_allowed(chat_id)):
                print(f"[away] {chat_id[:8]} excluded from keep-warm → skipped", flush=True)
                db_claim("UPDATE scheduled_replies SET status='cancelled' WHERE id=?", (batch_id,)); continue
            if away_mode() and override is None:
                dyn += away_block(chat_id, fan_text_all)
                smart = True
                if _CONTENT_REQ.search(fan_text_all):
                    dyn += "\n\n" + random.choice(LATER_HINTS)
                    try:
                        send_telegram_alert(f"💸 PPV/tartalom kérés (keep-warm módban): {item.get('fan_name') or chat_id}: „{last_line[:200]}”\n"
                                            f"A bot csak annyit mond, hogy majd később — ha ráérsz, küldd el te.",
                                            buttons=[[("💬 Chat megnyitása", f"https://www.fanvue.com/messages?chat={chat_id}")]])
                    except Exception as e:
                        print(f"[away] ppv alert: {e}")
            convo = build_convo(history, user_msg)
            reply = override if override is not None else ask_claude(dyn, user_msg, smart=smart, convo=convo, chat_id=chat_id)

            # ⚖️ Fanvue compliance safety net: never claim to be human; a direct question must get a clear "AI vagyok".
            if override is None and reply:
                need_confirm = (ai_mode == 'confirm') and not confirms_ai(reply)
                if _CLAIM_HUMAN.search(reply) or need_confirm:
                    print(f"[compliance] regenerating (claim_human={bool(_CLAIM_HUMAN.search(reply))}, need_confirm={need_confirm})", flush=True)
                    fix = AI_CONFIRM_HINT if (ai_mode == 'confirm' or need_confirm) else AI_ALREADY_HINT if prof2.get('ai_confirmed') else \
                        "⚖️ FONTOS: SOHA ne állítsd, hogy ember vagy vagy hogy nem vagy AI. Írd újra e nélkül."
                    r2 = ask_claude(dyn + "\n\n" + fix, user_msg, smart=True, convo=convo)
                    if r2 and not _CLAIM_HUMAN.search(r2) and (ai_mode != 'confirm' or confirms_ai(r2)):
                        reply = r2
                    else:
                        reply = AI_FALLBACK_CONFIRM if (ai_mode == 'confirm' or not prof2.get('ai_confirmed')) else AI_FALLBACK_ALREADY
                if not ai_mode and _MENTIONS_AI_OUT.search(reply):
                    print("[compliance] unsolicited AI mention → regenerating", flush=True)
                    r4 = ask_claude(dyn + "\n\n" + NO_AI_HINT, user_msg, smart=True, convo=convo)
                    if r4 and not _MENTIONS_AI_OUT.search(r4) and not _CLAIM_HUMAN.search(r4):
                        reply = r4
                    else:
                        keep = [x for x in re.split(r"(?<=[.!?…])\s+", reply) if not _MENTIONS_AI_OUT.search(x)]
                        reply = " ".join(keep).strip() or (random.choice(ADDRESS_DEFLECT) if _MEETUP_Q.search(fan_text_all) else "")
                if ai_mode == 'already' and not (confirms_ai(reply) or re.search(r"m[áa]r mondtam|tudod a v[áa]laszt|m[áa]r megbesz[ée]lt", reply, re.I)):
                    reply = AI_FALLBACK_ALREADY
                if _MEETUP_Q.search(fan_text_all) and re.search(
                        r"m[ée]g nem|majd egyszer|egyszer majd|tal[áa]n (k[ée]s[őo]bb|egyszer|majd)|ha (m[áa]r )?jobban megismer|el[őo]bb ismerj",
                        reply, re.I):
                    print("[compliance] soft meetup promise → regenerating", flush=True)
                    r3 = ask_claude(dyn + "\n\n" + MEETUP_HINT + "\n" + FV_MEETUP_REGEN, user_msg, smart=True, convo=convo)
                    reply = r3 if (r3 and not re.search(r"m[ée]g nem|majd egyszer|egyszer majd|tal[áa]n (k[ée]s[őo]bb|egyszer|majd)|ha (m[áa]r )?jobban megismer|el[őo]bb ismerj", r3, re.I)) \
                        else random.choice(ADDRESS_DEFLECT)

            if reply and not prof2.get('ppv_pending') and not ai_mode:
                _claimed = bool(_FAKE_SEND.search(reply)); _promised = bool(_PROMISE_SEND.search(reply))
                if _claimed:
                    reply = random.choice(FAKE_SEND_TEASE)
                if _claimed or _promised:
                    try: send_telegram_alert(f"💸 {item.get('fan_name') or chat_id}: ready to unlock — send the PPV manually now.")
                    except Exception: pass
            if not reply:
                db_claim("UPDATE scheduled_replies SET status='cancelled' WHERE id=?", (batch_id,)); continue

            time.sleep(random.uniform(2, 5))

            if send_fanvue_message(chat_id, reply):
                db_claim("UPDATE scheduled_replies SET status='sent' WHERE id=?", (batch_id,))
                db_query('UPDATE messages SET was_replied=1 WHERE msg_id=?', (item['fan_msg_id'],))
                now_iso = datetime.now().isoformat()
                save_message_to_db(f"bot_{now_iso}_{chat_id}", chat_id, item['fan_name'], MY_UUID, reply, now_iso, is_mine=True, facts_done=1, vision_done=1)
                db_query("UPDATE fan_profiles SET ppv_pending=0 WHERE chat_id=? AND ppv_pending=1", (chat_id,))
                if ai_mode and confirms_ai(reply):
                    db_query("UPDATE fan_profiles SET ai_confirmed=1 WHERE chat_id=?", (chat_id,))
                threading.Thread(target=maybe_update_summary, args=(chat_id, history), daemon=True).start()
                threading.Thread(target=refresh_fan_memory, args=(chat_id,), daemon=True).start()
                if not away_mode(): maybe_run_auto_ppv(chat_id, item['fan_name'])
                sent += 1
            else:
                # retry at most 2 more times, a minute apart (v12 re-asked Claude every 4s forever)
                retry_at = (datetime.now() + timedelta(seconds=60)).isoformat()
                db_claim("UPDATE scheduled_replies SET status=CASE WHEN COALESCE(attempts,0)>=2 THEN 'failed' ELSE 'pending' END, "
                         "attempts=COALESCE(attempts,0)+1, scheduled_time=? WHERE id=?", (retry_at, batch_id))
        except Exception as e:
            print(f"[send {chat_id}] {e}"); send_telegram_error(f"send {chat_id}: {e}")
    return sent

# ─────────────────────────────────────────────────────────────────────────────
# POLL LOOP
# ─────────────────────────────────────────────────────────────────────────────
polling_active = False
polling_thread = None
send_thread    = None
reng_thread    = None
ctrl_thread    = None
track_thread   = None
backfill_thread = None
_is_leader     = False
_has_poll_lock = False   # holds the poll lock (even when the Fanvue token is dead) → runs the control panel

def poll_loop():
    global _is_leader, _has_poll_lock
    errs = 0
    while polling_active:
        try:
            lock = acquire_poll_lock(); _has_poll_lock = lock
            if lock and get_fanvue_token():
                _is_leader = True
                if get_safe_mode():          # OFF = hands off: don't even fetch chats, so messages stay UNREAD for you
                    time.sleep(POLL_INTERVAL); continue
                sc, _ = process_new_messages()
                if sc: print(f"[{datetime.now()}] w={WORKER_ID} scan sched={sc}")
            else:
                _is_leader = False
            errs = 0
        except Exception as e:
            errs += 1; print(f"[scan #{errs}] {e}")
            if errs <= 3: send_telegram_error(f"scan #{errs}: {e}")
        time.sleep(POLL_INTERVAL)

def send_loop():
    while polling_active:
        try:
            if _is_leader:
                s = send_due_batches()
                if s: print(f"[{datetime.now()}] w={WORKER_ID} sent={s}")
        except Exception as e:
            print(f"[send] {e}")
        time.sleep(SEND_INTERVAL)

def start_polling():
    global polling_thread, send_thread, reng_thread, polling_active
    if os.environ.get('APP_NO_BOOT') == '1':
        print("[boot] APP_NO_BOOT set -> poller not started"); return False
    polling_active = True; started = False
    if polling_thread is None or not polling_thread.is_alive():
        polling_thread = threading.Thread(target=poll_loop, daemon=True); polling_thread.start(); started = True
    if send_thread is None or not send_thread.is_alive():
        send_thread = threading.Thread(target=send_loop, daemon=True); send_thread.start(); started = True
    if reng_thread is None or not reng_thread.is_alive():
        reng_thread = threading.Thread(target=run_reengagement_loop, daemon=True); reng_thread.start(); started = True
    global ctrl_thread, track_thread
    if ctrl_thread is None or not ctrl_thread.is_alive():
        ctrl_thread = threading.Thread(target=control_bot_loop, daemon=True); ctrl_thread.start(); started = True
    if track_thread is None or not track_thread.is_alive():
        track_thread = threading.Thread(target=tracking_loop, daemon=True); track_thread.start(); started = True
    global backfill_thread
    if backfill_thread is None or not backfill_thread.is_alive():
        backfill_thread = threading.Thread(target=backfill_loop, daemon=True); backfill_thread.start(); started = True
    return started

# ─────────────────────────────────────────────────────────────────────────────
# FANVUE TRACKING LINKS — one personal link per Telegram fan (created when he nears the link moment).
# Telegram bot writes a row (status='requested') into tg_tracking; we create the link here (we have the
# Fanvue login), and later read who signed up through it → his Telegram memory is linked automatically.
# Field names of Fanvue's tracking-link API aren't fully documented, so parsing is defensive + logged.
# ─────────────────────────────────────────────────────────────────────────────
def _ensure_tracking_table():
    db_query("""CREATE TABLE IF NOT EXISTS tg_tracking (tg_user_id TEXT PRIMARY KEY, status TEXT, link_uuid TEXT,
        url TEXT, requested_at TEXT, created_at TEXT, matched INTEGER DEFAULT 0, error TEXT)""")

def _pick(d, *keys):
    for k in keys:
        cur = d
        for part in k.split('.'):
            cur = cur.get(part) if isinstance(cur, dict) else None
        if cur: return cur
    return None

def create_tracking_link(tg_uid):
    name = f"tg{tg_uid}"[:40]
    for body in ({"name": name, "externalSocialPlatform": "telegram"}, {"name": name, "externalSocialPlatform": "other"}, {"name": name}):
        try:
            r = requests.post("https://api.fanvue.com/tracking-links", headers=get_headers(), json=body, timeout=15)
        except Exception as e:
            return None, None, f"err {e}"
        if r.status_code in (200, 201):
            d = r.json() if r.text else {}
            d = d.get('data', d) if isinstance(d, dict) else {}
            uuid_ = _pick(d, 'uuid', 'id', 'trackingLink.uuid')
            url = _pick(d, 'linkUrl', 'url', 'link', 'trackingUrl', 'shareUrl', 'fullUrl', 'trackingLink.url')
            if url and not str(url).startswith('http'): url = 'https://www.fanvue.com/' + str(url).lstrip('/')
            print(f"[tracking] created for {tg_uid}: uuid={uuid_} url={url} keys={list(d.keys())[:12]}", flush=True)
            return uuid_, url, None if (uuid_ and url) else f"unexpected response: {r.text[:200]}"
        last = f"{r.status_code} {r.text[:200]}"
        if r.status_code not in (400, 422): break
    return None, None, last

def _repair_failed_tracking():
    """v14.1: links were created on Fanvue but the URL field ('linkUrl') wasn't read → rows marked failed.
    Find them by name in the list and mark ready (no duplicates created)."""
    rows = db_query("SELECT tg_user_id FROM tg_tracking WHERE status='failed'") or []
    if not rows: return
    by_name = {}
    for page in range(1, 21):
        try:
            r = requests.get(f"https://api.fanvue.com/tracking-links?page={page}&size=50&limit=50", headers=get_headers(), timeout=15)
            if r.status_code != 200: break
            data = (r.json() or {}).get('data') or []
        except Exception:
            break
        new = 0
        for d in data:
            nm = d.get('name')
            if nm and nm not in by_name: by_name[nm] = d; new += 1
        if not data or not new: break
    for row in rows:
        d = by_name.get(f"tg{row['tg_user_id']}"[:40])
        url = _pick(d or {}, 'linkUrl', 'url')
        if d and url:
            if not str(url).startswith('http'): url = 'https://www.fanvue.com/' + str(url).lstrip('/')
            db_query("UPDATE tg_tracking SET status='ready', link_uuid=?, url=?, created_at=?, error=NULL WHERE tg_user_id=?",
                     (d.get('uuid'), url, datetime.now(timezone.utc).isoformat(), row['tg_user_id']))
        else:
            db_query("UPDATE tg_tracking SET status='requested', error=NULL WHERE tg_user_id=?", (row['tg_user_id'],))
    print(f"[tracking] repaired {len(rows)} failed row(s); example url: {next(iter(by_name.values()), {}).get('linkUrl')}", flush=True)

_tracking_alerted = [False]
_tracking_repaired = [False]
def tracking_links_tick():
    """Create requested links; then check which Fanvue users came through each link."""
    _ensure_tracking_table()
    if os.environ.get('USE_TRACKING_LINKS', '0') != '1':
        db_query("UPDATE tg_tracking SET status='off' WHERE status IN ('requested','failed')")
        return
    if not _tracking_repaired[0]:
        _tracking_repaired[0] = True
        try: _repair_failed_tracking()
        except Exception as e: print(f"[tracking] repair: {e}")
    for row in (db_query("SELECT tg_user_id FROM tg_tracking WHERE status='requested' LIMIT 10") or []):
        uuid_, url, err = create_tracking_link(row['tg_user_id'])
        if uuid_ and url:
            db_query("UPDATE tg_tracking SET status='ready', link_uuid=?, url=?, created_at=?, error=NULL WHERE tg_user_id=?",
                     (uuid_, url, datetime.now(timezone.utc).isoformat(), row['tg_user_id']))
            save_token('tracking_fail_alerted', '')
        else:
            db_query("UPDATE tg_tracking SET status='failed', error=? WHERE tg_user_id=?", (err, row['tg_user_id']))
            if not _tracking_alerted[0] and load_token('tracking_fail_alerted') != 'yes':
                _tracking_alerted[0] = True; save_token('tracking_fail_alerted', 'yes')
                send_telegram_alert(f"⚠️ Nem sikerült Fanvue tracking linket létrehozni ({(err or '')[:150]}). "
                                    f"A Telegram bot addig a sima linket küldi — szólj Claude-nak.")
    for row in (db_query("SELECT tg_user_id, link_uuid FROM tg_tracking WHERE status='ready' AND COALESCE(matched,0)=0 "
                           "ORDER BY created_at DESC LIMIT 30") or []):
        try:
            r = requests.get(f"https://api.fanvue.com/tracking-links/{row['link_uuid']}/users?limit=50", headers=get_headers(), timeout=15)
            if r.status_code != 200: continue
            for u in (r.json().get('data') or []):
                fv_uuid = _pick(u, 'uuid', 'userUuid', 'user.uuid', 'fan.uuid')
                if not fv_uuid: continue
                db_query("INSERT OR IGNORE INTO fan_profiles (chat_id, fan_name, total_messages, last_interaction) VALUES (?, ?, 0, ?)",
                         (fv_uuid, _pick(u, 'displayName', 'user.displayName', 'handle') or 'unknown', datetime.now().isoformat()))
                prof = db_query("SELECT tg_user_id, fan_name FROM fan_profiles WHERE chat_id=?", (fv_uuid,), fetch_one=True) or {}
                if not prof.get('tg_user_id'):
                    link_tg_fan(fv_uuid, row['tg_user_id'], prof.get('fan_name'), 'tracking link ✅')
                db_query("UPDATE tg_tracking SET matched=1 WHERE tg_user_id=?", (row['tg_user_id'],))
                break
        except Exception as e:
            print(f"[tracking] users {row.get('link_uuid')}: {e}")

def tracking_loop():
    time.sleep(60)
    while polling_active:
        try:
            if _is_leader: tracking_links_tick()
        except Exception as e:
            print(f"[tracking loop] {e}")
        time.sleep(180)

# ─────────────────────────────────────────────────────────────────────────────
# CONTROL PANEL — your Telegram alert bot doubles as the on/off switch for BOTH bots.
#   Fanvue bot:  safe_mode on = reads but does NOT send.     Telegram bot: tokens.tg_enabled = 'off'
# ─────────────────────────────────────────────────────────────────────────────
def tg_bot_enabled():  return (load_token('tg_enabled') or 'on') != 'off'
def set_tg_bot(on):    save_token('tg_enabled', 'on' if on else 'off')
def fanvue_enabled():  return not get_safe_mode()
def set_fanvue(on):    set_safe_mode(not on)

MENU_BUTTONS = [[("▶️ Mindkettő BE", "all:on"), ("⏸ Mindkettő KI", "all:off")],
                [("Fanvue BE", "fv:on"), ("Fanvue KI", "fv:off")],
                [("Telegram BE", "tg:on"), ("Telegram KI", "tg:off")],
                [("👥 Fanvue fanok", "fvlist"), ("👥 Telegram fanok", "tglist")],
                [("📊 Állapot", "status"), ("🔗 Fanvue újracsatl.", "reconnect")],
                [("🏝️ Távol (keep-warm)", "away:on"), ("🧑‍💻 Itt vagyok", "away:off")],
                [("🧠 Tanulás (letöltés + stílus)", "learn")]]

def _fan_rows_fv(search=None, limit=8):
    if search:
        qn = _norm_name(search)
        rows = db_query("SELECT chat_id, fan_name, is_paused FROM fan_profiles ORDER BY last_interaction DESC LIMIT 2000") or []
        return [r for r in rows if qn in _norm_name(r.get('fan_name'))][:limit]
    return db_query("SELECT chat_id, fan_name, is_paused FROM fan_profiles ORDER BY last_interaction DESC LIMIT ?", (limit,)) or []

def _fan_rows_tg(search=None, limit=8):
    try:
        if search:
            qn = _norm_name(search)
            rows = db_query("SELECT user_id, first_name, last_name, username, paused, msg_count FROM tg_fans ORDER BY created_at DESC LIMIT 5000") or []
            return [r for r in rows if qn in _norm_name(f"{r.get('first_name') or ''} {r.get('last_name') or ''} {r.get('username') or ''}")][:limit]
        return db_query("SELECT t.user_id, t.first_name, t.last_name, t.username, t.paused, t.msg_count FROM tg_fans t "
                        "LEFT JOIN (SELECT user_id, MAX(id) mid FROM tg_messages GROUP BY user_id) m ON m.user_id=t.user_id "
                        "ORDER BY m.mid DESC LIMIT ?", (limit,)) or []
    except Exception as e:
        print(f"[control] tg fans: {e}"); return []

def send_fan_list(kind, search=None):
    """Each fan = one button; tap toggles ⏸/▶️. Saved in the DB, so it survives restarts."""
    if kind == 'fv':
        rows = _fan_rows_fv(search)
        btns = [[(("⏸ " if r.get('is_paused') else "▶️ ") + (r.get('fan_name') or r['chat_id'][:8])[:30], f"fvp:{r['chat_id']}")] for r in rows]
        title = "👥 Fanvue fanok (▶️ = bot válaszol, ⏸ = megállítva). Koppints a váltáshoz:"
    else:
        rows = _fan_rows_tg(search)
        btns = [[(("⏸ " if r.get('paused') else "▶️ ") + tg_display(r)[:30] + f" · {r.get('msg_count') or 0}", f"tgp:{r['user_id']}")] for r in rows]
        title = "👥 Telegram fanok (▶️ = bot válaszol, ⏸ = megállítva). Koppints a váltáshoz:"
    if not rows:
        send_telegram_alert("Nem találtam ilyen fant." if search else "Még nincs fan."); return
    send_telegram_alert(title + ("\nKeresés: " + search if search else "\nTipp: /fan Péter — keresés név szerint"), buttons=btns)

def status_text():
    tok = load_token('token_status') or 'ok'
    exp = load_token('expires_at') or ''
    hb = load_token('tg_heartbeat') or ''
    try: tg_alive = (datetime.now(timezone.utc) - datetime.fromisoformat(hb)).total_seconds() < 600
    except Exception: tg_alive = False
    pend = db_query("SELECT COUNT(*) c FROM scheduled_replies WHERE status='pending'", fetch_one=True) or {}
    sent = db_query("SELECT COUNT(*) c FROM scheduled_replies WHERE status='sent' AND created_at >= date('now')", fetch_one=True) or {}
    linked = db_query("SELECT COUNT(*) c FROM fan_profiles WHERE tg_user_id IS NOT NULL", fetch_one=True) or {}
    return ("📊 ÁLLAPOT\n"
            f"Fanvue bot: {'🟢 BE (válaszol)' if fanvue_enabled() else '⏸ KI (csak olvas)'}\n"
            f"Fanvue kapcsolat: {'🟢 rendben' if tok != 'dead' and (load_token('refresh_token') or '') else '🔴 újracsatlakoztatás kell'}\n"
            f"Telegram bot: {'🟢 BE' if tg_bot_enabled() else '⏸ KI'} · {'él ✅' if tg_alive else 'nem jelentkezik ⚠️ (Railway-en fut?)'}\n"
            f"Függő válaszok: {pend.get('c', 0)} · ma elküldve: {sent.get('c', 0)} · összekötött TG fanok: {linked.get('c', 0)}\n"
            f"Keep-warm mód: {'🏝️ BE' if away_mode() else 'KI'} · Tanulás: {load_token('learn_status') or 'még nem futott'}"
            + (f"\n🧪 Csak ezeknek válaszol: {', '.join('@' + h for h in sorted(allowed_set())) or 'senkinek'} — más chatet nem nyit meg "
               f"(/fv_start név · /fv_stop név · /fv_list)" if only_fan() else ""))

def fv_allow_cmd(act, arg):
    """Per-fan Fanvue on/off from Telegram: /fv_start <handle or link>, /fv_stop <handle>, /fv_list."""
    if only_fan() != 'allowlist':
        return "A per-fan mód nincs bekapcsolva a szerveren (ONLY_FAN=allowlist kell). Szólj Claude-nak."
    al = allowed_set()
    if act == "fvallowlist":
        return ("🟢 Fanvue bot ezeknek válaszol: " + ", ".join("@" + h for h in sorted(al))) if al else "⏸ Senkinek nem válaszol most."
    hs = [norm_handle(x) for x in re.split(r"[\s,]+", arg or "") if norm_handle(x)]
    if not hs:
        return "Írd mögé a fan nevét, pl.: /fv_start csaby84  vagy  /fv_stop csaby84"
    if act == "fvallow":
        set_allowed(al | set(hs))
        cancel_stale_pending()
        set_away(True); set_fanvue(True)
        for h in hs:
            for cid in chat_ids_for(h): _lab_ready.pop(cid, None)
        unknown = [h for h in hs if not chat_ids_for(h)]
        return ("▶️ Fanvue bot BE: " + ", ".join("@" + h for h in hs) + " (az előzményeit betöltöm az első üzenet előtt)."
                + (f"\n⚠️ Ilyen nevű chatet még nem láttam: {', '.join(unknown)} — ha elírtad, /fv_stop {unknown[0]}." if unknown else "")
                + "\nMost aktív: " + ", ".join("@" + h for h in sorted(allowed_set())))
    left = al - set(hs)
    set_allowed(left)
    for h in hs:
        for cid in chat_ids_for(h):
            db_query("UPDATE scheduled_replies SET status='cancelled' WHERE chat_id=? AND status IN ('pending','sending')", (cid,))
    if not left:
        set_away(False); set_fanvue(False)
    return ("⏸ Fanvue bot KI: " + ", ".join("@" + h for h in hs) + "."
            + ("\nMég aktív: " + ", ".join("@" + h for h in sorted(left)) if left else "\nMost senkinek nem válaszol (bot KI)."))

def handle_control(data):
    """Run one control action; returns a short confirmation text."""
    if data in ("all:on", "all:off"):
        on = data.endswith("on")
        if on: cancel_stale_pending()
        set_fanvue(on); set_tg_bot(on)
        return "▶️ Mindkét bot BEKAPCSOLVA." if on else "⏸ Mindkét bot KIKAPCSOLVA (az üzeneteket elmentik, de nem válaszolnak)."
    if data in ("fv:on", "fv:off"):
        if data.endswith("on"): cancel_stale_pending()
        set_fanvue(data.endswith("on")); return "Fanvue bot: " + ("🟢 BE" if data.endswith("on") else "⏸ KI")
    if data in ("tg:on", "tg:off"):
        set_tg_bot(data.endswith("on")); return "Telegram bot: " + ("🟢 BE" if data.endswith("on") else "⏸ KI")
    if data == "status": return status_text()
    if data == "learn": return start_learning()
    if data == "away:on":
        cancel_stale_pending()
        set_away(True); set_fanvue(True)
        return "🏝️ KEEP-WARM mód BE: a bot a TE stílusodban válaszol, nem ad el semmit. Ha te írsz egy fannak, nála 5 percre félreáll. /back = vissza."
    if data == "away:off":
        set_away(False); set_fanvue(False)
        return "🧑‍💻 Visszajöttél: a Fanvue bot KI, te viszed a beszélgetéseket."
    if data in ("fvlist", "tglist"):
        send_fan_list(data[:2]); return ""
    if data.startswith("fvp:"):
        cid = data.split(":", 1)[1]
        r = db_query("SELECT fan_name, is_paused FROM fan_profiles WHERE chat_id=?", (cid,), fetch_one=True) or {}
        new = 0 if r.get('is_paused') else 1
        db_query("UPDATE fan_profiles SET is_paused=? WHERE chat_id=?", (new, cid))
        if new: db_query("UPDATE scheduled_replies SET status='cancelled' WHERE chat_id=? AND status='pending'", (cid,))
        return f"{'⏸ Megállítva' if new else '▶️ Újra válaszol'}: Fanvue „{r.get('fan_name') or cid}”"
    if data.startswith("tgp:"):
        tid = data.split(":", 1)[1]
        r = db_query("SELECT * FROM tg_fans WHERE user_id=?", (tid,), fetch_one=True) or {}
        new = 0 if r.get('paused') else 1
        db_query("UPDATE tg_fans SET paused=? WHERE user_id=?", (new, tid))
        return f"{'⏸ Megállítva' if new else '▶️ Újra válaszol'}: Telegram {tg_display(r)}"
    if data == "reconnect":
        send_telegram_alert("Koppints, lépj be a Fanvue-ba, és kész:", buttons=[[("🔗 Fanvue újracsatlakoztatása", make_reconnect_link())]])
        return ""
    if data.startswith("link:"):
        _, fv, tg = data.split(":", 2)
        name = (db_query("SELECT fan_name FROM fan_profiles WHERE chat_id=?", (fv,), fetch_one=True) or {}).get('fan_name')
        return "" if link_tg_fan(fv, tg, name, 'kézzel') else "Nem találom ezt a Telegramos fant."
    if data.startswith("unlink:"):
        unlink_tg_fan(data.split(":", 1)[1]); return "↩️ Visszavonva — nincs összekötve."
    if data.startswith("nolink:"):
        return "Rendben, nem kötöm össze."
    return ""

_CMD_MAP = {"/start": "menu", "/menu": "menu", "/status": "status", "/allapot": "status",
            "/be": "all:on", "/ki": "all:off", "/startall": "all:on", "/stopall": "all:off",
            "/fanvue_be": "fv:on", "/fanvue_ki": "fv:off", "/telegram_be": "tg:on", "/telegram_ki": "tg:off",
            "/reconnect": "reconnect", "/ujracsatlakozas": "reconnect",
            "/learn": "learn", "/tanulj": "learn", "/away": "away:on", "/tavol": "away:on", "/back": "away:off", "/vissza": "away:off",
            "/fv_start": "fvallow", "/fvstart": "fvallow", "/fv_stop": "fvdeny", "/fvstop": "fvdeny", "/fv_list": "fvallowlist", "/fvlist": "fvallowlist",
            "/fans": "fvlist", "/fanok": "fvlist", "/tgfans": "tglist", "/tgfanok": "tglist", "/fan": "search"}

def control_bot_loop():
    """Long-polls the alert bot for your commands / button taps. Only accepts YOUR chat id."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("[control] TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID missing — control panel off"); return
    api = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"
    try: requests.post(f"{api}/deleteWebhook", timeout=10)
    except Exception: pass
    try:
        requests.post(f"{api}/setMyCommands", json={"commands": [
            {"command": "menu", "description": "Vezérlőpult (be/ki, állapot)"},
            {"command": "status", "description": "Állapot"},
            {"command": "be", "description": "Mindkét bot BE"},
            {"command": "ki", "description": "Mindkét bot KI"},
            {"command": "reconnect", "description": "Fanvue újracsatlakoztatása"},
            {"command": "fans", "description": "Fanvue fanok — megállítás / indítás"},
            {"command": "tgfans", "description": "Telegram fanok — megállítás / indítás"},
            {"command": "fan", "description": "Fan keresése név szerint: /fan Péter"}]}, timeout=10)
    except Exception: pass
    offset = int(load_token('tg_ctrl_offset') or 0)
    if not offset:
        # first boot: skip old commands (an old "/ki" must not switch everything off)
        try:
            res = requests.get(f"{api}/getUpdates", params={"offset": -1}, timeout=15).json().get('result') or []
            if res: offset = res[-1]['update_id'] + 1; save_token('tg_ctrl_offset', str(offset))
        except Exception: pass
    while polling_active:
        if not (_has_poll_lock or _is_leader):
            time.sleep(5); continue
        try:
            r = requests.get(f"{api}/getUpdates", params={"offset": offset, "timeout": 25,
                             "allowed_updates": json.dumps(["message", "callback_query"])}, timeout=35)
            j = r.json()
            if not j.get('ok'):
                time.sleep(int((j.get('parameters') or {}).get('retry_after', 5))); continue
            for u in (j.get('result') or []):
                offset = u['update_id'] + 1; save_token('tg_ctrl_offset', str(offset))
                try:
                    if 'callback_query' in u:
                        cq = u['callback_query']
                        if str((cq.get('message') or {}).get('chat', {}).get('id')) != str(TELEGRAM_CHAT_ID): continue
                        requests.post(f"{api}/answerCallbackQuery", json={"callback_query_id": cq['id']}, timeout=10)
                        out = handle_control(cq.get('data') or '')
                        if out: send_telegram_alert(out, buttons=MENU_BUTTONS if cq.get('data') == 'status' else None)
                    elif 'message' in u:
                        m = u['message']
                        if str(m.get('chat', {}).get('id')) != str(TELEGRAM_CHAT_ID): continue
                        cmd = (m.get('text') or '').strip().split()[0].split('@')[0].lower() if (m.get('text') or '').strip() else ''
                        act = _CMD_MAP.get(cmd)
                        if act in ("fvallow", "fvdeny", "fvallowlist"):
                            arg_ = " ".join((m.get('text') or '').split()[1:]).strip()
                            send_telegram_alert(fv_allow_cmd(act, arg_)); continue
                        if act == "search":
                            q_ = " ".join((m.get('text') or '').split()[1:]).strip()
                            if not q_: send_telegram_alert("Írd mögé a nevet, pl.: /fan Péter")
                            else: send_fan_list('fv', q_); send_fan_list('tg', q_)
                            continue
                        if act == "menu":
                            send_telegram_alert(status_text() + "\n\nMit csináljak?", buttons=MENU_BUTTONS)
                        elif act:
                            out = handle_control(act)
                            if out: send_telegram_alert(out, buttons=MENU_BUTTONS if act == 'status' else None)
                except Exception as e:
                    print(f"[control] update err: {e}")
        except Exception as e:
            print(f"[control] {e}"); time.sleep(5)

# ─────────────────────────────────────────────────────────────────────────────
# AUTH
# ─────────────────────────────────────────────────────────────────────────────
def require_auth(f):
    @wraps(f)
    def w(*a, **k):
        key = request.args.get('key') or request.args.get('pw') or request.headers.get('X-Auth') or ''
        if key != DASHBOARD_PASSWORD:
            return {"error": "unauthorized"}, 401
        return f(*a, **k)
    return w

# ─────────────────────────────────────────────────────────────────────────────
# ROUTES
# ─────────────────────────────────────────────────────────────────────────────
@app.route('/')
def home(): return "Jazmin Bot v15 ✅", 200

@app.route('/status')
@require_auth
def status():
    p = db_query("SELECT COUNT(*) c FROM scheduled_replies WHERE status='pending'", fetch_one=True)
    return {"token_valid": get_fanvue_token() is not None, "polling_active": polling_active,
            "safe_mode": get_safe_mode(), "pending_batches": p['c'] if p else 0, "worker": WORKER_ID}, 200

@app.route('/safe_mode/<onoff>')
@require_auth
def safe_mode(onoff):
    set_safe_mode(onoff == 'on'); return {"safe_mode": onoff == 'on'}, 200

@app.route('/pause/<chat_id>')
@require_auth
def pause(chat_id):
    db_query("INSERT OR IGNORE INTO fan_profiles (chat_id, fan_name, total_messages, last_interaction) VALUES (?, 'unknown', 0, ?)", (chat_id, datetime.now().isoformat()))
    db_query("UPDATE fan_profiles SET is_paused=1 WHERE chat_id=?", (chat_id,))
    db_query("UPDATE scheduled_replies SET status='cancelled' WHERE chat_id=? AND status='pending'", (chat_id,))
    return {"paused": True, "chat_id": chat_id}, 200

@app.route('/resume/<chat_id>')
@require_auth
def resume(chat_id):
    db_query("UPDATE fan_profiles SET is_paused=0 WHERE chat_id=?", (chat_id,)); return {"paused": False, "chat_id": chat_id}, 200

@app.route('/ppv_sent/<chat_id>', methods=['POST', 'GET'])
@require_auth
def ppv_sent(chat_id):
    note = (request.json or {}).get('note', '') if request.is_json else request.args.get('note', '')
    db_query("INSERT OR IGNORE INTO fan_profiles (chat_id, fan_name, total_messages, last_interaction) VALUES (?, 'unknown', 0, ?)", (chat_id, datetime.now().isoformat()))
    db_query("UPDATE fan_profiles SET ppv_pending=1, ppv_note=? WHERE chat_id=?", (note, chat_id))
    return {"ok": True, "chat_id": chat_id, "ppv_pending": True}, 200

@app.route('/set_note/<chat_id>', methods=['POST'])
@require_auth
def set_note(chat_id):
    note = (request.json or {}).get('note', '')
    db_query("INSERT OR IGNORE INTO fan_profiles (chat_id, fan_name, total_messages, last_interaction) VALUES (?, 'unknown', 0, ?)", (chat_id, datetime.now().isoformat()))
    db_query("UPDATE fan_profiles SET fan_note=? WHERE chat_id=?", (note, chat_id))
    return {"ok": True}, 200

@app.route('/link_tg/<chat_id>', methods=['POST', 'GET'])
@require_auth
def link_tg(chat_id):
    h = (request.json or {}).get('tg_handle', '') if request.is_json else request.args.get('tg_handle', '')
    tg_uid = (request.json or {}).get('tg_user_id', '') if request.is_json else request.args.get('tg_user_id', '')
    if not tg_uid and h:
        found = find_tg_candidates(h)
        best = pick_tg_match(found) or (found[0][0][1] if found[0] else None)
        tg_uid = best['user_id'] if best else ''
    if not tg_uid:
        return {"ok": False, "error": "no matching Telegram fan"}, 404
    name = (db_query("SELECT fan_name FROM fan_profiles WHERE chat_id=?", (chat_id,), fetch_one=True) or {}).get('fan_name')
    ok = link_tg_fan(chat_id, tg_uid, name, 'dashboard')
    return {"ok": ok, "tg_user_id": tg_uid}, 200 if ok else 404

@app.route('/unlink_tg/<chat_id>', methods=['POST', 'GET'])
@require_auth
def unlink_tg(chat_id):
    unlink_tg_fan(chat_id); return {"ok": True}, 200

@app.route('/export_chats')
@require_auth
def export_chats():
    """Read-only plain-text transcripts for reviewing how the bots talk. ?src=tg|fv&since=YYYY-MM-DD&page=0&per=10"""
    from flask import Response
    src = request.args.get('src', 'tg'); since = request.args.get('since', '2026-10-16')
    page = int(request.args.get('page', 0)); per = min(int(request.args.get('per', 10)), 30)
    out = []
    if src == 'tg':
        ids = db_query("SELECT user_id, MAX(id) mid FROM tg_messages WHERE ts >= ? GROUP BY user_id ORDER BY mid DESC", (since,)) or []
        out.append(f"TELEGRAM chats active since {since}: {len(ids)} (page {page}, {per}/page)")
        for r in ids[page * per:(page + 1) * per]:
            f = db_query("SELECT * FROM tg_fans WHERE user_id=?", (r['user_id'],), fetch_one=True) or {}
            out.append(f"\n===== TG {r['user_id']} | {tg_display(f)} | state={f.get('state')} msgs={f.get('msg_count')} pics={f.get('pic_asks')} =====")
            for m in db_query("SELECT is_mine, text, ts FROM tg_messages WHERE user_id=? ORDER BY id", (r['user_id'],)) or []:
                out.append(f"{(m.get('ts') or '')[5:16]} {'J' if m['is_mine'] else 'F'}: {(m.get('text') or '').strip()}")
    else:
        ids = db_query("SELECT chat_id, MAX(timestamp) mt FROM messages WHERE timestamp >= ? GROUP BY chat_id ORDER BY mt DESC", (since,)) or []
        out.append(f"FANVUE chats active since {since}: {len(ids)} (page {page}, {per}/page)")
        for r in ids[page * per:(page + 1) * per]:
            p_ = db_query("SELECT fan_name, total_messages, warmth_points, tg_user_id, ai_confirmed, is_paused, ppv_pending FROM fan_profiles WHERE chat_id=?", (r['chat_id'],), fetch_one=True) or {}
            out.append(f"\n===== FV {r['chat_id'][:8]} | {p_.get('fan_name')} | msgs={p_.get('total_messages')} warmth={p_.get('warmth_points')} tg={'yes' if p_.get('tg_user_id') else 'no'} ai_conf={p_.get('ai_confirmed')} paused={p_.get('is_paused')} =====")
            seen = set()
            for m in db_query("SELECT is_mine, text, timestamp, msg_id FROM messages WHERE chat_id=? ORDER BY timestamp", (r['chat_id'],)) or []:
                t = (m.get('text') or '').strip()
                k = (m['is_mine'], t)
                if not t or k in seen and m['is_mine']: continue
                seen.add(k)
                who = ('J' if str(m.get('msg_id', '')).startswith('bot') else 'R') if m['is_mine'] else 'F'
                out.append(f"{(m.get('timestamp') or '')[5:16]} {who}: {t}")
    return Response("\n".join(out), mimetype='text/plain; charset=utf-8')

@app.route('/debug_chat_list')
@require_auth
def debug_chat_list():
    chats, status = get_chats()
    def shape(d, depth=0):
        if isinstance(d, dict) and depth < 2:
            return {k: shape(v, depth + 1) for k, v in d.items() if k not in ('text', 'displayName', 'handle', 'avatarUrl', 'bannerUrl')}
        return type(d).__name__
    return {"status": status, "count": len(chats), "first_shape": shape(chats[0]) if chats else None,
            "activity_parsed": [str(_chat_activity(c)[0]) for c in chats[:3]]}

@app.route('/style_report')
@require_auth
def style_report():
    from flask import Response
    import html as _h
    stats = load_token('style_stats') or '{}'
    guide = load_token('style_guide') or '(még nincs — futtasd a /learn parancsot)'
    try: bank = json.loads(load_token('style_examples') or '[]')
    except Exception: bank = []
    parts = [f"<h2>Tanulás állapota</h2><p>{_h.escape(load_token('learn_status') or '-')}</p>",
             f"<h2>Statisztika</h2><pre>{_h.escape(stats)}</pre>",
             f"<h2>Stílus-útmutató</h2><pre style='white-space:pre-wrap'>{_h.escape(guide)}</pre>",
             "<h2>Példák (valódi válaszaid)</h2>" + "".join(f"<p><b>FAN:</b> {_h.escape(p['fan'][:200])}<br><b>TE:</b> {_h.escape(p['me'][:260])}</p>" for p in bank[:30])]
    if request.args.get('preview') == '1':
        parts.append("<h2>Próba (NEM lett elküldve)</h2>" + "".join(
            f"<p><b>{_h.escape(n)}</b> — FAN: {_h.escape(f[:200])}<br><b>BOT (a te stílusodban):</b> {_h.escape(r or '')}</p>" for n, f, r in style_preview()))
    else:
        parts.append(f"<p><a href='?pw={DASHBOARD_PASSWORD}&preview=1'>▶ Próbaválaszok generálása (semmit nem küld el)</a></p>")
    return Response("<meta charset='utf-8'><meta name=viewport content='width=device-width,initial-scale=1'>"
                    "<body style='font-family:sans-serif;max-width:900px;margin:20px auto;padding:0 16px'>" + "".join(parts), mimetype='text/html')

@app.route('/control/<action>', methods=['POST', 'GET'])
@require_auth
def control(action):
    """Same switches as the Telegram panel: all:on, all:off, fv:on, fv:off, tg:on, tg:off, status."""
    out = handle_control(action)
    return {"ok": True, "message": out, "fanvue_on": fanvue_enabled(), "telegram_on": tg_bot_enabled()}, 200

@app.route('/fans')
@require_auth
def fans():
    return {"fans": db_query("SELECT chat_id, fan_name, handle, is_paused, total_messages, last_interaction, fan_note, ppv_pending, warmth_points FROM fan_profiles ORDER BY last_interaction DESC") or []}, 200

# ── Telegram re-login (new session for the Telegram bot, no terminal needed) ──
_TG_LOGIN_HTML = """<!doctype html><meta name=viewport content="width=device-width,initial-scale=1">
<title>Telegram login</title><body style="font-family:sans-serif;max-width:420px;margin:30px auto;padding:0 16px">
<h3>Jázmin Telegram újra-bejelentkezés</h3><p style="color:#b00">{msg}</p>
<form method=post>{fields}<button style="padding:10px 18px;margin-top:10px">{btn}</button></form>
<p style="color:#777;font-size:13px">A kódot a Telegram küldi a Jázmin fiókra. Az új munkamenet az adatbázisba kerül, a Telegram bot magától újraindul vele.</p></body>"""

def _tg_run(coro_fn):
    import asyncio
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro_fn())
    finally:
        loop.close()

@app.route('/tg_login', methods=['GET', 'POST'])
@require_auth
def tg_login():
    from telethon import TelegramClient
    from telethon.sessions import StringSession
    from telethon.errors import SessionPasswordNeededError
    api_id = int(os.environ.get('TG_API_ID', '0') or 0); api_hash = os.environ.get('TG_API_HASH', '')
    if not api_id or not api_hash:
        return "TG_API_ID / TG_API_HASH missing on the web service", 500
    step = request.form.get('step', '')
    field = lambda n, t, ph: f'<input name={n} type={t} placeholder="{ph}" style="width:100%;padding:10px;margin:6px 0" required>'
    try:
        if step == 'phone':
            phone = request.form.get('phone', '').strip()
            async def go():
                c = TelegramClient(StringSession(), api_id, api_hash); await c.connect()
                r = await c.send_code_request(phone); tmp = c.session.save(); await c.disconnect()
                return r.phone_code_hash, tmp
            h, tmp = _tg_run(go)
            save_token('tg_login_tmp', json.dumps({"phone": phone, "hash": h, "sess": tmp}))
            return _TG_LOGIN_HTML.format(msg="Kód elküldve a Telegram appba.", btn="Belépés",
                                         fields="<input type=hidden name=step value=code>" + field("code", "text", "Telegram kód"))
        if step in ('code', 'password'):
            st = json.loads(load_token('tg_login_tmp') or '{}')
            code = request.form.get('code', '').strip(); pw = request.form.get('password', '')
            async def go():
                c = TelegramClient(StringSession(st.get('sess', '')), api_id, api_hash); await c.connect()
                try:
                    if step == 'code':
                        await c.sign_in(st['phone'], code, phone_code_hash=st['hash'])
                    else:
                        await c.sign_in(password=pw)
                except SessionPasswordNeededError:
                    tmp = c.session.save(); await c.disconnect(); return None, tmp
                me = await c.get_me(); s_ = c.session.save(); await c.disconnect()
                return s_, getattr(me, 'first_name', '')
            res, extra = _tg_run(go)
            if res is None:
                st['sess'] = extra; save_token('tg_login_tmp', json.dumps(st))
                return _TG_LOGIN_HTML.format(msg="Kétlépcsős jelszó kell.", btn="Belépés",
                                             fields="<input type=hidden name=step value=password>" + field("password", "password", "Telegram jelszó (2FA)"))
            save_token('tg_session', res); save_token('tg_login_tmp', '')
            send_telegram_alert(f"✅ Telegram bejelentkezés kész ({extra}). A Telegram bot pár percen belül újraindul.")
            return _TG_LOGIN_HTML.format(msg=f"✅ Kész, bejelentkezve: {extra}. A Telegram bot magától újraindul.", btn="OK", fields="")
    except Exception as e:
        return _TG_LOGIN_HTML.format(msg=f"Hiba: {e}", btn="Kód kérése",
                                     fields="<input type=hidden name=step value=phone>" + field("phone", "text", "+36…"))
    return _TG_LOGIN_HTML.format(msg="", btn="Kód kérése",
                                 fields="<input type=hidden name=step value=phone>" + field("phone", "text", "Jázmin telefonszáma (+36…)"))

@app.route('/dashboard_data')
@require_auth
def dashboard_data():
    fans_ = db_query("SELECT chat_id, fan_name, handle, is_paused, total_messages, last_interaction, fan_note, ppv_pending, warmth_points FROM fan_profiles ORDER BY last_interaction DESC") or []
    p = db_query("SELECT COUNT(*) c FROM scheduled_replies WHERE status='pending'", fetch_one=True)
    s = db_query("SELECT COUNT(*) c FROM scheduled_replies WHERE status='sent' AND created_at >= date('now')", fetch_one=True)
    return {"safe_mode": get_safe_mode(), "polling_active": polling_active,
            "telegram_on": tg_bot_enabled(), "token_status": load_token('token_status') or 'ok',
            "pending_batches": p['c'] if p else 0, "sent_today": s['c'] if s else 0,
            "fan_count": len(fans_), "fans": fans_}, 200

@app.route('/dashboard')
def dashboard():
    if request.args.get('pw', '') != DASHBOARD_PASSWORD:
        return '''<!DOCTYPE html><html><body style="background:#000;color:#00d4ff;font-family:monospace;display:flex;align-items:center;justify-content:center;height:100vh;flex-direction:column;gap:16px;"><div style="font-size:2em;letter-spacing:4px;">⬡ J.A.R.V.I.S</div><form method="get" style="display:flex;gap:8px;"><input name="pw" type="password" placeholder="access code" style="background:#001a2e;border:1px solid #00d4ff;color:#00d4ff;padding:10px;outline:none;"><button style="background:#00d4ff;color:#000;border:none;padding:10px 20px;cursor:pointer;font-weight:bold;">ENTER</button></form></body></html>''', 401
    html_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'dashboard.html')
    try:
        with open(html_path, 'r', encoding='utf-8') as f: return f.read()
    except FileNotFoundError:
        return "dashboard.html not found", 500

def _reconnect_key_ok(rk):
    v = load_token('reconnect_key') or ''
    if not rk or ':' not in v: return False
    key, exp = v.split(':', 1)
    try: return key == rk and datetime.now(timezone.utc) < datetime.fromisoformat(exp)
    except Exception: return False

@app.route('/connect')
def connect():
    import hashlib
    if not (_reconnect_key_ok(request.args.get('rk', '')) or
            (request.args.get('key') or request.args.get('pw') or '') == DASHBOARD_PASSWORD):
        return ("Ez a link lejárt. Írd a Telegram vezérlő botnak: /reconnect — és kapsz egy újat.", 401)
    state = uuid.uuid4().hex
    verifier = base64.urlsafe_b64encode(os.urandom(40)).decode().rstrip('=')
    challenge = base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).decode().rstrip('=')
    save_token('oauth_state', state); save_token('oauth_verifier', verifier)
    from urllib.parse import urlencode
    url = "https://auth.fanvue.com/oauth2/auth?" + urlencode({
        'client_id': FANVUE_CLIENT_ID, 'redirect_uri': FANVUE_REDIRECT_URI,
        'response_type': 'code', 'scope': FANVUE_SCOPES, 'state': state,
        'code_challenge': challenge, 'code_challenge_method': 'S256'})
    return redirect(url, code=302)

@app.route('/callback')
def callback():
    code = request.args.get('code'); state = request.args.get('state')
    err = request.args.get('error')
    if err:
        return (f"Fanvue returned an error: <b>{err}</b> — {request.args.get('error_description','')}", 400)
    if not code:
        return (f"No code in callback. Full query: {dict(request.args)}", 400)
    granted = request.args.get('scope', '')
    if not state or state != (load_token('oauth_state') or ''):
        return ("Ez a bejelentkezés lejárt vagy már fel lett használva. Kérj új linket: /reconnect", 400)
    try:
        r = requests.post("https://auth.fanvue.com/oauth2/token",
            data={"grant_type": "authorization_code", "code": code, "redirect_uri": FANVUE_REDIRECT_URI,
                  "code_verifier": load_token('oauth_verifier') or ''},
            headers={"Content-Type": "application/x-www-form-urlencoded", "Authorization": get_basic_auth_header()},
            timeout=15)
        if r.status_code != 200:
            return (f"Token exchange failed: {r.status_code} {r.text[:400]}", 400)
        d = r.json()
        rt = d.get('refresh_token')
        if not rt:
            return (f"No refresh_token returned: {r.text[:400]}", 400)
        save_token('refresh_token', rt); save_token('oauth_state', ''); save_token('reconnect_key', '')
        if d.get('access_token'):
            save_token('access_token', d['access_token'])
            save_token('expires_at', (datetime.now() + timedelta(seconds=int(d.get('expires_in', 3600)) - 300)).isoformat())
            save_token('refreshed_at', datetime.now(timezone.utc).isoformat())
            access, msg = d['access_token'], "OK"
        else:
            with _token_refresh_lock:
                access, msg = refresh_fanvue_token(force=True)
        ok = bool(access)
        if ok:
            was_dead = load_token('token_status') == 'dead'
            save_token('token_status', 'ok')
            send_telegram_alert("🟢 Fanvue csatlakoztatva" + (" — a bot újra működik." if was_dead else "."))
        has_vault = ('read:media' in granted and 'read:creator' in granted) or not granted
        return (f"<meta name=viewport content='width=device-width'><div style='font-family:sans-serif;padding:24px'>"
                f"<h2>{'✅ Kész! A Fanvue bot csatlakoztatva.' if ok else '⚠️ Valami nem sikerült: ' + msg}</h2>"
                f"<p>Bezárhatod ezt az oldalt.</p>"
                f"{'' if has_vault else '<p>⚠️ Hiányzó jogosultság (read:creator/read:media) — a PPV nem fog működni.</p>'}</div>",
                200 if ok else 500)
    except Exception as e:
        return (f"Error: {e}", 500)

@app.route('/set_token', methods=['POST'])
@require_auth
def set_token():
    rt = (request.json or {}).get('refresh_token')
    if not rt: return {"error": "no refresh_token"}, 400
    with _token_refresh_lock:
        save_token('refresh_token', rt); save_token('expires_at', '')
        access, msg = refresh_fanvue_token(force=True)
    return {"saved": True, "test": msg}, 200

@app.route('/webhook', methods=['POST'])
def fanvue_webhook():
    _secret = os.environ.get('FANVUE_WEBHOOK_SECRET', '')
    if _secret:
        import hmac, hashlib
        _raw = request.get_data() or b""
        _sig = (request.headers.get('X-Fanvue-Signature') or request.headers.get('Fanvue-Signature')
                or request.headers.get('X-Signature') or '').strip()
        if _sig:
            _expected = hmac.new(_secret.encode(), _raw, hashlib.sha256).hexdigest()
            if not hmac.compare_digest(_sig.split('=')[-1].lower(), _expected.lower()):
                return {"ok": False, "error": "bad signature"}, 401
    d = request.get_json(force=True, silent=True) or {}
    sender = d.get('sender') or {}
    buyer = sender.get('uuid'); price = d.get('price'); ts = d.get('timestamp')
    name = sender.get('displayName') or sender.get('handle') or buyer or 'valaki'
    amt = f"${(price or 0)/100:.0f}" if price else ""
    if not buyer:
        return {"ok": True}, 200
    p = db_query("SELECT fan_name, auto_ppv_sent_at, auto_ppv_bought FROM fan_profiles WHERE chat_id=?", (buyer,), fetch_one=True)
    if p and p.get('auto_ppv_sent_at') and not p.get('auto_ppv_bought'):
        secs = None
        try:
            sent = datetime.fromisoformat(p['auto_ppv_sent_at'])
            bought = datetime.fromisoformat((ts or '').replace('Z', '+00:00'))
            secs = int((bought - sent).total_seconds())
        except Exception:
            pass
        db_query("UPDATE fan_profiles SET auto_ppv_bought=1 WHERE chat_id=?", (buyer,))
        add_warmth(buyer, 10)  # PPV purchase = big warmth boost
        speed = '🔥🔥 IMPULSE BUYER' if (secs is not None and secs < 120) else 'normal'
        tstr = f" — {secs}s after I sent it" if secs is not None else ""
        send_telegram_alert(f"💰 BUY! {p.get('fan_name') or name} bought the {amt} bundle{tstr}. ({speed})")
    else:
        add_warmth(buyer, 10)  # any purchase = warmth boost
        send_telegram_alert(f"💰 Purchase: {name} spent {amt}.")
    return {"ok": True}, 200

# ─────────────────────────────────────────────────────────────────────────────
# BOOT
# ─────────────────────────────────────────────────────────────────────────────
try:
    init_db(); _ensure_tracking_table(); print("[OK] DB ready")
except Exception as e:
    print(f"[ERR] init_db {e}")

# First launch of v13 → start with the Fanvue bot SWITCHED OFF (reads, doesn't reply). You switch it on
# from the dashboard or the Telegram control panel (/be). After that, restarts keep whatever you chose.
try:
    if load_token('v141_ai_reset') != 'yes':     # ai_confirmed set by unsolicited mentions (never asked) → clear once
        db_query("UPDATE fan_profiles SET ai_confirmed=0 WHERE COALESCE(ai_strikes,0)=0 AND COALESCE(ai_confirmed,0)=1")
        save_token('v141_ai_reset', 'yes')
    if load_token('v13_first_boot_done') != 'yes':
        set_safe_mode(True); save_token('v13_first_boot_done', 'yes')
        print("[boot] v13 first launch → Fanvue bot starts OFF (switch on from the control panel)")
    try:   # ONLY_FAN=off → Fanvue bot fully OFF (safe mode, keep-warm off); 'off' also matches no chat (fail-closed)
        if only_fan() == 'off':
            set_away(False); set_fanvue(False); save_token('lab_started', '')
            db_query("UPDATE scheduled_replies SET status='cancelled' WHERE status IN ('pending','sending')")
            print("[boot] ONLY_FAN=off → Fanvue bot OFF", flush=True)
    except Exception as e: print(f"[boot off] {e}")
    try:   # v15.6: allowlist seed (once per seed value) + switch on once
        seed = (os.environ.get('FV_ALLOW_SEED') or '').strip()
        if only_fan() == 'allowlist' and seed and load_token('fv_allow_seed') != seed:
            set_allowed(allowed_set() | {norm_handle(x) for x in seed.split(',')}); save_token('fv_allow_seed', seed)
            cancel_stale_pending(0); set_away(True); set_fanvue(True)
            print(f"[boot] allowlist seeded: {sorted(allowed_set())} → keep-warm ON for them only", flush=True)
    except Exception as e: print(f"[boot allow] {e}")
    try:   # v15.3: one-time switch-on for single-fan lab mode (only when ONLY_FAN + LAB_START are set; /back still turns it off)
        if only_fan() and only_fan() not in ('off', 'allowlist') and os.environ.get('LAB_START') == '1' and load_token('lab_started') != only_fan():
            cancel_stale_pending(0); set_away(True); set_fanvue(True); save_token('lab_started', only_fan())
            print(f"[boot] LAB mode ON for @{only_fan()} only", flush=True)
    except Exception as e: print(f"[boot lab] {e}")
    print(f"[boot] Fanvue bot is {'ON' if not get_safe_mode() else 'OFF'} · Telegram bot is {'ON' if (load_token('tg_enabled') or 'on') != 'off' else 'OFF'}")
except Exception as e:
    print(f"[ERR] boot state {e}")

try:
    env_rt = os.environ.get('FANVUE_REFRESH_TOKEN', '').strip()
    if env_rt and not load_token('refresh_token'):
        save_token('refresh_token', env_rt); refresh_fanvue_token(); print("[OK] refresh token bootstrapped from env")
except Exception as e:
    print(f"[ERR] token boot {e}")

try:
    start_polling(); print("[OK] polling + reengagement started")
except Exception as e:
    print(f"[ERR] start_polling {e}")

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))
