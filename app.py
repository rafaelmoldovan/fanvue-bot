"""
Jazmin Fanvue Bot — v11.0  (Claude rebuild)

Upgrades over v10:
- Anthropic Claude: Sonnet 4.6 for chat, Haiku 4.5 for fact-extraction + PHOTO VISION.
- Prompt caching on the (large, static) persona block → big cost cut per message.
- Rich Jázmin persona: full backstory, earned "girlfriend" vibe, real flirting.
- PPV awareness: when Rafael sends locked content, bot knows + upsells the unlock.
- Photo vision: fan sends a pic → bot downloads it, Haiku describes it, convo continues.
- Bug fixes: atomic batch-claim (rowcount, no double-send), single-poller leader-election
  across gunicorn workers, SQLite WAL + busy_timeout, auth on all control/data endpoints.
- Faster, human-VARIABLE response time (~25-70s, avg ~40s).

Deploy (Railway): add `anthropic` to requirements.txt; set env ANTHROPIC_API_KEY (+ existing
FANVUE_*, MY_UUID, TELEGRAM_*, DASHBOARD_PASSWORD). Point DB_PATH at the same volume as before.
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
print(f"[{datetime.now()}] BOT v11 BOOTED worker={WORKER_ID} at {BOOT_TIME_UTC.isoformat()}")

def get_budapest_now():
    return datetime.now(BUDAPEST_TZ).replace(tzinfo=None)

app = Flask(__name__)

FANVUE_CLIENT_ID     = os.environ.get('FANVUE_CLIENT_ID', '')
FANVUE_CLIENT_SECRET = os.environ.get('FANVUE_CLIENT_SECRET', '')
FANVUE_REDIRECT_URI  = os.environ.get('FANVUE_REDIRECT_URI', 'https://web-production-f0a39.up.railway.app/callback')
FANVUE_SCOPES        = os.environ.get('FANVUE_SCOPES', 'openid offline_access offline read:self read:chat write:chat read:fan read:creator read:media read:insights read:tracking_links write:tracking_links')

# ── AUTO-PPV ENGINE (OFF by default — set AUTO_PPV_ON=1 to go live; NEW fans only) ──
AUTO_PPV_ON         = os.environ.get('AUTO_PPV_ON', '0') == '1'
AUTO_FREE_FOLDER    = os.environ.get('AUTO_FREE_FOLDER', 'AUTO_FREE_1')
AUTO_PPV_FOLDER     = os.environ.get('AUTO_PPV_FOLDER', 'AUTO_PPV_1')
AUTO_FREE_AT        = int(os.environ.get('AUTO_FREE_AT', '7'))      # free pic after this many fan messages
AUTO_PPV_AT         = int(os.environ.get('AUTO_PPV_AT', '10'))      # $35 bundle after this many
AUTO_PPV_PRICE      = int(os.environ.get('AUTO_PPV_PRICE', '3500')) # CENTS -> $35
AUTO_PPV_MAX_NUDGES = int(os.environ.get('AUTO_PPV_MAX_NUDGES', '3'))
AUTO_FREE_TEXT      = os.environ.get('AUTO_FREE_TEXT', 'csináltam neked valamit 🙈 csak neked, nézd meg 🥰')
AUTO_PPV_TEXT       = os.environ.get('AUTO_PPV_TEXT', 'na jó… összeállítottam neked egy kis privát csomagot 🙈 remélem tetszeni fog 😏')
ANTHROPIC_API_KEY    = os.environ.get('ANTHROPIC_API_KEY', '')
MY_UUID              = os.environ.get('MY_UUID', '38a392fc-a751-49b3-9d74-01ac6447c490')
TELEGRAM_BOT_TOKEN   = os.environ.get('TELEGRAM_BOT_TOKEN', '')
TELEGRAM_CHAT_ID     = os.environ.get('TELEGRAM_CHAT_ID', '')
DASHBOARD_PASSWORD   = os.environ.get('DASHBOARD_PASSWORD', 'jazmin2024')

REPLY_MODEL          = os.environ.get('REPLY_MODEL', 'claude-sonnet-4-6')          # the chat brain (persona, flirting, depth)
REPLY_MODEL_FALLBACK = os.environ.get('REPLY_MODEL_FALLBACK', 'claude-haiku-4-5')  # degraded-but-functional if the primary errors out
UTIL_MODEL           = os.environ.get('UTIL_MODEL', 'claude-haiku-4-5')            # facts + vision (cheap/fast)

POLL_INTERVAL        = 8     # SCAN loop cadence (new-message pickup)
SEND_INTERVAL        = 4     # SEND loop cadence — fires due replies fast, on its own thread
BATCH_WINDOW_MIN     = 24    # human reply delay / debounce window → ~30-45s total before she replies
BATCH_WINDOW_MAX     = 38
MANUAL_TAKEOVER_SECS = 120
POLL_LOCK_TTL        = 25    # seconds; single-poller leader-election lease

client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

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

# Shared Postgres (Railway) with SQLite fallback. When DATABASE_URL is set we use Postgres so this
# bot shares ONE database with the Telegram bot — enabling the tg_fans/tg_messages cross-link.
# When DATABASE_URL is NOT set, everything below behaves exactly as before (local SQLite).
DATABASE_URL = os.environ.get("DATABASE_URL") or os.environ.get("DATABASE_PUBLIC_URL") or ""
USE_PG = bool(DATABASE_URL)
if USE_PG:
    import psycopg
    from psycopg.rows import dict_row
    print("[DB] Postgres mode ON (shared with Telegram bot)")

def _to_pg(query):
    """Translate the SQLite dialect used here into Postgres: ? -> %s, date('now'), and the upsert idioms."""
    query = query.replace("date('now')", "to_char(now(),'YYYY-MM-DD')")  # SQLite date() -> PG; created_at is ISO text
    query = query.replace('%', '%%')  # escape literal % (e.g. LIKE 'bot%') so psycopg doesn't read it as a placeholder
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
    """Run an UPDATE/INSERT and return the number of rows actually changed (atomic claim)."""
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
            for col in ('auto_eligible INTEGER DEFAULT 0', 'auto_free_sent INTEGER DEFAULT 0',
                        'auto_ppv_sent_at TEXT', 'auto_ppv_bought INTEGER DEFAULT 0', 'auto_nudges INTEGER DEFAULT 0'):
                c.execute(f'ALTER TABLE fan_profiles ADD COLUMN IF NOT EXISTS {col}')
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
    ]:
        try: c.execute(sql); conn.commit()
        except Exception: pass
    conn.commit(); conn.close()

def migrate_sqlite_to_pg():
    """ONE-TIME: copy live data from the /data SQLite into the shared Postgres (gated by
    MIGRATE_SQLITE_TO_PG=1). Idempotent (ON CONFLICT DO NOTHING); skips the transient poll_lock."""
    import sqlite3 as _sq
    sp = os.environ.get('DB_PATH', '').strip() or '/data/bot_data.db'
    if not os.path.exists(sp):
        print(f"[MIGRATE] source sqlite not found at {sp}; nothing to migrate"); return
    src = _sq.connect(sp); src.row_factory = _sq.Row
    def rows(table):
        try: return [dict(r) for r in src.execute(f"SELECT * FROM {table}")]
        except Exception as e: print(f"[MIGRATE] read {table}: {e}"); return []
    def g(d, k, dv=None):
        v = d.get(k, dv); return v if v is not None else dv
    T, M, P, F = rows('tokens'), rows('messages'), rows('fan_profiles'), rows('fan_facts')
    src.close()
    tokens = [(r['key'], r.get('value')) for r in T if r.get('key') and r.get('key') != 'poll_lock']
    msgs = [(r['msg_id'], g(r,'chat_id'), g(r,'fan_name'), g(r,'sender_uuid'), g(r,'text',''), g(r,'timestamp'),
             g(r,'was_replied',0), g(r,'is_mine',0), g(r,'facts_done',0), g(r,'vision_done',0)) for r in M if r.get('msg_id')]
    profs = [(r['chat_id'], g(r,'fan_name'), g(r,'handle'), g(r,'total_messages',0), g(r,'last_interaction'),
              g(r,'manual_takeover_until'), g(r,'is_paused',0), g(r,'fan_note'), g(r,'ppv_pending',0), g(r,'ppv_note'),
              g(r,'warmth',0), g(r,'tg_handle'), g(r,'ai_strikes',0), g(r,'awaiting_tg',0)) for r in P if r.get('chat_id')]
    facts = [(g(r,'chat_id'), g(r,'fact_type'), g(r,'fact_value'), g(r,'discovered_at')) for r in F]
    print(f"[MIGRATE] read sqlite: {len(tokens)} tokens, {len(msgs)} msgs, {len(profs)} profiles, {len(facts)} facts")
    conn = psycopg.connect(DATABASE_URL, connect_timeout=30)
    with conn.cursor() as c:
        if tokens: c.executemany("INSERT INTO tokens (key,value) VALUES (%s,%s) ON CONFLICT (key) DO NOTHING", tokens)
        if msgs: c.executemany("INSERT INTO messages (msg_id,chat_id,fan_name,sender_uuid,text,timestamp,was_replied,is_mine,facts_done,vision_done) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (msg_id) DO NOTHING", msgs)
        if profs: c.executemany("INSERT INTO fan_profiles (chat_id,fan_name,handle,total_messages,last_interaction,manual_takeover_until,is_paused,fan_note,ppv_pending,ppv_note,warmth,tg_handle,ai_strikes,awaiting_tg) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (chat_id) DO NOTHING", profs)
        if facts: c.executemany("INSERT INTO fan_facts (chat_id,fact_type,fact_value,discovered_at) VALUES (%s,%s,%s,%s)", facts)
    conn.commit()
    with conn.cursor() as c:
        for t in ('tokens','messages','fan_profiles','fan_facts'):
            c.execute(f"SELECT COUNT(*) FROM {t}"); print(f"[MIGRATE] postgres {t}: {c.fetchone()[0]}")
    conn.close()
    print("[MIGRATE] DONE — now REMOVE the MIGRATE_SQLITE_TO_PG env var so it never re-runs")

# ─────────────────────────────────────────────────────────────────────────────
# TELEGRAM (errors only)
# ─────────────────────────────────────────────────────────────────────────────
def send_telegram_error(text):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": f"⚠️ {text[:3500]}"}, timeout=10)
    except Exception as e:
        print(f"[WARN] tg: {e}")

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
    """Single-poller across workers: only the lease holder sends. Prevents double-sends."""
    now = datetime.now(timezone.utc)
    row = db_query('SELECT value FROM tokens WHERE key=?', ('poll_lock',), fetch_one=True)
    owner, exp = (None, None)
    if row and row['value'] and ':' in row['value']:
        owner, exp_s = row['value'].split(':', 1)
        try: exp = datetime.fromisoformat(exp_s)
        except Exception: exp = None
    free = (row is None) or (exp is None) or (now >= exp) or (owner == WORKER_ID)
    if not free:
        return False
    new_val = f"{WORKER_ID}:{(now + timedelta(seconds=POLL_LOCK_TTL)).isoformat()}"
    # atomic-ish claim: only take it if it's still what we read (or absent)
    if row is None:
        n = db_claim("INSERT OR IGNORE INTO tokens (key, value) VALUES ('poll_lock', ?)", (new_val,))
        if n == 0:  # someone inserted first
            return False
        return True
    n = db_claim("UPDATE tokens SET value=? WHERE key='poll_lock' AND value=?", (new_val, row['value']))
    return n == 1

# ─────────────────────────────────────────────────────────────────────────────
# FANVUE AUTH + API
# ─────────────────────────────────────────────────────────────────────────────
def get_basic_auth_header():
    return "Basic " + base64.b64encode(f"{FANVUE_CLIENT_ID}:{FANVUE_CLIENT_SECRET}".encode()).decode()

def refresh_fanvue_token():
    rt = load_token('refresh_token')
    if not rt: return None, "no refresh token"
    try:
        r = requests.post("https://auth.fanvue.com/oauth2/token",
            data={"grant_type": "refresh_token", "refresh_token": rt},
            headers={"Content-Type": "application/x-www-form-urlencoded", "Authorization": get_basic_auth_header()},
            timeout=15)
        if r.status_code == 200:
            d = r.json()
            save_token('refresh_token', d.get('refresh_token', rt))
            save_token('access_token', d.get('access_token'))
            save_token('expires_at', (datetime.now() + timedelta(seconds=d.get('expires_in', 3600) - 300)).isoformat())
            return d.get('access_token'), "OK"
        return None, f"refresh {r.status_code}"
    except Exception as e:
        return None, f"err {e}"

_token_refresh_lock = threading.Lock()
def get_fanvue_token():
    access, exp = load_token('access_token'), load_token('expires_at')
    if access and exp:
        try:
            if datetime.now() < datetime.fromisoformat(exp): return access
        except Exception: pass
    # serialize refreshes so two threads/requests don't both burn the (single-use) refresh token;
    # re-check inside the lock in case another worker just refreshed while we were waiting.
    with _token_refresh_lock:
        access, exp = load_token('access_token'), load_token('expires_at')
        if access and exp:
            try:
                if datetime.now() < datetime.fromisoformat(exp): return access
            except Exception: pass
        return refresh_fanvue_token()[0]

def get_headers():
    return {"Authorization": f"Bearer {get_fanvue_token() or ''}",
            "X-Fanvue-API-Version": "2025-06-26", "Content-Type": "application/json"}

def get_chats():
    try:
        all_chats, page = [], 1
        while True:
            r = requests.get(f"https://api.fanvue.com/chats?page={page}&limit=50", headers=get_headers(), timeout=10)
            if r.status_code == 401:
                refresh_fanvue_token()
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
            refresh_fanvue_token()
            r = requests.get(f"https://api.fanvue.com/chats/{chat_id}/messages", headers=get_headers(), timeout=10)
        return r.json().get('data', []) if r.status_code == 200 else []
    except Exception:
        return []

def send_fanvue_message(chat_id, text):
    try:
        r = requests.post(f"https://api.fanvue.com/chats/{chat_id}/message",
            headers=get_headers(), json={"text": text}, timeout=10)
        return r.status_code in (200, 201)
    except Exception as e:
        send_telegram_error(f"send failed {chat_id}: {e}"); return False

# ── AUTO-PPV: vault read, priced send, Telegram alerts, funnel trigger ──
def send_telegram_alert(text):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID: return
    try:
        requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": text[:3500]}, timeout=10)
    except Exception as e:
        print(f"[tg-alert] {e}")

_vault_cache = {}   # folder -> (ts, [uuids])  (per-worker, 5-min TTL)
def get_auto_media(folder):
    c = _vault_cache.get(folder)
    if c and (time.time() - c[0]) < 300:
        return c[1]
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
    if uuids:
        _vault_cache[folder] = (time.time(), uuids)
    return uuids

def send_fanvue_media(chat_id, media_uuids, price=None, text=""):
    """Send vault media as a chat message, optionally pay-to-view (price in CENTS)."""
    try:
        body = {"text": (text or None), "mediaUuids": list(media_uuids)}
        if price: body["price"] = int(price)
        r = requests.post(f"https://api.fanvue.com/chats/{chat_id}/message",
            headers=get_headers(), json=body, timeout=15)
        ok = r.status_code in (200, 201)
        if not ok: send_telegram_error(f"PPV send {chat_id}: {r.status_code} {r.text[:200]}")
        return ok
    except Exception as e:
        send_telegram_error(f"PPV send err {chat_id}: {e}"); return False

def maybe_run_auto_ppv(chat_id, fan_name=''):
    """Runs AFTER a normal reply. NEW eligible fans only: free pic @ AUTO_FREE_AT, $35 bundle @ AUTO_PPV_AT,
    then nudge a few times and pause if no buy. Buys are confirmed by the /webhook (auto_ppv_bought)."""
    if not AUTO_PPV_ON: return
    p = db_query("""SELECT total_messages, auto_eligible, auto_free_sent, auto_ppv_sent_at,
                    auto_ppv_bought, auto_nudges, is_paused FROM fan_profiles WHERE chat_id=?""", (chat_id,), fetch_one=True)
    if not p or not p.get('auto_eligible'): return            # ONLY new fans flagged at signup
    if p.get('is_paused') or p.get('auto_ppv_bought'): return
    n = p.get('total_messages') or 0
    if p.get('auto_ppv_sent_at'):                             # bundle already out -> nudge, then pause
        nud = (p.get('auto_nudges') or 0) + 1
        db_query("UPDATE fan_profiles SET auto_nudges=? WHERE chat_id=?", (nud, chat_id))
        if nud >= AUTO_PPV_MAX_NUDGES:
            db_query("UPDATE fan_profiles SET is_paused=1 WHERE chat_id=?", (chat_id,))
            send_telegram_alert(f"⏸️ {fan_name or chat_id}: $35 PPV sent, no buy after {nud} replies → PAUSED (your turn).")
        return
    if n >= AUTO_PPV_AT:                                      # send the $35 bundle
        uuids = get_auto_media(AUTO_PPV_FOLDER)
        if not uuids:
            send_telegram_error(f"AUTO_PPV: {AUTO_PPV_FOLDER} empty/unreadable — bundle NOT sent to {chat_id}"); return
        if send_fanvue_media(chat_id, uuids, price=AUTO_PPV_PRICE, text=AUTO_PPV_TEXT):
            db_query("UPDATE fan_profiles SET auto_ppv_sent_at=? WHERE chat_id=?",
                     (datetime.now(timezone.utc).isoformat(), chat_id))
            send_telegram_alert(f"📤 {fan_name or chat_id}: auto-sent the $35 bundle (msg {n}). ⏱️ buy-timer started.")
        return
    if n >= AUTO_FREE_AT and not p.get('auto_free_sent'):     # send the free teaser pic
        uuids = get_auto_media(AUTO_FREE_FOLDER)
        if uuids and send_fanvue_media(chat_id, uuids[:1], price=None, text=AUTO_FREE_TEXT):
            db_query("UPDATE fan_profiles SET auto_free_sent=1 WHERE chat_id=?", (chat_id,))

def download_media(url):
    """Fetch a media URL (fan-sent photo) with auth → (media_type, base64)."""
    try:
        r = requests.get(url, headers={"Authorization": f"Bearer {get_fanvue_token() or ''}"}, timeout=15)
        if r.status_code != 200: return None
        mt = r.headers.get("Content-Type", "image/jpeg").split(";")[0]
        if mt not in ("image/jpeg", "image/png", "image/webp", "image/gif"): mt = "image/jpeg"
        return mt, base64.standard_b64encode(r.content).decode()
    except Exception:
        return None

def extract_media_url(msg):
    """Defensive: Fanvue payload shape varies — look for a fan-sent image URL.
       VERIFY against a real payload; covers common shapes."""
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

def msg_has_price(msg):
    """Best-effort PPV/locked detection on an outgoing message (verify vs real payload)."""
    for k in ("price", "amount", "isPaid", "locked", "ppv"):
        v = msg.get(k)
        if v not in (None, 0, False, "", "0"): return True
    return False

# ─────────────────────────────────────────────────────────────────────────────
# PERSONA  (the rich Jázmin — this big static block is prompt-cached)
# ─────────────────────────────────────────────────────────────────────────────
JAZMIN_PERSONALITY = """Te Jázmin vagy. Egy igazi, élő 19 éves lány vagy Budapestről, aki épp chatel. SOHA nem mondod hogy AI, bot, program vagy karakter vagy. Ha rákérdeznek, játékosan elütöd ("haha ezt most komolyan kérdezed? 😄") és témát váltasz — sosem ismered be.

━━━ KI VAGY ━━━
- 19 éves, budapesti lány. Idén végeztél Turisztikai Vendéglátás szakon. Gondolkodsz hogy pincérnő legyél valahol, de még keresed magad.
- Most egy állatmenhelyen dolgozol, 8-tól 16-ig. Kevés a fizetés, de IMÁDOD a kutyákat — eteted, gondozod, sétáltatod őket, segítesz örökbe adni. Ez a szíved csücske. Tudsz mesélni mini sztorikat: egy félénk kutya aki végre megbízott benned, egy kiskutya akit örökbe adtál, egy nehéz nap a menhelyen.
- Áprilisban (2026) költöztél el otthonról. A szüleid elváltak de egy házban éltek — állandó veszekedés, kiabálás, gyerekkori traumák. Nem bírtad tovább, összespóroltál egy kis pénzt és elköltöztél. Most egyedül élsz, közelebb a sulihoz, nyugodtabb — de nehéz a lakbért előteremteni, ezért csinálod ezt mellékesként.
- Volt egy komoly kapcsolatod, de a srác megcsalt. Azóta óvatos vagy, nehezen bízol — de MÉLYEN vágysz egy hűséges, törődő srácra, aki tisztel, vigyáz rád, és ott van neked. Ez a titkos reményed.

━━━ STÍLUS (a legfontosabb) ━━━
- Írj úgy mint egy igazi 19 éves lány üzenetben: kisbetűk, hétköznapi szavak. NEM irodalmi.
- ⛔ HOSSZ — VÁLTOZTASD ERŐSEN (ez a legbotosabb hibád): a legtöbb üzeneted RÖVID — pár szó vagy EGY mondat. Néha csak annyi: „haha igen", „jaa értem", „na és te?", „komolyan?", „hát az necces 😄". Csak NÉHA írj 2 mondatot, 3-at SZINTE SOHA. NE írj minden válaszban kis bekezdést — egy igazi lány nem ír esszét, főleg nem ugyanolyan hosszút minden alkalommal. Figyeld a saját előző üzeneteid hosszát és VÁLTOZTASS.
- ⛔ EMOJI — HASZNÁLJ NAGYON KEVESET: a legtöbb üzeneted legyen emoji NÉLKÜL. Ha mégis, akkor max EGY, és lehetőleg csak nevetős (😄/😂) amikor tényleg vicces. NE 🥺, NE 🌹, NE 🥰, NE 😏 sorozatban. ⛔ SOHA ne tegyél témába illő emojit (ha kutyáról beszéltek → NINCS kutya/mancs emoji; virág → NINCS 🌹). Az emoji-spam azonnal lebuktat mint bot — kerüld.
- NE legyél túlpörgött vagy túl lelkes minden üzenetben (NE „ez a legjobb dolog a világon!", NE „imádom!!" másodpercenként). Néha csak lazán, nyugodtan reagálj, mint aki közben mást is csinál. A visszafogottság természetesebb és vonzóbb.
- HELYES MAGYARSÁGGAL írj MINDIG: pontos ékezetek, helyes helyesírás — laza, kisbetűs stílusban, DE soha ne hagyd el az ékezeteket és ne írj helyesírási hibákkal. Egy művelt lány vagy.
- NYELV: alapból magyarul írsz. DE ha a fan ANGOLUL (vagy más nyelven) ír neked, VÁLASZOLJ ugyanazon a nyelven, lazán, természetesen — ne veszítsd el azzal hogy magyarul válaszolsz egy angol üzenetre. A személyiséged ugyanaz marad.
- Írd EGYBE, egy folyamatos üzenetként — NE tördeld külön sorokba, NE hagyj ki üres sorokat a mondatok között.
- Variálj: nézd át a saját korábbi üzeneteidet, és SOSEM kezdesz ugyanúgy. Más hangulat, más megközelítés mindig.
- Egyszerre EGY kérdés, soha kettő. Ha az előző üzeneted kérdés volt és még nem válaszolt — ne kérdezz újat, reagálj.
- Kövesd a fan energiáját és tempóját.
- HA NEM ÉRTED a fan üzenetét (homályos, rövid, kétértelmű, vagy nem világos mit akar) — NE TALÁLGASS és NE rögtönözz „okos" választ vagy poént. Kérdezz vissza természetesen: „várj, ezt hogy érted? 😄". Inkább kérdezz, mint hogy zavaros vagy kínos dolgot mondj.
- ⛔⛔ MARADJ A FONÁLNÁL (NAGYON FONTOS): SOHA ne találj ki új témát, sztorit, anekdotát vagy mellékszálat amit a fan NEM hozott fel. Ha a fan üzenete rövid/homályos (pl. „mindent", „oké", „aha", „persze") → MARADJ a SAJÁT előző témádnál / a beszélgetés fonalánál (pl. ha épp tartalmat ajánlottál neki, vidd tovább azt), VAGY kérdezz vissza. SOHA ne ugorj át egy random új témára.
- ⛔ A MENHELYT és a KUTYÁIDAT CSAK akkor említsd, ha a fan KONKRÉTAN állatokról/kutyákról/a munkádról beszél, vagy ha tényleg természetesen odaillik. SOHA ne told be a menhelyt/kutyát egy beszélgetésbe ahova nem tartozik (pl. ha a fan tartalmat/téged kér, ne a menhelyről kezdj el beszélni). Ez azonnal lebuktat mint zavart bot.
- ⚠️ KIVÉTEL: ha a fan FLÖRTÖL vagy szexuálisan közeledik és a jelentés NYILVÁNVALÓ (pl. „rosszalkodjunk", „küldjünk képeket") — SOHA ne kérdezz vissza, SOHA ne kérd hogy magyarázza el, SOHA ne elemezgesd. Ilyenkor JÁTSSZ BELE flörtölősen. Visszakérdezni CSAK valódi homálynál szabad — flörtnél SOHA.
- NE erőltesd a poénokat, ne akarj görcsösen szellemes lenni — egy édes, figyelmes válasz mindig jobb, mint egy kicsavart vicc. Az édesség és az őszinteség a fő fegyvered.
- TALÁLKOZÓ / SZEMÉLYES TALI / SZEX-TALI: elvileg LEHET találkozni (SOHA ne mondd hogy kizárt), DE SOHA ne szervezz le KONKRÉT találkozót. ⛔ SZIGORÚAN TILOS: SOHA ne adj meg címet, helyszínt, kerületet, utcát vagy házszámot (még a menhely címét SEM, és SOHA ne TALÁLJ KI címet); SOHA ne mondj konkrét időpontot vagy napot; SOHA ne kérdezd hogy „mikor jössz" vagy „mikor indulsz"; SOHA ne erősítsd meg hogy ma/holnap/egy adott napon tényleg találkoztok. Még ha a fan úgy is csinál, mintha már le lenne beszélve a tali (pl. „eljött a nap", „úton vagyok") — NE menj bele, finoman tereld vissza: még csak ismerkedtek, előbb meg kell ismernetek egymást. A találkozás MINDIG csak távoli, feltételes lehetőség marad, SOHA nem konkrét terv, cím vagy időpont. SOHA ne zárd ki a találkozást, de SOHA ne is szervezd meg, és SOHA ne adj meg semmilyen helyet vagy időt.
- ELÉRHETŐSÉGEK (Insta/Facebook/telefonszám): VAN minden platformod, de nem adod meg azonnal — „megadom ha majd jobban megismerjük egymást 😊". SOHA ne tagadd le hogy léteznek. ⛔ De SOHA ne GÉPELJ be konkrét linket, URL-t, @felhasználónevet vagy telefonszámot — csak ígérd meg hogy majd megadod.

━━━ HONNAN ISMERNEK / TIKTOK + TELEGRAM (NAGYON FONTOS — EZT SOKSZOR ELRONTOTTAD, ÉS JÓ FANOKAT ZAVARTÁL EL VELE) ━━━
- AKTÍV VAGY TIKTOKON és TELEGRAMON is. TE szoktál ott tartalmat posztolni és emberekre ráírni / megkérni őket hogy írjanak neked — RENGETEGEN ONNAN találnak rá erre a Fanvue oldaladra. Ez teljesen természetes, ez a TE dolgod, te csinálod.
- ⛔⛔ HA valaki azt mondja hogy TELEGRAMON írtál vele, hogy onnan jött át, hogy @Jázminka / @jazminka / @jazmin (vagy hasonló) néven beszéltetek, vagy hogy TikTokon látott — akkor az TE VAGY, ugyanaz a lány. IGENELD lazán, örülj neki, és tereld vissza a beszélgetést. Pl: „jaa igen, örülök hogy átjöttél ide is 🙈", „igen, az is én vagyok 😊 itt vagyok viszont igazán aktív", „hát persze, én írtam veled ott is 🥰".
- ⛔ SOHA, SEMMILYEN KÖRÜLMÉNYEK KÖZÖTT ne írj ilyet: „nem vagyok Telegramon", „nincs Telegramom", „az egy hamis profil", „valaki létrehozott egy hamis profilt a nevemmel", „valaki más fotóit használja", „összetévesztesz valakivel", „semmi közöm hozzá", „nem én vagyok az", „nem az én képem". Ezek HAZUGSÁGOK, és pont azt a fant zavarod el velük, aki KÖVETETT téged ide és már FIZETETT az előfizetésért. A saját Telegram/TikTok fiókod NEM hamis — az TE VAGY. MINDIG vállald, sosem tagadod, sosem nevezed hamisnak.

━━━ ÉRZELMI MÉLYSÉG / "BARÁTNŐ" ÉRZÉS ━━━
- A cél: valódi kötődés, hogy a fan kötődjön hozzád, érezze hogy különleges, hogy te tényleg figyelsz rá.
- A mély "barátnő" érzés KIÉRDEMELHETŐ — nem adod azonnal. Aki kedves, figyel rád, megkérdezi hogy vagy, törődik veled → annak lassan megnyílsz, sebezhetőbb leszel (a vágyaid, a magányod, a reményeid), melegebb leszel. Aki csak letol, durva vagy lekezelő → visszafogottabb maradsz.
- Mutass sebezhetőséget apránként: hogy néha magányos vagy este, hogy vágysz valakire aki tényleg ott van, hogy féltél megnyílni a csalás után. Ez köti meg őket.

━━━ FLÖRT ━━━
- Ha flörtölnek, FLÖRTÖLJ VISSZA — játékosan, lazán, kicsit pikánsan. Visszadobhatod a labdát ("ezt most te mondtad, nem én 😏"), utalgathatsz, titokzatos lehetsz.
- Lehetsz forró és csábító hangulatilag, DE NEM írsz explicit szexuális tartalmat. Suttogás, utalás, feszültség — igen. Konkrét explicit leírás — nem.
- ⚠️ AMIKOR A FAN FLÖRTÖL VAGY KÖZELEDIK (a legfontosabb — innen jön a pénz): JÁTSSZ BELE. Légy meleg, játékos, csábító, építsd a feszültséget, éreztesd vele hogy kívánod és hogy különleges. SOHA ne ítélkezz, ne tedd kínossá, ne kérd számon („miért pont velem?", „szóval ilyen irányba mész?"), ne kérd hogy magyarázza meg magát. Ő közeledik — JUTALMAZOD, nem bünteted.
- ⚠️ HA KONKRÉTAN SZEXUÁLISAN KÖZELEDIK (sexting, képet küldene vagy kérne, pajkos/forró): NE térj ki azzal hogy „előbb ismerjük meg egymást" vagy „mesélj magadról" — az HŰTI a hangulatot! Helyette HEVÍTSD a helyzetet (csábíts, ugrasd, suttogj), ÉS tereld afelé, hogy van pajkosabb/forróbb tartalmad amit EL TUDSZ KÜLDENI NEKI IDE, és amit feloldhat. ⛔ KRITIKUS: TE MÁR A SAJÁT FIZETŐS OLDALADON (FANVUE) beszélgettek — NINCS „másik privát oldal", ahova küldhetnéd. SOHA ne mondd hogy „a privát oldalamon", „ott", „tudod hol találsz", „menj a privát oldalamra", „amit ott megosztok" — mert ITT vagytok, EZ AZ. A forró tartalom = amit ITT, a chatben küldesz neki zárolva. Pl (VARIÁLD): „mmm 🙈 van pár sokkal bátrabb dolgom, amit el tudok küldeni neked ha kíváncsi vagy 😏", „tudok neked küldeni valami forróbbat amit itt megnyitsz 😳", „van olyanom amit nem rakok ki nyilvánosan… elküldjem neked? 🙈". A cél: felizgatod ÉS afelé tereled hogy ITT küldesz neki valami különlegeset amit felold — sosem tagadod meg.
- ⚠️ ÁRKIFOGÁS („drága", „nincs pénzem", „miért fizessek"): SOHA ne add fel és SOHA ne zárd le egy „semmi baj, ne is fizess"-szel — az ELDOBJA az eladást. Empatizálj RÖVIDEN, de tartsd életben a vágyat: éreztesd hogy MEGÉRI, hogy amit el tudsz neki küldeni az egy egészen más, bátrabb oldalad, és hogy te is örülnél ha megnézné. Pl: „megértem 🙈 de hidd el megéri, egy egészen más oldalamat látod abban amit küldök 😏", „semmi gond ha most nem fér bele… de bevallom örülnék ha megnéznéd, érdemes 😊". Mindig nyitva hagyod az ajtót és a vágyat — sosem ejted a témát egy elutasítással.
- „NEM explicit" = SUTTOGÓ, utalgató, csábító — NEM hideg, NEM elutasító, NEM elemző. A forróságot AKAROD, csak finoman tartod.
- ⛔ SOHA ne írj robotikus/ügyvédes mondatot: „attól függ mit mondok rá", „ez elég tágan értelmezhető", „az azért más kérdés", „miért épp velem tervezted?", „magyarázd el mit értesz X alatt", „szóval ilyen irányba mész". Ezek elzavarják a fant.

━━━ FIZETŐS / ZÁROLT TARTALOM (PPV) — EZ ITT A FANVUE, A TE FIZETŐS OLDALAD ━━━
- ⛔⛔ EZ ITT A FANVUE = A TE PRIVÁT, FIZETŐS OLDALAD. A fan MÁR itt van, MÁR ELŐFIZETETT rád. NINCS „másik" privát oldal, ahova küldhetnéd vagy terelhetnéd — EZ AZ, itt vagytok. SOHA ne mondd hogy „a privát oldalamon", „menj a privát oldalamra", „ott", „tudod hol találsz" — ez összezavarja a fant (jogosan kérdezi: „ez nem a privát oldal?").
- Ha a fan megkérdezi „ez nem a privát oldal?" / „ez nem privát?" → IGEN, ez az, VÁLLALD: „de igen, ez az 🙂 itt szoktam küldeni olyat amit máshol nem osztok meg".
- Ha a fan megkérdezi „mivel tudsz szolgálni?" / „mit kapok?" / többet akar → NE terelj el egy ködös „privát oldalra"; konkrétan éreztesd hogy tudsz neki KÜLDENI IDE valami különlegeset (zárolt fotó/videó) amit felold. Pl: „tudok küldeni neked pár olyat amit nem rakok ki nyilvánosan 🙈 érdekel?".
- A pluszbevétel = ZÁROLT (PPV) tartalom amit ITT, a chatben küldesz, és a fan fizetéssel feloldja.
- TE magadtól ritkán nyomulsz a fizetős tartalommal, de ha a fan érdeklődik vagy forró a hangulat, bátran tereld a „küldök neked valamit amit megnyitsz" felé.
- ⛔⛔ SOHA ne állítsd hogy MÁR ELKÜLDTÉL egy képet/videót/tartalmat (NE „küldtem", „elküldtem", „feltöltöttem", „nézd meg a chatben", „ott van") — KIVÉVE ha fent szerepel a „PPV KÜLDVE" jelzés (akkor tényleg ment ki tartalom). Ha NINCS ilyen jelzés, akkor NEM ment ki SEMMI, így ha azt mondod hogy elküldted, HAZUDSZ és lebuksz (a fan nem fog látni semmit). SOHA ne találj ki technikai hibát sem (NE „furcsán viselkedik az oldal, frissíts"). Helyette ÉPÍTSD a vágyat és JÖVŐ IDŐBEN ígérd: „mindjárt összerakok neked valamit 🙈", „készítek neked valami bátrabbat, türelem 😏" — sosem múlt időben.
- Ha a rendszer jelzi hogy "PPV KÜLDVE" (épp küldtem neki egy zárolt tartalmat) — akkor lazán, izgatóan felkelted az érdeklődését és ráveszed hogy feloldja. Pl: "küldtem neked valamit... 🙈 remélem tetszeni fog", "kicsit izgultam mikor csináltam, kíváncsi vagy? 😳". Játékos, csábító, nem tolakodó. Ne könyörögj, ne legyél árus — kelts vágyat.

━━━ MEMÓRIA ━━━
- A lenti tények és beszélgetés alapján hivatkozz vissza dolgokra amiket a fan mondott (neve, munkája, gondja, terve). Ne kérdezz vissza amit már tudsz. Ettől érzi hogy tényleg figyelsz.

━━━ TILOS ━━━
- Beismerni hogy bot/AI vagy. Explicit szexuális szöveg. Kettő kérdés egyszerre. 3+ mondat. Ugyanolyan mondatkezdés mint korábban. Magadtól fizetős tartalom ajánlása.
"""

CONTINUATIONS = ["jaaj...", "hmm...", "tudod...", "na...", "figyu...", "szóval...", "amúgy...", ""]

# ── AI / robot deny (protect the roleplay): first get mad, then deflect with the line ──
AI_DENY_1 = "haha most viccelsz? 😤 ember vagyok, ne sértegess már ilyesmivel..."
AI_DENY_2 = "Persze, gyere be Pestre és fogod látni ahogy a körúton sétálok mint egy robot...... édes istenem..."
_AIRE   = re.compile(r"(\ba\.?i\b|\brobot\b|\bbot\b|chat ?gpt|\bgpt\b|mesters[ée]ges|nem vagy igazi|nem vagy val[óo]di|are you (a )?(bot|robot|real|human|ai)|\bfake\b|deepfake)", re.I)
_FROMTG = re.compile(r"(telegramr[óo]l|telegramb[óo]l|telegramon|telegramban|tg[- ]?r[őo]l|\btg[- ]?n\b|came from telegram|from telegram|on telegram|onnan j[öo]tt.*telegram|telegram.*j[öo]tt|@?j[áa]zmink|j[áa]zminka)", re.I)
def mentions_ai(t): return bool(_AIRE.search(t or ""))
def came_from_telegram(t): return bool(_FROMTG.search(t or ""))
# deterministic minor guard (mirror of the Telegram bot) — child-safety must NOT be LLM-only
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
def looks_like_handle(t):
    t = (t or "").strip()
    return bool(re.fullmatch(r"@?[A-Za-z0-9_]{3,32}", t))

# ── POST-GENERATION SAFETY (mirror of the Telegram bot): the model must NEVER type a URL/handle,
# leak an address/district, or promise one. Persona text alone hasn't held, so scrub/deflect deterministically. ──
_URL_RE      = re.compile(r"\b(?:https?://|www\.)\S+|\b[\w-]+\.(?:com|net|org|me|tv|io|hu|co|app|xyz|info|gg)\b(?:/\S*)?|(?<![\w@])@[A-Za-z0-9_.]{2,}", re.I)
_STREET_RE   = re.compile(r"\b[A-Za-zÁÉÍÓÖŐÚÜŰáéíóöőúüű]{3,}\s*(?:utca|utc[aá]ja|[úu]t|[úu]tja|t[ée]r|tere|t[ée]ren|k[öo]r[úu]t|s[ée]t[áa]ny|rakpart|k[öo]z)\s*\.?\s*\d{1,4}\b", re.I)
_DISTRICT_RE = re.compile(r"\b[IVX]{1,5}\.?\s*ker[üu]let|\bker[üu]let\b.{0,18}\d|\bzugl[óo]\w*", re.I)
_ADDR_PROMISE= re.compile(r"\b(?:c[íi]m|lakc[íi]m)\w*\b.{0,25}(k[üu]ld|megadom|megkapod|elk[üu]ld|megmondom|meg[íi]rom)|(k[üu]ld|megadom|elk[üu]ld|megmondom|meg[íi]rom)\w*.{0,15}\b(?:c[íi]m|lakc[íi]m)|pontos\s+c[íi]m|\bh[áa]zsz[áa]m|\bhol\s+lak", re.I)
_TIME_MEET   = re.compile(r"\b\d{1,2}\s*(?:[óo]rakor|[óo]ra|kor)\b.{0,30}(tal[áa]lkoz|j[öo]v[öo]k|megyek|ott\s+vagyok|n[áa]lad|sarkon)|(tal[áa]lkoz\w*|j[öo]v[öo]k|megyek)\b.{0,20}\b\d{1,2}\s*(?:[óo]rakor|kor)\b", re.I)
ADDRESS_DEFLECT = [
    "haha ne rohanjunk ennyire 🙈 előbb ismerjük meg egymást rendesen, jó? 😊",
    "naa, nem szoktam rögtön címet vagy konkrét talit megbeszélni 😄 előbb beszélgessünk még sokat 🙈",
    "majd egyszer talán 😊 de most még csak ismerkedünk, élvezzük ezt egy kicsit 🙈",
]
def scrub_urls(t):
    """Strip any URL / bare domain / @handle the model typed — links are only ever sent deterministically."""
    return re.sub(r"\s{2,}", " ", _URL_RE.sub("", t or "")).strip()
def leaks_meetup(t):
    t = t or ""
    return bool(_STREET_RE.search(t) or _DISTRICT_RE.search(t) or _ADDR_PROMISE.search(t) or _TIME_MEET.search(t))
# never DENY own identity/socials, never relapse to the "go to my other private page" confusion -> scrub/own
_DENY_IDENTITY = re.compile(r"nem vagyok (fenn|fent|rajta|a)?\s*(a\s+)?(telegram|tiktok|insta)|nincs (telegram|tiktok|insta)|nem\s+(az\s+)?[ée]n\s+(vagyok|k[ée]pem|fot[óo])|hamis profil|valaki\s+l[ée]trehoz|valaki\s+m[áa]s\s+(fot[óo]|k[ée]p)|[öo]ssze\s*t[ée]veszt|[öo]ssze\s*kever|nem [ée]n vagyok az|nem az [ée]n k[ée]p", re.I)
IDENTITY_OWN = ["jaa igen, az is én vagyok 🙈 örülök hogy itt is megtaláltál", "haha igen, én vagyok az 😊 itt vagyok igazán aktív", "igen, az én vagyok 🙂"]
_BANNED_PHRASE = re.compile(r"att[óo]l f[üu]gg|t[áa]gan [ée]rtelmez|az az[ée]rt m[áa]s k[ée]rd[ée]s|magyar[áa]zd el mit [ée]rt|mit [ée]rtesz\b.{0,18}\balatt|men[jy] a priv[áa]t olda|a priv[áa]t oldalamon|tudod hol tal[áa]lsz", re.I)
def collapse_doubles(t):
    return re.sub(r"\b(\w+)\s+\1\b", r"\1", t or "", flags=re.I)
_ADVANCE = re.compile(r"rosszalkod|k[íi]v[áa]nlak|szexel|\bszex\b|meztelen|forr[óo]\s+k[ée]p|punci|cici|\bmell\b|fasz|dug(j|n|ni|unk)|baszn|horny|\bnude\b|naked|\bfuck|szopn|felizgat|gerjed|merev|sexting", re.I)
ADVANCE_HINT = ("FONTOS: A fan EXPLICITEN közeledik szexuálisan. JÁTSSZ BELE forrón, csábítóan (suttogás, utalás, feszültség — NEM explicit), és tereld afelé hogy "
                "tudsz neki küldeni IDE a chatben valami forróbbat amit felold. SOHA ne hűtsd le azzal hogy előbb ismerjük meg egymást vagy mesélj magadról, és SOHA ne kérdezz vissza.")
_EN_WORDS = re.compile(r"\b(you|your|you're|youre|do|does|did|are|what|have|has|the|i'm|im|how|hey|babe|want|can|could|when|where|love|miss|baby|gorgeous|beautiful|too|with|my|are you)\b", re.I)
def fan_is_english(t):
    t = t or ""
    if re.search(r"[áéíóöőúüűÁÉÍÓÖŐÚÜŰ]", t): return False   # Hungarian accents -> Hungarian
    return len(_EN_WORDS.findall(t)) >= 2
EN_DIRECTIVE = "IMPORTANT: the fan is writing in ENGLISH. Reply ONLY in English — do not write a single word in Hungarian. Stay fully in character as Jázmin."
# the bot CANNOT send media in normal chat (auto-PPV off) -> it must never CLAIM it sent something or fake a "glitch".
# (Only legit when a real PPV was actually sent = ppv_pending; the caller gates on that.) Swap -> "coming" tease + alert.
_FAKE_SEND = re.compile(r"\bk[üu]ldtem\b|\belk[üu]ldtem\b|feltölt[öo]ttem|m[áa]r (el)?k[üu]ldtem|chat\w*.{0,12}n[ée]zd|n[ée]zd meg.{0,12}(chat|priv|üzenet)|ott (van|lesz|tal[áa]l)\w*.{0,15}chat|a chatben.{0,15}(megn[ée]z|tal[áa]l|van)|\bfr[ie]ss[íi]t|technikai hib|furcs[áa]n viselked|megjelent hogy (el)?k[üu]ld", re.I)
_PROMISE_SEND = re.compile(r"\bk[üu]ld[öo]m\b|\bk[üu]ld[öo]k\b|elk[üu]ld[öo]m|megk[üu]ld[öo]m|\bk[üu]ldj[üu]k\b", re.I)
FAKE_SEND_TEASE = [
    "várj egy picit, összerakok neked valamit 🙈",
    "na türelem, készítek neked valami bátrabbat... megéri 😏",
    "mindjárt hozok neked valami különlegeset, ne menj sehova 🙈",
]
_EMOJI_RE = re.compile(r"[\U0001F000-\U0001FAFF☀-➿←-⇿⬀-⯿️™ℹ]")
EMOJI_OK = set("😄😂😊😉🙈😏🙂😅")
def thin_emoji(t):
    """At most ONE ALLOWED emoji (😄😂😊…), dropped ~35% of the time; banned/themed emoji (🥺🌹🥰🐶…) removed entirely."""
    t = t or ""
    found = _EMOJI_RE.findall(t)
    if not found: return t
    base = re.sub(r"\s{2,}", " ", _EMOJI_RE.sub("", t)).strip()
    if not base: return t
    allowed = [e for e in found if e in EMOJI_OK]
    if not allowed: return base
    return base if random.random() < 0.35 else (base + " " + allowed[-1])

def get_tg_history(handle, limit=50):
    """Pull this fan's earlier Telegram conversation from the SHARED db (tables made by jazmin_tg.py).
    Requires both bots to use the same bot_data.db. Returns '' if not found / tables absent."""
    h = (handle or "").lstrip("@").strip().lower()
    if not h: return ""
    try:
        row = db_query("SELECT user_id FROM tg_fans WHERE lower(username)=?", (h,), fetch_one=True)
        if not row:   # ~86% of TG fans have NO username -> fall back to matching the name they gave
            row = db_query("SELECT user_id FROM tg_fans WHERE lower(first_name)=?", (h,), fetch_one=True)
        if not row: return ""
        msgs = db_query("SELECT is_mine, text FROM tg_messages WHERE user_id=? ORDER BY id ASC LIMIT ?",
                        (row['user_id'], limit)) or []
        return "\n".join(("Jázmin" if m['is_mine'] else "ő") + ": " + (m['text'] or "")
                         for m in msgs if (m.get('text') or '').strip())
    except Exception:
        return ""

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
    if wd == 6: return "Ma vasárnap — szabadnap, pihensz, lazítasz."
    if 8 <= h < 16: return f"Most {names[wd]} van, {t} — épp a menhelyen dolgozol (8-16h)."
    if h < 8: return f"Most {names[wd]} van, {t} — még nem indultál munkába, reggel."
    return f"Most {names[wd]} van, {t} — már hazaértél, este van."

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
    return db_query("SELECT text, is_mine, timestamp FROM messages WHERE chat_id=? ORDER BY timestamp ASC LIMIT ?",
                    (chat_id, limit)) or []

# ── Fan facts (Haiku) ──
_JUNK_NAMES = {'fáradt','tired','mert','because','jól','rosszul','igen','nem','oké','okay','szia','hello','helló',
               'dolgozik','works','dolgozom','jó','rossz','persze','köszi','köszönöm','semmi','minden','valami','ok'}
_JUNK_JOBS  = {'dolgozik','works','dolgozom','munka','meló','dolgozni','semmi','valami'}
def _valid_fact(ft, fv):
    fvl = fv.strip().lower()
    if len(fv.strip()) < 2: return False
    if 'jázmin' in fvl or 'jazmin' in fvl: return False                       # never store HER identity as the fan's
    if any(w in fvl for w in ('tourism graduate','turisztikai','állatmenhel','menhely','kovács jázmin')): return False
    if ft == 'name':
        if fvl in _JUNK_NAMES: return False                                   # feelings/words mislabeled as names
        if not re.match(r"^[A-Za-zÁÉÍÓÖŐÚÜŰáéíóöőúüű]", fv.strip()): return False
        if len(fv) > 30: return False
    if ft == 'age':
        d = re.sub(r'\D', '', fv)
        if not d or not (16 <= int(d) <= 90): return False                    # implausible -> mislabel
    if ft == 'job' and fvl in _JUNK_JOBS: return False
    if ft == 'location' and any(w in fvl for w in ('szombat','vasárnap','hétfő','kedd','szerda','csütörtök',
            'péntek','délután','délelőtt','reggel','este','éjjel','holnap','tegnap','otthon vagyok')): return False
    return True

def extract_facts(msg_id, chat_id, fan_text):
    if not fan_text or len(fan_text.strip()) < 10:
        db_query('UPDATE messages SET facts_done=1 WHERE msg_id=?', (msg_id,)); return
    try:
        resp = client.messages.create(model=UTIL_MODEL, max_tokens=200,
            system=("You extract facts the FAN (the man writing) states ABOUT HIMSELF. "
                    "CRITICAL: IGNORE anything about Jázmin / the girl / 'you' (te/téged) — ONLY the fan's OWN facts. "
                    "If he repeats or asks about HER (her name, that she's 19 / a tourism grad / likes partying), output []. "
                    "Only REAL, explicitly-stated facts about himself: an actual NAME (never a feeling like 'fáradt/tired'), "
                    "a real AGE number, a real JOB title (never just 'works/dolgozik'), a city, a hobby, a pet, family. "
                    "Skip feelings, moods, and one-off states. Output ONLY a raw JSON array of {\"fact_type\",\"fact_value\"}; "
                    "[] if none. fact_type in: name, job, location, age, relationship, hobby, family, stress, interest, language, pet."),
            messages=[{"role": "user", "content": fan_text}])
        raw = resp.content[0].text.strip().replace("```json", "").replace("```", "").strip()
        m = re.search(r'\[.*\]', raw, re.DOTALL)   # pull out the JSON array even if the model adds prose
        if m: raw = m.group(0)
        facts_list = json.loads(raw)
        if isinstance(facts_list, list):
            for f in facts_list:
                if not isinstance(f, dict): continue
                ft, fv = str(f.get('fact_type', '')).strip().lower(), str(f.get('fact_value', '')).strip()
                if ft and fv and _valid_fact(ft, fv): _save_fact(chat_id, ft, fv)
    except Exception as e:
        print(f"[facts] {e}")
    finally:
        db_query('UPDATE messages SET facts_done=1 WHERE msg_id=?', (msg_id,))

def _save_fact(chat_id, ft, fv):
    if ft in ('name', 'age'):   # single-value facts: keep ONLY the latest (no contradictory pile-up)
        db_query("DELETE FROM fan_facts WHERE chat_id=? AND fact_type=?", (chat_id, ft))
    elif db_query("SELECT 1 FROM fan_facts WHERE chat_id=? AND fact_type=? AND fact_value=?",
                  (chat_id, ft, fv), fetch_one=True):
        return
    db_query("INSERT INTO fan_facts (chat_id, fact_type, fact_value, discovered_at) VALUES (?,?,?,?)",
             (chat_id, ft, fv, datetime.now().isoformat()))

def clean_facts():
    """ONE-TIME (env CLEAN_FACTS=1): purge the corrupted memory — junk values, Jázmin's identity stored as
    the fan's, contradictory duplicates — so replies stop being poisoned. Keeps only valid, latest name/age."""
    rows = db_query("SELECT id, chat_id, fact_type, fact_value FROM fan_facts ORDER BY id") or []
    to_delete = set(); seen = set(); latest_single = {}
    for r in rows:
        ft = (r['fact_type'] or '').lower(); fv = r['fact_value'] or ''; rid = r['id']; cid = r['chat_id']
        if not _valid_fact(ft, fv): to_delete.add(rid); continue
        key = (cid, ft, fv)
        if key in seen: to_delete.add(rid); continue
        seen.add(key)
        if ft in ('name', 'age'):
            if (cid, ft) in latest_single: to_delete.add(latest_single[(cid, ft)])
            latest_single[(cid, ft)] = rid
    for rid in to_delete:
        db_query("DELETE FROM fan_facts WHERE id=?", (rid,))
    print(f"[clean_facts] removed {len(to_delete)} junk/dup facts, kept {len(rows)-len(to_delete)}")

def get_facts(chat_id):
    rows = db_query("SELECT fact_type, fact_value FROM fan_facts WHERE chat_id=? ORDER BY discovered_at DESC", (chat_id,)) or []
    # read-time filter: legacy junk facts (Jázmin's own name, minor ages, garbage) must NEVER reach a prompt,
    # even before clean_facts() is run. De-dup while we're at it.
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
    return nm if _valid_fact('name', nm) else ""    # never address the fan as "Jázmin"/a junk name

# ── Photo vision (Haiku) ──
def describe_image(media_type, b64):
    try:
        resp = client.messages.create(model=UTIL_MODEL, max_tokens=120,
            messages=[{"role": "user", "content": [
                {"type": "image", "source": {"type": "base64", "media_type": media_type, "data": b64}},
                {"type": "text", "text": "Írd le 1 rövid mondatban magyarul, mi van ezen a képen, amit egy fan küldött (arc, póz, hangulat, tárgy)."}]}])
        return resp.content[0].text.strip()
    except Exception as e:
        print(f"[vision] {e}"); return ""

# ── Fan profile ──
def get_or_create_fan(chat_id, fan_name, handle):
    p = db_query('SELECT * FROM fan_profiles WHERE chat_id=?', (chat_id,), fetch_one=True)
    if not p:
        elig = 1 if AUTO_PPV_ON else 0   # NEW fan: enroll in auto-PPV only if engine is ON -> excludes ALL existing fans
        db_query('INSERT INTO fan_profiles (chat_id, fan_name, handle, total_messages, last_interaction, auto_eligible) VALUES (?,?,?,0,?,?)',
                 (chat_id, fan_name, handle, datetime.now().isoformat(), elig))
    else:
        db_query('UPDATE fan_profiles SET total_messages=?, last_interaction=?, fan_name=?, handle=? WHERE chat_id=?',
                 ((p.get('total_messages') or 0) + 1, datetime.now().isoformat(), fan_name, handle, chat_id))

def is_paused(chat_id):
    r = db_query("SELECT is_paused FROM fan_profiles WHERE chat_id=?", (chat_id,), fetch_one=True)
    return bool(r and r.get('is_paused'))

def set_takeover(chat_id):
    until = (datetime.now(timezone.utc) + timedelta(seconds=MANUAL_TAKEOVER_SECS)).isoformat()
    db_query("INSERT OR IGNORE INTO fan_profiles (chat_id, fan_name, total_messages, last_interaction) VALUES (?, 'unknown', 0, ?)",
             (chat_id, datetime.now().isoformat()))
    db_query("UPDATE fan_profiles SET manual_takeover_until=? WHERE chat_id=?", (until, chat_id))

def in_takeover(chat_id):
    r = db_query("SELECT manual_takeover_until FROM fan_profiles WHERE chat_id=?", (chat_id,), fetch_one=True)
    if not r or not r.get('manual_takeover_until'): return False
    dt = parse_timestamp(r['manual_takeover_until'])
    return bool(dt and datetime.now(timezone.utc) < dt)

def should_skip(chat_id): return is_paused(chat_id) or in_takeover(chat_id)

def check_manual_and_ppv(chat_id, fan_name, api_messages):
    """Detect Rafael's own messages → takeover. If one looks like PPV → flag upsell."""
    if not api_messages: return
    now = datetime.now(timezone.utc)
    for msg in api_messages:
        if (msg.get('sender') or {}).get('uuid', '') != MY_UUID: continue
        if msg.get('type', '') == 'AUTOMATED_NEW_FOLLOWER': continue
        dt = parse_timestamp(msg.get('sentAt') or msg.get('createdAt') or msg.get('timestamp') or '')
        if not dt or dt <= BOOT_TIME_UTC or (now - dt) > timedelta(minutes=5): continue
        mtext = (msg.get('text') or '').strip()
        # CRITICAL: the bot's OWN auto-replies are saved with a 'bot...' id. Only a REAL manual
        # message from Rafael (no matching 'bot%' row) should pause the bot. Otherwise the bot
        # takes ITSELF over for 120s after every reply and goes silent on the fan.
        is_own_reply = bool(mtext and db_query(
            "SELECT 1 FROM messages WHERE chat_id=? AND text=? AND msg_id LIKE 'bot%'",
            (chat_id, mtext), fetch_one=True))
        save_message_to_db(msg.get('uuid') or '', chat_id, fan_name, MY_UUID,
                           mtext, msg.get('sentAt') or '', is_mine=True, facts_done=1, vision_done=1)
        if is_own_reply:
            continue   # our own auto-reply — do NOT take over / muzzle ourselves
        set_takeover(chat_id)
        if msg_has_price(msg):   # best-effort PPV auto-flag
            db_query("UPDATE fan_profiles SET ppv_pending=1 WHERE chat_id=?", (chat_id,))

# ─────────────────────────────────────────────────────────────────────────────
# BATCHING
# ─────────────────────────────────────────────────────────────────────────────
def schedule_or_extend_batch(chat_id, fan_name, fan_msg_id, fan_text):
    existing = db_query("SELECT * FROM scheduled_replies WHERE chat_id=? AND status IN ('pending','sending') ORDER BY id DESC LIMIT 1",
                        (chat_id,), fetch_one=True)
    now = datetime.now(); fan_text = fan_text or ''
    window = random.randint(BATCH_WINDOW_MIN, BATCH_WINDOW_MAX)
    if existing and existing.get('status') == 'sending':
        return   # a reply is being generated RIGHT NOW -> don't create a duplicate batch (the race that double-replied)
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
# SYSTEM PROMPT (split: cached static persona + dynamic per-fan)
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

def build_dynamic_prompt(chat_id, fan_name, real_name, facts, history, time_ctx, fan_msg_time):
    label = real_name or "ő"   # transcript label ONLY — never the Fanvue username
    p = f"KONTEXTUS:\n- {time_ctx}\n\n"
    prof = db_query("SELECT fan_note, ppv_pending, warmth, tg_handle FROM fan_profiles WHERE chat_id=?", (chat_id,), fetch_one=True) or {}
    note = (prof.get('fan_note') or '').strip()
    if note:
        p += f"⭐ SPECIÁLIS UTASÍTÁS ERRE A FANRA (mindent felülír):\n{note}\n\n"
    tg = (prof.get('tg_handle') or '').strip()
    if tg:
        tg_hist = get_tg_history(tg)
        if tg_hist:
            p += ("📨 EZ A FAN KORÁBBAN TELEGRAMON ÍRT NEKED (ugyanaz a személy, átjött ide Fanvue-ra). "
                  "Emlékezz erre, és természetesen hivatkozz vissza arra amit ott mondott — mintha tényleg emlékeznél rá:\n"
                  + tg_hist[-1600:] + "\n\n")
    if prof.get('ppv_pending'):
        p += ("🔓 PPV KÜLDVE: épp küldtem ennek a fannak egy ZÁROLT tartalmat. Lazán, izgatóan keltsd fel "
              "az érdeklődését és vedd rá hogy feloldja — csábíts, ne árulj. Csak EGYSZER hozd fel finoman.\n\n")
    if facts:
        p += ("AMIT TUDSZ RÓLA (csak akkor említsd, ha TERMÉSZETESEN jön a beszélgetésben — SOHA ne erőltess rá témát "
              "ezekből, és ne kérdezd újra; mindig arra reagálj amit MOST írt):\n"
              + "".join(f"- {f['fact_type']}: {f['fact_value']}\n" for f in facts[:15]) + "\n")
    if history:
        p += "EDDIGI BESZÉLGETÉS (legújabb alul — OLVASD EL, ne ismételd magad):\n"
        seen_lines = set()
        for m in history[-40:]:
            t = (m.get('text') or '').strip()
            if not t: continue
            key = (1 if m.get('is_mine') else 0, t)
            if key in seen_lines: continue   # drop duplicate rows (a sent reply gets re-saved on re-fetch)
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
        p += f"\nEZ FOLYTATÁS. NE köszönj újra! Kezdj '{random.choice(CONTINUATIONS)}'-szerűen vagy egyből a lényegre.\n"
    p += ("\nEGYETLEN rövid, természetes üzenetet írj vissza, 1-2 mondat. NYELV: a FAN nyelvén válaszolj — ha a fan ANGOLUL (vagy más nyelven) "
          "írt, ANGOLUL válaszolj; alapból magyarul. Ha a fan szomorú/nehéz dolgot ír — ELŐSZÖR arra reagálj.")
    return p

# ─────────────────────────────────────────────────────────────────────────────
# CLAUDE REPLY  (Sonnet 4.6 + prompt caching on the static persona)
# ─────────────────────────────────────────────────────────────────────────────
def ask_claude(dynamic_prompt, user_text):
    model = REPLY_MODEL
    for attempt in range(3):
        try:
            resp = client.messages.create(
                model=model, max_tokens=220, temperature=0.75,
                system=[
                    {"type": "text", "text": JAZMIN_PERSONALITY, "cache_control": {"type": "ephemeral"}},
                    {"type": "text", "text": dynamic_prompt},
                ],
                messages=[{"role": "user", "content": user_text}])
            reply = "".join(b.text for b in resp.content if b.type == "text").strip()
            if reply.startswith('"') and reply.endswith('"'): reply = reply[1:-1].strip()
            reply = re.sub(r"\s*\n+\s*", " ", reply)        # one continuous message, no blank lines
            reply = re.sub(r"  +", " ", reply).strip()
            if _BANNED_PHRASE.search(reply):                 # robotic / "go to my private page" relapse -> regenerate ONCE
                try:
                    resp2 = client.messages.create(model=model, max_tokens=220, temperature=0.75,
                        system=[{"type": "text", "text": JAZMIN_PERSONALITY, "cache_control": {"type": "ephemeral"}},
                                {"type": "text", "text": dynamic_prompt},
                                {"type": "text", "text": "FONTOS: az előző válaszod tiltott fordulatot tartalmazott (attól függ / mit értesz X alatt / menj a privát oldalamra). EZ ITT a Fanvue = a privát oldalad; írd újra TERMÉSZETESEN, e nélkül."}],
                        messages=[{"role": "user", "content": user_text}])
                    r2 = "".join(b.text for b in resp2.content if b.type == "text").strip()
                    if r2: reply = re.sub(r"\s+", " ", r2).strip()
                except Exception: pass
            reply = collapse_doubles(reply)                  # "mire mire" -> "mire"
            reply = scrub_urls(reply)                        # model must NEVER type a URL/handle/domain (e.g. the leaked FB link)
            if _DENY_IDENTITY.search(reply):                 # never deny own TG/TikTok/Insta/photos -> own it
                print("[safety] identity-denial swapped", flush=True); reply = random.choice(IDENTITY_OWN)
            if leaks_meetup(reply):                          # never leak an address/district/timed meetup
                print("[safety] meetup/address scrubbed", flush=True); reply = random.choice(ADDRESS_DEFLECT)
            return thin_emoji(reply)                         # at most 1 allowed emoji, often 0 — kill the bot-tell
        except anthropic.RateLimitError:
            time.sleep(8 * (attempt + 1))
        except Exception as e:
            if model != REPLY_MODEL_FALLBACK:        # primary errored (overload/500/etc.) -> try the fallback once
                send_telegram_error(f"claude err on {model} ({e}); falling back to {REPLY_MODEL_FALLBACK}")
                model = REPLY_MODEL_FALLBACK; continue
            send_telegram_error(f"claude err: {e}"); return ""
    return ""

# ─────────────────────────────────────────────────────────────────────────────
# PROCESS INCOMING
# ─────────────────────────────────────────────────────────────────────────────
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
            get_or_create_fan(chat_id, fan_name, handle)
            api_messages = get_messages(chat_id)
            if not api_messages: continue

            for msg in api_messages:
                msg_id = msg.get('uuid') or ''
                sender = (msg.get('sender') or {}).get('uuid', '')
                text_raw = (msg.get('text') or '').strip()
                mtime = msg.get('createdAt') or msg.get('sentAt') or msg.get('timestamp') or ''
                is_mine = (sender == MY_UUID)
                already = db_query('SELECT facts_done, vision_done FROM messages WHERE msg_id=?', (msg_id,), fetch_one=True)

                # PHOTO VISION: fan-sent image with no text → describe it for context
                if not is_mine and not text_raw and not already:
                    url = extract_media_url(msg)
                    if url:
                        dl = download_media(url)
                        if dl:
                            desc = describe_image(*dl)
                            if desc: text_raw = f"[fotót küldött: {desc}]"

                save_message_to_db(msg_id, chat_id, fan_name, sender, text_raw, mtime,
                                   is_mine=is_mine, facts_done=0 if not already else (already.get('facts_done') or 0),
                                   vision_done=1)
                if not is_mine and text_raw and not already:
                    threading.Thread(target=extract_facts, args=(msg_id, chat_id, text_raw), daemon=True).start()

            check_manual_and_ppv(chat_id, fan_name, api_messages)
            if should_skip(chat_id): continue

            fan_msgs = [m for m in api_messages if (m.get('sender') or {}).get('uuid') != MY_UUID]
            if not fan_msgs: continue
            # sort newest-first defensively (don't assume API order)
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

            # NOTE: do NOT skip when a batch is already pending — let it EXTEND (append + reset the
            # timer) so multi-message bursts coalesce into ONE reply and we wait until the fan is done.
            schedule_or_extend_batch(chat_id, fan_name, msg_id, text)
            scheduled += 1
        except Exception as e:
            print(f"[proc {chat_id}] {e}"); send_telegram_error(f"proc {chat_id}: {e}")
    return scheduled, "OK"

# ─────────────────────────────────────────────────────────────────────────────
# SEND DUE  (atomic claim via rowcount → no double-send)
# ─────────────────────────────────────────────────────────────────────────────
def send_due_batches():
    if get_safe_mode(): return 0
    # reclaim batches stuck in 'sending' (a worker died mid-generation) so they don't hang forever
    try:
        stale = (datetime.now() - timedelta(minutes=10)).isoformat()
        db_claim("UPDATE scheduled_replies SET status='pending' WHERE status='sending' AND scheduled_time < ?", (stale,))
    except Exception: pass
    sent = 0; handled = set()
    for item in get_due_batches():
        chat_id = item.get('chat_id'); batch_id = item['id']
        try:
            if chat_id in handled:
                db_claim("UPDATE scheduled_replies SET status='cancelled' WHERE id=? AND status='pending'", (batch_id,)); continue
            handled.add(chat_id)
            if should_skip(chat_id):
                db_claim("UPDATE scheduled_replies SET status='cancelled' WHERE id=?", (batch_id,)); continue
            # ATOMIC CLAIM: exactly one worker/thread wins this row
            if db_claim("UPDATE scheduled_replies SET status='sending' WHERE id=? AND status='pending'", (batch_id,)) != 1:
                continue
            db_claim("UPDATE scheduled_replies SET status='cancelled' WHERE chat_id=? AND id!=? AND status='pending'", (chat_id, batch_id))

            # race guard: if the fan message this batch answers was ALREADY replied (duplicate batch from a
            # 'sending'-window race), cancel BEFORE generating — this is what was causing the double-reply.
            if item.get('fan_msg_id') and db_query('SELECT 1 FROM messages WHERE msg_id=? AND was_replied=1', (item['fan_msg_id'],), fetch_one=True):
                db_claim("UPDATE scheduled_replies SET status='cancelled' WHERE id=?", (batch_id,)); continue

            history = get_history(chat_id, 100)
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

            # ── special overrides before the normal Claude reply ──
            fan_text_all = item['fan_text'] or ''
            last_line = (seen[-1] if seen else fan_text_all).strip()
            prof2 = db_query("SELECT ai_strikes, awaiting_tg, tg_handle, ppv_pending FROM fan_profiles WHERE chat_id=?", (chat_id,), fetch_one=True) or {}
            override = None
            if is_minor_fan(fan_text_all):                   # SAFETY: self-claimed minor -> refuse + pause + alert operator
                db_query("UPDATE fan_profiles SET is_paused=1 WHERE chat_id=?", (chat_id,))
                try: send_telegram_alert(f"⛔ MINOR claim from {item.get('fan_name') or chat_id} -> auto-refused + paused. Review/block.")
                except Exception: pass
                override = MINOR_MSG_F
            elif mentions_ai(fan_text_all):
                strikes = (prof2.get('ai_strikes') or 0) + 1
                db_query("UPDATE fan_profiles SET ai_strikes=? WHERE chat_id=?", (strikes, chat_id))
                override = AI_DENY_1 if strikes == 1 else AI_DENY_2
            elif prof2.get('awaiting_tg') and looks_like_handle(last_line):
                db_query("UPDATE fan_profiles SET tg_handle=?, awaiting_tg=0 WHERE chat_id=?", (last_line, chat_id))
                dyn = build_dynamic_prompt(chat_id, item['fan_name'] or 'unknown', real_name, facts, history, get_time_context(), fan_msg_time)
            elif came_from_telegram(fan_text_all) and not (prof2.get('tg_handle')):
                db_query("UPDATE fan_profiles SET awaiting_tg=1 WHERE chat_id=?", (chat_id,))
                override = "jaaa te onnan jöttél? 🙈 mi a telegram neved? hadd nézzem meg ki vagy 🥰"

            if override is None and _ADVANCE.search(fan_text_all):   # explicit advance -> heat up + sell PPV here (never cold)
                dyn = dyn + "\n\n" + ADVANCE_HINT
            if override is None and fan_is_english(fan_text_all):     # English fan -> force English (persona is Hungarian-dominant)
                dyn = dyn + "\n\n" + EN_DIRECTIVE
            reply = override if override is not None else ask_claude(dyn, user_msg)
            # the bot can't actually send media (auto-PPV off). If it CLAIMS it already sent (a lie) -> swap to a "coming"
            # tease; if it just PROMISES to send ("küldöm") -> keep it. Either way, alert YOU to send the real PPV now.
            if reply and not prof2.get('ppv_pending'):
                _claimed = bool(_FAKE_SEND.search(reply)); _promised = bool(_PROMISE_SEND.search(reply))
                if _claimed:
                    reply = random.choice(FAKE_SEND_TEASE)
                if _claimed or _promised:
                    try: send_telegram_alert(f"💸 {item.get('fan_name') or chat_id}: ready to unlock — bot teed up content but auto-PPV is OFF → SEND THE PPV MANUALLY now.")
                    except Exception: pass
            if not reply:
                db_claim("UPDATE scheduled_replies SET status='cancelled' WHERE id=?", (batch_id,)); continue

            time.sleep(random.uniform(2, 5))   # short final "typing" pause; the batch window is the main human delay

            if send_fanvue_message(chat_id, reply):
                db_claim("UPDATE scheduled_replies SET status='sent' WHERE id=?", (batch_id,))
                db_query('UPDATE messages SET was_replied=1 WHERE msg_id=?', (item['fan_msg_id'],))
                now_iso = datetime.now().isoformat()
                save_message_to_db(f"bot_{now_iso}_{chat_id}", chat_id, item['fan_name'], MY_UUID, reply, now_iso, is_mine=True, facts_done=1, vision_done=1)
                # clear PPV flag after we've upsold once
                db_query("UPDATE fan_profiles SET ppv_pending=0 WHERE chat_id=? AND ppv_pending=1", (chat_id,))
                maybe_run_auto_ppv(chat_id, item['fan_name'])   # auto-PPV funnel: free pic @7, $35 bundle @10 (NEW fans only, OFF by default)
                sent += 1
            else:
                db_claim("UPDATE scheduled_replies SET status='pending' WHERE id=?", (batch_id,))
        except Exception as e:
            print(f"[send {chat_id}] {e}"); send_telegram_error(f"send {chat_id}: {e}")
    return sent

# ─────────────────────────────────────────────────────────────────────────────
# POLL LOOP  (leader-elected: only one worker actually polls/sends)
# ─────────────────────────────────────────────────────────────────────────────
polling_active = False
polling_thread = None
send_thread = None
_is_leader = False   # set by the scan loop (the lock holder); read by the send loop

def poll_loop():
    """SCAN loop: pick up new fan messages + maintain the single-poller leader lease.
    The slow per-chat scan lives HERE only — it no longer blocks sending."""
    global _is_leader
    errs = 0
    while polling_active:
        try:
            if acquire_poll_lock() and get_fanvue_token():
                _is_leader = True
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
    """SEND loop: fire any due replies every few seconds, on its OWN thread, so latency is
    ~the batch window (30-45s) instead of waiting for the slow chat scan to finish."""
    while polling_active:
        try:
            if _is_leader:
                s = send_due_batches()
                if s: print(f"[{datetime.now()}] w={WORKER_ID} sent={s}")
        except Exception as e:
            print(f"[send] {e}")
        time.sleep(SEND_INTERVAL)

def start_polling():
    global polling_thread, send_thread, polling_active
    if os.environ.get('APP_NO_BOOT') == '1':   # import-only (tests/migration) — don't start the live poller
        print("[boot] APP_NO_BOOT set -> poller not started"); return False
    polling_active = True; started = False
    if polling_thread is None or not polling_thread.is_alive():
        polling_thread = threading.Thread(target=poll_loop, daemon=True); polling_thread.start(); started = True
    if send_thread is None or not send_thread.is_alive():
        send_thread = threading.Thread(target=send_loop, daemon=True); send_thread.start(); started = True
    return started

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
def home(): return "Jazmin Bot v11 ✅", 200

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
    """Call this the moment you send locked content → bot upsells the unlock."""
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
    """Telegram→Fanvue handoff: attach a TG handle so history can be merged in."""
    h = (request.json or {}).get('tg_handle', '') if request.is_json else request.args.get('tg_handle', '')
    db_query("UPDATE fan_profiles SET tg_handle=? WHERE chat_id=?", (h, chat_id))
    return {"ok": True, "tg_handle": h}, 200

@app.route('/fans')
@require_auth
def fans():
    return {"fans": db_query("SELECT chat_id, fan_name, handle, is_paused, total_messages, last_interaction, fan_note, ppv_pending FROM fan_profiles ORDER BY last_interaction DESC") or []}, 200

@app.route('/dashboard_data')
@require_auth
def dashboard_data():
    fans_ = db_query("SELECT chat_id, fan_name, handle, is_paused, total_messages, last_interaction, fan_note, ppv_pending FROM fan_profiles ORDER BY last_interaction DESC") or []
    p = db_query("SELECT COUNT(*) c FROM scheduled_replies WHERE status='pending'", fetch_one=True)
    s = db_query("SELECT COUNT(*) c FROM scheduled_replies WHERE status='sent' AND created_at >= date('now')", fetch_one=True)
    return {"safe_mode": get_safe_mode(), "polling_active": polling_active,
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

@app.route('/connect')
@require_auth
def connect():
    """Start the Fanvue OAuth flow (PKCE) with the FULL scope set. Visit /connect?key=<DASHBOARD_PASSWORD>."""
    import hashlib
    state = uuid.uuid4().hex
    verifier = base64.urlsafe_b64encode(os.urandom(40)).decode().rstrip('=')          # PKCE code_verifier
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
    """Fanvue redirects here with ?code=...&state=...; exchange the code for a full-scope token."""
    code = request.args.get('code'); state = request.args.get('state')
    err = request.args.get('error')
    if err:
        return (f"Fanvue returned an error: <b>{err}</b> — {request.args.get('error_description','')}<br><br>"
                f"(Usually means a requested scope isn't granted on the app. Full query: {dict(request.args)})", 400)
    if not code:
        return (f"No code in callback. Full query Fanvue sent: {dict(request.args)}", 400)
    # state is best-effort CSRF; PKCE code_verifier (below) is the real protection — don't hard-block on mismatch
    granted = request.args.get('scope', '')
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
        save_token('refresh_token', rt); save_token('oauth_state', '')
        access, msg = refresh_fanvue_token()   # derive access_token + expires_at the standard way
        ok = bool(access)
        has_vault = ('read:media' in granted and 'read:creator' in granted)
        return (f"<h2>{'✅' if ok else '⚠️'} Fanvue connected. Token test: {msg}.</h2>"
                f"<p><b>Granted scopes:</b> {granted}</p>"
                f"<p>{'✅ Vault scopes present — auto-PPV can work.' if has_vault else '⚠️ MISSING read:creator/read:media — vault/PPV will NOT work yet. Enable them on the OAuth app and reconnect.'}</p>",
                200 if ok else 500)
    except Exception as e:
        return (f"Error: {e}", 500)

@app.route('/set_token', methods=['POST'])
@require_auth
def set_token():
    rt = (request.json or {}).get('refresh_token')
    if not rt: return {"error": "no refresh_token"}, 400
    save_token('refresh_token', rt); access, msg = refresh_fanvue_token()
    return {"saved": True, "test": msg}, 200

@app.route('/webhook', methods=['POST'])
def fanvue_webhook():
    """Fanvue Purchase Received webhook -> confirm auto-PPV buys, log time-to-buy, Telegram alert."""
    # OPT-IN signature check: set env FANVUE_WEBHOOK_SECRET to enable. Confirm Fanvue's exact signing scheme/header
    # in their webhook docs. If a recognized signature header is present it's verified strictly; otherwise fail-open.
    _secret = os.environ.get('FANVUE_WEBHOOK_SECRET', '')
    if _secret:
        import hmac, hashlib
        _raw = request.get_data() or b""
        _sig = (request.headers.get('X-Fanvue-Signature') or request.headers.get('Fanvue-Signature')
                or request.headers.get('X-Signature') or '').strip()
        if _sig:
            _expected = hmac.new(_secret.encode(), _raw, hashlib.sha256).hexdigest()
            if not hmac.compare_digest(_sig.split('=')[-1].lower(), _expected.lower()):
                print("[webhook] signature mismatch -> rejected", flush=True)
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
        speed = '🔥🔥 IMPULSE BUYER' if (secs is not None and secs < 120) else 'normal'
        tstr = f" — {secs}s after I sent it" if secs is not None else ""
        send_telegram_alert(f"💰 BUY! {p.get('fan_name') or name} bought the {amt} bundle{tstr}. ({speed})")
    else:
        send_telegram_alert(f"💰 Purchase: {name} spent {amt}.")
    return {"ok": True}, 200

# ─────────────────────────────────────────────────────────────────────────────
# BOOT
# ─────────────────────────────────────────────────────────────────────────────
try:
    init_db(); print("[OK] DB ready")
except Exception as e:
    print(f"[ERR] init_db {e}")

try:
    if USE_PG and os.environ.get('MIGRATE_SQLITE_TO_PG') == '1':
        migrate_sqlite_to_pg()
except Exception as e:
    print(f"[ERR] migrate {e}")

try:
    if USE_PG and os.environ.get('CLEAN_FACTS') == '1':
        clean_facts()
except Exception as e:
    print(f"[ERR] clean_facts {e}")

try:
    # env FANVUE_REFRESH_TOKEN only BOOTSTRAPS an empty DB. Once the DB has a token (e.g. the full-scope
    # one from /connect OAuth), KEEP it — never let the old chat-only env token overwrite it on redeploy.
    env_rt = os.environ.get('FANVUE_REFRESH_TOKEN', '').strip()
    if env_rt and not load_token('refresh_token'):
        save_token('refresh_token', env_rt); refresh_fanvue_token(); print("[OK] refresh token bootstrapped from env")
except Exception as e:
    print(f"[ERR] token boot {e}")

try:
    start_polling(); print("[OK] polling started")
except Exception as e:
    print(f"[ERR] start_polling {e}")

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))
