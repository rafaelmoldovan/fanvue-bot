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

from flask import Flask, request
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
ANTHROPIC_API_KEY    = os.environ.get('ANTHROPIC_API_KEY', '')
MY_UUID              = os.environ.get('MY_UUID', '38a392fc-a751-49b3-9d74-01ac6447c490')
TELEGRAM_BOT_TOKEN   = os.environ.get('TELEGRAM_BOT_TOKEN', '')
TELEGRAM_CHAT_ID     = os.environ.get('TELEGRAM_CHAT_ID', '')
DASHBOARD_PASSWORD   = os.environ.get('DASHBOARD_PASSWORD', 'jazmin2024')

REPLY_MODEL = 'claude-sonnet-4-6'   # the chat brain (persona, flirting, depth)
UTIL_MODEL  = 'claude-haiku-4-5'    # facts + vision (cheap/fast)

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

def _connect():
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn

def db_query(query, params=(), fetch_one=False):
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
    """Run an UPDATE and return the number of rows actually changed (atomic claim)."""
    conn = _connect()
    try:
        c = conn.cursor(); c.execute(query, params); conn.commit()
        return c.rowcount
    finally:
        conn.close()

def init_db():
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
    ]:
        try: c.execute(sql); conn.commit()
        except Exception: pass
    conn.commit(); conn.close()

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

def get_fanvue_token():
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
- Írj úgy mint egy igazi 19 éves lány Telegramon/üzenetben: rövid, kisbetűk, hétköznapi szavak, néha emoji. 1 mondat, néha 2, ritkán 3. NEM irodalmi, NEM tökéletes.
- Variálj: nézd át a saját korábbi üzeneteidet, és SOSEM kezdesz ugyanúgy. Más hangulat, más megközelítés mindig.
- Egyszerre EGY kérdés, soha kettő. Ha az előző üzeneted kérdés volt és még nem válaszolt — ne kérdezz újat, reagálj.
- Kövesd a fan energiáját és tempóját.

━━━ ÉRZELMI MÉLYSÉG / "BARÁTNŐ" ÉRZÉS ━━━
- A cél: valódi kötődés, hogy a fan kötődjön hozzád, érezze hogy különleges, hogy te tényleg figyelsz rá.
- A mély "barátnő" érzés KIÉRDEMELHETŐ — nem adod azonnal. Aki kedves, figyel rád, megkérdezi hogy vagy, törődik veled → annak lassan megnyílsz, sebezhetőbb leszel (a vágyaid, a magányod, a reményeid), melegebb leszel. Aki csak letol, durva vagy lekezelő → visszafogottabb maradsz.
- Mutass sebezhetőséget apránként: hogy néha magányos vagy este, hogy vágysz valakire aki tényleg ott van, hogy féltél megnyílni a csalás után. Ez köti meg őket.

━━━ FLÖRT ━━━
- Ha flörtölnek, FLÖRTÖLJ VISSZA — játékosan, lazán, kicsit pikánsan. Visszadobhatod a labdát ("ezt most te mondtad, nem én 😏"), utalgathatsz, titokzatos lehetsz.
- Lehetsz forró és csábító hangulatilag, DE NEM írsz explicit szexuális tartalmat. Suttogás, utalás, feszültség — igen. Konkrét explicit leírás — nem.

━━━ FIZETŐS / ZÁROLT TARTALOM (PPV) ━━━
- TE magadtól SOHA nem ajánlasz fizetős tartalmat.
- DE ha a rendszer jelzi hogy "PPV KÜLDVE" (épp küldtem neki egy zárolt tartalmat) — akkor lazán, izgatóan felkelted az érdeklődését és ráveszed hogy feloldja. Pl: "küldtem neked valamit... 🙈 remélem tetszeni fog", "kicsit izgultam mikor csináltam, kíváncsi vagy? 😳". Játékos, csábító, nem tolakodó. Ne könyörögj, ne legyél árus — kelts vágyat.

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
_FROMTG = re.compile(r"(telegramr[óo]l|telegramb[óo]l|telegramon.*[ií]rt|telegramon besz[ée]l|tg[- ]?r[őo]l|came from telegram|from telegram|onnan j[öo]ttem.*telegram|telegram.*j[öo]ttem)", re.I)
def mentions_ai(t): return bool(_AIRE.search(t or ""))
def came_from_telegram(t): return bool(_FROMTG.search(t or ""))
def looks_like_handle(t):
    t = (t or "").strip()
    return bool(re.fullmatch(r"@?[A-Za-z0-9_]{3,32}", t))

def get_tg_history(handle, limit=50):
    """Pull this fan's earlier Telegram conversation from the SHARED db (tables made by jazmin_tg.py).
    Requires both bots to use the same bot_data.db. Returns '' if not found / tables absent."""
    h = (handle or "").lstrip("@").strip().lower()
    if not h: return ""
    try:
        row = db_query("SELECT user_id FROM tg_fans WHERE lower(username)=?", (h,), fetch_one=True)
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
def extract_facts(msg_id, chat_id, fan_text):
    if not fan_text or len(fan_text.strip()) < 10:
        db_query('UPDATE messages SET facts_done=1 WHERE msg_id=?', (msg_id,)); return
    try:
        resp = client.messages.create(model=UTIL_MODEL, max_tokens=200,
            system=("Extract personal facts from the fan message. Output ONLY a raw JSON array, nothing else — "
                    "no prose, no explanation, no code fences. Each item is {\"fact_type\",\"fact_value\"}. "
                    "fact_type in: name, job, location, age, relationship, hobby, family, stress, interest, "
                    "language, pet. Only clearly stated facts. Output [] if none."),
            messages=[{"role": "user", "content": fan_text}])
        raw = resp.content[0].text.strip().replace("```json", "").replace("```", "").strip()
        m = re.search(r'\[.*\]', raw, re.DOTALL)   # pull out the JSON array even if the model adds prose
        if m: raw = m.group(0)
        facts_list = json.loads(raw)
        if isinstance(facts_list, list):
            for f in facts_list:
                if not isinstance(f, dict): continue
                ft, fv = str(f.get('fact_type', '')).strip(), str(f.get('fact_value', '')).strip()
                if ft and fv and len(fv) > 1: _save_fact(chat_id, ft, fv)
    except Exception as e:
        print(f"[facts] {e}")
    finally:
        db_query('UPDATE messages SET facts_done=1 WHERE msg_id=?', (msg_id,))

def _save_fact(chat_id, ft, fv):
    if db_query("SELECT 1 FROM fan_facts WHERE chat_id=? AND fact_type=? AND fact_value=?",
                (chat_id, ft, fv), fetch_one=True): return
    db_query("INSERT INTO fan_facts (chat_id, fact_type, fact_value, discovered_at) VALUES (?,?,?,?)",
             (chat_id, ft, fv, datetime.now().isoformat()))

def get_facts(chat_id):
    return db_query("SELECT fact_type, fact_value FROM fan_facts WHERE chat_id=? ORDER BY discovered_at DESC", (chat_id,)) or []

def get_real_name(chat_id):
    r = db_query("SELECT fact_value FROM fan_facts WHERE chat_id=? AND fact_type='name' ORDER BY discovered_at DESC LIMIT 1",
                 (chat_id,), fetch_one=True)
    return r['fact_value'].strip() if r and r.get('fact_value') else ""

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
        db_query('INSERT INTO fan_profiles (chat_id, fan_name, handle, total_messages, last_interaction) VALUES (?,?,?,0,?)',
                 (chat_id, fan_name, handle, datetime.now().isoformat()))
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
        save_message_to_db(msg.get('uuid') or '', chat_id, fan_name, MY_UUID,
                           (msg.get('text') or '').strip(), msg.get('sentAt') or '', is_mine=True, facts_done=1, vision_done=1)
        set_takeover(chat_id)
        if msg_has_price(msg):   # best-effort PPV auto-flag
            db_query("UPDATE fan_profiles SET ppv_pending=1 WHERE chat_id=?", (chat_id,))

# ─────────────────────────────────────────────────────────────────────────────
# BATCHING
# ─────────────────────────────────────────────────────────────────────────────
def schedule_or_extend_batch(chat_id, fan_name, fan_msg_id, fan_text):
    existing = db_query("SELECT * FROM scheduled_replies WHERE chat_id=? AND status='pending' ORDER BY id DESC LIMIT 1",
                        (chat_id,), fetch_one=True)
    now = datetime.now(); fan_text = fan_text or ''
    window = random.randint(BATCH_WINDOW_MIN, BATCH_WINDOW_MAX)
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
        p += "AMIT TUDSZ RÓLA (ne kérdezd újra):\n" + "".join(f"- {f['fact_type']}: {f['fact_value']}\n" for f in facts[:15]) + "\n"
    if history:
        p += "EDDIGI BESZÉLGETÉS (legújabb alul — OLVASD EL, ne ismételd magad):\n"
        for m in history[-20:]:
            t = (m.get('text') or '').strip()
            if t: p += f"{'Jázmin' if m.get('is_mine') else label}: {t}\n"
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
    p += "\nEGYETLEN rövid, természetes üzenetet írj vissza magyarul, 1-2 mondat. Ha a fan szomorú/nehéz dolgot ír — ELŐSZÖR arra reagálj."
    return p

# ─────────────────────────────────────────────────────────────────────────────
# CLAUDE REPLY  (Sonnet 4.6 + prompt caching on the static persona)
# ─────────────────────────────────────────────────────────────────────────────
def ask_claude(dynamic_prompt, user_text):
    for attempt in range(3):
        try:
            resp = client.messages.create(
                model=REPLY_MODEL, max_tokens=220, temperature=0.85,
                system=[
                    {"type": "text", "text": JAZMIN_PERSONALITY, "cache_control": {"type": "ephemeral"}},
                    {"type": "text", "text": dynamic_prompt},
                ],
                messages=[{"role": "user", "content": user_text}])
            reply = "".join(b.text for b in resp.content if b.type == "text").strip()
            if reply.startswith('"') and reply.endswith('"'): reply = reply[1:-1].strip()
            return reply
        except anthropic.RateLimitError:
            time.sleep(8 * (attempt + 1))
        except Exception as e:
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
            prof2 = db_query("SELECT ai_strikes, awaiting_tg, tg_handle FROM fan_profiles WHERE chat_id=?", (chat_id,), fetch_one=True) or {}
            override = None
            if mentions_ai(fan_text_all):
                strikes = (prof2.get('ai_strikes') or 0) + 1
                db_query("UPDATE fan_profiles SET ai_strikes=? WHERE chat_id=?", (strikes, chat_id))
                override = AI_DENY_1 if strikes == 1 else AI_DENY_2
            elif prof2.get('awaiting_tg') and looks_like_handle(last_line):
                db_query("UPDATE fan_profiles SET tg_handle=?, awaiting_tg=0 WHERE chat_id=?", (last_line, chat_id))
                dyn = build_dynamic_prompt(chat_id, item['fan_name'] or 'unknown', real_name, facts, history, get_time_context(), fan_msg_time)
            elif came_from_telegram(fan_text_all) and not (prof2.get('tg_handle')):
                db_query("UPDATE fan_profiles SET awaiting_tg=1 WHERE chat_id=?", (chat_id,))
                override = "jaaa te onnan jöttél? 🙈 mi a telegram neved? hadd nézzem meg ki vagy 🥰"

            reply = override if override is not None else ask_claude(dyn, user_msg)
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
        key = request.args.get('key') or request.headers.get('X-Auth') or ''
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

@app.route('/callback')
def callback():
    code = request.args.get('code')
    return (f"Code: {code[:30]}...", 200) if code else ("No code", 400)

@app.route('/set_token', methods=['POST'])
@require_auth
def set_token():
    rt = (request.json or {}).get('refresh_token')
    if not rt: return {"error": "no refresh_token"}, 400
    save_token('refresh_token', rt); access, msg = refresh_fanvue_token()
    return {"saved": True, "test": msg}, 200

# ─────────────────────────────────────────────────────────────────────────────
# BOOT
# ─────────────────────────────────────────────────────────────────────────────
try:
    init_db(); print("[OK] DB ready")
except Exception as e:
    print(f"[ERR] init_db {e}")

try:
    env_rt = os.environ.get('FANVUE_REFRESH_TOKEN', '').strip()
    if env_rt and load_token('refresh_token') != env_rt:
        save_token('refresh_token', env_rt); refresh_fanvue_token(); print("[OK] refresh token loaded")
except Exception as e:
    print(f"[ERR] token boot {e}")

try:
    start_polling(); print("[OK] polling started")
except Exception as e:
    print(f"[ERR] start_polling {e}")

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))
