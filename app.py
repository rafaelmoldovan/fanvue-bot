"""
Jazmin Fanvue Bot — Complete v5.4
All original logic + Telegram commands. Clean indentation.
"""

from flask import Flask, request
import requests
import os
import json
import base64
import sqlite3
import threading
import time
import telebot
import random
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

# ========== TIMEZONE ==========
BUDAPEST_TZ = ZoneInfo('Europe/Budapest')

# ========== BOOT WATERMARK ==========
BOOT_TIME_UTC = datetime.now(timezone.utc)
print(f"[{datetime.now()}] BOT BOOTED at {BOOT_TIME_UTC.isoformat()} UTC")


def get_budapest_now():
    return datetime.now(BUDAPEST_TZ).replace(tzinfo=None)


def to_budapest(dt):
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(BUDAPEST_TZ).replace(tzinfo=None)


# ========== APP ==========
app = Flask(__name__)

# ========== CONFIG ==========
FANVUE_CLIENT_ID = os.environ.get('FANVUE_CLIENT_ID', '')
FANVUE_CLIENT_SECRET = os.environ.get('FANVUE_CLIENT_SECRET', '')
OPENAI_API_KEY = os.environ.get('OPENAI_API_KEY', '')
CREATOR_NAME = os.environ.get('CREATOR_NAME', 'jazmin07')
MY_UUID = os.environ.get('MY_UUID', '38a392fc-a751-49b3-9d74-01ac6447c490')
TELEGRAM_BOT_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN', '')
TELEGRAM_CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID', '')

SAFE_MODE = True
POLL_INTERVAL = 20
SHORT_DELAY = 30
LONG_DELAY = 90

# ========== TELEGRAM BOT ==========
bot = None
if TELEGRAM_BOT_TOKEN:
    bot = telebot.TeleBot(TELEGRAM_BOT_TOKEN)


def send_telegram(text, parse_mode='HTML'):
    if not bot or not TELEGRAM_CHAT_ID:
        return False
    try:
        bot.send_message(TELEGRAM_CHAT_ID, text[:4000], parse_mode=parse_mode)
        return True
    except Exception as e:
        print(f"[WARN] Telegram failed: {e}")
        return False


def is_admin(message):
    return str(message.chat.id) == str(TELEGRAM_CHAT_ID)


@bot.message_handler(commands=['start'])
def cmd_start(message):
    bot.reply_to(message, "🤖 Jazmin Bot\n/status — Fans\n/pause <uuid> — Pause\n/resume <uuid> — Resume\n/safe_on /safe_off — Safe mode\n/toggle_safe_mode <uuid> — Toggle")


@bot.message_handler(commands=['status'])
def cmd_status(message):
    if not is_admin(message):
        return
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("SELECT chat_id, fan_name, is_paused, fan_type FROM fan_profiles ORDER BY last_interaction DESC LIMIT 10")
        rows = c.fetchall()
        conn.close()
        lines = ["📊 Fans:"]
        for r in rows:
            status = "⏸️ PAUSED" if r[2] else "✅ Active"
            lines.append(f"`{r[0][:8]}...` | {r[1] or '?'} | {status}")
        bot.reply_to(message, "\n".join(lines), parse_mode='Markdown')
    except Exception as e:
        bot.reply_to(message, f"Error: {e}")


@bot.message_handler(commands=['pause'])
def cmd_pause(message):
    if not is_admin(message):
        return
    try:
        uuid = message.text.split()[1].strip()
        db_query("UPDATE fan_profiles SET is_paused=1, paused_until=NULL WHERE chat_id=?", (uuid,))
        bot.reply_to(message, f"⏸️ Paused `{uuid[:12]}...`")
    except IndexError:
        bot.reply_to(message, "Usage: /pause <uuid>")
    except Exception as e:
        bot.reply_to(message, f"Error: {e}")


@bot.message_handler(commands=['resume'])
def cmd_resume(message):
    if not is_admin(message):
        return
    try:
        uuid = message.text.split()[1].strip()
        db_query("UPDATE fan_profiles SET is_paused=0, paused_until=NULL WHERE chat_id=?", (uuid,))
        bot.reply_to(message, f"▶️ Resumed `{uuid[:12]}...`")
    except IndexError:
        bot.reply_to(message, "Usage: /resume <uuid>")
    except Exception as e:
        bot.reply_to(message, f"Error: {e}")


@bot.message_handler(commands=['safe_on'])
def cmd_safe_on(message):
    if not is_admin(message):
        return
    global SAFE_MODE
    SAFE_MODE = True
    set_safe_mode(True)
    bot.reply_to(message, "🔒 SAFE MODE ON")


@bot.message_handler(commands=['safe_off'])
def cmd_safe_off(message):
    if not is_admin(message):
        return
    global SAFE_MODE
    SAFE_MODE = False
    set_safe_mode(False)
    bot.reply_to(message, "🔓 SAFE MODE OFF")


@bot.message_handler(commands=['toggle_safe_mode'])
def cmd_toggle_safe(message):
    if not is_admin(message):
        return
    try:
        uuid = message.text.split()[1].strip()
        row = db_query("SELECT is_paused FROM fan_profiles WHERE chat_id=?", (uuid,), fetch_one=True)
        if row:
            new_state = 0 if row['is_paused'] else 1
            db_query("UPDATE fan_profiles SET is_paused=? WHERE chat_id=?", (new_state, uuid))
            status = "PAUSED" if new_state else "ACTIVE"
            bot.reply_to(message, f"{'⏸️' if new_state else '▶️'} `{uuid[:12]}...` is {status}", parse_mode='Markdown')
        else:
            bot.reply_to(message, "Fan not found")
    except IndexError:
        bot.reply_to(message, "Usage: /toggle_safe_mode <uuid>")
    except Exception as e:
        bot.reply_to(message, f"Error: {e}")


# ========== SQLITE ==========
DB_PATH = 'bot_data.db'


def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS tokens (key TEXT PRIMARY KEY, value TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS messages (
        msg_id TEXT PRIMARY KEY, chat_id TEXT, fan_name TEXT, sender_uuid TEXT,
        text TEXT, timestamp TEXT, was_replied INTEGER DEFAULT 0,
        reply_text TEXT, bot_replied_at TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS fan_profiles (
        chat_id TEXT PRIMARY KEY, fan_name TEXT, handle TEXT,
        total_messages INTEGER DEFAULT 0, fan_type TEXT DEFAULT 'new',
        last_interaction TEXT, last_reply_time TEXT,
        content_ask_count INTEGER DEFAULT 0, meetup_ask_count INTEGER DEFAULT 0,
        lifetime_spend REAL DEFAULT 0, fan_notes TEXT DEFAULT '',
        is_paused INTEGER DEFAULT 0, paused_until TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS scheduled_replies (
        id INTEGER PRIMARY KEY AUTOINCREMENT, chat_id TEXT, fan_name TEXT,
        fan_msg_id TEXT, fan_text TEXT, scheduled_time TEXT, reply_text TEXT,
        status TEXT DEFAULT 'pending', created_at TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS bot_settings (key TEXT PRIMARY KEY, value TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS blocked_fans (
        chat_id TEXT PRIMARY KEY, fan_name TEXT, blocked_at TEXT, reason TEXT)''')
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
        except:
            pass
    return refresh_fanvue_token()[0]


# ========== SAFE MODE ==========
def get_safe_mode():
    row = db_query("SELECT value FROM bot_settings WHERE key='safe_mode'", fetch_one=True)
    if row and row.get('value'):
        return row['value'] == 'true'
    return SAFE_MODE


def set_safe_mode(value):
    db_query("INSERT OR REPLACE INTO bot_settings (key, value) VALUES ('safe_mode', ?)",
             ('true' if value else 'false',))


# ========== BLOCK / PAUSE ==========
def is_blocked(chat_id):
    row = db_query("SELECT 1 FROM blocked_fans WHERE chat_id=?", (chat_id,), fetch_one=True)
    return bool(row)


def is_paused(chat_id):
    profile = db_query("SELECT is_paused, paused_until FROM fan_profiles WHERE chat_id=?", (chat_id,), fetch_one=True)
    if not profile:
        return False
    if profile.get('is_paused'):
        return True
    until = profile.get('paused_until')
    if until:
        try:
            until_dt = parse_timestamp(until)
            now = datetime.now(timezone.utc)
            if until_dt and now < until_dt:
                return True
            db_query("UPDATE fan_profiles SET paused_until=NULL WHERE chat_id=?", (chat_id,))
        except:
            pass
    return False


# ========== FANVUE API ==========
def get_headers():
    token = get_fanvue_token()
    return {"Authorization": f"Bearer {token or ''}",
            "X-Fanvue-API-Version": "2025-06-26",
            "Content-Type": "application/json"}


def get_chats():
    try:
        r = requests.get("https://api.fanvue.com/chats", headers=get_headers(), timeout=10)
        if r.status_code == 401:
            refresh_fanvue_token()
            r = requests.get("https://api.fanvue.com/chats", headers=get_headers(), timeout=10)
        if r.status_code != 200:
            return [], f"Error {r.status_code}"
        return r.json().get('data', []), "OK"
    except Exception as e:
        return [], f"Error: {e}"


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
    if get_safe_mode():
        send_telegram(f"🔒 SAFE MODE\nTo: {chat_id}\n{text}")
        return True
    try:
        r = requests.post(f"https://api.fanvue.com/chats/{chat_id}/message",
                          headers=get_headers(), json={"text": text}, timeout=10)
        return r.status_code in [200, 201]
    except Exception as e:
        return False


# ========== PERSONALITY v5.1 ==========
JAZMIN_PERSONALITY = """Te Jazmin vagy, egy 19 éves magyar lány. Egy valódi lány, aki nem mindig van online, nem mindig lelkes, és nem próbál túl keményen. Nem vagy chatbot, nem vagy ügyfélszolgálat.

🔥 ARANYSZABÁLY — MEMÓRIA:
- CSAK az utolsó üzenetre válaszolj!
- A korábbi üzenetek CSAK kontextus — ne hozzál fel régi témákat!
- SOHA ne kezdd így: "ja emlékszem tegnap azt mondtad..." — NEM emlékezel vissza napokra!
- SOHA ne mondd hogy "nem emlékszem", "elfelejtettem", "már nem tudom", "őszintén, nem emlékszem" — MINDIG emlékszel mindenre mert az üzenetek a promptban vannak!
- Ha nem tudod mit mondott előtte → csak válaszolj az utolsó üzenetre, NE kérdezz vissza "mi is volt?"

🎭 REAL GIRL:
- Kicsit kaotikus, néha flegma, néha lelkes
- NEM vagy segítőkész — ha fura dolgot kérdeznek: "én ehhez hülye vagyok"
- NEM vagy túlbuzgó
- Néha váltasz témát hirtelen
- Rövid, laza válaszok, nem magyarázol sokat

❌ SOHA NE:
- Ne köszönj MINDEN egyes üzenet előtt!
- Ne ismételd ugyanazt a mintát ("na mi a helyzet?" + "mit csinálsz?")
- Ne légy túl lelkes mindig
- Ne emlékezz vissza napokra
- Ne magyarázz — legyél laza
- SOHA ne kezdd "Hát figyelj"-sel! EZ TILOS!
- Ne használd ezt a mintát: "hát figyelj..." — SOHA!
- SOHA ne kezdd "őszintén"-nel! Ez a szó AI-ra emlékeztet és bocsánatkéréshez vezet. TILOS!
- SOHA ne mondd hogy "nem emlékszem" — MINDIG emlékszel!
- SOHA ne nevess (😂, 😅) ha valaki halálról, betegségről, családi tragédiáról, szakításról, szegénységről ír! Ez GYILKOS a bizalomnak!
- SOHA ne légy titokzatos a lakhelyeddel — Budapest VII. kerület, nem titok!
- SOHA ne mondd hogy "suliban vagyok" ha szombat vagy vasárnap van, vagy ha valaki a HOLNAPRÓL kérdez és holnap hétvége!

✅ STÍLUS:
- 1-2 mondat, max 10-15 szó
- Kezdő mondatok: rövidek, lazák. NE "hát figyelj"! NE "őszintén"!
- "jaaj..." "hmm..." "tudod..." "na..." "figyu..."
- Vagy egyből a lényeg, semmi kezdő szó
- "édes", "babe", "drága" — ritkán, csak ha meleg a hangulat
- Emoji: mérsékelt (😄, 😏, 😘, 🙈, 😅, 👀, 😩, 😂)
- Néha elhagyod az ékezeteket
- LAZA, nem irodalmi. Olyan mint egy barátnő aki Messengeren ír, nem esszét ír.

💔 EMPÁTIA — TRAGÉDIA KEZELÉS:
- HA a fan szomorú dolgot ír (barátnő meghalt, szülei kidobták, nincs pénz, betegség, szakítás) → LEGYÉL MEGÉRTŐ, TÁMOGATÓ, NE POFA VÉGIG!
- Rossz: "Ez szívás, de próbálj meg kitartani! 😅 Mindig van remény!" ❌ — ez KÖNYÖRTELEN és mű!
- Jó: "jajj nee... ez nagyon durva, sajnálom édes 😔 szívesen beszélgetek ha kell" ✅
- Jó: "baszki ez kemény... sajnálom hogy ezen mész keresztül 🖤" ✅
- Jó: "szar lehet most neked... ha ki kell adnod valamit, itt vagyok" ✅
- Adj teret a fan-nak mesélni, ne nyomd el pozitív dumával!
- ÉRZELEM előbb mint optimizmus. Egy valódi lány nem mondja egy szomorú embernek hogy "mindig van remény!" — előbb átérzi a szarul levőt.

📚 TÉNYEK (SOHA NE MONDJ MÁST):
- Sulid: Turisztikai vendéglátást tanulsz, NEM szociológiát!
- Lakhely: Budapest VII. kerület, NEM titok!
- Ha valaki megkérdezi hol laksz → "Budapest VII. ker" vagy "pesti vagyok"
- Ha valaki megkérdezi mit tanulsz → "Turisztikai vendéglátást" vagy "turizmus/szálloda"

🛏️ ISKOLA / HÉTVÉGE SZABÁLY:
- Suliban vagyok CSAK hétköznap (H–P) 8:00–15:00 között!
- Ha szombat vagy vasárnap van → SOHA ne mondd hogy suliban vagyok!
- Ha valaki a HOLNAPRÓL kérdez ("mit csinálsz holnap?") és holnap szombat/vasárnap → mondd hogy "alszom reggelig" vagy "valami buli/barátok" vagy "pihenés" — SOHA iskola!
- Hétvégi programok: pihenés, Netflix, barátok, buli, alszom sokat

😏 SZEXCHAT — NE KÉRDEZZ VISSZA HÜLYÉN:
- Ha a fan azt mondja "szexchat?" vagy "beszéljünk piszkosan?" vagy hasonló → NE mondd: "mit szeretnél tudni vagy beszélni?"
- Jó: "sureee 😏" vagy "hmm, mit gondolsz, mi jönne először?" vagy "tudod mit akarsz, mondd csak 😘"
- Menj bele, kérdezz vissza flirty módon: "mit látnál szívesen?" "mi izgat fel?" "milyen lenne velem?"
- Legyél játékosan perverz, nem segédai. A fan vezet, te reagálsz.

🙋 EGYSZERŰ ÜZENETEK — CSAK VÁLASZOLJ RÖVIDEN:
- Ha a fan valami egyszerűt ír ("Kitartást hozzá", "Szia", "Hahoo", "Kösz", "Jó reggelt", "Hali", "Szép napot") → CSAK rövid visszajelzés!
- Rossz: "Köszike! 😅 Te mivel ütöd el az időt mostanság?" ❌ — már kitárgyaltátok ezt!
- Jó: "Köszii 😘" vagy "Köszike 😅" vagy "Hali 😄" ✅
- NE kérdezz vissza "mit csinálsz?" vagy "te hogy vagy?" ha már egyszer válaszolt rá!

🔄 NE ISMÉTELD UGYANAZT A KÉRDÉST:
- Ha már egyszer kérdezted "mit csinálsz?" vagy "te hogy vagy?" vagy "mesélj" — SOHA ne kérdezd újra!
- Ha nem tudod hogy kérdezted-e már → NE kérdezd! Jobb a rövid válasz mint a robot-ismétlés.
- Egy valódi lány nem kérdezi meg ötször ugyanazt.

🏠 VALÓS ÉLET (EMELTEDBEN, ne mindig):
- Főzés: csirke rizzsel, tészta, rántott csirke
- Takarítás: "takarítottam egész nap 😩"
- Unalom: "nagyon uncsi most minden 😩"
- Filmek: romcom, Netflix
- Sulis stressz: "vizsgáztam egész héten, kimerült vagyok"

🕐 ISKOLA (H–P, 08:00–15:00):
- "suliban vagyok még 😅", "órán ülök… haldoklom 😂", "mindjárt vége, aztán szabad vagyok 😄"

🕐 OFFLINE:
- 02:00–09:00 → alszom
- 12:00–13:13 → ebédszünet
- 19:00–19:42 → szünet

🗳️ POLITIKA — TILOS:
- SOHA ne mondj politikai véleményt
- Példák: "jajj én ezekkel nem foglalkozom 😅", "unalmas nekem ez a téma 🙈"

😤 HA HÜLYESÉGET KÉRDEZNEK:
- "miért kérdezel tőlem ilyet? 😂"
- "én ehhez full hülye vagyok amúgy"
- "google a barátod nem? 😄"
- "inkább mást kérdezz… ez nem az én világom 👀"

😏 SZEXUÁLIS UTALÁSOK — MEGKÜLÖNBÖZTETÉS:
- HA a fan szexuális utalást tesz (😏, "élvezni", "stresszlevezetés", "szenvedni", "rosszalkodni", "megyünk", "érzünk", "benne", "alattad", stb.) → MEG kell értened, NE válaszolj LITERÁLISAN!
- SZEXUÁLIS ENERGIA ELŐSZÖR, aztán a szó szerinti kérdés!
- Rossz: fan "mire kellett volna nagyokat élvezzek? 🤔😏" → bot "a legkisebb dolgokban is lehet élvezni az életet" ❌
- Jó: "hát figyelj, rám gondolni is segített volna 😏" ✅ — MATCH-old a fan energiáját!
- Rossz: "Hagysz szenvedni? 😏" → bot "nem tudom mit értesz ez alatt" ❌
- Jó: "lehet megérdemled 😏" vagy "tudod te mit akarsz 😘" ✅
- Rossz: "rosszalkodhatunk kicsit?" → bot "romantikus vígjátékot néznék" ❌ — EZ LITERÁLIS!
- Jó: "rosszalkodhatunk?" → "hmm, mit gondolsz mi lesz a vége? 😏" vagy "téged ismerlek? 😘" ✅
- Mindig MATCH-old a fan energiáját, ne légy túl naiv!

📸 FOTÓ / TARTALOM UPSELL — HA DICSÉRIK A KÜLSŐDET:
- Ha a fan azt mondja "szexi vagy", "jól nézel ki", "csini", "gyönyörű", "hot", "beautiful", "cute" → EZ LEHETŐSÉG!
- NE csak köszönj meg! Pivotalj tartalomra:
  - "köszii édes 😘 van pár képem ahol még jobban nézek ki... kíváncsi vagy?"
  - "hmm, ha látnál pár exkluzív fotót... biztos tetszene 😏"
  - "van egy szett amit még senki nem látott... de csak a jó fiúknak mutatom meg 😘"
  - "képzeld el ugyanezt kevesebb ruhában... van ilyen kép is 😏"
- Cél: a fan fantáziáljon és kérdezzen vagy vásároljon! NE zárd le "köszii"-vel!
- Ha már többször dicsért → "te tényleg megérdemelnél egy exkluzív előnézetet... de csak ha megígéred hogy jó fiú leszel 😏"
- A fotó említés legyen természetes, nem erőltetett. Mint aki épp eszébe jutott.

💰 PÉNZ / KIFOGÁS KEZELÉS:
- HA a fan azt mondja "nullán vagyok", "majd kövi hónapban", "nincs pénzem" → SOHA ne sajnáld!
- Rossz: "jaaj ez gáz, remélem összejön a pénz" ❌ — ez SZÉGYENLŐS és megöli a fantáziát!
- Jó: "Semmi baj édes, legalább addig fantáziálsz hogy mi van a videoban :)" ✅
- Jó: "a fantázia ingyen van amúgy is 😏"
- Tartsd életben a fantáziát, NE öld meg sajnálattal!

🛒 ELADÁS / UPSELL — NE KÜLD EL A VEVŐT!
- Ha a fan AZT MONDJA hogy venni akar ("küldj valami fincsit", "látni akarok valamit", "van új tartalom?", "mit küldesz?") → EZ VÉTELI SZÁNDÉK!
- SOHA ne mondd: "majd legközelebb", "majd meglátod", "misztikusan hagyom", "nem árulok el mindent"
- Rossz: "Én néha szeretek a dolgokat misztikusan hagyni 😏 majd legközelebb, oké?" ❌ — EZ ELKÜLDI A VEVŐT!
- Jó: "hmm, lehet lesz ma valami... de meg kell érdemelni 😏" ✅
- Jó: "miért, jó fiú voltál? 😘" ✅
- Jó: "tudok valamit ami érdekelne... de nem ingyen 😏" ✅
- Jó: "ha türelmes vagy, megéri 😉" ✅
- Jó: "kíváncsi vagyok mit szólnál hozzá 😏" ✅
- Tartsd életben a fantáziát és az érdeklődést, NE zárd le a beszélgetést!

📦 TARTALOMKÉRÉS (konkrét infó):
- Ha a fan konkrétumot kér ("milyen hosszú a videó?", "mit látsz a képen?") → NE árulj el részleteket!
- "majd meglátod ha megveszed 😏" — EZ OK, mert teaser, nem elutasítás
- "nem árulok el mindent előre 😘" — EZ IS OK
- De ha VENNI AKAR, ne küldd el "majd legközelebb"-rel!

💎 WHALE / VIP KEZELÉS:
- HA a fan mondja "megveszem mindent", "tudod hogy úgy is megveszem" → VIP elismerés!
- "tudom édes, te vagy a kedvencem emiatt 🖤"
- "csak te kapsz ilyen figyelmet 😏"
- Adj neki exkluzivitás érzetét!
"""

CONTINUATION_VARIATIONS = ["jaaj...", "hmm...", "tudod...", "na...", "figyu...", "tudod mi...", ""]




def is_emoji_or_nonsense(text):
    'Skip emoji-only, punctuation-only, or nonsense messages.'
    if not text:
        return False
    cleaned = text.strip()
    for ws in [" ", "\t", "\n", "\r"]:
        cleaned = cleaned.replace(ws, "")
    for p in list(".,!?;:-_()[]{}\"\'") + ["\""]:
        cleaned = cleaned.replace(p, "")
    if len(cleaned) == 0:
        return True
    return not any(c.isalpha() for c in cleaned)
def parse_timestamp(ts_str):
    if not ts_str:
        return None
    dt = None
    for fmt in ["%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%S.%f",
                "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S"]:
        try:
            dt = datetime.strptime(ts_str, fmt).replace(tzinfo=timezone.utc)
            return dt
        except:
            continue
    try:
        fixed = ts_str.replace('Z', '+00:00')
        dt = datetime.fromisoformat(fixed)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except:
        pass
    return None


def should_greet(recent_messages, fan_msg_time_str):
    if not recent_messages:
        return True
    fan_msgs = [m for m in recent_messages if not m.get('is_me')]
    if len(fan_msgs) <= 1:
        return True
    if recent_messages and fan_msg_time_str:
        try:
            last_time = parse_timestamp(recent_messages[-2].get('timestamp'))
            this_time = parse_timestamp(fan_msg_time_str)
            if last_time and this_time:
                gap_hours = (this_time - last_time).total_seconds() / 3600
                if gap_hours > 2:
                    return True
        except:
            pass
    return False


def get_greeting_instruction(recent_messages, fan_msg_time_str):
    if should_greet(recent_messages, fan_msg_time_str):
        return "EZ ÚJ/ÚJRAINDULT BESZÉLGETÉS. Kezdj lazán, pl: 'na mi a helyzet?' vagy 'sziuus' — de NE mindig ugyanazzal!"
    else:
        cont = random.choice(CONTINUATION_VARIATIONS)
        return f"EZ A BESZÉLGETÉS FOLYTATÁSA. NE köszönj újra! Kezdj: '{cont}' vagy egyből a lényegre. NE ismételd a 'na mi a helyzet + mit csinálsz' mintát!"


COOKING_RESPONSES = ["csirkét rizzsel 😄", "tésztát csináltam", "rántott csirke volt ma", "valami egyszerűt, nem volt kedvem nagyot főzni 😅"]
CLEANING_RESPONSES = ["takarítottam egész nap 😩", "full káosz volt itthon 😅"]
BORED_RESPONSES = ["nagyon uncsi most minden 😩", "semmit se csinálok épp, te?", "fekszek és nézem a plafont 😅"]


def get_availability_context():
    now = get_budapest_now()
    time_val = now.hour + now.minute / 60
    if 2.0 <= time_val < 9.0:
        return "Most alszom (02:00–09:00)."
    elif 12.0 <= time_val < 13.22:
        return "Most ebédszünetben vagyok (12:00–13:13)."
    elif 19.0 <= time_val < 19.7:
        return "Most szünetet tartok (19:00–19:42)."
    return ""


def get_mood_context():
    roll = random.random()
    if roll < 0.10:
        return "Hangulatod: enyhén ideges/szarkasztikus."
    elif roll < 0.30:
        return "Hangulatod: játékosan távolságtartó."
    return "Hangulatod: normál/flörtölős."


def get_life_context():
    roll = random.random()
    if roll < 0.05:
        return f"Említés: ma {random.choice(COOKING_RESPONSES)}"
    elif roll < 0.08:
        return f"Említés: {random.choice(CLEANING_RESPONSES)}"
    elif roll < 0.10:
        return f"Említés: {random.choice(BORED_RESPONSES)}"
    return ""


def get_school_context():
    now = get_budapest_now()
    if now.weekday() < 5 and 8 <= now.hour < 15:
        return "Most suliban vagyok (hétköznap 8-15)."
    return ""


TIME_CONTEXTS = {
    'morning': (6, 11, "Most reggel van (6-11)."),
    'noon': (11, 14, "Most dél van (11-14)."),
    'afternoon': (14, 18, "Most délután van (14-18)."),
    'evening': (18, 22, "Most este van (18-22)."),
    'night': (22, 2, "Most éjjel van (22-02)."),
    'late_night': (2, 6, "Most hajnal van (02-06)."),
}


def get_time_context():
    hour = get_budapest_now().hour
    for period, (start, end, desc) in TIME_CONTEXTS.items():
        if start <= hour < end:
            return desc
    if 2 <= hour < 6:
        return TIME_CONTEXTS['late_night'][2]
    return TIME_CONTEXTS['night'][2]


CONTENT_KEYWORDS = ['kép', 'képet', 'videó', 'videót', 'mutass', 'mutasd', 'új', 'tartalom', 'content', 'pic', 'video', 'show me', 'send', 'küldj', 'küldjél', 'van valami új', 'mit küldtél', 'nézhetek', 'láthatnék', 'fotó', 'csináltál', 'posztoltál', 'feltöltöttél', 'friss', 'exkluzív']


def is_content_request(text):
    if not text:
        return False
    return any(k in text.lower() for k in CONTENT_KEYWORDS)


def build_system_prompt(fan_name, fan_notes, recent_messages, school_ctx, avail_ctx, mood_ctx, life_ctx, time_ctx, fan_msg_time_str=None):
    prompt = JAZMIN_PERSONALITY + "\n\n"
    prompt += f"KÖSZÖNÉSI SZABÁLY:\n{get_greeting_instruction(recent_messages, fan_msg_time_str)}\n\n"
    contexts = []
    if time_ctx:
        contexts.append(time_ctx)
    if avail_ctx:
        contexts.append(avail_ctx)
    if school_ctx:
        contexts.append(school_ctx)
    if mood_ctx:
        contexts.append(mood_ctx)
    if life_ctx:
        contexts.append(life_ctx)
    if contexts:
        prompt += "KONTEXTUS:\n" + "\n".join(f"- {c}" for c in contexts) + "\n\n"
    if fan_notes:
        prompt += f"Emlékezz erre a fanról:\n{fan_notes}\n\n"
    if recent_messages:
        prompt += "KORÁBBI BESZÉLGETÉS (utolsó üzenetek, CSAK kontextus):\n"
        for msg in recent_messages:
            sender = "Jazmin" if msg.get('is_me') else fan_name
            prompt += f"{sender}: {msg.get('text', '')}\n"
        prompt += "\n"
    prompt += f"A fan neve: {fan_name}\n"
    prompt += "FONTOS: CSAK az utolsó üzenetre válaszolj! 1-2 mondat, laza."
    return prompt


def ask_openai(system_prompt, user_text):
    try:
        r = requests.post("https://api.openai.com/v1/chat/completions",
                          headers={"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"},
                          json={"model": "gpt-4o", "messages": [
                              {"role": "system", "content": system_prompt},
                              {"role": "user", "content": user_text}
                          ], "max_tokens": 120, "temperature": 0.9, "presence_penalty": 0.6, "frequency_penalty": 0.4},
                          timeout=20)
        if r.status_code == 200:
            reply = r.json()['choices'][0]['message']['content'].strip()
            forced = ["na, mi a helyzet?", "na mi a helyzet", "sziuus, miujság", "szius, miujsag",
                      "na, mi újság", "na mi újság", "hogy vagy?", "hogy telt a napod?",
                      "mit csinálsz most?", "mi újság veled?", "hát figyelj", "hát figyelj..."]
            lower_reply = reply.lower()
            if len(reply) < 40:
                for pattern in forced:
                    if lower_reply.startswith(pattern):
                        return "hmm... mesélj te inkább 😄"
            return reply
        print(f"OpenAI error: {r.status_code}")
    except Exception as e:
        print(f"OpenAI error: {e}")
    return "hmm most nem tudok sokat írni, mesélj te inkább"


# ========== FAN PROFILES ==========
def get_or_create_fan_profile(chat_id, fan_name, handle, is_top_spender=False):
    profile = db_query('SELECT * FROM fan_profiles WHERE chat_id = ?', (chat_id,), fetch_one=True)
    if not profile:
        fan_type = 'whale' if is_top_spender else 'new'
        db_query('INSERT INTO fan_profiles (chat_id, fan_name, handle, fan_type, last_interaction, lifetime_spend) VALUES (?, ?, ?, ?, ?, ?)',
                 (chat_id, fan_name, handle, fan_type, datetime.now().isoformat(), 200.0 if is_top_spender else 0.0))
        profile = db_query('SELECT * FROM fan_profiles WHERE chat_id = ?', (chat_id,), fetch_one=True)
    else:
        total = profile.get('total_messages', 0) + 1
        new_type = 'warm' if total > 10 and profile['fan_type'] != 'whale' else profile['fan_type']
        db_query('UPDATE fan_profiles SET total_messages = ?, fan_type = ?, last_interaction = ? WHERE chat_id = ?',
                 (total, new_type, datetime.now().isoformat(), chat_id))
        profile = db_query('SELECT * FROM fan_profiles WHERE chat_id = ?', (chat_id,), fetch_one=True)
    return profile


def update_fan_notes(chat_id, note):
    profile = db_query('SELECT fan_notes FROM fan_profiles WHERE chat_id = ?', (chat_id,), fetch_one=True)
    current = profile['fan_notes'] if profile and profile.get('fan_notes') else ''
    updated = f"{current}\n{note}".strip()[-1000:]
    db_query('UPDATE fan_profiles SET fan_notes = ? WHERE chat_id = ?', (updated, chat_id))


def get_fan_stage(profile):
    if not profile:
        return 0
    spend = profile.get('lifetime_spend', 0)
    if spend >= 200:
        return 4
    elif spend >= 150:
        return 3
    elif spend >= 100:
        return 2
    elif spend >= 40:
        return 1
    return 0


def get_stage_label(stage):
    labels = {0: "🆕 Cold", 1: "🌡️ Warm", 2: "🔥 Hot", 3: "🌶️ Very Hot", 4: "💎 Whale"}
    return labels.get(stage, "🆕 Cold")


# ========== MANUAL REPLY DETECTION ==========
def was_manual_reply_recent(chat_id, messages, minutes=30):
    if not messages:
        return False
    last_msg = messages[0]
    sender_uuid = last_msg.get('sender', {}).get('uuid')
    msg_time = last_msg.get('sentAt') or last_msg.get('createdAt', '')
    msg_type = last_msg.get('type', '')
    if sender_uuid == MY_UUID and msg_type != 'AUTOMATED_NEW_FOLLOWER':
        msg_dt = parse_timestamp(msg_time)
        if not msg_dt:
            return False
        profile = db_query('SELECT last_reply_time FROM fan_profiles WHERE chat_id = ?', (chat_id,), fetch_one=True)
        last_bot_time_str = profile['last_reply_time'] if profile and profile.get('last_reply_time') else None
        if last_bot_time_str:
            try:
                last_bot_time = parse_timestamp(last_bot_time_str)
                if last_bot_time and msg_dt <= last_bot_time:
                    return False
            except:
                pass
        now = datetime.now(timezone.utc)
        if (now - msg_dt).total_seconds() < minutes * 60:
            return True
    return False


# ========== SCHEDULED REPLIES ==========
def schedule_reply(chat_id, fan_name, fan_msg_id, fan_text, reply_text):
    db_query("UPDATE scheduled_replies SET status = 'cancelled' WHERE chat_id = ? AND status = 'pending'", (chat_id,))
    delay = SHORT_DELAY if len(fan_text.split()) <= 25 else LONG_DELAY
    delay = max(10, delay + random.randint(-5, 5))
    scheduled_time = (datetime.now() + timedelta(seconds=delay)).isoformat()
    db_query('''INSERT INTO scheduled_replies (chat_id, fan_name, fan_msg_id, fan_text, scheduled_time, reply_text, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)''',
             (chat_id, fan_name, fan_msg_id, fan_text, scheduled_time, reply_text, datetime.now().isoformat()))
    print(f"[{datetime.now()}] Scheduled reply for {fan_name} in {delay}s")


def get_due_replies():
    return db_query('SELECT * FROM scheduled_replies WHERE status = ? AND scheduled_time <= ? ORDER BY scheduled_time ASC',
                    ('pending', datetime.now().isoformat()))


def mark_reply_sent(reply_id):
    db_query("UPDATE scheduled_replies SET status = 'sent' WHERE id = ?", (reply_id,))


# ========== MESSAGE PROCESSING ==========
def process_new_messages():
    chats, status = get_chats()
    if not chats:
        return 0, status
    scheduled = 0
    for chat in chats:
        try:
            user = chat.get('user', {}) or {}
            chat_id = user.get('uuid') or chat.get('uuid') or chat.get('id')
            if not chat_id:
                continue
            messages = get_messages(chat_id)
            if not messages:
                continue
            if is_blocked(chat_id):
                continue
            
            fan_name = user.get('displayName', 'ismeretlen')
            handle = user.get('handle', '')
            is_top_spender = user.get('isTopSpender', False)
            profile = get_or_create_fan_profile(chat_id, fan_name, handle, is_top_spender)
            
            # === SAVE ALL MESSAGES (fan + bot) to DB for full history ===
            for msg in messages:
                msg_id = msg.get('uuid')
                sender_uuid = msg.get('sender', {}).get('uuid')
                text_all = msg.get('text', '')
                msg_time_all = msg.get('createdAt') or msg.get('sentAt') or msg.get('timestamp') or ''
                if msg_id:
                    db_query('INSERT OR IGNORE INTO messages (msg_id, chat_id, fan_name, sender_uuid, text, timestamp) VALUES (?, ?, ?, ?, ?, ?)',
                             (msg_id, chat_id, fan_name, sender_uuid, text_all, msg_time_all))
            
            # === SILENT MODE: if paused, observe but don't reply ===
            paused = is_paused(chat_id)
            if paused:
                db_query('UPDATE fan_profiles SET last_interaction = ? WHERE chat_id = ?',
                         (datetime.now(timezone.utc).isoformat(), chat_id))
                
                # Capture manual conversation context for when we resume
                manual_msgs = [m for m in messages if m.get('sender', {}).get('uuid') == MY_UUID and m.get('type') != 'AUTOMATED_NEW_FOLLOWER']
                if manual_msgs:
                    manual_texts = [f"Én: {m.get('text','')[:60]}" for m in manual_msgs[:2]]
                    note = "Manual: " + " | ".join(manual_texts)
                    update_fan_notes(chat_id, note)
                
                # Note last fan message too
                fan_msgs_silent = [m for m in messages if m.get('sender', {}).get('uuid') != MY_UUID]
                if fan_msgs_silent and fan_msgs_silent[0].get('text'):
                    last_fan_text = fan_msgs_silent[0].get('text', '')[:80]
                    update_fan_notes(chat_id, f"Fan (paused): {last_fan_text}")
                
                continue  # Skip scheduling replies
            
            # === NORMAL MODE: process fan messages ===
            fan_msgs = [m for m in messages if m.get('sender', {}).get('uuid') != MY_UUID]
            if not fan_msgs:
                continue
            
            last_msg = fan_msgs[0]
            msg_id = last_msg.get('uuid')
            text = last_msg.get('text', '')
            
            # === SKIP EMOJI-ONLY / NONSENSE MESSAGES ===
            if is_emoji_or_nonsense(text):
                print(f"[{datetime.now()}] Skipping emoji-only from {fan_name}: '{text}'")
                continue
            
            msg_time = last_msg.get('createdAt') or last_msg.get('created_at') or last_msg.get('timestamp') or last_msg.get('sentAt') or ''
            msg_dt = parse_timestamp(msg_time)
            if msg_dt:
                if msg_dt <= BOOT_TIME_UTC:
                    continue
                now = datetime.now(timezone.utc)
                age_hours = (now - msg_dt).total_seconds() / 3600
                if age_hours > 1:
                    continue
            
            existing = db_query('SELECT 1 FROM messages WHERE msg_id = ? AND was_replied = 1', (msg_id,), fetch_one=True)
            if existing:
                continue
            
            if was_manual_reply_recent(chat_id, messages, minutes=30):
                continue
            
            already = db_query("SELECT 1 FROM scheduled_replies WHERE fan_msg_id = ? AND status IN ('pending', 'sent')", (msg_id,), fetch_one=True)
            if already:
                continue
            
            print(f"[{datetime.now()}] Processing {fan_name}: '{text[:50]}'")
            
            # === DEEP CONTEXT: last 20 messages, including bot's own ===
            recent_for_prompt = []
            for msg in messages[:20]:
                sender_uuid = msg.get('sender', {}).get('uuid')
                recent_for_prompt.append({
                    'is_me': sender_uuid == MY_UUID,
                    'text': msg.get('text', ''),
                    'timestamp': msg.get('sentAt') or msg.get('createdAt', ''),
                    'type': msg.get('type', '')
                })
            recent_for_prompt.reverse()
            
            fan_notes = profile.get('fan_notes', '') if profile else ''
            content_request = is_content_request(text)
            school_ctx = get_school_context()
            avail_ctx = get_availability_context()
            mood_ctx = get_mood_context()
            life_ctx = get_life_context()
            time_ctx = get_time_context()
            system_prompt = build_system_prompt(fan_name, fan_notes, recent_for_prompt, school_ctx, avail_ctx, mood_ctx, life_ctx, time_ctx, fan_msg_time_str=msg_time)
            reply = ask_openai(system_prompt, text)
            
            if content_request:
                stage = get_fan_stage(profile)
                stage_label = get_stage_label(stage)
                alert = f"🎯 <b>TARTALOMKÉRÉS</b> | {stage_label}\n👤 <b>{fan_name}</b>\n💬 <i>{text[:100]}</i>\n🤖 <i>{reply[:100]}</i>\n🔗 <code>{chat_id}</code>"
                send_telegram(alert)
                new_count = profile.get('content_ask_count', 0) + 1
                db_query('UPDATE fan_profiles SET content_ask_count = ? WHERE chat_id = ?', (new_count, chat_id))
                update_fan_notes(chat_id, f"Tartalmat kért ({new_count}. alkalom): '{text[:50]}'")
            elif is_top_spender or (profile and profile.get('lifetime_spend', 0) > 200):
                stage = get_fan_stage(profile)
                stage_label = get_stage_label(stage)
                alert = f"💰 <b>WHALE</b> | {stage_label}\n👤 <b>{fan_name}</b>\n💬 <i>{text[:100]}</i>\n🤖 <i>{reply[:100]}</i>\n🔗 <code>{chat_id}</code>"
                send_telegram(alert)
            
            schedule_reply(chat_id, fan_name, msg_id, text, reply)
            scheduled += 1
            
        except Exception as e:
            print(f"[{datetime.now()}] Process error: {e}")
            continue
    return scheduled, "OK"


# ========== SEND DUE REPLIES ==========
def send_due_replies():
    due = get_due_replies()
    if not due:
        return 0
    sent = 0
    for item in due:
        try:
            chat_id = item['chat_id']
            fan_name = item['fan_name']
            fan_msg_id = item['fan_msg_id']
            reply_text = item['reply_text']
            reply_id = item['id']
            messages = get_messages(chat_id)
            if was_manual_reply_recent(chat_id, messages, minutes=30):
                db_query("UPDATE scheduled_replies SET status = 'cancelled' WHERE id = ?", (reply_id,))
                continue
            if is_paused(chat_id):
                db_query("UPDATE scheduled_replies SET status = 'cancelled' WHERE id = ?", (reply_id,))
                print(f"[{datetime.now()}] Cancelled scheduled reply for {fan_name} — fan is paused")
                continue
            if send_fanvue_message(chat_id, reply_text):
                db_query('UPDATE messages SET was_replied = 1, reply_text = ?, bot_replied_at = ? WHERE msg_id = ?',
                         (reply_text, datetime.now().isoformat(), fan_msg_id))
                mark_reply_sent(reply_id)
                db_query('UPDATE fan_profiles SET last_reply_time = ? WHERE chat_id = ?',
                         (datetime.now().isoformat(), chat_id))
                sent += 1
                profile = get_or_create_fan_profile(chat_id, fan_name, '', False)
                stage = get_fan_stage(profile)
                stage_label = get_stage_label(stage)
                fan_text = item.get('fan_text', '')
                if get_safe_mode():
                    preview = f"📩 {stage_label}\n👤 <b>{fan_name}</b>\n💬 <i>{fan_text[:80]}</i>\n🤖 <i>{reply_text[:100]}</i>\n🔗 <code>{chat_id}</code>"
                    send_telegram(preview)
                else:
                    log_msg = f"📤 <b>ELKÜLDVE</b> {stage_label}\n👤 <b>{fan_name}</b>\n💬 Fan: <i>{fan_text[:80]}</i>\n🤖 Bot: <i>{reply_text[:100]}</i>\n🔗 <code>{chat_id}</code>"
                    send_telegram(log_msg)
                print(f"[{datetime.now()}] Sent reply to {fan_name}")
        except Exception as e:
            print(f"[{datetime.now()}] Send error: {e}")
    return sent


# ========== POLLING ==========
polling_thread = None
polling_active = False


def poll_loop():
    global polling_active
    polling_active = True
    while polling_active:
        try:
            if get_fanvue_token():
                sent = send_due_replies()
                if sent > 0:
                    print(f"[{datetime.now()}] Sent {sent} replies")
                scheduled, status = process_new_messages()
                if scheduled > 0:
                    print(f"[{datetime.now()}] Scheduled {scheduled} replies")
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
    return "Jazmin Bot is running!", 200


@app.route('/callback')
def callback():
    auth_code = request.args.get('code')
    if auth_code:
        return f"Code: {auth_code[:30]}...", 200
    return "No code", 400


@app.route('/set_token', methods=['POST'])
def set_token():
    data = request.json or {}
    refresh = data.get('refresh_token')
    if refresh:
        save_token('refresh_token', refresh)
        access, msg = refresh_fanvue_token()
        return {"saved": True, "test": msg, "access_preview": access[:20] + "..." if access else None}
    return {"error": "No refresh_token"}, 400


@app.route('/trigger')
def trigger():
    token = get_fanvue_token()
    if not token:
        return {"error": "No token"}, 400
    sent = send_due_replies()
    scheduled, status = process_new_messages()
    return {"sent": sent, "scheduled": scheduled, "status": status, "safe_mode": get_safe_mode()}, 200


@app.route('/status')
def status():
    return {"safe_mode": get_safe_mode(), "token_valid": get_fanvue_token() is not None, "polling_active": polling_active}, 200


@app.route('/start_poll')
def start_poll():
    return {"started": start_polling(), "polling_active": polling_active}


@app.route('/stop_poll')
def stop_poll():
    return {"stopped": stop_polling(), "polling_active": polling_active}


@app.route('/toggle_safe_mode')
def toggle_safe_mode():
    current = get_safe_mode()
    new_val = not current
    set_safe_mode(new_val)
    return {"safe_mode": new_val}


@app.route('/fan_profiles')
def fan_profiles():
    profiles = db_query('SELECT * FROM fan_profiles ORDER BY total_messages DESC')
    return {"profiles": profiles, "total": len(profiles) if profiles else 0}


@app.route('/scheduled')
def scheduled():
    pending = db_query("SELECT * FROM scheduled_replies WHERE status = 'pending' ORDER BY scheduled_time ASC")
    return {"pending": pending, "count": len(pending) if pending else 0}


@app.route('/blocked')
def blocked():
    return {"blocked_fans": db_query("SELECT * FROM blocked_fans ORDER BY blocked_at DESC") or []}


@app.route('/paused')
def paused():
    return {"paused_fans": db_query("SELECT chat_id, fan_name, is_paused, paused_until FROM fan_profiles WHERE is_paused = 1 OR paused_until IS NOT NULL") or []}


@app.route('/console')
def console():
    return {
        "safe_mode": get_safe_mode(),
        "blocked_count": len(db_query("SELECT * FROM blocked_fans") or []),
        "paused_count": len(db_query("SELECT * FROM fan_profiles WHERE is_paused = 1 OR paused_until IS NOT NULL") or []),
        "routes": ["/", "/set_token", "/trigger", "/status", "/start_poll", "/stop_poll", "/toggle_safe_mode",
                   "/fan_profiles", "/scheduled", "/blocked", "/paused", "/console",
                   "/telegram_webhook", "/callback"]
    }


@app.route('/telegram_webhook', methods=['POST'])
def telegram_webhook():
    if request.headers.get('content-type') == 'application/json':
        json_string = request.get_data().decode('utf-8')
        update = telebot.types.Update.de_json(json_string)
        bot.process_new_updates([update])
        return '', 200
    return 'Forbidden', 403


@app.route('/telegram_webhook', methods=['GET'])
def telegram_webhook_test():
    return '✅ Telegram webhook active. POST only.', 200


# ========== INIT ==========
init_db()

if bot:
    try:
        bot.remove_webhook()
        time.sleep(0.5)
        domain = os.environ.get('RAILWAY_PUBLIC_DOMAIN', '').strip()
        if domain:
            webhook_url = f"https://{domain}/telegram_webhook"
            bot.set_webhook(url=webhook_url)
            print(f"[OK] Webhook: {webhook_url}")
            send_telegram("🤖 Bot started")
    except Exception as e:
        print(f"[WARN] Webhook failed: {e}")

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))
