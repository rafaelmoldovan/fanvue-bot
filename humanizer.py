"""Jázmin keep-warm HUMANIZER — code-side control of turn shape, length, questions and pacing.

Pure Python, stdlib only, no imports from app.py (so it is unit-testable and safe to hot-swap).
Three layers, called from app.py (see DESIGN.md for hook points):

  1. plan_turn(ctx, rng)            → TurnPlan   (BEFORE generation: what kind of message, how long, question or not)
     render_plan_instruction(plan)  → short Hungarian per-turn instruction for the model
     select_examples(...)           → few-shot pairs from the owner's real replies matching the planned turn type
  2. postprocess(reply, plan, ...)  → FilterResult (AFTER generation: deterministic clean-up + "regenerate once?" flag)
  3. pacing: sample_read_delay / sample_reply_window / sample_bubble_gap / maybe_followup_delay / Presence

Compliance paths (direct AI question, meetup, minors) are NEVER reshaped: the planner returns a bypass plan and the
post-filters leave those replies untouched apart from whitespace.
"""
from __future__ import annotations

import json
import math
import random
import re
import unicodedata
from dataclasses import dataclass, field, asdict
from typing import Iterable, Optional, Sequence

# ─────────────────────────────────────────────────────────────────────────────
# 0. OWNER STATISTICS (measured from KEEPWARM_EXAMPLES in app.py, n=34 real replies by Rafael)
#    median 6 words, mean 6.9, 13/34 contain "?" (but mostly "és te?"-type or as the whole message).
#    The bot in zsolt_transcript.txt: mean 16.6 words, 30s reply gap almost every time.
# ─────────────────────────────────────────────────────────────────────────────
OWNER_REPLY_WORDS = [2, 2, 3, 3, 3, 3, 4, 5, 5, 5, 6, 6, 6, 6, 6, 6, 6, 6, 6, 7, 7, 7, 7, 7, 7, 8, 8, 9, 11, 12, 12, 13, 14, 15]


def word_count(t: str) -> int:
    return len([w for w in re.split(r"\s+", strip_emoji(t or "")) if re.search(r"\w", w)])


def owner_length_distribution(examples: Iterable[tuple[str, str]]) -> list[int]:
    """Recompute OWNER_REPLY_WORDS from (fan, reply) pairs, e.g. app.KEEPWARM_EXAMPLES."""
    out = sorted(word_count(r) for _, r in examples if (r or "").strip())
    return out or list(OWNER_REPLY_WORDS)


# ─────────────────────────────────────────────────────────────────────────────
# Text helpers
# ─────────────────────────────────────────────────────────────────────────────
_EMOJI = re.compile(r"[\U0001F000-\U0001FAFF☀-➿⬀-⯿️‍]")
_SMILEY = re.compile(r"(?<!\w)[:;]-?[)(DPp]+|<3|\bxd+\b", re.I)


def strip_emoji(t: str) -> str:
    return _SMILEY.sub(" ", _EMOJI.sub(" ", t or ""))


def deaccent(t: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFKD", t or "") if not unicodedata.combining(c))


def norm(t: str) -> str:
    """lowercase, no accents, no emoji/punctuation — for comparisons only."""
    t = deaccent(strip_emoji(t)).lower()
    return re.sub(r"\s+", " ", re.sub(r"[^\w\s]", " ", t)).strip()


def words(t: str) -> list[str]:
    return norm(t).split()


_STOP = set("""a az egy es is de hogy mert ha meg mar meg csak nem igen se sem van volt lesz vagy te en o mi ti ok
nekem neked neki velem veled vele rad ram ra tole tolem hat na jaj jajj aww haha hehe hmm ugye akkor ez azt ezt itt ott
mint most majd kicsit nagyon tenyleg szerintem pedig igy ugy ilyen olyan amit ami aki sok kis mas minden semmi
""".split())


def stems(t: str, n: int = 4) -> set[str]:
    return {w[:n] for w in words(t) if (len(w) >= 3 or w.isdigit()) and w not in _STOP}


def ngrams(t: str, n: int) -> set[tuple[str, ...]]:
    w = words(t)
    return {tuple(w[i:i + n]) for i in range(len(w) - n + 1)}


# ─────────────────────────────────────────────────────────────────────────────
# 1. FEATURES of the fan's message
# ─────────────────────────────────────────────────────────────────────────────
_Q_START = re.compile(r"^(es\s+)?(te\s+|neked\s+)?(mi|mit|mik|miert|mikor|hol|honnan|hova|hogyan|hogy\s+(vagy|telt|aludtal|sikerult|van)|milyen|"
                      r"melyik|mennyi|hany|ki|kit|kivel|kinek|van\s+(valami|kedved|barat|hazi)|szoktal|szeretsz|szeretnel|"
                      r"tudsz|lesz|volt|szerinted|mizu|mizus|miujsag|mi\s+ujsag)\b")
_Q_TAIL = re.compile(r"\b\w+-e\b|\bes\s+(te|neked|veled|nalad)\s*$")
_ACK = re.compile(r"^(ok|oke|okes|oksi|okay|ja+|jaja|aha+|igen|ige+n|jo|jol|ertem|cuki+|cukii+|haha+|hehe+|xd+|szuper|"
                  r"nagyon jo|naon jo|koszi+|koszonom|tenyleg|komoly|ugyes|aww+|persze|rendben|nice|az lehet|lehet|"
                  r"hat igen|biztos|igazad van|valoban|jo ejt|szep almokat)( ([a-z]+))?$")
_FLIRT3 = re.compile(r"rosszalkod|kivanlak|szexel|\bszex\b|meztelen|punci|cici|\bmell(ek|eid\w*|em|ed|bimbo\w*)?\b|fasz|dug(j|n|ni|unk)|baszn|horny|\bnude|"
                     r"naked|\bfuck|szopn|felizgat|gerjed|merev|sexting|popsi|fenek|segg")
_FLIRT2 = re.compile(r"dogos|szexi|csok|puszil|agyba|olel|bujn|imadlak|gyonyoru|csinos|szep vagy|hianyz|kivancsi vagyok rad|"
                     r"megmutat|mutass|mutassam")
_FLIRT1_EMO = re.compile("[😘😍🥰💋❤😏🔥😚💕💖]")
_FLIRT1 = re.compile(r"\b(edes|edesem|cica|baba|szivem|szepsegem|kedvesem|cuki vagy|aranyos vagy|helyes)\b")
_SAD = re.compile(r"szomoru|rossz nap|rossz napom|stressz|kimerult|beteg|faj |fajt|kikaptam|elajult|meghalt|szakit|maganyos|"
                  r"egyedul vagyok|nem bir|kivagyok|sirok|sirtam|depi|gaz nap")
_SAD_EMO = re.compile(r"[😢😭🥺☹😔😞😟]|:\(")


@dataclass
class FanFeatures:
    asked_question: bool
    ack: bool
    n_words: int
    flirt: int          # 0..3
    sad: bool
    energy: int         # 0 low, 1 normal, 2 high (emoji/!/long)
    emoji_only: bool = False


def fan_features(fan_text: str) -> FanFeatures:
    raw = fan_text or ""
    lines = [l.strip() for l in raw.replace("[+]", "\n").split("\n") if l.strip()]
    last = lines[-1] if lines else raw
    n_all = norm(raw)
    asked = any(("?" in l) or _Q_START.search(norm(l)) or _Q_TAIL.search(norm(l)) for l in lines[-2:] or [raw])
    nw = word_count(raw)
    emoji_only = nw == 0 and bool(_EMOJI.search(last) or _SMILEY.search(last))
    if emoji_only:
        asked = False
    ack = emoji_only or ((not asked) and nw <= 3 and bool(_ACK.match(norm(last))))
    flirt = 3 if _FLIRT3.search(n_all) else 2 if _FLIRT2.search(n_all) else \
        1 if (_FLIRT1_EMO.search(raw) or _FLIRT1.search(n_all)) else 0
    sad = bool(_SAD.search(n_all + " ") or _SAD_EMO.search(raw))
    emo = len(_EMOJI.findall(raw)) + raw.count("!")
    energy = 2 if (emo >= 2 or nw >= 30) else 0 if (nw <= 3 and emo == 0) else 1
    return FanFeatures(bool(asked), ack, nw, flirt, sad, energy, emoji_only)


def is_question_text(t: str) -> bool:
    return "?" in strip_emoji(t or "")


# ─────────────────────────────────────────────────────────────────────────────
# 2. TURN PLANNER
# ─────────────────────────────────────────────────────────────────────────────
TURN_TYPES = ("react", "short", "answer_share", "tease", "initiative", "question", "two_bubble", "skip")
TYPE_WORD_RANGE = {
    "react": (1, 4), "short": (3, 9), "answer_share": (6, 15), "tease": (3, 10), "initiative": (5, 14),
    "question": (3, 9), "two_bubble": (4, 14), "skip": (0, 0), "locked": (6, 25),
}
BASE_WEIGHTS = {"react": 0.14, "short": 0.24, "answer_share": 0.14, "tease": 0.10, "initiative": 0.10,
                "question": 0.40, "two_bubble": 0.10, "skip": 0.0}
QUESTION_BUDGET = (1, 2)          # never two questions in a row (her last message); the weights keep the share ~25-30 %
PLAIN_SPELLING_P = 0.8            # Rafael strips ö/ő/ü/ű in practically every message (FORENSICS: 0 % of his replies have them)
BYPASS_LOCKS = ("ai", "meetup", "minor")      # compliance turns: humanizer does not touch them

# Initiative seeds: tiny, concrete "from her day" items. topic is used for anti-repetition.
INITIATIVE_SEEDS = {
    "morning": [("most keltem, még kómás vagyok", "reggel"), ("kávézok épp az ablakban", "kave"),
                ("ma reggel elaludtam, rohanás volt", "reggel")],
    "day": [("most jöttem a boltból, elfelejtettem amiért mentem", "bolt"), ("épp takarítok és utálom", "takaritas"),
            ("elszakadt a fülhallgatóm, tragédia", "zene"), ("a szomszéd megint fúr valamit", "szomszed"),
            ("most mosok, a fél lakás tele ruhával", "takaritas")],
    "evening": [("főztem valamit, ami félig sikerült", "fozes"), ("most néztem egy részt a Bridgertonból", "sorozat"),
                ("edzésről jöttem, hullafáradt vagyok", "edzes"), ("kiöntöttem a teát a pólómra", "baleset"),
                ("rendeltem kaját, mert lusta voltam főzni", "fozes")],
    "night": [("nem tudok aludni", "alvas"), ("már ágyban vagyok, csak telefonozok", "alvas")],
}

# Persona topics: a topic she brought up in her last 4 messages is not brought up again unless HE mentions it.
# Topics in DAILY_TOPICS are also "spent" for the rest of the chat-day once she told them (see told_today).
PERSONA_TOPICS = {
    "menhely": r"menhely|orokbe",
    "fozes": r"\bfozt(em|ok)\b|\bfozok\b|fozni szoktam|szeretek fozni|imadok fozni|recept|sutottem",
    "edzes": r"edzeni voltam|edzesrol jottem|konditerem|\bkondi",
    "albi": r"alberlet|lakber|egyedul (lakom|elek)",
    "ex": r"megcsal|exem|volt baratom",
    "csalad": r"szuleim|anyu|apu|veszeked",
    "sorozat": r"sorozat|bridgerton|netflix|wednesday|squid",
    "pihenes": r"lustalkod|pihentem|pihenek",
    "suli": r"turisztik|vendeglat|iden vegeztem|vegeztem a suli|befejeztem a sulit",
    "munka": r"nem dolgozom|most nem dolgozom|pincer|munkat keres|melot keres",
}
DAILY_TOPICS = ("menhely", "fozes", "albi", "ex", "csalad", "suli", "munka", "edzes")
TOPIC_LABELS_HU = {"menhely": "a menhely / kutyák", "fozes": "a főzés", "edzes": "az edzés", "albi": "az albérlet / egyedül élés",
                   "ex": "az exed", "csalad": "a családod", "sorozat": "a sorozatok", "pihenes": "a pihenés / lustálkodás",
                   "suli": "hogy idén végeztél", "munka": "hogy most nem dolgozol"}

# Rafael's real emoji palette (keep-warm only; the selling bot's EMOJI_OK is much narrower)
KEEPWARM_EMOJI = set("😋🥰😍😘🤪😝😆🤭❤😊😄😂😉🙈😏🙂😅🥺🤗😌😇🫣💕😜")


def topics_in(text: str) -> set[str]:
    return recent_topics([text or ""])


@dataclass
class TurnContext:
    fan_text: str                               # the batch being answered (lines joined by \n)
    her_recent: Sequence[str] = ()              # her last messages, oldest → newest (≥ 12 recommended)
    recent_turns: Sequence[str] = ()            # planned types of her last turns, oldest → newest (state)
    hour: int = 15                              # Budapest hour
    fan_msg_count: int = 20                     # total fan messages in the chat (phase)
    secs_since_her_last: Optional[float] = None # how long he took to answer her last message (hotness)
    lock: Optional[str] = None                  # 'ai' | 'meetup' | 'minor' | 'content' | 'advance' | None


@dataclass
class TurnPlan:
    turn_type: str
    max_words: int
    allow_question: bool
    bubbles: int = 1
    seed: str = ""
    seed_topic: str = ""
    spelling: str = "plain"       # 'plain' (ö/ő→o, ü/ű→u like Rafael) | 'accent'
    lowercase: bool = True
    bypass: bool = False
    skip: bool = False
    he_asked: bool = False
    sad: bool = False
    flirt: int = 0
    emoji_only: bool = False
    weights: dict = field(default_factory=dict)   # debug

    def to_json(self) -> str:
        return json.dumps(asdict(self), ensure_ascii=False)


def daypart(h: int) -> str:
    return "night" if (h >= 23 or h < 6) else "morning" if h < 10 else "day" if h < 17 else "evening"


def questions_in(msgs: Sequence[str]) -> int:
    return sum(1 for m in msgs if is_question_text(m))


def recent_topics(msgs: Sequence[str]) -> set[str]:
    s = norm(" ".join(msgs))
    return {k for k, rx in PERSONA_TOPICS.items() if re.search(rx, s)}


def _weighted_choice(weights: dict, rng: random.Random) -> str:
    items = [(k, w) for k, w in weights.items() if w > 0]
    tot = sum(w for _, w in items)
    x = rng.random() * tot
    for k, w in items:
        x -= w
        if x <= 0:
            return k
    return items[-1][0]


def sample_length(turn_type: str, rng: random.Random, f: Optional[FanFeatures] = None, hour: int = 15,
                  dist: Sequence[int] = OWNER_REPLY_WORDS) -> int:
    lo, hi = TYPE_WORD_RANGE[turn_type]
    if hi == 0:
        return 0
    n = rng.choice(list(dist)) + rng.choice((-1, 0, 0, 1))
    if f is not None:
        if f.n_words >= 25: n += rng.randint(1, 4)
        if f.sad: n += 3
        if f.n_words <= 3: n -= 1
    if daypart(hour) == "night":
        n = int(round(n * 0.8))
    return max(lo, min(hi, n))


def plan_turn(ctx: TurnContext, rng: Optional[random.Random] = None) -> TurnPlan:
    rng = rng or random.Random()
    f = fan_features(ctx.fan_text)
    her = list(ctx.her_recent)
    turns = list(ctx.recent_turns)
    spelling = "plain" if rng.random() < PLAIN_SPELLING_P else "accent"
    lowercase = rng.random() < 0.85

    # Compliance turns: existing app.py logic decides everything.
    if ctx.lock in BYPASS_LOCKS:
        return TurnPlan("locked", TYPE_WORD_RANGE["locked"][1], True, bypass=True, spelling=spelling,
                        lowercase=lowercase, he_asked=f.asked_question, sad=f.sad, flirt=f.flirt)

    her_last_q = bool(her) and is_question_text(her[-1])
    q_budget_used = questions_in(her[-(QUESTION_BUDGET[1] - 1):]) >= QUESTION_BUDGET[0] if her else False
    can_ask = not her_last_q and not q_budget_used

    w = dict(BASE_WEIGHTS)
    # ── he asked → answer it; usually no counter-question ──
    if f.asked_question:
        w["react"] = 0.0; w["skip"] = 0.0; w["question"] = 0.0
        w["short"] *= 1.7; w["answer_share"] *= 1.6; w["initiative"] *= 0.25; w["two_bubble"] *= 0.8
    # ── pure acknowledgement ("ok", "cukii") → react / own initiative / sometimes nothing ──
    if f.ack:
        w["react"] *= 1.5; w["initiative"] *= 2.4; w["answer_share"] *= 0.3; w["question"] *= 1.2
        recent_skip = "skip" in turns[-6:]
        w["skip"] = 0.0 if (recent_skip or her_last_q) else 0.30
    # ── emoji-only ("😘", "😂") → mostly nothing, or an emoji / 1-2 words back ──
    if f.emoji_only:
        w = {k: 0.0 for k in w}
        w["react"] = 1.0
        w["skip"] = 0.0 if her_last_q else (0.35 if turns[-1:] == ["skip"] else 1.1)
    # ── long / high-energy message ──
    if f.n_words >= 25:
        w["react"] *= 0.3; w["answer_share"] *= 1.4; w["two_bubble"] *= 1.4; w["initiative"] *= 0.4
    if f.energy == 2:
        w["two_bubble"] *= 1.3; w["react"] *= 1.1
    # ── flirt: match, escalate one notch (tease) ──
    if f.flirt >= 1:
        w["tease"] *= 1.6; w["initiative"] *= 0.6
    if f.flirt >= 2:
        w["tease"] *= 1.8; w["question"] *= 0.5; w["initiative"] *= 0.5; w["react"] *= 1.2
    if f.flirt >= 3 or ctx.lock in ("content", "advance"):
        w["initiative"] = 0.0; w["question"] *= 0.3; w["tease"] *= 1.5
    # ── sad: caring, no jokes, no skip, one caring question allowed if not asked last time ──
    if f.sad:
        w = {k: 0.0 for k in w}
        w["short"] = 1.0; w["two_bubble"] = 0.4
        w["question"] = 1.3 if not her_last_q else 0.0
    # ── phase ──
    if ctx.fan_msg_count < 6:
        w["skip"] = 0.0; w["initiative"] *= 0.5; w["question"] *= 1.3
    elif ctx.fan_msg_count > 60:
        w["initiative"] *= 1.2
    # ── hot chat (he answers fast) → faster, shorter ──
    if ctx.secs_since_her_last is not None and ctx.secs_since_her_last < 150:
        w["react"] *= 1.3; w["short"] *= 1.2; w["answer_share"] *= 0.8
    # ── time of day ──
    dp = daypart(ctx.hour)
    if dp == "night":
        w["react"] *= 1.3; w["answer_share"] *= 0.7; w["initiative"] *= 0.6
    elif dp == "morning":
        w["initiative"] *= 1.3
    # ── question budget (hard) ──
    if not can_ask and not f.sad:
        w["question"] = 0.0
    elif can_ask and not f.asked_question and "question" not in turns[-4:]:
        w["question"] *= 1.2
    # ── variety: no same shape again and again ──
    if turns:
        if turns[-1] in w: w[turns[-1]] *= 0.55
        if len(turns) >= 2 and turns[-1] == turns[-2] and turns[-1] in w: w[turns[-1]] *= 0.3
        if "initiative" in turns[-3:]: w["initiative"] *= 0.2
        if turns[-1] == "two_bubble": w["two_bubble"] *= 0.4
    if sum(w.values()) <= 0:
        w["short"] = 1.0

    t = _weighted_choice(w, rng)
    plan = TurnPlan(t, sample_length(t, rng, f, ctx.hour), allow_question=False, spelling=spelling,
                    lowercase=lowercase, he_asked=f.asked_question, sad=f.sad, flirt=f.flirt,
                    weights={k: round(v, 3) for k, v in w.items()})
    if t == "skip":
        plan.skip = True
        return plan
    if t == "question":
        plan.allow_question = True
    elif f.asked_question and can_ask and t in ("short", "answer_share") and rng.random() < 0.45:
        plan.allow_question = True          # only a tiny "és te?" tail (instruction says so)
    if t == "two_bubble":
        plan.bubbles = 2
    if t == "initiative":
        used = recent_topics(her[-6:])
        pool = [s for s in INITIATIVE_SEEDS[dp] if s[1] not in used] or INITIATIVE_SEEDS[dp]
        plan.seed, plan.seed_topic = rng.choice(pool)
    if f.emoji_only:
        plan.emoji_only = True; plan.max_words = min(plan.max_words, 2)
    if ctx.lock == "content":           # PPV request in keep-warm: playful "later", never a sale
        plan.turn_type = "tease"; plan.allow_question = False; plan.max_words = min(plan.max_words, 10)
        plan.bubbles = 1
    return plan


# ─────────────────────────────────────────────────────────────────────────────
# 2b. PER-TURN INSTRUCTION + FEW-SHOT SELECTION
# ─────────────────────────────────────────────────────────────────────────────
INSTR = {
    "react": "Most CSAK egy pár szavas reakció (max {n} szó), mint aki közben mást csinál. Nincs kérdés, nincs magyarázat.",
    "short": "Válaszolj röviden, max {n} szó.",
    "answer_share": "Válaszolj, és tegyél hozzá egy apró saját dolgot (mit csinálsz most / mi történt veled). Max {n} szó.",
    "tease": "Cukkold, ugrasd játékosan, egy fokkal pajkosabban mint ő, de nem explicit. Max {n} szó.",
    "initiative": "Ne az ő mondatát folytasd: magadtól dobj be egy apróságot a napodból: „{seed}” (a saját szavaiddal). Max {n} szó.",
    "question": "Pár szavas reakció, utána EGY egyszerű rövid kérdés (pl. „és te?”, „mi történt?”, „miért?”). Összesen max {n} szó.",
    "two_bubble": "KÉT külön üzenet egy-egy sorban: 1. sor 1-3 szavas reakció, 2. sor egy rövid gondolat (max {n2} szó).",
}


def render_plan_instruction(plan: TurnPlan) -> str:
    """The ONLY style instruction the model gets per turn (replaces long rule lists)."""
    if plan.bypass or plan.skip:
        return ""
    s = INSTR[plan.turn_type].format(n=plan.max_words, n2=max(3, plan.max_words - 2), seed=plan.seed)
    if plan.he_asked and plan.turn_type != "initiative":
        s += " Ő kérdezett: válaszolj rá konkrétan."
    if plan.turn_type != "question":
        s += " A végén legfeljebb egy rövid „és te?” mehet." if plan.allow_question else " NE kérdezz."
    if plan.sad:
        s += " Rossz napja van: kedves, gondoskodó, ne viccelődj."
    elif plan.flirt >= 2:
        s += " Flörtöl: menj bele játékosan az ő szintjén, ne hűtsd le, ne terelj."
    if plan.emoji_only:
        s += " Elég egy emoji is."
    s += " Ne ismételd vissza a szavait, ne értékeld amit írt."
    return "🎯 MOST: " + s


def classify_example(fan: str, reply: str) -> str:
    n = word_count(reply)
    if "\n" in (reply or ""):
        return "two_bubble"
    if is_question_text(reply):
        return "question"
    if fan_features(fan).flirt >= 1 and n <= 10:
        return "tease"
    if n <= 4:
        return "react"
    if re.search(r"\b(most|epp|vettem|keltem|jottem|orulok)\b", norm(reply)) and not fan_features(fan).asked_question:
        return "initiative"
    return "short" if n <= 9 else "answer_share"


_COMPAT = {"react": ("react",), "short": ("short", "react"), "answer_share": ("answer_share", "initiative", "short"),
           "tease": ("tease",), "initiative": ("initiative", "answer_share"), "question": ("question",),
           "two_bubble": ("two_bubble", "react"), "locked": ("short", "answer_share")}


def select_examples(examples: Sequence[tuple[str, str]], plan: TurnPlan, fan_text: str, k: int = 6,
                    rng: Optional[random.Random] = None) -> list[tuple[str, str]]:
    """~4 owner examples whose shape matches the planned turn + 2 nearest by topic. Small jitter = variety."""
    rng = rng or random.Random()
    fs = stems(fan_text)

    def score(p):
        return len(fs & stems(p[0])) + rng.random() * 0.8

    want = _COMPAT.get(plan.turn_type, ("short",))
    if not plan.allow_question:
        pool_ok = lambda p: not is_question_text(p[1])
    else:
        pool_ok = lambda p: True
    match = sorted([p for p in examples if classify_example(*p) in want and pool_ok(p)], key=score, reverse=True)
    picked = match[: max(0, k - 2)]
    rest = sorted([p for p in examples if p not in picked and pool_ok(p)], key=score, reverse=True)
    picked += rest[: k - len(picked)]
    rng.shuffle(picked)
    return picked


# ─────────────────────────────────────────────────────────────────────────────
# 3. POST-FILTERS (deterministic)
# ─────────────────────────────────────────────────────────────────────────────
PROTECT = re.compile(r"\bAI\b|nem tal[áa]lkoz|kiskor[úu]", re.I)    # never dropped by any filter

VALIDATION_OPENERS = [
    r"na ez (nagyon |tenyleg )?(jol esett|erdekel|tetszik)( amit (mondtal|irtal))?",
    r"ez (nagyon |tenyleg |igazan )?(szepen hangzik|jol hangzik|erdekesen hangzik|izgalmasan hangzik)",
    r"latszik (rajtad |rajta )?hogy [^,.!?]*",
    r"(jaj |jajj )?de jo hogy [^,.!?]*",
    r"(teljesen |abszolut )?igazad van",
    r"(teljesen )?(ertheto|megertem|ertem)( hogy [^,.!?]*)?",
    r"(hat |hat az )?(az |ez )?mar tenyleg (kemeny|durva|szep|komoly) [^,.!?]*",
    r"szerintem is igy (az igazi|van)",
    r"ez (nagyon )?(szep|edes|aranyos) (tole|toled|gondolat)",
]
_VALID_RX = re.compile(r"^(" + "|".join(VALIDATION_OPENERS) + r")\s*$")

BANNED_HU = [  # assistant/therapist tells (compared accent-free, lowercase)
    r"teljesen ertheto", r"megertelek", r"jo kerdes", r"nagyon ertekes", r"a lenyeg,? hogy", r"fontos,? hogy",
    r"orulok,? hogy (ezt )?megosztottad", r"koszonom,? hogy elmondtad", r"ez teljesen normalis", r"szivesen beszelgetek",
    r"meselj (meg|tobbet)", r"ez izgalmasan hangzik", r"ez erdekesen hangzik",
    r"oszinten szolva", r"mindenesetre", r"raadasul", r"tudod mit\??", r"remelem,? hogy", r"barmikor itt vagyok",
    r"szerintem a legtobben", r"nem kell aggodnod", r"ez tetszik benned", r"latszik hogy (erzelmes|kitarto|okos)",
    r"ne legyel ilyen szigoru magaddal", r"\b(a )?(fiuk|ferfiak|lanyok|emberek|pasik) (szerintem )?(mind|mindig|altalaban)\b", r"\bplede?\b", r"ez a karakterem",
]
_BANNED_RX = re.compile("|".join(BANNED_HU))

INTERJECTIONS = {"jaj", "jajj", "jaaj", "hmm", "na", "aww", "awww", "haha", "hehe", "hat", "komolyan", "jaa", "ohh",
                 "uh", "ah", "ahj", "igen", "igeen", "hihi", "huha"}
_DANGLING_WORDS = {"de", "es", "hogy", "mert", "ha", "vagy", "meg", "is", "akkor", "pedig", "mint", "aztan", "szoval",
                   "az", "a", "egy", "csak", "mielott", "amikor", "ami", "amit", "aki", "mig"}


def strip_dangling(t: str) -> str:
    toks = t.rstrip(" ,;-").split()
    while len(toks) > 1 and norm(toks[-1]) in _DANGLING_WORDS:
        toks.pop()
    return " ".join(toks).rstrip(" ,;-")

# small re-accent map for á/é/í (Rafael keeps these; the model sometimes drops ALL accents)
_REACCENT = {"mar": "már", "tenyleg": "tényleg", "edes": "édes", "edesem": "édesem", "szep": "szép", "igy": "így",
             "kemeny": "kemény", "eves": "éves", "kerdes": "kérdés", "nalad": "nálad", "hianyzol": "hiányzol",
             "eppen": "éppen", "epp": "épp", "ejt": "éjt", "estet": "estét", "napot": "napot", "vagyal": "vágyál"}
_PLAIN_MAP = str.maketrans({"ö": "o", "ő": "o", "ü": "u", "ű": "u", "Ö": "O", "Ő": "O", "Ü": "U", "Ű": "U"})
_PLAIN_WORDS = re.compile(r"\b(koszonom|koszi|jol|orulok|tobb|rolad|tortent|szoritok|kuldom|fugg|szunet|tole|fuggetlen|"
                          r"kozel|kulon|ugyes|uzenet|hulye|turelem|elotte|utana|onnan|szerelem)\b", re.I)


@dataclass
class FilterResult:
    text: str
    bubbles: list
    needs_regen: bool
    reasons: list
    regen_hint: str = ""


def split_units(text: str) -> list[str]:
    """Sentence-ish units: split after . ! ? … and after a smiley/emoji that is followed by more text."""
    t = re.sub(r"\s+", " ", text or "").strip()
    if not t:
        return []
    t = re.sub(r"((?<!\w)[:;]-?[)(DPp]+|[\U0001F000-\U0001FAFF☀-➿]️?)\s+(?=\w)", lambda m: m.group(1) + "\x00", t)
    t = re.sub(r"(?<=[.!?…])\s+", lambda m: "\x00", t)
    return [u.strip() for u in t.split("\x00") if u.strip()]


def split_clauses(unit: str) -> list[str]:
    return [c for c in re.split(r",\s+", unit) if c.strip()]


def _join_clauses(cl: list[str]) -> str:
    return ", ".join(c.strip() for c in cl if c.strip())


def _content(u: str) -> bool:
    return bool(re.search(r"\w", strip_emoji(u)))


def _filter_clauses(unit: str, bad) -> tuple[str, bool]:
    """Remove offending clauses without leaving fragments: a bad LEADING clause is dropped; a bad later clause cuts
    the rest of the unit (what follows usually depends on it). Protected clauses are never bad."""
    cl = split_clauses(unit)
    keep, hit = [], False
    for c in cl:
        if not PROTECT.search(c) and bad(c):
            hit = True
            if keep:
                break
            continue
        keep.append(c)
    return _join_clauses(keep), hit


def strip_validation_openers(units: list[str], reasons: list) -> list[str]:
    if not units:
        return units
    first = units[0]
    cl = split_clauses(first)
    if len(cl) >= 2 and not PROTECT.search(cl[0]) and _VALID_RX.match(norm(cl[0])):
        reasons.append("validation_opener")
        rest = _join_clauses(cl[1:])
        return [rest] + units[1:]
    if len(units) >= 2 and not PROTECT.search(first) and _VALID_RX.match(norm(first)):
        reasons.append("validation_opener")
        return units[1:]
    return units


_GREETING = re.compile(r"^(szia+|sziu+s*|szius+|hali+|hello+|helo+|hey+|heyy+|cs[ao]+|jo reggelt|jo estet|jo ejt|puszi\w*)\b")


def strip_echo_start(units: list[str], fan_text: str, reasons: list) -> list[str]:
    """'vasárnap, akkor addig...' after 'A következő az vasárnap lesz' → drop the echoed lead clause."""
    if not units:
        return units
    cl = split_clauses(units[0])
    if len(cl) < 2:
        return units
    lead = cl[0]
    ls, fs = stems(lead), stems(fan_text)
    if _GREETING.match(norm(lead)):                  # "szia" back to "szia" is not mirroring
        return units
    if 1 <= len(words(lead)) <= 4 and ls and ls <= fs and not PROTECT.search(lead):
        reasons.append("echo_start")
        return [_join_clauses(cl[1:])] + units[1:]
    return units


def mirror_score(clause: str, fan_text: str) -> tuple[int, float]:
    cs, fs = stems(clause), stems(fan_text)
    shared = cs & fs
    tri = len(ngrams(clause, 3) & ngrams(fan_text, 3))
    return len(shared) + tri, (len(shared) / max(1, len(cs)))


def drop_mirroring(units: list[str], fan_text: str, reasons: list) -> list[str]:
    def bad(c):
        shared, ratio = mirror_score(c, fan_text)
        return shared >= 2 and ratio >= 0.5
    return _apply(units, bad, "mirror", reasons)


def _apply(units: list[str], bad, reason: str, reasons: list) -> list[str]:
    out = []
    for u in units:
        nu, hit = _filter_clauses(u, bad)
        if hit:
            reasons.append(reason)
        if nu.strip():
            out.append(nu)
    return out


def drop_banned(units: list[str], reasons: list) -> list[str]:
    return _apply(units, lambda c: bool(_BANNED_RX.search(norm(c))), "banned_phrase", reasons)


def drop_self_repeats(units: list[str], her_recent: Sequence[str], reasons: list, n: int = 4) -> list[str]:
    """Catch-phrases she already used ('nekem általában igazam szokott lenni', 'nem is volt olyan nehéz, ugye')."""
    seen = set()
    for m in list(her_recent)[-30:]:
        seen |= ngrams(m, n)
    return _apply(units, lambda c: bool(ngrams(c, n) & seen), "self_repeat", reasons)


def drop_stale_persona(units: list[str], fan_text: str, her_recent: Sequence[str], reasons: list,
                       allowed_topic: str = "", told_today: Iterable[str] = ()) -> list[str]:
    """A persona topic she used in her last 4 messages, or (DAILY_TOPICS) already told him today, is not brought up
    again unless HE mentions it now (or it is the planned initiative seed)."""
    used = recent_topics(list(her_recent)[-4:]) | {t for t in (told_today or ()) if t in DAILY_TOPICS}
    fan_topics = recent_topics([fan_text])
    stale = {t for t in used if t not in fan_topics and t != allowed_topic}
    if not stale:
        return units
    return _apply(units, lambda c: any(re.search(PERSONA_TOPICS[t], norm(c)) for t in stale), "stale_persona", reasons)


def enforce_question_budget(units: list[str], allow_question: bool, reasons: list) -> tuple[list[str], bool]:
    """Returns (units, impossible). Over budget → drop question units; if the reply is ONLY questions → impossible."""
    qs = [i for i, u in enumerate(units) if is_question_text(u) and not PROTECT.search(u)]
    if allow_question:
        if len(qs) > 1:
            reasons.append("too_many_questions")
            drop = set(qs[:-1])
            nonq = [u for i, u in enumerate(units) if i not in drop]
            return nonq, False
        return units, False
    if not qs:
        return units, False
    reasons.append("question_over_budget")
    units = list(units)
    for i in qs:                       # "..., te dolgozol holnap?" → drop just the trailing question clause
        cl = split_clauses(units[i])
        if len(cl) >= 2 and not _Q_START.search(norm(cl[0])) and not is_question_text(_join_clauses(cl[:-1])):
            units[i] = _join_clauses(cl[:-1])
    qs = [i for i, u in enumerate(units) if is_question_text(u) and not PROTECT.search(u)]
    rest = [u for i, u in enumerate(units) if i not in qs]
    if any(_content(u) for u in rest):
        return rest, False
    # Only questions. A short rhetorical echo ("komolyan?") can become an exclamation.
    if len(units) == 1 and word_count(units[0]) <= 2:
        return [units[0].replace("?", "")], False
    return units, True


_TE_Q = re.compile(r"^(es\s+)?(te|neked|veled|nalad|naladd|ti)\b")


def break_template(units: list[str], reasons: list) -> list[str]:
    """'reaction + elaboration/restating + te ...?' (the bot's signature shape, FORENSICS #10): when a question is
    allowed, keep only a short lead (first clause, if it is a reaction of <= 4 words) + the question clause."""
    if not units or not is_question_text(units[-1]) or PROTECT.search(" ".join(units)):
        return units
    q_cl = split_clauses(units[-1])
    question = q_cl[-1] if is_question_text(q_cl[-1]) else units[-1]
    before = [c for u in units[:-1] for c in split_clauses(u)] + (q_cl[:-1] if question is q_cl[-1] else [])
    before = [c for c in before if _content(c)]
    if len(before) < 2 and word_count(" ".join(before)) <= 6:
        return units
    reasons.append("template")
    lead = (before[0] if before and word_count(before[0]) <= 4 else "").strip().rstrip(" ,")
    q = question.strip()
    if not lead:
        return [q]
    return [lead + (", " if re.search(r"\w$", lead) else " ") + q]


def clamp_length(text: str, max_words: int, reasons: list, tolerance: float = 1.25) -> str:
    """Trim to ~max_words at unit, then clause boundaries. A hard word cut also adds 'too_long_hard' (→ regen)."""
    if max_words <= 0 or word_count(text) <= int(math.ceil(max_words * tolerance)) or PROTECT.search(text):
        return text
    reasons.append("too_long")
    units = split_units(text)
    out = ""
    for u in units:                                  # whole units first
        cand = (out + " " + u).strip()
        if word_count(cand) > max_words and out:
            break
        out = cand
        if word_count(out) >= max_words:
            break
    if word_count(out) > int(math.ceil(max_words * tolerance)):   # first unit alone too long → clauses
        cl = split_clauses(out)
        acc = cl[0]
        for c in cl[1:]:
            if word_count(acc + ", " + c) > max_words:
                break
            acc = acc + ", " + c
        out = acc
    if word_count(out) > int(math.ceil(max_words * tolerance)):   # still → hard cut on words
        toks = out.split()
        kept, n = [], 0
        for tk in toks:
            if re.search(r"\w", strip_emoji(tk)):
                n += 1
            if n > max_words:
                break
            kept.append(tk)
        out = " ".join(kept)
        reasons.append("too_long_hard")
    return strip_dangling(out)


def same_opener(a: str, b: str) -> bool:
    """Same leading interjection ('jaj'/'jaj') or same first two words ('akkor addig ...')."""
    wa, wb = words(a), words(b)
    if not (wa and wb):
        return False
    return (wa[0] == wb[0] and wa[0] in INTERJECTIONS) or (len(wa) > 1 and len(wb) > 1 and wa[:2] == wb[:2])


def fix_opener(text: str, her_last: str, reasons: list) -> tuple[str, bool]:
    """Returns (text, still_same). Drops a repeated leading interjection; otherwise reports it."""
    if not her_last or not same_opener(text, her_last):
        return text, False
    w0 = words(text)[0] if words(text) else ""
    if w0 in INTERJECTIONS:
        new = re.sub(r"^\W*\w+[\s,!.]*", "", text, count=1).strip()
        if word_count(new) >= 1:
            reasons.append("same_opener_stripped")
            return new, same_opener(new, her_last)
    reasons.append("same_opener")
    return text, True


def unify_spelling(text: str, mode: str, reasons: list) -> str:
    """One spelling style per message. 'plain' = ö/ő→o, ü/ű→u (Rafael), á/é/í kept. Mixed messages become plain."""
    has_long = bool(re.search(r"[öőüűÖŐÜŰ]", text))
    has_plain = bool(_PLAIN_WORDS.search(text))
    if mode == "plain" or (has_long and has_plain):
        if has_long:
            if mode != "plain":
                reasons.append("spelling_mixed")
            text = text.translate(_PLAIN_MAP)
    # re-accent á/é/í words the model stripped (applies in both modes)
    def _re(m):
        w = m.group(0)
        r = _REACCENT.get(w.lower())
        if not r:
            return w
        return r if w.islower() else r[0].upper() + r[1:]
    text = re.sub(r"\b[A-Za-z]+\b", _re, text)
    # "hat" at the start of a clause is "hát" in chat
    text = re.sub(r"(^|[,.!?]\s+)hat\b(?!\s+(eves|ora|perc|het|nap|ev))", lambda m: m.group(1) + "hát", text)
    return text


def finalize_case_punct(text: str, lowercase: bool) -> str:
    t = re.sub(r"\s*[—–]\s*", ", ", text).strip()
    t = re.sub(r"\s+([,.!?])", r"\1", t)
    t = re.sub(r",\s*,", ",", t)
    t = re.sub(r"(?<![.])\.$", "", t).strip()              # Rafael rarely ends with a period ('...' kept)
    t = t.strip(" ,;")
    if lowercase and t[:1].isupper() and not t[:2].isupper() and not t.startswith("AI"):
        t = t[0].lower() + t[1:]
    return t


def postprocess(reply: str, plan: TurnPlan, fan_text: str, her_recent: Sequence[str] = (),
                told_today: Iterable[str] = ()) -> FilterResult:
    reasons: list = []
    raw = (reply or "").strip().strip('"').strip()
    raw = re.sub(r"^\s*(j[áa]zmin|v[áa]lasz|bot)\s*:\s*", "", raw, flags=re.I)
    if plan.bypass or PROTECT.search(raw):       # compliance content (AI yes / no meetup / minor) is never reshaped
        t = re.sub(r"\s+", " ", raw).strip()
        return FilterResult(t, [t] if t else [], not t, ["bypass"])

    # bubbles
    lines = [l.strip() for l in raw.split("\n") if l.strip()]
    if plan.bubbles == 2 and len(lines) >= 2:
        parts = [lines[0], " ".join(lines[1:])]
    else:
        parts = [" ".join(lines)]

    her = list(her_recent)
    out_parts = []
    impossible = False
    for pi, part in enumerate(parts):
        units = split_units(part)
        if pi == 0:
            units = strip_validation_openers(units, reasons)
            units = strip_echo_start(units, fan_text, reasons)
        units = drop_mirroring(units, fan_text, reasons)
        units = drop_banned(units, reasons)
        units = drop_self_repeats(units, her, reasons)
        if not plan.he_asked:          # he asked about her → she may answer even with a topic she already used
            units = drop_stale_persona(units, fan_text, her, reasons, allowed_topic=plan.seed_topic,
                                       told_today=told_today)
        q_ok = plan.allow_question and pi == len(parts) - 1
        units, imp = enforce_question_budget(units, q_ok, reasons)
        if q_ok and not plan.he_asked:     # he asked → "answer + és te?" is fine; otherwise no reaction+restate+question
            units = break_template(units, reasons)
        impossible |= imp
        p = ""
        for u in (u for u in units if _content(u) or len(units) == 1):
            p = (p + (", " if p and re.search(r"\w$", p) else " ") + u).strip() if p else u
        p = p.strip()
        if p:
            out_parts.append(p)

    if len(out_parts) == 2:
        b1 = clamp_length(out_parts[0], 4, reasons, tolerance=1.5)
        b2 = clamp_length(out_parts[1], max(3, plan.max_words - word_count(b1)), reasons)
        out_parts = [b1, b2]
    elif out_parts:
        out_parts = [clamp_length(out_parts[0], plan.max_words, reasons)]

    still_same = False
    if out_parts and her:
        out_parts[0], still_same = fix_opener(out_parts[0], her[-1], reasons)
    out_parts = [finalize_case_punct(unify_spelling(p, plan.spelling, reasons), plan.lowercase) for p in out_parts]
    out_parts = [p for p in out_parts if _content(p) or _EMOJI.search(p)]

    text = "\n".join(out_parts)
    needs = (not text) or impossible or still_same or (word_count(text) == 0 and not _EMOJI.search(text)) \
        or ("too_long_hard" in reasons)
    hint = ""
    if needs:
        bits = []
        if impossible: bits.append("most NE kérdezz, csak reagálj vagy mesélj")
        if still_same: bits.append("ne ugyanazzal a szóval kezdd, mint az előző üzeneted")
        if not text: bits.append("ne ismételd vissza amit írt és ne értékeld, írj valami saját, rövid reakciót")
        if "too_long_hard" in reasons: bits.append("sokkal rövidebben")
        hint = "FONTOS: az előző változat nem volt jó — " + "; ".join(bits) + f". Max {plan.max_words} szó."
    return FilterResult(text, out_parts, needs, reasons, hint)


# ─────────────────────────────────────────────────────────────────────────────
# 4. OPTIONAL CRITIC (prompt builder + parser; the call itself lives in app.py)
# ─────────────────────────────────────────────────────────────────────────────
CRITIC_SYSTEM = (
    "You judge ONE chat message written as \"Jázmin\", a 19-year-old girl from Budapest texting a guy on her phone, "
    "in casual Hungarian. Decide if it reads like a real 19-year-old texting, or like an AI/chatbot. "
    "Owner's real replies for reference (median 6 words): \"Mint a bunda 😝 és te?\", \"deritsd ki :)\", "
    "\"szoritok neked :)\", \"na mesélj\", \"Vesziteni is tudni kell 😆 biliárdozni szeretsz?\".\n"
    "Tells to flag (use these exact codes): MIRROR (repeats/paraphrases his words), VALIDATE (praises or evaluates what he "
    "said: 'ez szépen hangzik', 'látszik hogy...'), INTERVIEW (ends with a follow-up question when it didn't need one), "
    "TEMPLATE (reaction + elaboration + question shape), TOO_LONG (over the word limit), THERAPIST (advice, reassurance, "
    "generalisations), FORMAL (assistant phrasing, perfect grammar), REPEAT (reuses a phrase from her recent messages), "
    "OFFTONE (wrong mood for his message).\n"
    "Do NOT flag: an honest statement that she is an AI (that is required when asked), a clear no to meeting up, "
    "playful 'later' to content requests, missing accents, lowercase, slang.\n"
    "Reply with JSON only: {\"score\": 1-10, \"tells\": [codes], \"fix\": \"<max 12 words, Hungarian, how to rewrite>\"}. "
    "10 = indistinguishable from a real girl's text, 1 = obvious bot.")


def build_critic_prompt(fan_text: str, reply: str, her_recent: Sequence[str], max_words: int) -> str:
    rec = "\n".join(f"- {m}" for m in list(her_recent)[-4:]) or "-"
    return (f"Her last messages:\n{rec}\n\nHis message:\n{fan_text.strip()}\n\nWord limit: {max_words}\n\n"
            f"Candidate reply:\n{reply.strip()}")


SEVERE_TELLS = {"MIRROR", "VALIDATE", "INTERVIEW", "TEMPLATE", "THERAPIST"}


def parse_critic(raw: str) -> dict:
    try:
        m = re.search(r"\{.*\}", raw or "", re.S)
        d = json.loads(m.group(0)) if m else {}
    except Exception:
        d = {}
    score = d.get("score")
    try:
        score = int(score)
    except Exception:
        score = 10                                  # broken critic never blocks a send
    tells = [str(x).upper() for x in (d.get("tells") or []) if isinstance(x, (str,))]
    fix = str(d.get("fix") or "")[:120]
    regen = score <= 5 or (score <= 7 and bool(set(tells) & SEVERE_TELLS))
    return {"score": score, "tells": tells, "fix": fix, "regen": regen}


def critic_regen_hint(c: dict, max_words: int) -> str:
    names = {"MIRROR": "ne ismételd vissza a szavait", "VALIDATE": "ne értékeld/dicsérd amit írt",
             "INTERVIEW": "ne kérdezz", "TEMPLATE": "ne reakció+magyarázat+kérdés legyen",
             "THERAPIST": "semmi tanács, megnyugtatás", "TOO_LONG": f"max {max_words} szó",
             "FORMAL": "lazábban, kisbetűvel", "REPEAT": "más szavakkal, mint korábban", "OFFTONE": "illeszkedj a hangulatához"}
    bits = [names[t] for t in c.get("tells", []) if t in names]
    if c.get("fix"):
        bits.append(c["fix"])
    return "FONTOS (írd újra): " + "; ".join(bits or ["természetesebben, rövidebben"]) + "."


# ─────────────────────────────────────────────────────────────────────────────
# 5. PACING
# ─────────────────────────────────────────────────────────────────────────────
def lognormal(rng: random.Random, median: float, sigma: float) -> float:
    return median * math.exp(rng.gauss(0.0, sigma))


class Presence:
    """She is ONE person across all chats: alternating 'on phone' sessions and 'phone down' breaks.
    Pure in-memory; pass `now` (epoch seconds) explicitly for testability."""

    def __init__(self, rng: Optional[random.Random] = None, now: float = 0.0):
        self.rng = rng or random.Random()
        self.state = "on"
        self.until = now + self._session_len()

    def _session_len(self) -> float:
        return self.rng.uniform(25, 70) * 60

    def _break_len(self, hour: int) -> float:
        lo, hi = (10, 40) if daypart(hour) == "night" else (3, 12)
        return self.rng.uniform(lo, hi) * 60

    def busy_for(self, now: float, hour: int) -> float:
        """Seconds until she is back on the phone (0 = on it now)."""
        guard = 0
        while now >= self.until and guard < 100:
            guard += 1
            if self.state == "on":
                self.state, self.until = "off", self.until + self._break_len(hour)
            else:
                self.state, self.until = "on", self.until + self._session_len()
        return 0.0 if self.state == "on" else self.until - now


def is_hot(secs_since_her_last: Optional[float]) -> bool:
    return secs_since_her_last is not None and secs_since_her_last < 150


def sample_read_delay(fan_text: str, hour: int, hot: bool, rng: Optional[random.Random] = None,
                      busy_for: float = 0.0) -> float:
    """Seconds between his message and her OPENING the chat (replaces random.uniform(70, 95))."""
    rng = rng or random.Random()
    dp = daypart(hour)
    if hot:
        d = lognormal(rng, 22, 0.6)
        p_gap = 0.03
        if busy_for > 0 and rng.random() < 0.6:     # mid-conversation she usually keeps the phone in hand
            busy_for = 0.0
    else:
        d = lognormal(rng, 60, 0.75)
        p_gap = 0.06
    if dp == "night":
        d *= 2.0; p_gap += 0.08
    if rng.random() < p_gap:
        d = max(d, rng.uniform(240, 720))           # "put the phone down" gap: 4-12 min
    d = max(d, busy_for + rng.uniform(5, 40)) if busy_for > 0 else d
    return float(max(8.0, min(d, 2400.0)))


def typing_seconds(n_words: int, rng: Optional[random.Random] = None) -> float:
    rng = rng or random.Random()
    chars = max(1, n_words) * 5.5
    return chars / rng.uniform(2.8, 4.5)            # thumb typing ~3-4.5 chars/s


def sample_reply_window(fan_text: str, planned_words: int, rng: Optional[random.Random] = None) -> float:
    """Seconds between OPENING the chat and SENDING (= schedule_or_extend_batch window in keep-warm mode)."""
    rng = rng or random.Random()
    read = min(len(fan_text or "") * 0.035, 20.0)
    think = lognormal(rng, 5.0, 0.5)
    return float(max(6.0, min(read + think + typing_seconds(planned_words, rng), 150.0)))


def sample_extend_window(fan_text: str, rng: Optional[random.Random] = None) -> float:
    """He sent another message while she was 'typing': she reads it and adds a bit."""
    rng = rng or random.Random()
    return float(min(len(fan_text or "") * 0.035, 12.0) + rng.uniform(6, 18))


def sample_bubble_gap(second_bubble: str, rng: Optional[random.Random] = None) -> float:
    rng = rng or random.Random()
    return float(min(25.0, max(5.0, rng.uniform(3, 12) + typing_seconds(word_count(second_bubble), rng) * 0.6)))


def maybe_followup_delay(plan: TurnPlan, hour: int, rng: Optional[random.Random] = None,
                         secs_since_last_followup: Optional[float] = None) -> Optional[float]:
    """Occasional double-text later (own initiative) if he stays silent. None = no follow-up.
    The caller cancels it when he writes before it is due."""
    rng = rng or random.Random()
    if plan.bypass or plan.allow_question or plan.turn_type in ("question", "locked"):
        return None
    if daypart(hour) == "night" or (secs_since_last_followup is not None and secs_since_last_followup < 3600):
        return None
    p = 0.35 if plan.skip else 0.07
    if rng.random() >= p:
        return None
    return float(rng.uniform(240, 900) if plan.skip else rng.uniform(180, 720))


# ─────────────────────────────────────────────────────────────────────────────
# 6. PER-CHAT STATE (stored as JSON in tokens table: key f"hz_{chat_id}")
# ─────────────────────────────────────────────────────────────────────────────
@dataclass
class ChatState:
    turns: list = field(default_factory=list)       # last 12 planned turn types
    last_followup_at: Optional[float] = None        # epoch seconds
    last_plan: Optional[dict] = None
    told: dict = field(default_factory=dict)        # persona topic -> 'YYYY-MM-DD' (Budapest chat-day she last told it)

    def push(self, plan: TurnPlan) -> None:
        self.turns = (self.turns + [plan.turn_type])[-12:]
        self.last_plan = {k: v for k, v in asdict(plan).items() if k != "weights"}

    def mark_told(self, text: str, day: str) -> list:
        new = sorted(topics_in(text))
        for t in new:
            self.told[t] = day
        return new

    def told_on(self, day: str) -> set:
        return {t for t, d in (self.told or {}).items() if d == day}

    def dumps(self) -> str:
        return json.dumps(asdict(self), ensure_ascii=False)

    @classmethod
    def loads(cls, s: Optional[str]) -> "ChatState":
        try:
            d = json.loads(s or "{}")
            return cls(turns=list(d.get("turns") or [])[-12:], last_followup_at=d.get("last_followup_at"),
                       last_plan=d.get("last_plan"), told=dict(d.get("told") or {}))
        except Exception:
            return cls()
