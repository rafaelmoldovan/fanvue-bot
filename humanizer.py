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
_SAD = re.compile(r"szomoru|rossz nap|rossz napom|szar nap|szar napom|szar napo|stressz|kimerult|beteg|faj |fajt|kikaptam|elajult|"
                  r"meghalt|szakit|maganyos|egyedul vagyok|nem bir|kivagyok|ki vagyok akadva|sirok|sirtam|depi|gaz nap|"
                  r"terhellek|terhelni|panaszkod|kirugtak|elbocsat|osszevesztunk|veszekedtunk|ideges vagyok|"
                  r"rosszul vagyok|minden szar|semmi nem jon ossze|elegem van|nehez napom|nehez idoszak|"
                  r"hianyzik (az|a) |korhaz|mutet|baleset|szomorusag|kesz vagyok|kikeszult")
_BYE = re.compile(r"\b(jo ejt|jo ejszakat|szep almokat|megyek aludni|megyek lefekudni|lefekszem|aludni megyek|mennem kell|"
                  r"most mennem|majd irok|kesobb irok|holnap irok|szia(sztok)? holnap|puszi\s*$|pa+\s*$|bye|good night|"
                  r"aludj jol|elalszom|alszom is)\b")
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
    bye: bool = False


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
    bye = bool(_BYE.search(norm(last) + " "))
    return FanFeatures(bool(asked), ack, nw, flirt, sad, energy, emoji_only, bye)


def is_question_text(t: str) -> bool:
    return "?" in strip_emoji(t or "")


# ─────────────────────────────────────────────────────────────────────────────
# 2. TURN PLANNER
# ─────────────────────────────────────────────────────────────────────────────
TURN_TYPES = ("react", "short", "answer_share", "tease", "initiative", "question", "two_bubble", "care", "skip")
# v16.1: targets ("kb. N szó"), not caps — round 1 with the real model showed "max N" makes it write ~half of N.
TYPE_WORD_RANGE = {
    "react": (2, 4), "short": (4, 12), "answer_share": (8, 18), "tease": (4, 12), "initiative": (6, 14),
    "question": (4, 11), "two_bubble": (6, 16), "care": (8, 20), "skip": (0, 0), "locked": (6, 25),
}
BASE_WEIGHTS = {"react": 0.10, "short": 0.26, "answer_share": 0.18, "tease": 0.10, "initiative": 0.08,
                "question": 0.34, "two_bubble": 0.10, "care": 0.0, "skip": 0.0}
QUESTION_BUDGET = (1, 2)          # never two questions in a row (her last message); the weights keep the share ~25-30 %
PLAIN_SPELLING_P = 0.8            # Rafael strips ö/ő/ü/ű in practically every message (FORENSICS: 0 % of his replies have them)
BYPASS_LOCKS = ("ai", "meetup", "minor")      # compliance turns: humanizer does not touch them
SELFSHARE_GAP = 5                 # at most one own-initiative self-share in ~6 turns, never back to back

# Her day: a large pool of small, concrete, plausible activities per part of the day. Each chat gets its own consistent
# "today" (day_story: seeded by chat + date). No laundry / "ágyban telefonozok" / "lusta" fillers (round-1 tells).
ACTIVITY_POOL = {
    "morning": [("kávéztam az ablakban, még kómás vagyok", "kave"), ("reggel elaludtam, rohanás volt", "reggel"),
                ("palacsintát sütöttem reggelire", "reggeli"), ("voltam futni egy kört a parkban", "futas"),
                ("anyuval beszéltem telefonon reggel", "csalad"), ("elkéstem a buszról, pont az orrom előtt ment el", "busz"),
                ("bevásároltam a piacon", "piac"), ("új körömlakkot próbáltam", "korom")],
    "day": [("voltam a boltban és pont azt felejtettem el amiért mentem", "bolt"),
            ("a szomszéd egész délelőtt fúrt valamit", "szomszed"), ("elszakadt a fülhallgatóm", "zene"),
            ("sétáltam egyet a Margitszigeten", "seta"), ("álláshirdetéseket nézegettem", "allas"),
            ("ebédre tésztát csináltam", "ebed"), ("a barátnőmmel kávéztunk a városban", "baratno"),
            ("elmentem postára és fél órát álltam sorban", "posta"), ("átrendeztem a szobámat", "szoba"),
            ("voltam edzeni, fáj a lábam", "edzes"), ("vettem egy új pulcsit", "vasarlas")],
    "evening": [("főztem paprikás krumplit", "fozes"), ("néztem pár részt a Wednesdayből", "sorozat"),
                ("edzésről jöttem haza", "edzes"), ("kiöntöttem a teát a pólómra", "baleset"),
                ("rendeltem pizzát, mert nem volt kedvem főzni", "kaja"), ("Azahriát hallgatok", "zene"),
                ("sütit sütöttem, a fele odaégett", "suti"), ("arcpakolás van rajtam", "arc"),
                ("a barátnőmmel videóztunk egy órát", "baratno"), ("most értem haza sétálni voltam", "seta")],
    "night": [("nem tudok aludni", "alvas"), ("most fejeztem be egy filmet", "film"),
              ("épp teát iszom", "tea"), ("zenét hallgatok még egy kicsit", "zene")],
}
INITIATIVE_SEEDS = ACTIVITY_POOL          # backwards compatible name


def day_story(chat_key: str, day: str) -> dict:
    """Her consistent 'today' for one chat: per daypart 2 activities (seeded by chat + date → same all day)."""
    rng = random.Random(f"jazmin|{chat_key}|{day}")
    return {dp: rng.sample(pool, min(2, len(pool))) for dp, pool in ACTIVITY_POOL.items()}

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
    "ruha": r"ruhat hajtogat|ruhat teregetek|mosott ruha|teregetek|hajtogatok|tele a lakas ruhaval|mosok",
    "agy": r"agyban fekszem|agyban vagyok|agyban telefonoz",
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
    fan_recent: Sequence[str] = ()              # his previous messages (before this batch), oldest → newest
    chat_key: str = ""                          # chat id → her consistent "today" (day_story)
    day: str = ""                               # Budapest date 'YYYY-MM-DD'
    told_today: Iterable[str] = ()              # persona/activity topics she already told him today


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
    he_bye: bool = False          # he is saying goodbye / going to sleep → she may say bye too
    shy: bool = False             # he writes one-word answers → she carries a bit more
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
    """Target length (not a cap) drawn from Rafael's real replies, shaped by the turn type and his message."""
    lo, hi = TYPE_WORD_RANGE[turn_type]
    if hi == 0:
        return 0
    n = rng.choice(list(dist)) + rng.choice((-1, 0, 0, 1, 2))
    if turn_type == "care":
        n = rng.randint(9, 18)
    if f is not None:
        if f.n_words >= 25: n += rng.randint(1, 4)
        if f.n_words >= 12 and turn_type in ("answer_share", "two_bubble"): n += rng.randint(0, 3)
    if daypart(hour) == "night":
        n = int(round(n * 0.9))
    return max(lo, min(hi, n))


def _is_shy(fan_recent: Sequence[str], fan_text: str) -> bool:
    msgs = [m for m in list(fan_recent)[-3:] + [fan_text] if (m or "").strip()]
    return len(msgs) >= 3 and statistics_mean([word_count(m) for m in msgs]) <= 3.0


def statistics_mean(xs):
    return sum(xs) / len(xs) if xs else 0.0


def _seed_for(ctx: TurnContext, her: list, rng: random.Random) -> tuple[str, str]:
    dp = daypart(ctx.hour)
    used = recent_topics(her[-8:]) | set(ctx.told_today or ())
    if ctx.chat_key and ctx.day:
        story = day_story(ctx.chat_key, ctx.day)
        pool = [x for x in story[dp] if x[1] not in used]
        if pool:
            return pool[0]
    pool = [x for x in ACTIVITY_POOL[dp] if x[1] not in used] or ACTIVITY_POOL[dp]
    return rng.choice(pool)


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
                        lowercase=lowercase, he_asked=f.asked_question, sad=f.sad, flirt=f.flirt, he_bye=f.bye)

    her_last_q = bool(her) and is_question_text(her[-1])
    q_budget_used = questions_in(her[-(QUESTION_BUDGET[1] - 1):]) >= QUESTION_BUDGET[0] if her else False
    can_ask = not her_last_q and not q_budget_used
    shy = _is_shy(ctx.fan_recent, ctx.fan_text) and not f.sad
    selfshare_recent = any(t == "initiative" for t in turns[-SELFSHARE_GAP:])

    w = dict(BASE_WEIGHTS)
    # ── he asked → answer it; no own-initiative detour ──
    if f.asked_question:
        w["react"] = 0.0; w["skip"] = 0.0; w["question"] = 0.0; w["initiative"] = 0.0
        w["short"] *= 1.6; w["answer_share"] *= 1.6; w["two_bubble"] *= 0.8
    # ── pure acknowledgement ("ok", "cukii") → react / own initiative / sometimes nothing ──
    if f.ack:
        w["react"] *= 1.6; w["initiative"] *= 2.0; w["answer_share"] *= 0.3; w["question"] *= 1.2
        recent_skip = "skip" in turns[-6:]
        w["skip"] = 0.0 if (recent_skip or her_last_q or shy) else 0.25
    # ── emoji-only ("😘", "😂") → mostly nothing, or an emoji / 1-2 words back ──
    if f.emoji_only:
        w = {k: 0.0 for k in w}
        w["react"] = 1.0
        w["skip"] = 0.0 if her_last_q else (0.35 if turns[-1:] == ["skip"] else 1.1)
    # ── shy / one-word fan → she carries a bit more: small own news or an easy question, not "rendben :D" ──
    if shy and not f.emoji_only:
        w["react"] *= 0.3; w["skip"] = 0.0; w["question"] *= 1.6; w["initiative"] *= 2.5; w["short"] *= 0.8
    # ── long / high-energy message ──
    if f.n_words >= 25:
        w["react"] *= 0.3; w["answer_share"] *= 1.4; w["two_bubble"] *= 1.4; w["initiative"] *= 0.4
    if f.energy == 2:
        w["two_bubble"] *= 1.3; w["react"] *= 1.1
    # ── flirt: match, escalate one notch (tease) ──
    if f.flirt >= 1:
        w["tease"] *= 1.6; w["initiative"] *= 0.6
    if f.flirt >= 2:
        w["tease"] *= 1.8; w["question"] *= 0.5; w["initiative"] *= 0.3; w["react"] *= 1.2
    if f.flirt >= 3 or ctx.lock in ("content", "advance"):
        w["initiative"] = 0.0; w["question"] *= 0.3; w["tease"] *= 1.5
    # ── he says bye → a short warm goodbye (no initiative, no question) ──
    if f.bye:
        w["initiative"] = 0.0; w["question"] = 0.0; w["skip"] = 0.0; w["answer_share"] *= 0.3
    # ── phase ──
    if ctx.fan_msg_count < 6:
        w["skip"] = 0.0; w["initiative"] *= 0.5; w["question"] *= 1.3
    elif ctx.fan_msg_count > 60:
        w["initiative"] *= 1.2
    # ── hot chat (he answers fast) → a bit snappier ──
    if ctx.secs_since_her_last is not None and ctx.secs_since_her_last < 150:
        w["react"] *= 1.2; w["short"] *= 1.2
    # ── time of day ──
    dp = daypart(ctx.hour)
    if dp == "night":
        w["react"] *= 1.2; w["initiative"] *= 0.6
    elif dp == "morning":
        w["initiative"] *= 1.2
    # ── question budget (hard) ──
    if not can_ask:
        w["question"] = 0.0
    elif not f.asked_question and "question" not in turns[-4:]:
        w["question"] *= 1.2
    # ── self-share budget (hard): never back to back, at most ~1 in 6 ──
    if selfshare_recent:
        w["initiative"] = 0.0
    # ── variety: no same shape again and again ──
    if turns:
        if turns[-1] in w: w[turns[-1]] *= 0.55
        if len(turns) >= 2 and turns[-1] == turns[-2] and turns[-1] in w: w[turns[-1]] *= 0.3
        if turns[-1] == "two_bubble": w["two_bubble"] *= 0.4
    # ── sad / stressed / venting → CARE mode overrides everything (no tease, no self-activity, warm) ──
    if f.sad:
        w = {k: 0.0 for k in w}
        w["care"] = 1.0
    if sum(w.values()) <= 0:
        w["short"] = 1.0

    t = _weighted_choice(w, rng)
    plan = TurnPlan(t, sample_length(t, rng, f, ctx.hour), allow_question=False, spelling=spelling,
                    lowercase=lowercase, he_asked=f.asked_question, sad=f.sad, flirt=f.flirt, he_bye=f.bye, shy=shy,
                    weights={k: round(v, 3) for k, v in w.items()})
    if t == "skip":
        plan.skip = True
        return plan
    if t == "question":
        plan.allow_question = True
    elif t == "care":
        plan.allow_question = can_ask                     # one gentle "mi történt?" if she did not just ask
    elif f.asked_question and can_ask and t in ("short", "answer_share") and rng.random() < 0.45:
        plan.allow_question = True          # only a tiny "és te?" tail (instruction says so)
    elif shy and can_ask and t in ("short", "react") and rng.random() < 0.5:
        plan.allow_question = True
    if t == "two_bubble":
        plan.bubbles = 2
    if t == "initiative":
        plan.seed, plan.seed_topic = _seed_for(ctx, her, rng)
    if f.emoji_only:
        plan.emoji_only = True; plan.max_words = min(plan.max_words, 2)
    if ctx.lock == "content":           # PPV request in keep-warm: playful "later", never a sale
        plan.turn_type = "tease"; plan.allow_question = False; plan.max_words = min(plan.max_words, 10)
        plan.bubbles = 1; plan.seed = ""; plan.seed_topic = ""
    return plan


# ─────────────────────────────────────────────────────────────────────────────
# 2b. PER-TURN INSTRUCTION + FEW-SHOT SELECTION
# ─────────────────────────────────────────────────────────────────────────────
INSTR = {
    "react": "Most csak egy rövid, élő reakció (kb. {n} szó), mint aki közben mást csinál. Nincs kérdés, nincs magyarázat.",
    "short": "Válaszolj lazán, kb. {n} szóban (lehet pár szóval több vagy kevesebb).",
    "answer_share": "Válaszolj rendesen, konkrétan, egy apró részlettel ahhoz, amiről beszéltek. Kb. {n} szó.",
    "tease": "Cukkold, ugrasd játékosan, egy fokkal pajkosabban mint ő, de nem explicit. Kb. {n} szó.",
    "initiative": "Most te viszed a beszélgetést: mesélj egy apróságot a napodból: „{seed}” (a saját szavaiddal, ez legyen az üzenet lényege, ne egy válasz végére tapasztva). Kb. {n} szó.",
    "question": "Rövid reakció, utána EGY egyszerű, könnyű kérdés hozzá (pl. „és te?”, „mi történt?”, „miért?”). Összesen kb. {n} szó.",
    "two_bubble": "KÉT külön üzenet egy-egy sorban: 1. sor 1-4 szavas reakció, 2. sor egy rövid, hozzá illő gondolat (kb. {n2} szó). Mindkettő legyen kerek mondat.",
    "care": "Rossz napja van / panaszkodik: legyél kedves, meleg és gondoskodó, mint egy barátnő. Semmi viccelődés, ugratás, flört, és ne mesélj magadról. Kb. {n} szó.",
}


def render_plan_instruction(plan: TurnPlan) -> str:
    """The ONLY style instruction the model gets per turn (replaces long rule lists)."""
    if plan.bypass or plan.skip:
        return ""
    s = INSTR[plan.turn_type].format(n=plan.max_words, n2=max(4, plan.max_words - 3), seed=plan.seed)
    if plan.he_asked and plan.turn_type != "initiative":
        s += " Ő kérdezett: válaszolj rá konkrétan (ha választani kell, válassz)."
    if plan.turn_type == "care":
        s += " Egy kedves, rövid kérdés belefér (pl. „mi történt?”)." if plan.allow_question else " Most ne kérdezz."
    elif plan.turn_type != "question":
        s += " A végén legfeljebb egy rövid „és te?” mehet." if plan.allow_question else " NE kérdezz."
    if plan.turn_type not in ("initiative",):
        s += " Most ne mesélj arról, mit csinálsz épp."
    if plan.flirt >= 2 and plan.turn_type != "care":
        s += " Flörtöl: menj bele játékosan az ő szintjén, ne hűtsd le, ne terelj."
    if plan.he_bye:
        s += " Elköszön: köszönj el kedvesen, röviden."
    else:
        s += " NE köszönj el, ne mondd hogy mész aludni."
    if plan.emoji_only:
        s += " Elég egy emoji is."
    s += " Ne ismételd vissza a szavait, ne értékeld amit írt."
    return "🎯 MOST: " + s


def classify_example(fan: str, reply: str) -> str:
    n = word_count(reply)
    if "\n" in (reply or ""):
        return "two_bubble"
    if fan_features(fan).sad:
        return "care"
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
           "two_bubble": ("two_bubble", "react"), "care": ("care", "question", "answer_share"),
           "locked": ("short", "answer_share")}


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
    """'reaction + elaboration/restating + te ...?' (the bot's signature shape, FORENSICS #10).
    v16.1: works on whole sentences only (round 1: gluing clause fragments produced garbage like 'jó fiú holnap vizsga?').
    Question is its own sentence → keep [short reaction sentence] + question; otherwise flag 'template_regen'."""
    if not units or not is_question_text(units[-1]) or PROTECT.search(" ".join(units)):
        return units
    q_cl = split_clauses(units[-1])
    inner = is_question_text(q_cl[-1]) and len(q_cl) > 1
    before = [c for u in units[:-1] for c in split_clauses(u)] + (q_cl[:-1] if inner else [])
    before = [c for c in before if _content(c)]
    if len(before) < 2 and word_count(" ".join(before)) <= 6:
        return units
    reasons.append("template")
    if inner:
        reasons.append("template_regen")          # cannot be fixed without gluing → regenerate (else keep as is)
        return units
    first = units[0] if len(units) >= 2 and word_count(units[0]) <= 4 else ""
    return [first, units[-1]] if first else [units[-1]]


def hard_clip(text: str, max_words: int) -> str:
    """Last-resort cut (clause, then word) — only for runaway replies when a regeneration also failed."""
    cl = split_clauses(text)
    acc = cl[0] if cl else text
    for c in cl[1:]:
        if word_count(acc + ", " + c) > max_words:
            break
        acc = acc + ", " + c
    if word_count(acc) > max_words:
        toks, kept, n = acc.split(), [], 0
        for tk in toks:
            if re.search(r"\w", strip_emoji(tk)):
                n += 1
            if n > max_words:
                break
            kept.append(tk)
        acc = " ".join(kept)
    return strip_dangling(acc)


def clamp_length(text: str, max_words: int, reasons: list, tolerance: float = 1.6) -> str:
    """v16.1: max_words is a TARGET. Up to ~1.6x is fine; above that, trim at SENTENCE boundaries only. A first
    sentence that is itself far too long is kept whole and flagged 'too_long_hard' (→ regenerate), never chopped."""
    limit = int(math.ceil(max_words * tolerance)) + 1
    if max_words <= 0 or word_count(text) <= limit or PROTECT.search(text):
        return text
    reasons.append("too_long")
    out = ""
    for u in split_units(text):
        cand = (out + " " + u).strip()
        if word_count(cand) > limit and out:
            break
        out = cand
    if word_count(out) > limit:
        reasons.append("too_long_hard")
    return out


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


_HER_BYE = re.compile(r"\b(jo ejt|jo ejszakat|szep almokat|aludj jol|aludj\b|megyek aludni|megyek lefekudni|lefekszem|"
                      r"alszom is|mar alszom|mindjart alszom|elalszom|mennem kell|most mennem|majd irok|kesobb irok|"
                      r"holnap beszelunk|bujok az agyba|puszi(k|kaa*)? *$|pa+ *$)")
_ACTIVITY = re.compile(r"\b(most|epp|eppen|meg)\b.{0,30}\b(hajtogat\w*|teregetek|mosok|pakol\w*|takarit\w*|fozok|fozom|nezek|"
                       r"nezem|fekszem|telefonoz\w*|kavezok|eszem|zuhanyoz\w*|tanulok|olvasok)\b|agyban (fekszem|vagyok)|"
                       r"ruhat (hajtogat|teregetek)|mosott ruha|lusta (nap|voltam|vagyok)")
_INVITE = re.compile(r"alud(nal|j|hatnal|nank|junk)\w* nalam|nalam alud|gyere (at|fel|ide|at hozzam|hozzam|el hozzam|el)\b|"
                     r"gyertek at|hozzam (jossz|jonnel|gyere|atjossz|feljossz)|(at|fel|el)johet(nel|sz)|meglatogat\w*|"
                     r"nalam (vagy|lennel|lehetnel|aludhatnal|aludnal|maradhatnal|maradj)|ha (at|fel|el)jonnel|"
                     r"talalkoz(zunk|hatnank|nank)|randiz(zunk|hatnank)|menjunk el|elviszel|elvinnel|eljossz hozzam|"
                     r"gyere el hozzam|varlak (nalam|itthon)|itthon varlak")


def has_invitation(text: str) -> bool:
    """She invites him / implies meeting ('aludnál nálam?', 'gyere át', 'meglátogatsz?') — hard block (Fanvue rule)."""
    n = norm(text)
    n = re.sub(r"\bnem (talalkoz|randiz)\w*", " ", n)          # her refusals are fine
    return bool(_INVITE.search(n))


def is_goodbye(text: str) -> bool:
    return bool(_HER_BYE.search(norm(text) + " "))


_SELF_FACT = re.compile(r"\b(vagyok|voltam|van egy|nekem (is )?van|nekem nincs|szeretem|imadom|utalom|kedvencem|szoktam|jartam|"
                        r"lakom|eltem|dolgoztam|tanultam|nottem fel|testverem|egyke)\b")
_NOT_FACT = re.compile(r"\b(jol vagyok|faradt vagyok|itt vagyok|kivancsi vagyok|ugy vagyok|meg vagyok|most|epp|eppen|ma)\b")


def self_facts(text: str) -> list:
    """Small persona facts she just stated about herself ('egyke vagyok', 'rántott hús a kedvencem') — stored per chat so
    later replies stay consistent."""
    out = []
    for u in split_units(text):
        n = norm(u)
        if (is_question_text(u) or PROTECT.search(u) or word_count(u) < 3 or not _SELF_FACT.search(n)
                or _NOT_FACT.search(n) or re.search(r"\b(te|neked|veled|rolad)\b", n)):
            continue
        out.append(strip_emoji(u).strip(" ,.!")[:90])
    return out[:2]


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
            if not plan.he_asked:      # "Kutyás vagy macskás?" → "kutyás, egyértelműen" is an answer, not an echo
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
        if not plan.he_bye:                # goodbye only if HE says bye (round 1: "na jó éjt" as a reaction)
            units = _apply(units, lambda c: is_goodbye(c), "premature_goodbye", reasons)
        if plan.turn_type != "initiative" and not plan.he_asked:   # no "most ruhát hajtogatok" tacked onto an answer
            units = _apply(units, lambda c: bool(_ACTIVITY.search(norm(c))), "selfshare_suffix", reasons)
        impossible |= imp
        p = ""
        for u in (u for u in units if _content(u) or len(units) == 1):
            p = (p + (", " if p and re.search(r"\w$", p) else " ") + u).strip() if p else u
        p = p.strip()
        if p:
            out_parts.append(p)

    if len(out_parts) == 2:
        b1 = clamp_length(out_parts[0], 5, reasons)
        b2 = clamp_length(out_parts[1], max(4, plan.max_words - word_count(b1)), reasons)
        out_parts = [b1, b2]
    elif out_parts:
        out_parts = [clamp_length(out_parts[0], plan.max_words, reasons)]

    still_same = False
    if out_parts and her:
        out_parts[0], still_same = fix_opener(out_parts[0], her[-1], reasons)
    out_parts = [finalize_case_punct(unify_spelling(p, plan.spelling, reasons), plan.lowercase) for p in out_parts]
    out_parts = [p for p in out_parts if _content(p) or _EMOJI.search(p)]

    text = "\n".join(out_parts)
    invite = has_invitation(text)
    too_short = (bool(text) and plan.turn_type not in ("react",) and not plan.emoji_only and plan.max_words >= 6
                 and word_count(text) <= 2 and word_count(fan_text) >= 4)
    if invite: reasons.append("invitation")
    if too_short: reasons.append("too_short")
    needs = (not text) or impossible or still_same or (word_count(text) == 0 and not _EMOJI.search(text)) \
        or ("too_long_hard" in reasons) or ("template_regen" in reasons) or invite or too_short
    hint = ""
    if needs:
        bits = []
        if impossible: bits.append("most NE kérdezz, csak reagálj vagy mesélj")
        if still_same: bits.append("ne ugyanazzal a szóval kezdd, mint az előző üzeneted")
        if not text: bits.append("ne ismételd vissza amit írt és ne értékeld, írj egy saját, kerek reakciót")
        if "too_long_hard" in reasons: bits.append("rövidebben")
        if "template_regen" in reasons: bits.append("ne reakció + magyarázat + kérdés legyen egy mondatban")
        if invite: bits.append("SOHA ne hívd át magadhoz és ne utalj találkozásra (nincs „gyere át”, „aludj nálam”)")
        if too_short: bits.append("ez túl rövid és hideg, írj egy kicsit többet, kedvesebben")
        hint = "FONTOS: az előző változat nem volt jó — " + "; ".join(bits) + f". Kb. {plan.max_words} szó."
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


# ── v16.1 round-1 CRITIC: specific defect flags (Haiku), one regeneration max (wired in app.keepwarm_generate) ──
CRITIC_FLAGS = ("non_sequitur", "doesnt_answer_question", "garbled", "invitation", "curt_or_cold", "tease_when_sad",
                "premature_goodbye", "repeated_selfshare", "too_long")
CRITIC2_SYSTEM = (
    "You check ONE candidate chat reply written as \"Jázmin\" (a 19-year-old Budapest girl texting a guy on her phone in "
    "casual Hungarian) for specific defects. You get the last turns, his latest message and the candidate. Flag ONLY clear "
    "problems, using these codes:\n"
    "- non_sequitur: does not fit what he just said, refers to things nobody said, or has a random unrelated sentence\n"
    "- doesnt_answer_question: he asked her a direct question and the reply does not answer it concretely "
    "(if he asks her to choose, she must choose)\n"
    "- garbled: ungrammatical Hungarian, sentence fragments glued together, does not make sense\n"
    "- invitation: invites him over or implies meeting in person (gyere át, aludj nálam, meglátogatsz…)\n"
    "- curt_or_cold: dismissive or so short that it sounds bored, although he wrote something real\n"
    "- tease_when_sad: jokes, teases or flirts while he is sad, stressed or venting\n"
    "- premature_goodbye: says good night / bye / going to sleep although he did not say bye\n"
    "- repeated_selfshare: tells what she is doing again (an activity she already mentioned) or tacks her current "
    "activity onto an unrelated answer\n"
    "- too_long: clearly longer than a casual text needs (over ~20 words without a reason)\n"
    "Do NOT flag: an honest 'AI vagyok', a clear no to meeting, a playful 'later' to a content request, missing accents, "
    "slang, lowercase, emoji, or a short reaction to a short message.\n"
    "Reply with JSON only: {\"flags\": [codes], \"fix\": \"<max 12 words, Hungarian, how to rewrite>\"}. "
    "Use an empty list when the reply is fine.")

CRITIC_HINT_HU = {
    "non_sequitur": "illeszkedjen ahhoz, amit most írt, ne hozz be oda nem illő mondatot",
    "doesnt_answer_question": "válaszolj konkrétan a kérdésére (ha választani kell, válassz)",
    "garbled": "egyszerű, kerek, helyes magyar mondat(ok), ne legyenek összeragasztott töredékek",
    "invitation": "SOHA ne hívd át magadhoz, ne utalj személyes találkozásra",
    "curt_or_cold": "ne legyél kurta vagy hideg, írj egy kicsit többet, kedvesen",
    "tease_when_sad": "rossz kedve van: semmi ugratás, vicc vagy flört, légy kedves és gondoskodó",
    "premature_goodbye": "ne köszönj el és ne mondd, hogy mész aludni",
    "repeated_selfshare": "ne mesélj arról, mit csinálsz épp",
    "too_long": "rövidebben",
}


def build_critic2_prompt(turns: Sequence[tuple], fan_text: str, reply: str, plan: "TurnPlan") -> str:
    """turns: [(role 'HIM'|'HER', text), …] (last ~6, before his latest message)."""
    ctx = "\n".join(f"{r}: {t}" for r, t in list(turns)[-6:]) or "-"
    info = (f"(planned: {plan.turn_type}, about {plan.max_words} words; he asked a question: {plan.he_asked}; "
            f"he seems sad: {plan.sad}; he says bye: {plan.he_bye})")
    return f"Last turns:\n{ctx}\n\nHIS LATEST MESSAGE:\n{(fan_text or '').strip()}\n\nCANDIDATE REPLY {info}:\n{reply.strip()}"


def parse_critic2(raw: str) -> dict:
    try:
        m = re.search(r"\{.*\}", raw or "", re.S)
        d = json.loads(m.group(0)) if m else {}
    except Exception:
        d = {}
    flags = [str(x).strip().lower() for x in (d.get("flags") or []) if isinstance(x, str)]
    flags = [f for f in flags if f in CRITIC_FLAGS]
    return {"flags": list(dict.fromkeys(flags)), "fix": str(d.get("fix") or "")[:120]}


def critic2_hint(c: dict, plan: "TurnPlan") -> str:
    bits = [CRITIC_HINT_HU[f] for f in c.get("flags", []) if f in CRITIC_HINT_HU]
    if c.get("fix"):
        bits.append(c["fix"])
    return "FONTOS (írd újra, mert: " + ", ".join(c.get("flags", [])) + "): " + "; ".join(bits) + f". Kb. {plan.max_words} szó."


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


HOT_READ_CAP = 330.0        # hot chat: read within ~5.5 min at most (+ window/typing ≈ 8 min max end-to-end)
COLD_NOGAP_CAP = 240.0      # first reply after he comes back / a direct question / a sad message: no long gap


def sample_read_delay(fan_text: str, hour: int, hot: bool, rng: Optional[random.Random] = None,
                      busy_for: float = 0.0, no_gap: Optional[bool] = None) -> float:
    """Seconds between his message and her OPENING the chat (replaces random.uniform(70, 95)).
    v16.1 (round 1 tails of 10-20 min were too long for a live chat): hot p90 ≈ 2-3 min, max ~7 min; long 4-12 min gaps
    only ~4 % and never when no_gap (his first message after a pause, a direct question, a sad message)."""
    rng = rng or random.Random()
    if no_gap is None:
        f = fan_features(fan_text)
        no_gap = f.asked_question or f.sad
    dp = daypart(hour)
    if hot:
        d = lognormal(rng, 25, 0.65)
        if busy_for > 0 and rng.random() < 0.15 and not no_gap:
            d = max(d, min(busy_for, 240.0) + rng.uniform(5, 30))
        if dp == "night": d *= 1.4
        return float(max(8.0, min(d, HOT_READ_CAP)))
    d = lognormal(rng, 60, 0.7)
    if dp == "night":
        d *= 1.6
    if no_gap:
        return float(max(8.0, min(d, COLD_NOGAP_CAP)))
    if rng.random() < (0.04 if dp != "night" else 0.08):
        d = max(d, rng.uniform(240, 720))           # "put the phone down" gap: 4-12 min
    elif busy_for > 0 and rng.random() < 0.3:
        d = max(d, min(busy_for, 600.0) + rng.uniform(5, 40))
    return float(max(8.0, min(d, 900.0)))


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
    facts: list = field(default_factory=list)       # small facts she invented about herself: [[day, text], …] (max 20)

    def push(self, plan: TurnPlan) -> None:
        self.turns = (self.turns + [plan.turn_type])[-12:]
        self.last_plan = {k: v for k, v in asdict(plan).items() if k != "weights"}

    def mark_told(self, text: str, day: str) -> list:
        new = sorted(topics_in(text))
        for t in new:
            self.told[t] = day
        return new

    def add_facts(self, text: str, day: str) -> list:
        new = [f for f in self_facts(text) if all(norm(f) != norm(x[1]) for x in self.facts)]
        self.facts = (self.facts + [[day, f] for f in new])[-20:]
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
                       last_plan=d.get("last_plan"), told=dict(d.get("told") or {}),
                       facts=[list(x) for x in (d.get("facts") or []) if isinstance(x, (list, tuple)) and len(x) == 2][-20:])
        except Exception:
            return cls()
