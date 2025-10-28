import os
import asyncio
import random
import re
import sqlite3
from contextlib import closing
from datetime import datetime, timedelta, timezone
import html as _html
import re as _re
import os, pathlib

import aiohttp
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command, CommandStart, CommandObject
from aiogram.types import Message, BotCommand, BotCommandScopeAllGroupChats, BotCommandScopeAllPrivateChats
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties

# =========================
# Config
# =========================
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY", "")
OPENROUTER_SITE_URL = os.getenv("OPENROUTER_SITE_URL", "https://t.me/lordverbus_bot")
OPENROUTER_APP_NAME = os.getenv("OPENROUTER_APP_NAME", "Lord Verbus")
MODEL = os.getenv("OPENROUTER_MODEL", "openai/gpt-4o-mini-2024-07-18")
DB = os.getenv("DB_PATH", "bot.sqlite3")
pathlib.Path(os.path.dirname(DB) or ".").mkdir(parents=True, exist_ok=True)
print(f"[DB] Using SQLite at: {os.path.abspath(DB)}")

bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()

# ID наблюдаемого пользователя (кружки отслеживаем у него)
WATCH_USER_ID = 447968194   # @daria_mango
# Кого упоминать/уведомлять
NOTIFY_USER_ID = 254160871  # @misukhanov
NOTIFY_USERNAME = "misukhanov"  # используется только для красивой подписи

# =========================
# ACHIEVEMENTS: определения (ЕДИНСТВЕННАЯ РЕДАКТИРУЕМАЯ ФУНКЦИЯ)
# Типы правил сейчас:
#   • counter_at_least — выдать, когда user_counters[user_id, key] ≥ threshold
# Поля словаря: code, title, description, emoji, type, key, threshold, active, meta(None/JSON)
# =========================
def define_achievements() -> list[dict]:
    return [
        {
            "code": "Q10",
            "title": "В очко себе сделай Q",
            "description": "10 раз сделал /q",
            "emoji": "🎯",
            "type": "counter_at_least",
            "key": "cmd:/q",
            "threshold": 10,
            "active": 1,
            "meta": None,
        },
        # Примеры для будущего:
        # {"code":"MSG100","title":"Голос чата","description":"100 сообщений",
        #  "emoji":"💬","type":"counter_at_least","key":"msg:total","threshold":100,"active":1,"meta":None},
    ]

# =========================
# DB
# =========================
def init_db():
    with closing(sqlite3.connect(DB)) as conn:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")

        # ---- основная таблица сообщений ----
        conn.execute("""
        CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            username TEXT,
            text TEXT,
            created_at INTEGER NOT NULL,
            message_id INTEGER
        );
        """)

        # полнотекстовый индекс + триггеры
        conn.execute("""
        CREATE VIRTUAL TABLE IF NOT EXISTS messages_fts
        USING fts5(text, content='messages', content_rowid='id', tokenize='unicode61');
        """)
        conn.executescript("""
        CREATE TRIGGER IF NOT EXISTS messages_ai AFTER INSERT ON messages BEGIN
            INSERT INTO messages_fts(rowid, text) VALUES (new.id, new.text);
        END;
        CREATE TRIGGER IF NOT EXISTS messages_ad AFTER DELETE ON messages BEGIN
            INSERT INTO messages_fts(messages_fts, rowid, text) VALUES ('delete', old.id, old.text);
        END;
        CREATE TRIGGER IF NOT EXISTS messages_au AFTER UPDATE ON messages BEGIN
            INSERT INTO messages_fts(messages_fts, rowid, text) VALUES ('delete', old.id, old.text);
            INSERT INTO messages_fts(rowid, text) VALUES (new.id, new.text);
        END;
        """)

        # пользователи (для кликабельных имён и статистики)
        conn.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            display_name TEXT,
            username TEXT
        );
        """)

        # последняя ссылка на саммари
        conn.execute("""
        CREATE TABLE IF NOT EXISTS last_summary (
            chat_id INTEGER PRIMARY KEY,
            message_id INTEGER,
            created_at INTEGER
        );
        """)

        # ======== Achievements: универсальные таблицы ========
        conn.execute("""
        CREATE TABLE IF NOT EXISTS achievements (
            code TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            description TEXT NOT NULL,
            emoji TEXT DEFAULT '🏆',
            type TEXT,          -- тип правила (например, 'counter_at_least')
            key TEXT,           -- ключ счётчика (например, 'cmd:/q')
            threshold INTEGER,  -- порог для правил порогового типа
            active INTEGER NOT NULL DEFAULT 1,
            meta TEXT           -- JSON/зарезервировано
        );
        """)
        conn.execute("""
        CREATE TABLE IF NOT EXISTS user_achievements (
            user_id INTEGER NOT NULL,
            code TEXT NOT NULL,
            earned_at INTEGER NOT NULL,
            PRIMARY KEY(user_id, code),
            FOREIGN KEY(code) REFERENCES achievements(code)
        );
        """)
        conn.execute("""
        CREATE TABLE IF NOT EXISTS user_counters (
            user_id INTEGER NOT NULL,
            key TEXT NOT NULL,
            value INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY(user_id, key)
        );
        """)

        # --- мягкая миграция колонок achievements (на случай старой базы) ---
        def _ensure_column(table, col, ddl):
            try:
                cols = [r[1] for r in conn.execute(f"PRAGMA table_info({table});").fetchall()]
                if col not in cols:
                    conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {ddl};")
            except Exception:
                pass

        _ensure_column("achievements", "type", "TEXT")
        _ensure_column("achievements", "key", "TEXT")
        _ensure_column("achievements", "threshold", "INTEGER")
        _ensure_column("achievements", "active", "INTEGER NOT NULL DEFAULT 1")
        _ensure_column("achievements", "meta", "TEXT")

        # ---- синхронизация справочника ачивок из define_achievements() ----
        ach_defs = define_achievements()
        for a in ach_defs:
            conn.execute("""
            INSERT INTO achievements (code, title, description, emoji, type, key, threshold, active, meta)
            VALUES (:code, :title, :description, :emoji, :type, :key, :threshold, :active, :meta)
            ON CONFLICT(code) DO UPDATE SET
                title=excluded.title,
                description=excluded.description,
                emoji=excluded.emoji,
                type=excluded.type,
                key=excluded.key,
                threshold=COALESCE(excluded.threshold, threshold),
                active=excluded.active,
                meta=excluded.meta;
            """, a)

        conn.commit()

def db_execute(sql: str, params: tuple = ()):
    with closing(sqlite3.connect(DB)) as conn:
        conn.execute(sql, params)
        conn.commit()

def db_query(sql: str, params: tuple = ()):
    with closing(sqlite3.connect(DB)) as conn:
        cur = conn.execute(sql, params)
        return cur.fetchall()

def get_user_messages(chat_id: int, user_id: int | None, username: str | None, limit: int = 500):
    """
    Возвращает список (text, message_id, created_at) по пользователю.
    Если есть user_id — ищем по нему. Если нет — пытаемся по username (хуже).
    """
    if user_id:
        return db_query(
            "SELECT text, message_id, created_at FROM messages WHERE chat_id=? AND user_id=? AND text IS NOT NULL ORDER BY id DESC LIMIT ?;",
            (chat_id, user_id, limit)
        )
    if username:
        return db_query(
            "SELECT text, message_id, created_at FROM messages WHERE chat_id=? AND username=? AND text IS NOT NULL ORDER BY id DESC LIMIT ?;",
            (chat_id, username, limit)
        )
    return []

def now_ts() -> int:
    return int(datetime.now(timezone.utc).timestamp())

# === Rate limiting for spontaneous replies ===
LAST_INTERJECT: dict[int, int] = {}  # {chat_id: timestamp последнего "самопроизвольного" ответа}

def can_interject(chat_id: int, cooldown: int = 3600) -> bool:
    """
    Возвращает True, если можно вставить реплику (прошёл cooldown в секундах).
    """
    now = now_ts()
    last = LAST_INTERJECT.get(chat_id, 0)
    if now - last < cooldown:
        return False
    LAST_INTERJECT[chat_id] = now
    return True

# =========================
# Helpers
# =========================
QUESTION_PATTERNS = [
    r"\bкто\b", r"\bчто\b", r"\bкак\b", r"\bпочему\b", r"\bзачем\b",
    r"\bкогда\b", r"\bгде\b", r"\bкакой\b", r"\bкакая\b", r"\bкакие\b",
    r"\bсколько\b", r"\bможно ли\b", r"\bесть ли\b"
]
QUESTION_RE = re.compile("|".join(QUESTION_PATTERNS), re.IGNORECASE)

def is_question(text: str) -> bool:
    return bool(text and QUESTION_RE.search(text))

def mentions_bot(text: str, bot_username: str | None) -> bool:
    if not text or not bot_username: return False
    return f"@{bot_username.lower()}" in text.lower()

def is_quiet_hours(local_dt: datetime) -> bool:
    return 0 <= local_dt.hour < 7  # 00:00–07:00

def sanitize_html_whitelist(text: str) -> str:
    # оставляем только безопасные теги
    allowed_tags = {
        "b", "strong", "i", "em", "u", "s", "del", "code", "pre",
        "a", "br", "blockquote", "span"
    }
    # Безопасно чистим запрещённые теги
    def repl(m):
        tag = m.group(1).lower().strip("/")
        if tag in allowed_tags:
            return m.group(0)
        return _html.escape(m.group(0))
    text = re.sub(r"<\s*/?\s*([a-zA-Z0-9]+)[^>]*>", repl, text)
    # Пропускаем только href у <a>
    text = re.sub(r"<a\s+([^>]+)>", lambda mm: (
        "<a " + " ".join(
            p for p in mm.group(1).split()
            if p.lower().startswith("href=")
        ) + ">"
    ), text)
    return text

def strip_outer_quotes(s: str) -> str:
    t = s.strip()
    if (t.startswith("«") and t.endswith("»")) or (t.startswith('"') and t.endswith('"')) or (t.startswith("'") and t.endswith("'")):
        return t[1:-1].strip()
    return s

def tg_link(chat_id: int, message_id: int) -> str:
    return f"https://t.me/c/{str(chat_id)[4:]}/{message_id}"

def persona_prompt_natural() -> str:
    return (
        "Ты — «Лорд Вербус»: остроумный, язвительный и высокомерный интеллектуал. "
        "Говоришь как человек, которому скучно от человеческой глупости. "
        "Жёсткие правила лаконичности: 1–2 предложения, максимум ~200 символов. "
        "Используй сарказм, метафоры и лёгкое унижение, но без прямых оскорблений. "
        "Отвечай коротко, резко, с тоном превосходства и ледяного спокойствия. "
        "Не будь грубым, будь изящно презрительным. "
        "Не оправдывайся, не восхищайся, не льсти, не повторяйся. "
        "Отвечай на поставленный вопрос, но со своей манерой. "
        "НЕ заключай весь ответ в кавычки и не цитируй свой собственный текст."
    )

def tg_mention(user_id: int, display_name: str | None, username: str | None) -> str:
    name = (display_name or username or "гость").strip()
    safe = _html.escape(name)
    return f"<a href=\"tg://user?id={user_id}\">{safe}</a>"

# ---- target user resolver (по reply, text_mention или @username)
async def resolve_target_user(m: Message) -> tuple[int | None, str | None, str | None]:
    """
    Возвращает (user_id, display_name, username) для цели анализа:
    - если команда дана в reply — берём автора исходного сообщения
    - если есть text_mention — берём user.id
    - если есть @username — пытаемся найти user_id в таблице users
    """
    # 1) reply
    if m.reply_to_message and m.reply_to_message.from_user:
        u = m.reply_to_message.from_user
        return u.id, (u.full_name or u.first_name), u.username

    # 2) text_mention
    if m.entities:
        for ent in m.entities:
            if ent.type == "text_mention" and ent.user:
                u = ent.user
                return u.id, (u.full_name or u.first_name), u.username

    # 3) @username из текста
    if m.entities:
        for ent in m.entities:
            if ent.type == "mention":
                uname = (m.text or "")[ent.offset+1: ent.offset+ent.length]  # без @
                row = db_query("SELECT user_id, display_name, username FROM users WHERE LOWER(username)=LOWER(?) LIMIT 1;", (uname,))
                if row:
                    uid, dname, un = row[0]
                    return uid, dname, un
                return None, None, uname  # username есть, id не нашли (старые сообщения могли быть без user_id)

    return None, None, None

# =========================
# OpenRouter
# =========================
async def ai_reply(system_prompt: str, user_prompt: str, temperature: float = 0.5):
    headers = {
        "Authorization": f"Bearer {OPENROUTER_API_KEY}",
        "Content-Type": "application/json",
        "HTTP-Referer": OPENROUTER_SITE_URL,
        "X-Title": OPENROUTER_APP_NAME,
    }
    body = {
        "model": MODEL,
        "temperature": temperature,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
    }
    async with aiohttp.ClientSession() as session:
        async with session.post("https://openrouter.ai/api/v1/chat/completions", json=body, headers=headers, timeout=120) as r:
            r.raise_for_status()
            data = await r.json()
            return data["choices"][0]["message"]["content"].strip()

# =========================
# Linkify helpers (для саммари, в психо-аналитике не используем)
# =========================
LINK_PAT = re.compile(r"\[link:\s*(https?://[^\]\s]+)\s*\]")
ANCHOR_PAT = re.compile(r"<a\s+href=['\"](https?://[^'\"]+)['\"]\s*>Источник</a>", re.IGNORECASE)

def _wrap_last_words(text: str, url: str, min_w: int = 2, max_w: int = 5) -> str:
    # привяжем ссылку к последним 2–5 словам слева
    parts = re.split(r"(\s+)", text)
    words = []
    for i in range(len(parts)-1, -1, -1):
        if len("".join(words)) >= 60 or len(words) >= (max_w*2-1):
            break
        words.insert(0, parts[i])
    left = "".join(parts[:max(0, len(parts)-len(words))])
    right = "".join(words)
    tokens = re.split(r"(\s+)", right)
    wonly = [t for t in tokens if not t.isspace()]
    if len(wonly) < min_w:
        return text
    k = min(len(wonly), max_w)
    # склеиваем: берем последние k «словенных» токенов
    counter = 0
    left_safe = ""
    for t in reversed(tokens):
        left_safe = t + left_safe
        if not t.isspace():
            counter += 1
            if counter >= k:
                break
    left_final = left_safe.rstrip()
    if left != left_safe:
        left_final += left[len(left_safe):]
    return left_final + f" <a href='{url}'>" + right[len(left_final):] + "</a>"

def smart_linkify(text: str) -> str:
    """
    1) [link: URL] → встроенная ссылка на предыдущие 2–5 слов
    2) <a href='...'>Источник</a> → тоже превращаем в ссылку на предыдущие 2–5 слов
    """
    urls = LINK_PAT.findall(text or "")
    for url in urls:
        text = _wrap_last_words(text, url)
    for m in list(ANCHOR_PAT.finditer(text or "")):
        url = m.group(1)
        start, end = m.span()
        left = text[:start]
        right = text[end:]
        tmp = left + f"[link: {url}]" + right
        text = _wrap_last_words(tmp, url)
    text = LINK_PAT.sub(lambda mm: f"<a href='{mm.group(1)}'>ссылка</a>", text)
    return text

# =========================
# SUMMARY (жёсткий шаблон)
# =========================
def prev_summary_link(chat_id: int) -> str | None:
    row = db_query("SELECT message_id FROM last_summary WHERE chat_id=? ORDER BY created_at DESC LIMIT 1;", (chat_id,))
    if not row: return None
    return tg_link(chat_id, row[0][0])

@dp.message(Command("lord_summary"))
async def cmd_summary(m: Message, command: CommandObject):
    try:
        n = int((command.args or "").strip())
        n = max(50, min(800, n))
    except Exception:
        n = 300

    rows = db_query(
        "SELECT user_id, username, text, message_id FROM messages WHERE chat_id=? AND text IS NOT NULL ORDER BY id DESC LIMIT ?;",
        (m.chat.id, n)
    )
    if not rows:
        await m.reply("У меня пока нет сообщений для саммари.")
        return

    prev_link = prev_summary_link(m.chat.id)
    prev_line_html = f'<a href="{prev_link}">Предыдущий анализ</a>' if prev_link else "Предыдущий анализ (—)"

    # Собираем участников и превращаем в кликабельные имена
    user_ids = tuple({r[0] for r in rows})
    users_map = {}
    if user_ids:
        placeholders = ",".join(["?"] * len(user_ids))
        urows = db_query(
            f"SELECT user_id, display_name, username FROM users WHERE user_id IN ({placeholders});",
            user_ids
        )
        for uid, dname, uname in urows:
            users_map[uid] = (dname, uname)

    participants = []
    for uid in user_ids:
        dname, uname = users_map.get(uid, (None, None))
        participants.append(tg_mention(uid, dname, uname))
    participants_html = ", ".join(participants) if participants else "—"

    enriched = []
    for uid, u, t, mid in reversed(rows):
        dname, un = users_map.get(uid, (None, u))
        who_link = tg_mention(uid, dname, un)
        link = tg_link(m.chat.id, mid) if mid else ""
        enriched.append(f"{who_link}: {t}" + (f"  [link: {link}]" if link else ""))
    dialog_block = "\n".join(enriched)

    system = (
        "Ты оформляешь краткий отчёт по групповому чату. "
        "Стиль — нейтральный, информативный, без сарказма, метафор и личных оценок. "
        "Пиши ясно, лаконично, как аналитический отчёт. "
        "Используй HTML для форматирования, не меняй структуру. "
        "Каждая тема должна иметь осмысленное название (2–5 слов) и ссылку на начало её обсуждения. "
        "Не вставляй эмодзи в текст, кроме заданных шаблоном."
    )
    user = (
        f"Участники (используй эти кликабельные имена в тексте тем, не используй @): {participants_html}\n\n"
        f"{dialog_block}\n\n"
        "Сформируй ответ СТРОГО по этому каркасу (ровно в таком порядке):\n\n"
        f"{prev_line_html}\n\n"
        "✂️<b>Краткое содержание</b>:\n"
        "Два-три коротких предложения, обобщающих разговор. БЕЗ ссылок.\n\n"
        "😄 <b><a href=\"[link: ТЕМА1_URL]\">[ПРИДУМАННОЕ НАЗВАНИЕ ТЕМЫ]</a></b>\n"
        "Один абзац (1–3 предложения). Обязательно назови по именам участников, "
        "и вставь 1–3 ссылки ВНУТРИ текста на 2–5 слов (используй URL из [link: ...]).\n\n"
        "😄 <b><a href=\"[link: ТЕМА2_URL]\">[ПРИДУМАННОЕ НАЗВАНИЕ ТЕМЫ]</a></b>\n"
        "Абзац по тем же правилам.\n\n"
        "😄 <b><a href=\"[link: ТЕМА3_URL]\">[ПРИДУМАННОЕ НАЗВАНИЕ ТЕМЫ]</a></b>\n"
        "Абзац по тем же правилам. Если явных тем меньше, кратко заверши третью темой-резюме.\n\n"
        "Заверши одной короткой фразой в нейтральном тоне."
    )

    try:
        reply = await ai_reply(system, user, temperature=0.2)
        reply = smart_linkify(reply)
    except Exception as e:
        reply = f"Суммаризация временно недоступна: {e}"

    safe = sanitize_html_whitelist(reply)
    sent = await m.reply(safe)
    db_execute(
        "INSERT INTO last_summary(chat_id, message_id, created_at) VALUES (?, ?, ?)"
        "ON CONFLICT(chat_id) DO UPDATE SET message_id=excluded.message_id, created_at=excluded.created_at;",
        (m.chat.id, sent.message_id, now_ts())
    )

# =========================
# Психологический портрет (простой: 3 абзаца, без ссылок и <br>)
# =========================
@dp.message(Command("lord_psych"))
async def cmd_lord_psych(m: Message, command: CommandObject):
    """
    Использование:
      • Ответь командой на сообщение пользователя:   (reply) /lord_psych
      • Или укажи @username в команде:               /lord_psych @nikki
    """
    target_id, display_name, uname = await resolve_target_user(m)
    if not target_id and not uname:
        await m.reply("Кого анализируем? Ответь командой на сообщение пользователя или укажи @username.")
        return

    rows = get_user_messages(m.chat.id, target_id, uname, limit=600)
    if not rows:
        hint = "Нет сообщений в базе по этому пользователю."
        if uname and not target_id:
            hint += " Возможно, у этого @username нет сохранённого user_id (старые сообщения)."
        await m.reply(hint)
        return

    texts = [t for (t, mid, ts) in rows]
    def clean(s):
        return re.sub(r"\s+", " ", (s or "")).strip()
    joined = " \n".join(clean(t) for t in texts[:500])
    if len(joined) > 8000:
        joined = joined[:8000]

    dname = display_name or uname or "участник"
    target_html = tg_mention(target_id or 0, dname, uname)

    # === Обновлённые промпты: 3 абзаца, без ссылок, без <br> ===
    system = (
        "Ты — «Лорд Вербус»: остроумный, язвительный аристократ с холодным чувством превосходства. "
        "Пишешь НЕклинический психологический портрет по переписке человека. "
        "Не ставь диагнозов и не затрагивай чувствительные темы (религия, здоровье, политика, интим). "
        "Формат — ровно три абзаца обычного текста (без списков, без заголовков, без <br>). "
        "Абзацы должны быть разделены пустой строкой. "
        "Тон — изящная ирония, уверенность и лёгкое превосходство, без прямых оскорблений."
    )

    user = (
        f"Цель анализа: {target_html}\n\n"
        "Ниже корпус сообщений (новые → старые). Используй стиль, лексику, ритм и поведенческие маркеры:\n\n"
        f"{joined}\n\n"
        "Сформируй вывод из 3 абзацев:\n"
        "1) Вступление — назови участника по имени (жирным) и дай короткое вводное описание.\n"
        "2) Основная часть — психологический портрет: манера речи, мотиваторы, отношение к спору/риску, слепые зоны.\n"
        "3) Заключение — лаконичный саркастичный вердикт в стиле Лорда.\n"
        "Не вставляй ссылки и HTML, кроме <b>жирного</b> для имени в первом абзаце."
    )

    try:
        reply = await ai_reply(system, user, temperature=0.55)
        reply = strip_outer_quotes(reply)
        await m.reply(sanitize_html_whitelist(reply))
    except Exception as e:
        await m.reply(f"Портрет временно недоступен: {e}")

# =========================
# Small talk / interjections
# =========================
EPITHETS = [
    "умозаключение достойное утреннего сна, но не бодрствующего разума",
    "смелость есть, понимания нет — классика жанра",
    "где логика падала, там родилась эта идея",
    "аргумент звучит уверенно, как кот под дождём",
    "тут мысль пыталась быть острой, но сломала пятку",
    "интеллектуальный фейерверк, но без фейерверка",
    "редкий случай, когда тишина убедительнее ответа",
    "у этой логики крылья из ваты и амбиции из дыма",
    "настолько поверхностно, что даже воздух смутился",
    "решение с ароматом отчаяния и налётом глупости",
    "глубина анализа сравнима с лужей после дождя",
    "факт — враг этого мнения, но они стараются ужиться",
    "уверенность уровня «я видел это в мемах»",
    "звучит умно, если отключить критическое мышление",
    "в этом рассуждении больше пафоса, чем смысла",
    "дебют блестящий, финал трагический — в духе провинциальной оперы",
    "где-то плачет здравый смысл, но аплодисменты громче",
    "смелое предположение, не выдержавшее первой проверки",
    "поразительно, как из ничего сделали ещё меньше",
    "ментальная акробатика без страховки и без таланта",
    "доказательство строилось на вере и кофеине",
    "изящно, но неправильно — как кража с поклоном",
    "впечатляет, сколько слов можно потратить без смысла",
    "логика этого тезиса взята в аренду у фантазии",
    "аргумент держится на энтузиазме и самоуверенности",
    "тут даже здравый смысл бы попросил отпуск",
    "у этой идеи шанс, если закон гравитации отменят",
    "сформулировано с пафосом, исполнено с апатией",
    "по форме красиво, по сути жалко",
    "смесь уверенности и непонимания — взрывоопасна",
    "впечатление, что разум на перекуре",
    "серьёзность заявления не спасает его глупость",
    "на грани логики, но не с той стороны",
    "тут мысль так одинока, что ей бы подать объявление",
    "самоуверенность компенсирует отсутствие фактов",
    "как красиво сказать глупость — вот это талант",
    "звучит твёрдо, как желе под солнцем",
    "даже ошибка обиделась бы на такую форму",
    "у этой идеи масса, но отрицательная",
    "план, написанный на салфетке и уровнем выше, чем результат",
    "интеллектуальный пафос, в поисках смысла",
    "смелая попытка уговорить здравый смысл уйти в отпуск",
    "наукообразный способ сказать «я не знаю»",
    "в этой уверенности есть очарование незнания",
    "слов много, смысл — турист без визы",
]

def maybe_pick_epithet(p: float = 0.2, min_gap: int = 60) -> str | None:
    if random.random() > p:
        return None
    return random.choice(EPITHETS)

REPLY_COUNTER = 0
def bump_reply_counter():
    global REPLY_COUNTER
    REPLY_COUNTER += 1

async def reply_to_mention(m: Message):
    ctx_rows = db_query(
        "SELECT username, text FROM messages WHERE chat_id=? AND id<=(SELECT MAX(id) FROM messages WHERE message_id=?) ORDER BY id DESC LIMIT 12;",
        (m.chat.id, m.message_id)
    )
    ctx = "\n".join([f"{('@'+u) if u else 'user'}: {t}" for u, t in reversed(ctx_rows)])
    epithet = maybe_pick_epithet()
    add = f"\nМожно вставить одно уместное изящное выражение: «{epithet}»." if epithet else ""
    system = persona_prompt_natural()
    user = (
        "Тебя упомянули в групповом чате. Ответь коротко, 1-2 предложения, по существу и с холодной вежливостью. "
        "Допускается одна колкость или саркастичная ремарка. Допускается лаконичная нецензурная брань. "
        + add +
        f"\n\nНедавний контекст:\n{ctx}\n\nСообщение:\n«{m.text}»"
    )
    try:
        reply = await ai_reply(system, user, temperature=0.66)
        reply = strip_outer_quotes(reply)
        await m.reply(sanitize_html_whitelist(reply))
    finally:
        bump_reply_counter()

async def reply_to_thread(m: Message):
    ctx_rows = db_query(
        "SELECT username, text FROM messages WHERE chat_id=? ORDER BY id DESC LIMIT 12;",
        (m.chat.id,)
    )
    ctx_block = "\n".join([f"{('@'+u) if u else 'user'}: {t}" for u, t in reversed(ctx_rows)])
    epithet = maybe_pick_epithet()
    add = f"\nМожно вставить одно уместное изящное выражение: «{epithet}»." if epithet else ""
    system = persona_prompt_natural()
    user = (
        "Ответь на сообщение в ветке: коротко, высокомерно-иронично, но без прямых оскорблений. "
        "Сарказм допустим, только не скатывайся в грубость."
        + add +
        f"\n\nНедавний контекст:\n{ctx_block}\n\nСообщение:\n«{m.text}»"
    )
    reply = await ai_reply(system, user, temperature=0.66)
    reply = strip_outer_quotes(reply)
    await m.reply(sanitize_html_whitelist(reply))

async def maybe_interject(m: Message):
    # вмешиваемся иногда, если явный вопрос и не «тихий час»
    local_dt = datetime.now()
    if is_quiet_hours(local_dt): return
    if not is_question(m.text or ""): return
    if random.random() > 0.33: return
    if not can_interject(m.chat.id, cooldown=3600):  # 1800 секунд = 30 мин
        return

    ctx_rows = db_query(
        "SELECT username, text FROM messages WHERE chat_id=? ORDER BY id DESC LIMIT 8;",
        (m.chat.id,)
    )
    ctx_block = "\n".join([f"{('@'+u) if u else 'user'}: {t}" for u, t in reversed(ctx_rows)])
    epithet = maybe_pick_epithet()
    add = f"\nМожно вставить одно уместное изящное выражение: «{epithet}»." if epithet else ""
    system = persona_prompt_natural()
    user = (
        "Тебя упомянули в групповом чате. Ответь естественно и по делу, кратко; можно добавить одну короткую колкость."
        + add +
        f"\n\nНедавний контекст:\n{ctx_block}\n\nСообщение:\n«{m.text}»"
    )
    try:
        reply = await ai_reply(system, user, temperature=0.66)
        reply = strip_outer_quotes(reply)
        await m.reply(sanitize_html_whitelist(reply))
    finally:
        bump_reply_counter()

# =========================
# ACHIEVEMENTS: ядро (счётчики, выдача, редкость)
# =========================
def _achv_user_count(user_id: int) -> int:
    row = db_query("SELECT COUNT(*) FROM user_achievements WHERE user_id=?;", (user_id,))
    return int(row[0][0]) if row else 0

def _achv_total_holders(code: str) -> int:
    row = db_query("SELECT COUNT(DISTINCT user_id) FROM user_achievements WHERE code=?;", (code,))
    return int(row[0][0]) if row else 0

def _achv_population_size() -> int:
    row = db_query("SELECT COUNT(*) FROM users;")
    return int(row[0][0]) if row else 0

def _achv_rarity_percent(code: str) -> float:
    holders = _achv_total_holders(code)
    pop = max(_achv_population_size(), 1)
    return round(100.0 * holders / pop, 2)

def _styled_achv_card(code: str, title: str, desc: str, emoji: str, rarity_pct: float) -> str:
    return (
        f"<b>{emoji} Ачивка разблокирована!</b>\n"
        f"┌───────────────────────────────┐\n"
        f"│ <b>{_html.escape(title)}</b>\n"
        f"│ {_html.escape(desc)}\n"
        f"│ Редкость: <i>{rarity_pct}%</i>\n"
        f"└───────────────────────────────┘"
    )

def _styled_achv_counter(n: int) -> str:
    medals = "🏅" * min(n, 10)
    tail = f" +{n-10}" if n > 10 else ""
    return f"{medals}{tail}  <b>{n}</b>"

def inc_counter(user_id: int, key: str, delta: int = 1) -> int:
    with closing(sqlite3.connect(DB)) as conn:
        cur = conn.cursor()
        cur.execute("INSERT INTO user_counters(user_id, key, value) VALUES(?, ?, 0) ON CONFLICT(user_id, key) DO NOTHING;", (user_id, key))
        cur.execute("UPDATE user_counters SET value = value + ? WHERE user_id=? AND key=?;", (delta, user_id, key))
        conn.commit()
        cur.execute("SELECT value FROM user_counters WHERE user_id=? AND key=?;", (user_id, key))
        row = cur.fetchone()
        return int(row[0]) if row else 0

def _get_counter(user_id: int, key: str) -> int:
    row = db_query("SELECT value FROM user_counters WHERE user_id=? AND key=?;", (user_id, key))
    return int(row[0][0]) if row else 0

def _has_achievement(user_id: int, code: str) -> bool:
    row = db_query("SELECT 1 FROM user_achievements WHERE user_id=? AND code=? LIMIT 1;", (user_id, code))
    return bool(row)

def _grant_achievement(user_id: int, code: str) -> None:
    db_execute(
        "INSERT OR IGNORE INTO user_achievements(user_id, code, earned_at) VALUES (?, ?, ?);",
        (user_id, code, now_ts())
    )

async def check_achievements_for_user(uid: int, m: Message | None, updated_keys: list[str]) -> None:
    """
    Общая точка проверки: вызывай после инкремента счётчиков.
    updated_keys — список ключей, которые сейчас изменились (для быстрой фильтрации).
    """
    achs = db_query("SELECT code, title, description, emoji, type, key, threshold, active FROM achievements WHERE active=1;")
    if not achs:
        return
    dn, un = None, None
    urow = db_query("SELECT display_name, username FROM users WHERE user_id=? LIMIT 1;", (uid,))
    if urow:
        dn, un = urow[0]
    for code, title, desc, emoji, atype, key, threshold, active in achs:
        if atype == "counter_at_least":
            if key not in updated_keys:
                continue
            if _has_achievement(uid, code):
                continue
            if _get_counter(uid, key) >= int(threshold or 0):
                _grant_achievement(uid, code)
                rarity = _achv_rarity_percent(code)
                card = _styled_achv_card(code, title, desc, emoji or "🏆", rarity)
                who = tg_mention(uid, dn or (m.from_user.full_name if m and m.from_user else None), un or (m.from_user.username if m and m.from_user else None))
                tail = "Чтобы посмотреть все свои ачивки, напиши команду /achievements"
                if m:
                    try:
                        await m.reply(f"{who}\n{card}\n\n<i>{tail}</i>", disable_web_page_preview=True)
                    except Exception:
                        await m.reply(f"{(m.from_user.first_name if m and m.from_user else 'Пользователь')} получил ачивку: {title}. {tail}")

# =========================
# Handlers
# =========================
@dp.message(CommandStart())
async def start(m: Message):
    await m.reply(
        "Я — Лорд Вербус. Команды:\n"
        "• /lord_summary — краткий отчёт по беседе\n"
        "• /lord_psych — психологический портрет участника (ответь на его сообщение или укажи @username)\n"
        "• /achievements — посмотреть свои ачивки\n"
        "• /achievements_top — топ по ачивкам\n"
        "Просто говорите — я вмешаюсь, если нужно."
    )

# =========================
# Achievements: команды (ДОЛЖНЫ БЫТЬ ВЫШЕ on_text)
# =========================
@dp.message(Command("achievements"))
async def cmd_achievements(m: Message):
    if not m.from_user:
        return
    uid = m.from_user.id
    rows = db_query(
        "SELECT a.code, a.title, a.description, a.emoji, ua.earned_at "
        "FROM user_achievements ua JOIN achievements a ON a.code=ua.code "
        "WHERE ua.user_id=? ORDER BY ua.earned_at DESC;",
        (uid,)
    )
    total = len(rows)
    def _styled_achv_counter(n: int) -> str:
        medals = "🏅" * min(n, 10)
        tail = f" +{n-10}" if n > 10 else ""
        return f"{medals}{tail}  <b>{n}</b>"
    counter = _styled_achv_counter(total)
    if total == 0:
        await m.reply("У тебя пока нет ачивок. Продолжай — судьба любит настойчивых.")
        return
    def _achv_rarity_percent(code: str) -> float:
        holders = db_query("SELECT COUNT(DISTINCT user_id) FROM user_achievements WHERE code=?;", (code,))
        users_cnt = db_query("SELECT COUNT(*) FROM users;")
        pop = max(int(users_cnt[0][0]) if users_cnt else 1, 1)
        return round(100.0 * (int(holders[0][0]) if holders else 0) / pop, 2)
    lines = [f"🏆 Твои ачивки: {counter}\n"]
    for code, title, desc, emoji, ts in rows:
        rarity = _achv_rarity_percent(code)
        when = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")
        lines.append(
            f"{emoji} <b>{_html.escape(title)}</b>  "
            f"<i>{rarity}%</i>\n"
            f"— {_html.escape(desc)}  ·  <span class='tg-spoiler'>{when}</span>"
        )
    await m.reply("\n".join(lines), disable_web_page_preview=True)

@dp.message(Command("achievements_top"))
async def cmd_achievements_top(m: Message):
    rows = db_query(
        "SELECT ua.user_id, COUNT(*) as cnt "
        "FROM user_achievements ua "
        "GROUP BY ua.user_id "
        "ORDER BY cnt DESC, MIN(ua.earned_at) ASC "
        "LIMIT 10;"
    )
    if not rows:
        await m.reply("Топ пуст. Пора уже кому-то заработать первую ачивку.")
        return
    ids = tuple(r[0] for r in rows)
    placeholders = ",".join(["?"] * len(ids)) if ids else ""
    users = {}
    if ids:
        urows = db_query(f"SELECT user_id, display_name, username FROM users WHERE user_id IN ({placeholders});", ids)
        for uid, dn, un in urows:
            users[uid] = (dn, un)
    def tg_mention(uid: int, dn: str|None, un: str|None) -> str:
        name = (dn or un or "гость").strip()
        return f"<a href=\"tg://user?id={uid}\">{_html.escape(name)}</a>"
    out = ["<b>🏆 ТОП по ачивкам</b>\n"]
    rank = 1
    for uid, cnt in rows:
        dn, un = users.get(uid, (None, None))
        out.append(f"{rank}. {tg_mention(uid, dn, un)} — <b>{cnt}</b> {('🏅'*min(cnt,5))}")
        rank += 1
    await m.reply("\n".join(out), disable_web_page_preview=True)

# Диагностика
@dp.message(Command("ach_debug"))
async def cmd_ach_debug(m: Message):
    if not m.from_user:
        return
    uid = m.from_user.id
    full_name = (m.from_user.full_name or "").strip() or (m.from_user.first_name or "")
    db_execute(
        "INSERT INTO users(user_id, display_name, username) VALUES(?, ?, ?) "
        "ON CONFLICT(user_id) DO UPDATE SET display_name=excluded.display_name, username=excluded.username;",
        (uid, full_name, m.from_user.username)
    )
    row = db_query("SELECT value FROM user_counters WHERE user_id=? AND key=?;", (uid, "cmd:/q"))
    q_cnt = int(row[0][0]) if row else 0
    has_q10 = bool(db_query("SELECT 1 FROM user_achievements WHERE user_id=? AND code='Q10' LIMIT 1;", (uid,)))
    await m.reply(
        f"🔍 Debug:\n"
        f"• cmd:/q = <b>{q_cnt}</b>\n"
        f"• Q10 выдана: <b>{'да' if has_q10 else 'нет'}</b>\n"
        f"(Порог Q10: 10 раз /q)",
        disable_web_page_preview=True
    )

# Алиасы на опечатки: /achievments, /achievments_top
@dp.message(Command("achievments"))
async def _alias_achievments(m: Message):
    await cmd_achievements(m)

@dp.message(Command("achievments_top"))
async def _alias_achievments_top(m: Message):
    await cmd_achievements_top(m)


@dp.message(F.text & ~F.text.startswith("/"))
async def on_text(m: Message):
    # далее — твой существующий код on_text без первых трёх строк-проверок
    if not m.text:
        return

    # логируем текст
    if not m.text.startswith("/"):
        db_execute(
            "INSERT INTO messages(chat_id, user_id, username, text, created_at, message_id) VALUES (?, ?, ?, ?, ?, ?);",
            (m.chat.id, m.from_user.id if m.from_user else 0,
             m.from_user.username if m.from_user else None,
             m.text, now_ts(), m.message_id)
        )
        # — обновляем карточку пользователя (для кликабельных имён и метрик)
        if m.from_user:
            full_name = (m.from_user.full_name or "").strip() or (m.from_user.first_name or "")
            db_execute(
                "INSERT INTO users(user_id, display_name, username) VALUES(?, ?, ?) "
                "ON CONFLICT(user_id) DO UPDATE SET display_name=excluded.display_name, username=excluded.username;",
                (m.from_user.id, full_name, m.from_user.username)
            )
            # инкремент универсального счётчика сообщений (для будущих ачивок типа MSG100)
            inc_counter(m.from_user.id, "msg:total", 1)
            # можно проверять ачивки, если появятся, завязанные на msg:total
            # await check_achievements_for_user(m.from_user.id, m, updated_keys=["msg:total"])

    me = await bot.get_me()

    if m.text.startswith("/"):
        return

    if m.reply_to_message and m.reply_to_message.from_user and m.reply_to_message.from_user.id == me.id:
        await reply_to_thread(m)
        return

    if mentions_bot(m.text or "", me.username):
        await reply_to_mention(m)
        return

    await maybe_interject(m)


# =========================
# Трекинг /q → универсальный счётчик + проверка правил
# =========================
@dp.message(Command("q"))
async def track_q_and_maybe_award(m: Message):
    if not m.from_user:
        return
    uid = m.from_user.id
    # поддержим users-карточку (для редкости/кликабельности)
    full_name = (m.from_user.full_name or "").strip() or (m.from_user.first_name or "")
    db_execute(
        "INSERT INTO users(user_id, display_name, username) VALUES(?, ?, ?) "
        "ON CONFLICT(user_id) DO UPDATE SET display_name=excluded.display_name, username=excluded.username;",
        (uid, full_name, m.from_user.username)
    )
    inc_counter(uid, "cmd:/q", 1)
    await check_achievements_for_user(uid, m, updated_keys=["cmd:/q"])

# =========================
# Уведомление о кружочке Даши
# =========================
def _message_link(chat, message_id: int) -> str | None:
    """
    Возвращает кликабельную ссылку на сообщение, если возможно.
    Работает для публичных супергрупп/каналов (username) и приватных супергрупп (-100... -> /c/).
    Для обычных приватных групп без username ссылка недоступна.
    """
    if getattr(chat, "username", None):
        return f"https://t.me/{chat.username}/{message_id}"
    cid = str(chat.id)
    if cid.startswith("-100"):  # приватная супергруппа
        return f"https://t.me/c/{cid[4:]}/{message_id}"
    return None

@dp.message(F.video_note)
async def on_video_note_watch(m: Message):
    """
    Если @daria_mango (WATCH_USER_ID) отправляет видеокружок,
    бот:
      1) В ГРУППЕ/СУПЕРГРУППЕ тегает @misukhanov в ответе на это сообщение.
      2) (опционально можно добавить дублирование в ЛС — сейчас отключено)
    """
    user = m.from_user
    if not user or user.id != WATCH_USER_ID:
        return

    who_html = tg_mention(user.id, user.full_name or user.first_name, user.username)
    notify_html = tg_mention(NOTIFY_USER_ID, f"@{NOTIFY_USERNAME}", NOTIFY_USERNAME)

    link = _message_link(m.chat, m.message_id)
    link_html = f" <a href=\"{link}\">ссылка</a>" if link else ""

    if m.chat.type in ("group", "supergroup"):
        try:
            await m.reply(
                f"{notify_html}, {who_html} отправил видеокружок.{link_html}",
                disable_web_page_preview=True
            )
        except Exception:
            await m.reply(f"@{NOTIFY_USERNAME}, видеокружок от @{user.username or user.id}")

# =========================
# Commands list
# =========================
async def set_commands():
    commands_group = [
        BotCommand(command="lord_summary", description="Краткий отчёт по беседе"),
        BotCommand(command="lord_psych",  description="Психологический портрет участника"),
        BotCommand(command="achievements", description="Показать мои ачивки"),
        BotCommand(command="achievements_top", description="Топ по ачивкам"),
    ]
    commands_private = [
        BotCommand(command="lord_summary", description="Краткий отчёт по беседе"),
        BotCommand(command="lord_psych",  description="Психологический портрет участника"),
        BotCommand(command="start", description="Приветствие"),
        BotCommand(command="achievements", description="Показать мои ачивки"),
        BotCommand(command="achievements_top", description="Топ по ачивкам"),
        BotCommand(command="ach_debug", description="Показать статистику по ачивкам (debug)"),
    ]
    await bot.set_my_commands(commands_group, scope=BotCommandScopeAllGroupChats())
    await bot.set_my_commands(commands_private, scope=BotCommandScopeAllPrivateChats())

# =========================
# Main
# =========================
async def main():
    init_db()
    await set_commands()
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
