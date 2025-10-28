import os
import asyncio
import random
import re
import sqlite3
from contextlib import closing
from datetime import datetime, timedelta, timezone
import html as _html
import os, pathlib

import aiohttp
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command, CommandStart, CommandObject
from aiogram.types import Message, BotCommand, BotCommandScopeAllGroupChats, BotCommandScopeAllPrivateChats
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties

# ============================================================
# Config
# ============================================================
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

# --- кто может видеть /ach_progress (скрытая админ-команда)
# добавь сюда свои реальные user_id (через запятую).
ADMIN_IDS = {320872593}

# --- уведомления о кружках у конкретного пользователя
WATCH_USER_ID = 447968194   # @daria_mango
NOTIFY_USER_ID = 254160871  # @misukhanov
NOTIFY_USERNAME = "misukhanov"

# ============================================================
# Achievements: определения (ЕДИНСТВЕННАЯ РЕДАКТИРУЕМАЯ ФУНКЦИЯ)
# Типы правил:
#   • counter_at_least — выдать, когда user_counters[user_id, key] ≥ threshold
#   • counter_at_least_monthly — как выше, но key строится с YYYY-MM (см. month_key)
# Поля: code, title, description, emoji, type, key, threshold, active, meta(None/JSON)
# ============================================================
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

        # ===== PACK 1: Стикеры (всего за всё время) =====
        {"code":"STK50","title":"Стикер-спамер","description":"Отправил 50 стикеров",
         "emoji":"🥉","type":"counter_at_least","key":"sticker:total","threshold":50,"active":1,"meta":None},
        {"code":"STK500","title":"Мастер стикер-спама","description":"Отправил 500 стикеров",
         "emoji":"🥈","type":"counter_at_least","key":"sticker:total","threshold":500,"active":1,"meta":None},
        {"code":"STK5000","title":"Даша, ну ты и ебанутая","description":"Отправила 5000 стикеров",
         "emoji":"🥇","type":"counter_at_least","key":"sticker:total","threshold":5000,"active":1,"meta":None},

        # ===== PACK 2: Сообщения за месяц (учитываем все НЕ-командные события) =====
        {"code":"MSGM150","title":"Залетный хуй","description":"Всего 150 сообщений за месяц, ничтожество",
         "emoji":"🥉","type":"counter_at_least_monthly","key":"msg:month","threshold":150,"active":1,"meta":None},
        {"code":"MSGM1000","title":"Завсегдатай чата","description":"1000 сообщений за месяц, можно уважать",
         "emoji":"🥈","type":"counter_at_least_monthly","key":"msg:month","threshold":1000,"active":1,"meta":None},
        {"code":"MSGM5000","title":"Как же я люблю попиздеть","description":"5000 сообщений за месяц, ебанутое создание!",
         "emoji":"🥇","type":"counter_at_least_monthly","key":"msg:month","threshold":5000,"active":1,"meta":None},

        # ===== PACK 3: Голосовые за месяц =====
        {"code":"VOIM10","title":"Любитель потрещать","description":"Высрал/-а 10 голосовых за месяц",
         "emoji":"🥉","type":"counter_at_least_monthly","key":"voice:month","threshold":10,"active":1,"meta":None},
        {"code":"VOIM100","title":"Да закрой ты варежку","description":"100 голосовых за месяц",
         "emoji":"🥈","type":"counter_at_least_monthly","key":"voice:month","threshold":100,"active":1,"meta":None},
        {"code":"VOIM1000","title":"Конченая мразь","description":"1000 голосовых за месяц, не, ну это пиздец. Нет слов, вызывайте дурку!",
         "emoji":"🥇","type":"counter_at_least_monthly","key":"voice:month","threshold":1000,"active":1,"meta":None},

                # ===== PACK: Тест «testtest» (всего за всё время) =====
        {"code":"TT1","title":"Тест-драйв","description":"Один раз написал слово testtest",
         "emoji":"🧪","type":"counter_at_least","key":"testtest:total","threshold":1,"active":1,"meta":None},
        {"code":"TT3","title":"Повторюшка","description":"Трижды написал слово testtest",
         "emoji":"🧪","type":"counter_at_least","key":"testtest:total","threshold":3,"active":1,"meta":None},
        {"code":"TT5","title":"Тестоман","description":"Пять раз написал слово testtest",
         "emoji":"🧪","type":"counter_at_least","key":"testtest:total","threshold":5,"active":1,"meta":None},

    ]

# ============================================================
# DB: схема, миграции, сидирование справочника ачивок
# ============================================================
def init_db():
    """Инициализация БД: таблицы сообщений/пользователей/ачивок/счётчиков + сидирование achievements."""
    with closing(sqlite3.connect(DB)) as conn:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")

        # ---- лог сообщений (для саммари и поведенки)
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

        # ---- FTS индекс и триггеры (под поиск по сообщениям)
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

        # ---- карточки пользователей (имена для кликабельных @ и метрик)
        conn.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            display_name TEXT,
            username TEXT
        );
        """)

        # ---- запоминание последнего саммари (ссылка на сообщение)
        conn.execute("""
        CREATE TABLE IF NOT EXISTS last_summary (
            chat_id INTEGER PRIMARY KEY,
            message_id INTEGER,
            created_at INTEGER
        );
        """)

        # ---- справочник ачивок, выдачи и счётчики
        conn.execute("""
        CREATE TABLE IF NOT EXISTS achievements (
            code TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            description TEXT NOT NULL,
            emoji TEXT DEFAULT '🏆',
            type TEXT,
            key TEXT,
            threshold INTEGER,
            active INTEGER NOT NULL DEFAULT 1,
            meta TEXT
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

        # --- мягкая миграция колонок achievements (на случай старых баз)
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

        # ---- сидирование справочника ачивок из define_achievements()
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

# ============================================================
# DB utils: быстрые обёртки
# ============================================================
def db_execute(sql: str, params: tuple = ()):
    """Выполнить SQL без возврата результата (commit)."""
    with closing(sqlite3.connect(DB)) as conn:
        conn.execute(sql, params)
        conn.commit()

def db_query(sql: str, params: tuple = ()):
    """Выполнить SQL и вернуть все строки resultset."""
    with closing(sqlite3.connect(DB)) as conn:
        cur = conn.execute(sql, params)
        return cur.fetchall()

def get_user_messages(chat_id: int, user_id: int | None, username: str | None, limit: int = 500):
    """Вернуть последние тексты пользователя в чате (для портрета/саммари)."""
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
    """Текущий UNIX-timestamp (UTC)."""
    return int(datetime.now(timezone.utc).timestamp())

# ============================================================
# Helpers: текст, ключи, форматирование, упоминания
# ============================================================
QUESTION_PATTERNS = [
    r"\bкто\b", r"\bчто\b", r"\bкак\b", r"\bпочему\b", r"\bзачем\b",
    r"\bкогда\b", r"\bгде\b", r"\bкакой\b", r"\bкакая\b", r"\bкакие\b",
    r"\bсколько\b", r"\bможно ли\b", r"\bесть ли\b"
]
QUESTION_RE = re.compile("|".join(QUESTION_PATTERNS), re.IGNORECASE)

def is_bot_command(m: Message) -> bool:
    """Проверка, что сообщение — команда (entity type=bot_command в начале)."""
    if not m or not getattr(m, "entities", None) or not m.text:
        return False
    for e in m.entities:
        if e.type == "bot_command" and e.offset == 0:
            return True
    return False

def month_key(prefix: str, dt: datetime | None = None) -> str:
    """Построить ключ счётчика за текущий месяц: prefix:YYYY-MM."""
    d = dt or datetime.now(timezone.utc)
    return f"{prefix}:{d.strftime('%Y-%m')}"

def is_question(text: str) -> bool:
    """Есть ли в тексте вопросительные паттерны (для auto-interject)."""
    return bool(text and QUESTION_RE.search(text))

def mentions_bot(text: str, bot_username: str | None) -> bool:
    """Проверка @упоминания бота."""
    if not text or not bot_username: return False
    return f"@{bot_username.lower()}" in text.lower()

def is_quiet_hours(local_dt: datetime) -> bool:
    """Тихие часы: не вмешиваемся с 00:00 до 07:00 локального времени."""
    return 0 <= local_dt.hour < 7

def sanitize_html_whitelist(text: str) -> str:
    """Пропускаем только безопасные HTML-теги (для ответов ИИ и т.п.)."""
    allowed_tags = {"b","strong","i","em","u","s","del","code","pre","a","br","blockquote","span"}
    def repl(m):
        tag = m.group(1).lower().strip("/")
        return m.group(0) if tag in allowed_tags else _html.escape(m.group(0))
    text = re.sub(r"<\s*/?\s*([a-zA-Z0-9]+)[^>]*>", repl, text)
    text = re.sub(r"<a\s+([^>]+)>", lambda mm: (
        "<a " + " ".join(p for p in mm.group(1).split() if p.lower().startswith("href=")) + ">"
    ), text)
    return text

def strip_outer_quotes(s: str) -> str:
    """Снять внешние кавычки у ответа ИИ, если он процитировал себя."""
    t = s.strip()
    if (t.startswith("«") and t.endswith("»")) or (t.startswith('"') and t.endswith('"')) or (t.startswith("'") and t.endswith("'")):
        return t[1:-1].strip()
    return s

def tg_link(chat_id: int, message_id: int) -> str:
    """Ссылка t.me/c/... на сообщение супергруппы."""
    return f"https://t.me/c/{str(chat_id)[4:]}/{message_id}"

def persona_prompt_natural() -> str:
    """Системный промпт для «Лорда Вербуса» — коротко и колко."""
    return (
        "Ты — «Лорд Вербус»: остроумный, язвительный и высокомерный интеллектуал. "
        "Говоришь как человек, которому скучно от человеческой глупости. "
        "Жёсткие правила лаконичности: 1–2 предложения, максимум ~200 символов. "
        "Используй сарказм, метафоры и лёгкое унижение, но без прямых оскорблений. "
        "Отвечай коротко, резко, с тоном превосходства и ледяного спокойствия. "
        "Не оправдывайся, не восхищайся, не льсти, не повторяйся. "
        "НЕ заключай весь ответ в кавычки и не цитируй свой собственный текст."
    )

def tg_mention(user_id: int, display_name: str | None, username: str | None) -> str:
    """HTML-упоминание пользователя по user_id (кликабельно в Telegram)."""
    name = (display_name or username or "гость").strip()
    safe = _html.escape(name)
    return f"<a href=\"tg://user?id={user_id}\">{safe}</a>"

# ============================================================
# Helpers: сервисные для ачивок
# ============================================================
def get_all_families():
    """Сгруппировать ачивки по (type, key) → список по threshold ASC."""
    rows = db_query(
        "SELECT code, title, description, emoji, type, key, threshold "
        "FROM achievements WHERE active=1 ORDER BY type, key, COALESCE(threshold, 0) ASC;"
    )
    fams = {}
    for r in rows:
        fams.setdefault((r[4], r[5]), []).append(r)
    return fams

def friendly_family_title(atype: str, akey: str) -> str:
    """Человеческие заголовки для семейств ачивок."""
    m = {
        ("counter_at_least", "sticker:total"): "Стикеры (всего)",
        ("counter_at_least_monthly", "msg:month"): "Сообщения за месяц",
        ("counter_at_least_monthly", "voice:month"): "Голосовые за месяц",
        ("counter_at_least", "cmd:/q"): "Команда /q",
        ("counter_at_least", "testtest:total"): "Тестовое слово «testtest»",
    }
    return m.get((atype, akey), akey)

def user_current_tier(family_rows: list[tuple], value: int):
    """Определить текущую ступень (бронза/серебро/золото) по текущему value."""
    tier = None
    for code, title, desc, emoji, atype, akey, thr in family_rows:
        thr = int(thr or 0)
        if value >= thr:
            tier = (code, title, emoji, thr)
        else:
            break
    return tier

def family_next_threshold(family_rows: list[tuple], current_thr: int | None):
    """Найти следующий порог после current_thr. Если None — вернуть самый первый."""
    for code, title, desc, emoji, atype, akey, thr in family_rows:
        thr = int(thr or 0)
        if current_thr is None or thr > int(current_thr):
            return thr
    return None

def get_achievement_by_code(code: str):
    """Получить запись ачивки по коду."""
    rows = db_query(
        "SELECT code, title, description, emoji, type, key, threshold, active FROM achievements WHERE code=? LIMIT 1;",
        (code.strip().upper(),)
    )
    return rows[0] if rows else None

def get_family_by_code(code: str):
    """Семья ачивок по одному коду (все с тем же type+key, отсортированные по threshold)."""
    a = get_achievement_by_code(code)
    if not a:
        return []
    _code, _title, _desc, _emoji, atype, akey, _thr, _active = a
    rows = db_query(
        "SELECT code, title, description, emoji, type, key, threshold "
        "FROM achievements WHERE active=1 AND type=? AND key=? ORDER BY COALESCE(threshold, 0) ASC;",
        (atype, akey),
    )
    return rows

def resolve_counter_key_for_user(atype: str, key_prefix: str) -> str:
    """Построить реальный ключ counters для данного типа (учесть YYYY-MM для monthly)."""
    if atype == "counter_at_least":
        return key_prefix
    elif atype == "counter_at_least_monthly":
        return month_key(key_prefix)
    return key_prefix

# ============================================================
# AI: вызов OpenRouter
# ============================================================
async def ai_reply(system_prompt: str, user_prompt: str, temperature: float = 0.5):
    """Вызов OpenRouter Chat Completions с промптами system/user."""
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

# ============================================================
# Linkify helpers (для саммари)
# ============================================================
LINK_PAT = re.compile(r"\[link:\s*(https?://[^\]\s]+)\s*\]")
ANCHOR_PAT = re.compile(r"<a\s+href=['\"](https?://[^'\"]+)['\"]\s*>Источник</a>", re.IGNORECASE)

def _wrap_last_words(text: str, url: str, min_w: int = 2, max_w: int = 5) -> str:
    """Привязать ссылку к последним 2–5 словам слева от [link: URL]."""
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
    """[link: URL] → встроенная ссылка на предыдущие 2–5 слов; «Источник» → тоже."""
    urls = LINK_PAT.findall(text or "")
    for url in urls:
        text = _wrap_last_words(text, url)
    for m in list(ANCHOR_PAT.finditer(text or "")):
        url = m.group(1)
        start, end = m.span()
        left = text[:start]; right = text[end:]
        tmp = left + f"[link: {url}]" + right
        text = _wrap_last_words(tmp, url)
    return LINK_PAT.sub(lambda mm: f"<a href='{mm.group(1)}'>ссылка</a>", text)

# ============================================================
# SUMMARY (жесткий шаблон)
# ============================================================
def prev_summary_link(chat_id: int) -> str | None:
    """Ссылка на предыдущее сообщение с саммари (если есть)."""
    row = db_query("SELECT message_id FROM last_summary WHERE chat_id=? ORDER BY created_at DESC LIMIT 1;", (chat_id,))
    return tg_link(chat_id, row[0][0]) if row else None

@dp.message(Command("lord_summary"))
async def cmd_summary(m: Message, command: CommandObject):
    """Сгенерировать саммари последних сообщений чата в фиксированном HTML-формате."""
    try:
        n = int((command.args or "").strip()); n = max(50, min(800, n))
    except Exception:
        n = 300

    rows = db_query(
        "SELECT user_id, username, text, message_id FROM messages WHERE chat_id=? AND text IS NOT NULL ORDER BY id DESC LIMIT ?;",
        (m.chat.id, n)
    )
    if not rows:
        await m.reply("У меня пока нет сообщений для саммари."); return

    prev_link = prev_summary_link(m.chat.id)
    prev_line_html = f'<a href="{prev_link}">Предыдущий анализ</a>' if prev_link else "Предыдущий анализ (—)"

    # участники → кликабельные имена
    user_ids = tuple({r[0] for r in rows})
    users_map = {}
    if user_ids:
        placeholders = ",".join(["?"] * len(user_ids))
        urows = db_query(f"SELECT user_id, display_name, username FROM users WHERE user_id IN ({placeholders});", user_ids)
        for uid, dname, uname in urows:
            users_map[uid] = (dname, uname)

    participants = [tg_mention(uid, *users_map.get(uid, (None, None))) for uid in user_ids]
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
        "Один абзац (1–3 предложения). Обязательно назови по именам участников, и вставь 1–3 ссылки ВНУТРИ текста.\n\n"
        "😄 <b><a href=\"[link: ТЕМА2_URL]\">[ПРИДУМАННОЕ НАЗВАНИЕ ТЕМЫ]</a></b>\n"
        "Абзац по тем же правилам.\n\n"
        "😄 <b><a href=\"[link: ТЕМА3_URL]\">[ПРИДУМАННОЕ НАЗВАНИЕ ТЕМЫ]</a></b>\n"
        "Абзац по тем же правилам. Если явных тем меньше, заверши третью темой-резюме.\n\n"
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

# ============================================================
# Психологический портрет (3 абзаца, без ссылок и <br>)
# ============================================================
@dp.message(Command("lord_psych"))
async def cmd_lord_psych(m: Message, command: CommandObject):
    """Психологический портрет участника по его сообщениям (reply или @username)."""
    target_id, display_name, uname = await resolve_target_user(m)
    if not target_id and not uname:
        await m.reply("Кого анализируем? Ответь командой на сообщение пользователя или укажи @username.")
        return

    rows = get_user_messages(m.chat.id, target_id, uname, limit=600)
    if not rows:
        hint = "Нет сообщений в базе по этому пользователю."
        if uname and not target_id:
            hint += " Возможно, у этого @username нет сохранённого user_id (старые сообщения)."
        await m.reply(hint); return

    texts = [t for (t, mid, ts) in rows]
    def clean(s): return re.sub(r"\s+", " ", (s or "")).strip()
    joined = " \n".join(clean(t) for t in texts[:500])
    if len(joined) > 8000: joined = joined[:8000]

    dname = display_name or uname or "участник"
    target_html = tg_mention(target_id or 0, dname, uname)

    system = (
        "Ты — «Лорд Вербус»: остроумный, язвительный аристократ. "
        "Пишешь НЕклинический психологический портрет. "
        "Формат — ровно три абзаца обычного текста (без списков/заголовков/<br>). "
        "Тон — изящная ирония, без прямых оскорблений."
    )
    user = (
        f"Цель анализа: {target_html}\n\n"
        f"{joined}\n\n"
        "Сформируй вывод из 3 абзацев: 1) вступление с <b>жирным</b> именем; 2) основная часть; 3) лаконичный вердикт."
    )

    try:
        reply = await ai_reply(system, user, temperature=0.55)
        reply = strip_outer_quotes(reply)
        await m.reply(sanitize_html_whitelist(reply))
    except Exception as e:
        await m.reply(f"Портрет временно недоступен: {e}")

# ============================================================
# Small-talk / авто-вставки
# ============================================================
EPITHETS = [
    "умозаключение достойное утреннего сна, но не бодрствующего разума",
    "смелость есть, понимания нет — классика жанра",
    "где логика падала, там родилась эта идея",
    # ... (остальные фразы оставил как в твоём файле)
]

LAST_INTERJECT: dict[int, int] = {}  # {chat_id: ts последней самовставки}
REPLY_COUNTER = 0

def maybe_pick_epithet(p: float = 0.2) -> str | None:
    """С вероятностью p вернуть случайную колкость."""
    return random.choice(EPITHETS) if random.random() <= p else None

def bump_reply_counter():
    """Служебный счётчик для отладки."""
    global REPLY_COUNTER; REPLY_COUNTER += 1

def can_interject(chat_id: int, cooldown: int = 3600) -> bool:
    """Антиспам на авто-вставки: не чаще, чем раз в cooldown сек."""
    now = now_ts(); last = LAST_INTERJECT.get(chat_id, 0)
    if now - last < cooldown: return False
    LAST_INTERJECT[chat_id] = now; return True

async def reply_to_mention(m: Message):
    """Ответ на явное упоминание бота @username в чате."""
    ctx_rows = db_query(
        "SELECT username, text FROM messages WHERE chat_id=? AND id<=(SELECT MAX(id) FROM messages WHERE message_id=?) ORDER BY id DESC LIMIT 12;",
        (m.chat.id, m.message_id)
    )
    ctx = "\n".join([f"{('@'+u) if u else 'user'}: {t}" for u, t in reversed(ctx_rows)])
    epithet = maybe_pick_epithet()
    add = f"\nМожно вставить одно уместное изящное выражение: «{epithet}»." if epithet else ""
    system = persona_prompt_natural()
    user = (
        "Тебя упомянули в групповом чате. Ответь коротко, 1–2 предложения."
        + add + f"\n\nНедавний контекст:\n{ctx}\n\nСообщение:\n«{m.text}»"
    )
    try:
        reply = await ai_reply(system, user, temperature=0.66)
        reply = strip_outer_quotes(reply)
        await m.reply(sanitize_html_whitelist(reply))
    finally:
        bump_reply_counter()

async def reply_to_thread(m: Message):
    """Ответ в треде на реплай к боту."""
    ctx_rows = db_query("SELECT username, text FROM messages WHERE chat_id=? ORDER BY id DESC LIMIT 12;", (m.chat.id,))
    ctx_block = "\n".join([f"{('@'+u) if u else 'user'}: {t}" for u, t in reversed(ctx_rows)])
    epithet = maybe_pick_epithet()
    add = f"\nМожно вставить одно уместное изящное выражение: «{epithet}»." if epithet else ""
    system = persona_prompt_natural()
    user = "Ответь на сообщение в ветке: коротко, иронично, но без грубости." + add + f"\n\nКонтекст:\n{ctx_block}\n\nСообщение:\n«{m.text}»"
    reply = await ai_reply(system, user, temperature=0.66)
    reply = strip_outer_quotes(reply)
    await m.reply(sanitize_html_whitelist(reply))

async def maybe_interject(m: Message):
    """Иногда вмешиваемся сами, если увидели вопрос и не тихие часы."""
    local_dt = datetime.now()
    if is_quiet_hours(local_dt): return
    if not is_question(m.text or ""): return
    if random.random() > 0.33: return
    if not can_interject(m.chat.id, cooldown=3600): return

    ctx_rows = db_query("SELECT username, text FROM messages WHERE chat_id=? ORDER BY id DESC LIMIT 8;", (m.chat.id,))
    ctx_block = "\n".join([f"{('@'+u) if u else 'user'}: {t}" for u, t in reversed(ctx_rows)])
    epithet = maybe_pick_epithet()
    add = f"\nМожно вставить одно уместное изящное выражение: «{epithet}»." if epithet else ""
    system = persona_prompt_natural()
    user = "Ответь естественно и по делу, кратко; можно одну колкость." + add + f"\n\nКонтекст:\n{ctx_block}\n\nСообщение:\n«{m.text}»"
    try:
        reply = await ai_reply(system, user, temperature=0.66)
        reply = strip_outer_quotes(reply)
        await m.reply(sanitize_html_whitelist(reply))
    finally:
        bump_reply_counter()

# ============================================================
# Achievements: ядро (счётчики, выдача, редкость)
# ============================================================
def _achv_user_count(user_id: int) -> int:
    """Сколько ачивок у конкретного пользователя уже выдано."""
    row = db_query("SELECT COUNT(*) FROM user_achievements WHERE user_id=?;", (user_id,))
    return int(row[0][0]) if row else 0

def _achv_total_holders(code: str) -> int:
    """Сколько разных пользователей получили ачивку code."""
    row = db_query("SELECT COUNT(DISTINCT user_id) FROM user_achievements WHERE code=?;", (code,))
    return int(row[0][0]) if row else 0

def _achv_population_size() -> int:
    """Общее число пользователей (для процента редкости)."""
    row = db_query("SELECT COUNT(*) FROM users;")
    return int(row[0][0]) if row else 0

def _achv_rarity_percent(code: str) -> float:
    """Редкость ачивки в процентах среди всех пользователей."""
    holders = _achv_total_holders(code)
    pop = max(_achv_population_size(), 1)
    return round(100.0 * holders / pop, 2)

def _styled_achv_card(code: str, title: str, desc: str, emoji: str, rarity_pct: float) -> str:
    """Красивая карточка ачивки."""
    return (
        f"<b>{emoji} Ачивка разблокирована!</b>\n"
        f"┌───────────────────────────────┐\n"
        f"│ <b>{_html.escape(title)}</b>\n"
        f"│ {_html.escape(desc)}\n"
        f"│ Редкость: <i>{rarity_pct}%</i>\n"
        f"└───────────────────────────────┘"
    )

def _styled_achv_counter(n: int) -> str:
    """Медальки + число ачивок."""
    medals = "🏅" * min(n, 10)
    tail = f" +{n-10}" if n > 10 else ""
    return f"{medals}{tail}  <b>{n}</b>"

def inc_counter(user_id: int, key: str, delta: int = 1) -> int:
    """Инкремент счётчика user_counters[user_id, key] на delta и вернуть новое значение."""
    with closing(sqlite3.connect(DB)) as conn:
        cur = conn.cursor()
        cur.execute("INSERT INTO user_counters(user_id, key, value) VALUES(?, ?, 0) ON CONFLICT(user_id, key) DO NOTHING;", (user_id, key))
        cur.execute("UPDATE user_counters SET value = value + ? WHERE user_id=? AND key=?;", (delta, user_id, key))
        conn.commit()
        cur.execute("SELECT value FROM user_counters WHERE user_id=? AND key=?;", (user_id, key))
        row = cur.fetchone()
        return int(row[0]) if row else 0

def _get_counter(user_id: int, key: str) -> int:
    """Получить текущее значение счётчика user_counters[user_id, key]."""
    row = db_query("SELECT value FROM user_counters WHERE user_id=? AND key=?;", (user_id, key))
    return int(row[0][0]) if row else 0

def _has_achievement(user_id: int, code: str) -> bool:
    """Проверить, выдана ли уже пользователю ачивка code."""
    row = db_query("SELECT 1 FROM user_achievements WHERE user_id=? AND code=? LIMIT 1;", (user_id, code))
    return bool(row)

def _grant_achievement(user_id: int, code: str) -> None:
    """Выдать ачивку (идемпотентно)."""
    db_execute("INSERT OR IGNORE INTO user_achievements(user_id, code, earned_at) VALUES (?, ?, ?);", (user_id, code, now_ts()))

async def check_achievements_for_user(uid: int, m: Message | None, updated_keys: list[str]) -> None:
    """
    Центральная проверка: вызывай ПОСЛЕ инкремента счётчиков.
    Работает ПО СЕМЕЙСТВАМ: для каждого (type, key) проверяются все пороги (tiers)
    по возрастанию и выдаются ВСЕ недостающие ступени, на которые хватает значения.
    В monthly-семействах ключ строится через month_key(key).
    updated_keys — список конкретных ключей, которые только что изменились (например "sticker:total" или "msg:month:2025-10").
    Принимаем также «сырой» ключ семейства (например "msg:month"), чтобы ресканы не промахивались.
    """
    achs = db_query(
        "SELECT code, title, description, emoji, type, key, threshold "
        "FROM achievements WHERE active=1;"
    )
    if not achs:
        return

    # Имя/юзернейм для красивого упоминания
    dn, un = None, None
    urow = db_query("SELECT display_name, username FROM users WHERE user_id=? LIMIT 1;", (uid,))
    if urow:
        dn, un = urow[0]

    # Сгруппировать по семействам: (type, key) -> [(code, title, desc, emoji, thr), ...]
    families: dict[tuple[str, str], list[tuple[str, str, str, str, int]]] = {}
    for code, title, desc, emoji, atype, key_field, threshold in achs:
        families.setdefault((atype, key_field), []).append(
            (code, title, desc, emoji, int(threshold or 0))
        )

    # Для каждого семейства: рассчитать реальный ключ (учитывая monthly), проверить пороги
    for (atype, key_field), rows in families.items():
        # Определяем реальный ключ счётчика
        if atype == "counter_at_least_monthly":
            real_key = month_key(key_field)
        else:
            real_key = key_field

        # Триггер по updated_keys: допускаем как real_key, так и «сырой» family key
        if (real_key not in updated_keys) and (key_field not in updated_keys):
            continue

        # Текущее значение
        val = _get_counter(uid, real_key)

        # Проверяем пороги по возрастанию и выдаём всё, чего ещё нет
        rows_sorted = sorted(rows, key=lambda r: r[4])  # r[4] = threshold
        for code, title, desc, emoji, thr in rows_sorted:
            if thr <= 0:
                continue
            if not _has_achievement(uid, code) and val >= thr:
                _grant_and_announce(uid, code, title, desc, emoji, m, dn, un)



def _grant_and_announce(uid: int, code: str, title: str, desc: str, emoji: str, m: Message | None,
                        dn: str | None, un: str | None):
    """Выдать ачивку и красиво объявить об этом в чате."""
    _grant_achievement(uid, code)
    rarity = _achv_rarity_percent(code)
    card = _styled_achv_card(code, title, desc, emoji or "🏆", rarity)
    who = tg_mention(uid, dn or (m.from_user.full_name if m and m.from_user else None),
                          un or (m.from_user.username if m and m.from_user else None))
    tail = "Чтобы посмотреть все свои ачивки, напиши команду /achievements"
    if m:
        try:
            asyncio.create_task(m.reply(f"{who}\n{card}\n\n<i>{tail}</i>", disable_web_page_preview=True))
        except Exception:
            asyncio.create_task(m.reply(f"{(m.from_user.first_name if m and m.from_user else 'Пользователь')} получил ачивку: {title}. {tail}"))

# ============================================================
# Команды пользователя (ДОЛЖНЫ стоять выше on_text)
# ============================================================
@dp.message(CommandStart())
async def start(m: Message):
    """/start — приветствие и короткая справка по командах."""
    await m.reply(
        "Я — Лорд Вербус. Команды:\n"
        "• /lord_summary — краткий отчёт по беседе\n"
        "• /lord_psych — психологический портрет участника\n"
        "• /achievements — посмотреть свои ачивки\n"
        "• /achievements_top — топ по ачивкам\n"
        "Просто говорите — я вмешаюсь, если нужно."
    )

@dp.message(Command("achievements"))
async def cmd_achievements(m: Message):
    """Показать список твоих ачивок с редкостью и датой получения."""
    if not m.from_user: return
    uid = m.from_user.id
    rows = db_query(
        "SELECT a.code, a.title, a.description, a.emoji, ua.earned_at "
        "FROM user_achievements ua JOIN achievements a ON a.code=ua.code "
        "WHERE ua.user_id=? ORDER BY ua.earned_at DESC;",
        (uid,)
    )
    total = len(rows)
    counter = _styled_achv_counter(total)
    if total == 0:
        await m.reply("У тебя пока нет ачивок. Продолжай — судьба любит настойчивых."); return
    def _rarity(code: str) -> float:
        holders = db_query("SELECT COUNT(DISTINCT user_id) FROM user_achievements WHERE code=?;", (code,))
        users_cnt = db_query("SELECT COUNT(*) FROM users;")
        pop = max(int(users_cnt[0][0]) if users_cnt else 1, 1)
        return round(100.0 * (int(holders[0][0]) if holders else 0) / pop, 2)
    lines = [f"🏆 Твои ачивки: {counter}\n"]
    for code, title, desc, emoji, ts in rows:
        rarity = _rarity(code)
        when = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")
        lines.append(f"{emoji} <b>{_html.escape(title)}</b>  <i>{rarity}%</i>\n— {_html.escape(desc)}  ·  <span class='tg-spoiler'>{when}</span>")
    await m.reply("\n".join(lines), disable_web_page_preview=True)

@dp.message(Command("achievements_top"))
async def cmd_achievements_top(m: Message):
    """Топ пользователей по количеству полученных ачивок (TOP-10)."""
    rows = db_query(
        "SELECT ua.user_id, COUNT(*) as cnt FROM user_achievements ua GROUP BY ua.user_id ORDER BY cnt DESC, MIN(ua.earned_at) ASC LIMIT 10;"
    )
    if not rows:
        await m.reply("Топ пуст. Пора уже кому-то заработать первую ачивку."); return
    ids = tuple(r[0] for r in rows)
    placeholders = ",".join(["?"] * len(ids)) if ids else ""
    users = {}
    if ids:
        urows = db_query(f"SELECT user_id, display_name, username FROM users WHERE user_id IN ({placeholders});", ids)
        for uid, dn, un in urows:
            users[uid] = (dn, un)
    out = ["<b>🏆 ТОП по ачивкам</b>\n"]
    for rank, (uid, cnt) in enumerate(rows, start=1):
        dn, un = users.get(uid, (None, None))
        out.append(f"{rank}. {tg_mention(uid, dn, un)} — <b>{cnt}</b> {('🏅'*min(cnt,5))}")
    await m.reply("\n".join(out), disable_web_page_preview=True)

@dp.message(Command("ach_debug"))
async def cmd_ach_debug(m: Message):
    """
    /ach_debug             — быстрый обзор по /q
    /ach_debug <CODE>      — детальная диагностика по ачивке (например, VOIM10)
    """
    if not m.from_user: return
    uid = m.from_user.id
    # поддержим карточку пользователя (для редкости/имен)
    full_name = (m.from_user.full_name or "").strip() or (m.from_user.first_name or "")
    db_execute(
        "INSERT INTO users(user_id, display_name, username) VALUES(?, ?, ?) "
        "ON CONFLICT(user_id) DO UPDATE SET display_name=excluded.display_name, username=excluded.username;",
        (uid, full_name, m.from_user.username)
    )

    args = (m.text or "").split()
    if len(args) == 1:
        row = db_query("SELECT value FROM user_counters WHERE user_id=? AND key=?;", (uid, "cmd:/q"))
        q_cnt = int(row[0][0]) if row else 0
        has_q10 = bool(db_query("SELECT 1 FROM user_achievements WHERE user_id=? AND code='Q10' LIMIT 1;", (uid,)))
        await m.reply(
            f"🔍 Debug (быстро):\n• cmd:/q = <b>{q_cnt}</b>\n• Q10 выдана: <b>{'да' if has_q10 else 'нет'}</b>\n(Порог Q10: 10 раз /q)",
            disable_web_page_preview=True
        )
        return

    code = args[1].strip().upper()
    a = get_achievement_by_code(code)
    if not a:
        await m.reply(f"Не нашёл ачивку с кодом <b>{_html.escape(code)}</b>."); return

    acode, title, desc, emoji, atype, akey, thr, _active = a
    real_key = resolve_counter_key_for_user(atype, akey)
    row = db_query("SELECT value FROM user_counters WHERE user_id=? AND key=?;", (uid, real_key))
    val = int(row[0][0]) if row else 0
    has_it = bool(db_query("SELECT 1 FROM user_achievements WHERE user_id=? AND code=? LIMIT 1;", (uid, acode)))

    family = get_family_by_code(acode)
    tier_line = ""
    if len(family) >= 2:
        current_tier = None
        for c_code, c_title, c_desc, c_emoji, c_type, c_key, c_thr in family:
            if val >= int(c_thr or 0):
                current_tier = (c_code, c_title, c_desc, c_emoji, c_thr)
        if current_tier:
            t_code, t_title, _t_desc, t_emoji, t_thr = current_tier
            tier_line = f"\n• Текущая ступень: {t_emoji} <b>{_html.escape(t_title)}</b> (порог {t_thr})"
        else:
            first_thr = int(family[0][6] or 0)
            tier_line = f"\n• Текущая ступень: — (до бронзы осталось {max(first_thr - val, 0)})"

    pct = (val / max(int(thr or 1), 1)) * 100 if thr else 0.0
    pct = round(min(pct, 999.99), 2)

    await m.reply(
        f"{emoji or '🏆'} <b>{_html.escape(title)}</b> [{acode}]\n"
        f"Описание: {_html.escape(desc)}\nТип: <code>{atype}</code>\nКлюч счётчика: <code>{real_key}</code>\n"
        f"Порог: <b>{thr}</b>\nТекущее значение: <b>{val}</b>  ({pct}%)\nВыдана: <b>{'да' if has_it else 'нет'}</b>{tier_line}",
        disable_web_page_preview=True
    )

# --- алиасы на частые опечатки
@dp.message(Command("achievments"))
async def _alias_achievments(m: Message):
    """Алиас на /achievements (опечатка)."""
    await cmd_achievements(m)

@dp.message(Command("achievments_top"))
async def _alias_achievments_top(m: Message):
    """Алиас на /achievements_top (опечатка)."""
    await cmd_achievements_top(m)

# ============================================================
# Скрытая админ-команда: общий прогресс по всем ачивкам
# ============================================================
@dp.message(Command("ach_progress"))
async def cmd_ach_progress(m: Message):
    """
    /ach_progress            — сводка по всем семьям ачивок и всем пользователям (текущая ступень и прогресс).
    /ach_progress <CODE>     — сводка только по семье данного кода.
    Видит только ADMIN_IDS.
    """
    if not m.from_user or m.from_user.id not in ADMIN_IDS:
        return  # скрытая админ-команда

    args = (m.text or "").split(maxsplit=1)
    code_filter = args[1].strip().upper() if len(args) == 2 else None

    families = get_all_families()
    if code_filter:
        a = get_achievement_by_code(code_filter)
        if not a:
            await m.reply(f"Не нашёл ачивку <b>{_html.escape(code_filter)}</b>."); return
        acode, title, desc, emoji, atype, akey, _thr, _active = a
        only_rows = get_family_by_code(acode) or [a]
        families = {(atype, akey): only_rows}

    user_rows = db_query("SELECT user_id, display_name, username FROM users;")
    users = {u: (dn, un) for (u, dn, un) in user_rows}
    if not users:
        await m.reply("Нет пользователей в базе."); return

    month_suffix = datetime.now(timezone.utc).strftime("%Y-%m")

    all_keys = []
    fam_real_key = {}
    for (atype, akey), rows in families.items():
        rk = month_key(akey) if atype == "counter_at_least_monthly" else akey
        fam_real_key[(atype, akey)] = rk
        all_keys.append(rk)
    all_keys = list(dict.fromkeys(all_keys))

    cnt_map = {}
    if all_keys:
        placeholders = ",".join(["?"] * len(all_keys))
        cnt_rows = db_query(f"SELECT user_id, key, value FROM user_counters WHERE key IN ({placeholders});", tuple(all_keys))
        for uid, key, val in cnt_rows:
            cnt_map[(uid, key)] = int(val)

    head_lines = []
    if not code_filter:
        head_lines.append("<b>📊 Прогресс по всем ачивкам (текущий статус)</b>")
        if any(k[0] == "counter_at_least_monthly" for k in families.keys()):
            head_lines.append(f"Текущий месяц: <code>{month_suffix}</code>")
    else:
        fam_name = friendly_family_title(atype, akey)
        head_lines.append(f"<b>📊 Прогресс: {fam_name}</b> [{code_filter}]")
        if atype == "counter_at_least_monthly":
            head_lines.append(f"Текущий месяц: <code>{month_suffix}</code>")

    lines = head_lines + [""]

    for uid in sorted(users.keys()):
        dn, un = users.get(uid, (None, None))
        mention = tg_mention(uid, dn, un)

        per_user_lines = []
        for (atype, akey), fam_rows in families.items():
            fam_rows_sorted = sorted(fam_rows, key=lambda r: int(r[6] or 0))
            display_name = friendly_family_title(atype, akey)
            real_key = fam_real_key[(atype, akey)]
            val = cnt_map.get((uid, real_key), 0)

            tier = user_current_tier(fam_rows_sorted, val) if len(fam_rows_sorted) >= 2 else None

            if len(fam_rows_sorted) >= 2:
                if tier:
                    t_code, t_title, t_emoji, t_thr = tier
                    nxt = family_next_threshold(fam_rows_sorted, t_thr)
                    if nxt is not None:
                        need = max(nxt - val, 0)
                        per_user_lines.append(f"• {display_name}: {t_emoji} <b>{_html.escape(t_title)}</b> — {val}/{nxt} (до след.: {need})")
                    else:
                        per_user_lines.append(f"• {display_name}: {t_emoji} <b>{_html.escape(t_title)}</b> — {val} (макс)")
                else:
                    first_thr = int(fam_rows_sorted[0][6] or 0)
                    need = max(first_thr - val, 0)
                    per_user_lines.append(f"• {display_name}: — — — {val}/{first_thr} (до бронзы: {need})")
            else:
                only_thr = int((fam_rows_sorted[0][6] or 0)) if fam_rows_sorted else 0
                if only_thr > 0:
                    got = bool(db_query("SELECT 1 FROM user_achievements WHERE user_id=? AND code=? LIMIT 1;", (uid, fam_rows_sorted[0][0])))
                    per_user_lines.append(f"• {display_name}: {'✅' if got or val>=only_thr else ''} {val}/{only_thr}")
                else:
                    per_user_lines.append(f"• {display_name}: {val}")

        if per_user_lines:
            lines.append(f"{mention}")
            lines.extend(per_user_lines)
            lines.append("")
        if len(lines) > 6000:
            lines.append("…"); break

    text = "\n".join(lines).strip() or "Нет данных для отображения."
    await m.reply(text, disable_web_page_preview=True)

@dp.message(Command("ach_rescan"))
async def cmd_ach_rescan(m: Message):
    """
    /ach_rescan                 — пересчитать и ДОвыдать ачивки только себе
    /ach_rescan <@user|user_id> — пересчитать для указанного пользователя
    /ach_rescan all             — пересчитать для всех пользователей
    Видит только ADMIN_IDS. Ачивки, которых ещё не было — будут выданы с объявлениями.
    """
    if not m.from_user or m.from_user.id not in ADMIN_IDS:
        return  # скрытая админ-команда

    arg = (m.text or "").split(maxsplit=1)
    target = (arg[1].strip() if len(arg) == 2 else None) or str(m.from_user.id)

    # Собираем список user_id для ресканинга
    user_ids: list[int] = []
    if target.lower() == "all":
        rows = db_query("SELECT DISTINCT user_id FROM users;")
        user_ids = [int(r[0]) for r in rows] if rows else []
        if not user_ids:
            await m.reply("Пользователи не найдены."); return
    else:
        # либо @username, либо raw id
        if target.startswith("@"):
            uname = target[1:]
            row = db_query("SELECT user_id FROM users WHERE username=? LIMIT 1;", (uname,))
            if not row:
                await m.reply(f"Пользователь @{uname} не найден в базе."); return
            user_ids = [int(row[0][0])]
        else:
            try:
                user_ids = [int(target)]
            except ValueError:
                await m.reply("Ожидал @username или user_id или all."); return

    # Рескан по каждому пользователю
    total_granted = 0
    for uid in user_ids:
        # Ключи, которые «считаются изменёнными» — чтобы триггернуть проверку всех ачивок этого юзера
        keys_rows = db_query("SELECT DISTINCT key FROM user_counters WHERE user_id=?;", (uid,))
        updated_keys = [r[0] for r in (keys_rows or [])]

        # Дополнительно на всякий случай добавим свежие ежемесячные ключи для всех известных семейств
        ach_rows = db_query("SELECT DISTINCT key, type FROM achievements WHERE active=1;")
        for k, at in (ach_rows or []):
            if at == "counter_at_least_monthly":
                mk = month_key(k)
                if mk not in updated_keys:
                    updated_keys.append(mk)

        # Перед проверкой запомним, сколько было ачивок
        before_cnt = db_query("SELECT COUNT(*) FROM user_achievements WHERE user_id=?;", (uid,))
        before = int(before_cnt[0][0]) if before_cnt else 0

        await check_achievements_for_user(uid, m, updated_keys=updated_keys)

        after_cnt = db_query("SELECT COUNT(*) FROM user_achievements WHERE user_id=?;", (uid,))
        after = int(after_cnt[0][0]) if after_cnt else 0
        total_granted += max(after - before, 0)

    who = "всем" if target.lower() == "all" else (target)
    await m.reply(f"Рескан завершён ({who}). Выдано новых ачивок: <b>{total_granted}</b>.", disable_web_page_preview=True)

@dp.message(Command("ach_reset_counters"))
async def cmd_ach_reset_counters(m: Message):
    """
    /ach_reset_counters              — сбросить ВСЕ счётчики (только для админов)
    /ach_reset_counters <@user|id>   — сбросить счётчики у конкретного пользователя
    """
    if not m.from_user or m.from_user.id not in ADMIN_IDS:
        return  # скрытая админ-команда

    arg = (m.text or "").split(maxsplit=1)
    target = arg[1].strip() if len(arg) == 2 else "all"

    if target.lower() == "all":
        db_execute("DELETE FROM user_counters;")
        await m.reply("Счётчики <b>всех</b> пользователей сброшены.", disable_web_page_preview=True)
        return

    # один пользователь
    if target.startswith("@"):
        uname = target[1:]
        row = db_query("SELECT user_id FROM users WHERE username=? LIMIT 1;", (uname,))
        if not row:
            await m.reply(f"Не нашёл пользователя @{uname} в базе."); return
        uid = int(row[0][0])
    else:
        try:
            uid = int(target)
        except ValueError:
            await m.reply("Ожидал: all | @username | user_id"); return

    db_execute("DELETE FROM user_counters WHERE user_id=?;", (uid,))
    await m.reply(f"Счётчики пользователя <code>{uid}</code> сброшены.", disable_web_page_preview=True)

@dp.message(Command("ach_editstat")))
async def cmd_ach_editstat(m: Message):
    """
    /ach_editstat @username ACH_CODE NEW_VALUE
    Пример: /ach_editstat @nickname MSGM150 200
    — найдёт ачивку по коду, вычислит реальный ключ счётчика (учтёт monthly) и выставит NEW_VALUE,
      затем триггернёт проверку и выдачу недостающих ступеней.
    """
    if not m.from_user or m.from_user.id not in ADMIN_IDS:
        return  # скрытая админ-команда

    parts = (m.text or "").split()
    if len(parts) != 4:
        await m.reply("Формат: /ach_editstat @username ACH_CODE NEW_VALUE"); return

    who, code, new_val_s = parts[1], parts[2].upper(), parts[3]
    try:
        new_val = int(new_val_s)
    except ValueError:
        await m.reply("NEW_VALUE должен быть целым числом."); return

    # resolve user
    if who.startswith("@"):
        uname = who[1:]
        row = db_query("SELECT user_id FROM users WHERE username=? LIMIT 1;", (uname,))
        if not row:
            await m.reply(f"Не нашёл пользователя @{uname} в базе."); return
        uid = int(row[0][0])
    else:
        try:
            uid = int(who)
        except ValueError:
            await m.reply("Ожидал @username или user_id."); return

    a = get_achievement_by_code(code)
    if not a:
        await m.reply(f"Ачивка с кодом <b>{_html.escape(code)}</b> не найдена."); return

    acode, title, desc, emoji, atype, akey, thr, _active = a
    real_key = resolve_counter_key_for_user(atype, akey)

    # upsert value
    db_execute(
        "INSERT INTO user_counters(user_id, key, value) VALUES(?, ?, ?) "
        "ON CONFLICT(user_id, key) DO UPDATE SET value=excluded.value;",
        (uid, real_key, new_val)
    )

    # триггерим проверку (и по реальному ключу, и по «сырому» префиксу — на всякий случай)
    await check_achievements_for_user(uid, m, updated_keys=[real_key, akey])

    await m.reply(
        f"Обновлено: <code>{uid}</code> — [{code}] <code>{real_key}</code> = <b>{new_val}</b>.\nПорог текущей ступени: <b>{thr}</b>.",
        disable_web_page_preview=True
    )


# ============================================================
# Хэндлеры событий (стикеры/войсы/тексты) — считать counters и выдавать ачивки
# ============================================================
@dp.message(F.sticker)
async def on_sticker(m: Message):
    """Стикер: +1 к sticker:total и +1 к msg:month (как «сообщение месяца»)."""
    if not m.from_user: return
    uid = m.from_user.id
    full_name = (m.from_user.full_name or "").strip() or (m.from_user.first_name or "")
    db_execute(
        "INSERT INTO users(user_id, display_name, username) VALUES(?, ?, ?) "
        "ON CONFLICT(user_id) DO UPDATE SET display_name=excluded.display_name, username=excluded.username;",
        (uid, full_name, m.from_user.username)
    )
    inc_counter(uid, "sticker:total", 1)
    await check_achievements_for_user(uid, m, updated_keys=["sticker:total"])

    # учитываем как «сообщение месяца»
    k = month_key("msg:month")
    inc_counter(uid, k, 1)
    await check_achievements_for_user(uid, m, updated_keys=[k])

@dp.message(F.voice)
async def on_voice(m: Message):
    """Голосовое: +1 к voice:month и +1 к msg:month."""
    if not m.from_user: return
    uid = m.from_user.id
    full_name = (m.from_user.full_name or "").strip() or (m.from_user.first_name or "")
    db_execute(
        "INSERT INTO users(user_id, display_name, username) VALUES(?, ?, ?) "
        "ON CONFLICT(user_id) DO UPDATE SET display_name=excluded.display_name, username=excluded.username;",
        (uid, full_name, m.from_user.username)
    )
    k_voice = month_key("voice:month"); inc_counter(uid, k_voice, 1)
    await check_achievements_for_user(uid, m, updated_keys=[k_voice])

    k_msg = month_key("msg:month");    inc_counter(uid, k_msg, 1)
    await check_achievements_for_user(uid, m, updated_keys=[k_msg])

@dp.message(F.text & ~F.text.startswith("/"))
async def on_text(m: Message):
    """Любой некомандный текст: лог в messages, обновление users, +1 к msg:month, и три роута ответа ИИ."""
    if not m.text: return

    # логируем текст
    db_execute(
        "INSERT INTO messages(chat_id, user_id, username, text, created_at, message_id) VALUES (?, ?, ?, ?, ?, ?);",
        (m.chat.id, m.from_user.id if m.from_user else 0, m.from_user.username if m.from_user else None, m.text, now_ts(), m.message_id)
    )
    # обновляем карточку пользователя
    if m.from_user:
        full_name = (m.from_user.full_name or "").strip() or (m.from_user.first_name or "")
        db_execute(
            "INSERT INTO users(user_id, display_name, username) VALUES(?, ?, ?) "
            "ON CONFLICT(user_id) DO UPDATE SET display_name=excluded.display_name, username=excluded.username;",
            (m.from_user.id, full_name, m.from_user.username)
        )

    # +1 к «сообщениям за месяц»
    k = month_key("msg:month")
    inc_counter(m.from_user.id, k, 1)
    await check_achievements_for_user(m.from_user.id, m, updated_keys=[k])

        # Если сообщение содержит слово "testtest" — считаем как тестовый инкремент
    if re.search(r"\btesttest\b", m.text, flags=re.IGNORECASE):
        inc_counter(m.from_user.id, "testtest:total", 1)
        await check_achievements_for_user(m.from_user.id, m, updated_keys=["testtest:total"])

    # ответы ИИ (упоминание / ответ в тред / иногда вмешаться)
    me = await bot.get_me()
    if m.reply_to_message and m.reply_to_message.from_user and m.reply_to_message.from_user.id == me.id:
        await reply_to_thread(m); return
    if mentions_bot(m.text or "", me.username):
        await reply_to_mention(m); return
    await maybe_interject(m)

# ============================================================
# Трекинг /q → счётчик + проверка правил
# ============================================================
@dp.message(Command("q"))
async def track_q_and_maybe_award(m: Message):
    """Команда /q: +1 к cmd:/q и проверка ачивки Q10."""
    if not m.from_user: return
    uid = m.from_user.id
    full_name = (m.from_user.full_name or "").strip() or (m.from_user.first_name or "")
    db_execute(
        "INSERT INTO users(user_id, display_name, username) VALUES(?, ?, ?) "
        "ON CONFLICT(user_id) DO UPDATE SET display_name=excluded.display_name, username=excluded.username;",
        (uid, full_name, m.from_user.username)
    )
    inc_counter(uid, "cmd:/q", 1)
    await check_achievements_for_user(uid, m, updated_keys=["cmd:/q"])

# ============================================================
# Уведомление о видео-кружке от WATCH_USER_ID
# ============================================================
def _message_link(chat, message_id: int) -> str | None:
    """Сформировать кликабельную ссылку на сообщение (для публичных и приватных супергрупп)."""
    if getattr(chat, "username", None):
        return f"https://t.me/{chat.username}/{message_id}"
    cid = str(chat.id)
    if cid.startswith("-100"):
        return f"https://t.me/c/{cid[4:]}/{message_id}"
    return None

@dp.message(F.video_note)
async def on_video_note_watch(m: Message):
    """Если нужный пользователь отправил кружочек — тегнуть адресата в чате с ссылкой на сообщение."""
    user = m.from_user
    if not user or user.id != WATCH_USER_ID: return
    who_html = tg_mention(user.id, user.full_name or user.first_name, user.username)
    notify_html = tg_mention(NOTIFY_USER_ID, f"@{NOTIFY_USERNAME}", NOTIFY_USERNAME)
    link = _message_link(m.chat, m.message_id)
    link_html = f" <a href=\"{link}\">ссылка</a>" if link else ""
    if m.chat.type in ("group", "supergroup"):
        try:
            await m.reply(f"{notify_html}, {who_html} отправил видеокружок.{link_html}", disable_web_page_preview=True)
        except Exception:
            await m.reply(f"@{NOTIFY_USERNAME}, видеокружок от @{user.username or user.id}")

# ============================================================
# Команды в меню Telegram
# ============================================================
async def set_commands():
    """Завести команды в UI Телеграма (в группах и в личке)."""
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

# ============================================================
# Main
# ============================================================
async def main():
    """Точка входа: init DB, команды, запуск поллинга."""
    init_db()
    await set_commands()
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
