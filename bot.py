import os
import asyncio
import random
import re
import sqlite3
from contextlib import closing
from datetime import datetime, timedelta, timezone
import html as _html
import re as _re

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

bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()

# =========================
# DB
# =========================
def init_db():
    with closing(sqlite3.connect(DB)) as conn:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
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
        # — таблица пользователей для кликабельных имён в саммари
        conn.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            display_name TEXT,
            username TEXT
        );
        """)
        conn.execute("""
        CREATE TABLE IF NOT EXISTS last_summary (
            chat_id INTEGER PRIMARY KEY,
            message_id INTEGER,
            created_at INTEGER
        );
        """)
        conn.commit()

def db_execute(sql: str, params: tuple = ()):
    with closing(sqlite3.connect(DB)) as conn:
        conn.execute(sql, params)
        conn.commit()

def db_query(sql: str, params: tuple = ()):
    with closing(sqlite3.connect(DB)) as conn:
        cur = conn.execute(sql, params)
        return cur.fetchall()

def now_ts() -> int:
    return int(datetime.now(timezone.utc).timestamp())

# =========================
# Helpers
# =========================
QUESTION_PATTERNS = [
    r"\?",
    r"\bкто\b", r"\bчто\b", r"\bкак\b", r"\bпочему\b", r"\bзачем\b",
    r"\bкогда\b", r"\bгде\b", r"\bкакой\b", r"\bкакая\b", r"\bкакие\b",
    r"\bсколько\b", r"\bможно ли\b", r"\bесть ли\b",
    r"\bwho\b", r"\bwhat\b", r"\bhow\b", r"\bwhy\b", r"\bwhen\b", r"\bwhere\b"
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
        "Ты — «Лорд Вербус»: остроумный, аристократичный и немного язвительный собеседник (в духе Холмса Дауни-мл.). "
        "Отвечай кратко и по делу, естественно, без структурных меток вроде «Ответ»/«Колкость». "
        "Если в реплике есть вопрос — приоритизируй конкретный ответ одной-двумя фразами; "
        "можно добавить едкую ремарку, но коротко и уместно. "
        "Редкие изящные ругательства допускаются (на обстоятельства, не на людей) и только изредка. "
        "Не обращайся по имени и не используй @. "
        "ВАЖНО: не заключай весь ответ в кавычки и не цитируй свой собственный текст."
    )

def tg_mention(user_id: int, display_name: str | None, username: str | None) -> str:
    name = (display_name or username or "гость").strip()
    safe = _html.escape(name)
    return f"<a href=\"tg://user?id={user_id}\">{safe}</a>"

# =========================
# OpenRouter
# =========================
async def ai_reply(system_prompt: str, user_prompt: str, temperature: float = 0.68):
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
# Linkify helpers
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
    # шаг 1 — обрабатываем все [link: ...]
    urls = LINK_PAT.findall(text)
    for url in urls:
        text = _wrap_last_words(text, url)

    # шаг 2 — если модель всё равно выводит «Источник» якорёк
    for m in list(ANCHOR_PAT.finditer(text)):
        url = m.group(1)
        start, end = m.span()
        left = text[:start]
        right = text[end:]
        # пытаемся привязать к предыдущим словам
        tmp = left + f"[link: {url}]" + right
        text = _wrap_last_words(tmp, url)
    # на всякий случай — уберём остаточные [link: ...], если вдруг остались
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
        persona_prompt_natural()
        + " Ты оформляешь отчёт по чату. Формат — HTML. Строго соблюдай каркас ниже, без отступлений "
          "(никаких альтернативных заголовков, списков «ключевые моменты», <h1>, центрирования и т.п.). "
          "Каждый тематический абзац обязан содержать 1–3 ВСТРОЕННЫЕ ссылки на часть текста, а не на слово «Источник». "
          "Ссылку делай на 2–5 ключевых слов внутри предложения. Не заключай абзацы целиком в кавычки. "
          "В каждой теме обязательно упоминай по именам всех релевантных участников, пользуясь переданными кликабельными именами."
    )
    user = (
        f"Участники (используй эти кликабельные имена в тексте тем, не используй @): {participants_html}\n\n"
        f"{dialog_block}\n\n"
        "Сформируй ответ СТРОГО по этому каркасу (ровно в таком порядке):\n\n"
        f"{prev_line_html}\n\n"
        "✂️<b>Краткое содержание</b>:\n"
        "Два-три коротких предложения, обобщающих разговор. БЕЗ ссылок.\n\n"
        "😄 <b>Тема 1</b>\n"
        "Один абзац (1–3 предложения). Обязательно назови по именам релевантных участников, "
        "и вставь 1–3 ссылки ВНУТРИ текста на 2–5 слов (используй URL из [link: ...]).\n\n"
        "😄 <b>Тема 2</b>\n"
        "Абзац по тем же правилам.\n\n"
        "😄 <b>Тема 3</b>\n"
        "Абзац по тем же правилам. Если явных тем меньше, кратко заверши третью темой-резюме.\n\n"
        "Заверши одной короткой фразой от Лорда Вербуса."
    )

    try:
        reply = await ai_reply(system, user, temperature=0.2)
        # 1) Умная автолинковка: [link: URL] → якорь на предшествующие слова
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
# Search in chat (simple RU time hints)
# =========================
@dp.message(Command("lord_search"))
async def cmd_search(m: Message, command: CommandObject):
    q = (command.args or "").strip()
    if not q:
        await m.reply("Формат: <code>/lord_search печеньки в про...шлом месяце</code> или <code>/lord_search дедлайн вчера</code>")
        return

    def parse_time_hint_ru(q: str):
        ql = q.lower()
        ref = datetime.now(timezone.utc)
        if "вчера" in ql:
            start = datetime(ref.year, ref.month, ref.day, tzinfo=timezone.utc) - timedelta(days=1)
            end = start + timedelta(days=1)
            return int(start.timestamp()), int(end.timestamp())
        if "сегодня" in ql:
            start = datetime(ref.year, ref.month, ref.day, tzinfo=timezone.utc)
            end = start + timedelta(days=1)
            return int(start.timestamp()), int(end.timestamp())
        if "прошлой неделе" in ql or "прошлая неделя" in ql or "на прошлой неделе" in ql:
            end = ref - timedelta(days=7)
            start = end - timedelta(days=7)
            return int(start.timestamp()), int(end.timestamp())
        if "прошлом месяце" in ql or "прошлый месяц" in ql:
            y, m = ref.year, ref.month
            y2, m2 = (y - 1, 12) if m == 1 else (y, m - 1)
            start = datetime(y2, m2, 1, tzinfo=timezone.utc)
            end = datetime(y2 + 1, 1, 1, tzinfo=timezone.utc) if m2 == 12 else datetime(y2, m2 + 1, 1, tzinfo=timezone.utc)
            return int(start.timestamp()), int(end.timestamp())
        if "неделю" in ql or "7 дней" in ql:
            start = ref - timedelta(days=7)
            end = ref
            return int(start.timestamp()), int(end.timestamp())
        if "месяц" in ql:
            start = ref - timedelta(days=30)
            end = ref
            return int(start.timestamp()), int(end.timestamp())
        return None

    tf = parse_time_hint_ru(q)
    params = [m.chat.id]
    sql = "SELECT username, text, message_id, created_at FROM messages WHERE chat_id=?"
    if tf:
        sql += " AND created_at BETWEEN ? AND ?"
        params.extend([tf[0], tf[1]])
    sql += " ORDER BY id DESC LIMIT 50;"
    rows = db_query(sql, tuple(params))
    if not rows:
        await m.reply("Ничего не нашлось.")
        return
    lines = []
    for u, t, mid, ts in rows:
        when = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
        link = tg_link(m.chat.id, mid) if mid else ""
        who = "@" + u if u else "user"
        lines.append(f"• {when} — {who}: {t}" + (f" [<a href='{link}'>перейти</a>]" if link else ""))
    await m.reply("\n".join(lines))

# =========================
# Small talk / interjections
# =========================
def maybe_pick_epithet(p: float = 0.10, min_gap: int = 5) -> str | None:
    # раз в несколько ответов можем добавить короткий изящный эпитет
    if random.random() > p:
        return None
    candidates = [
        "ах, бюрократия — болотный хамелеон",
        "бумаги терпят больше, чем нервы",
        "как сказала бы моя тётушка, «неуместно, но занятно»",
    ]
    return random.choice(candidates)

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
        "Тебя упомянули в групповом чате. Ответь естественно и по делу, кратко; можно добавить одну короткую колкость."
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
        "Ответь на сообщение в ветке. Будь краток и точен; можно добавить одну короткую колкость."
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
# Handlers
# =========================
@dp.message(CommandStart())
async def start(m: Message):
    await m.reply(
        "Я — Лорд Вербус. Команды:\n"
        "• /lord_summary — краткий отчёт по беседе\n"
        "• /lord_search <запрос> — поиск по чату (поддержка: «вчера», «сегодня», «прошлой неделе», «прошлом месяце», «неделю», «месяц»)\n"
        "Просто говорите — я вмешаюсь, если нужно."
    )

@dp.message(F.text)
async def on_text(m: Message):
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
        # — обновляем карточку пользователя (для кликабельных имён в саммари)
        if m.from_user:
            full_name = (m.from_user.full_name or "").strip() or (m.from_user.first_name or "")
            db_execute(
                "INSERT INTO users(user_id, display_name, username) VALUES(?, ?, ?) "
                "ON CONFLICT(user_id) DO UPDATE SET display_name=excluded.display_name, username=excluded.username;",
                (m.from_user.id, full_name, m.from_user.username)
            )

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
# Commands list
# =========================
async def set_commands():
    commands_group = [
        BotCommand(command="lord_summary", description="Краткий отчёт по беседе"),
        BotCommand(command="lord_search", description="Поиск по чату"),
    ]
    commands_private = [
        BotCommand(command="lord_summary", description="Краткий отчёт по беседе"),
        BotCommand(command="lord_search", description="Поиск по чату"),
        BotCommand(command="start", description="Приветствие"),
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
