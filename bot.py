import os
import asyncio
import random
import re
import sqlite3
from contextlib import closing
from datetime import datetime, timedelta, timezone

import aiohttp
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command, CommandStart, CommandObject
from aiogram.types import Message, BotCommand, BotCommandScopeAllGroupChats, BotCommandScopeAllPrivateChats
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
import html as _html
import re as _re

# ---------------- ENV ----------------
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

BOT_TOKEN = (
    os.getenv("BOT_TOKEN")
    or os.getenv("TELEGRAM_BOT_TOKEN")
    or os.getenv("TOKEN")
)
OPENROUTER_API_KEY = (
    os.getenv("OPENROUTER_API_KEY")
    or os.getenv("OPENROUTER_KEY")
    or os.getenv("OR_API_KEY")
    or os.getenv("OR_KEY")
)
OPENROUTER_SITE_URL = os.getenv("OPENROUTER_SITE_URL", "https://example.com")
OPENROUTER_APP_NAME = os.getenv("OPENROUTER_APP_NAME", "lord-verbus")
MODEL = os.getenv("OPENROUTER_MODEL", "mistralai/mistral-nemo")

print("[ENV] BOT_TOKEN:", bool(BOT_TOKEN))
print("[ENV] OPENROUTER_API_KEY:", bool(OPENROUTER_API_KEY))
print("[ENV] OPENROUTER_MODEL:", MODEL)

if not BOT_TOKEN or not OPENROUTER_API_KEY:
    raise SystemExit("[Lord Verbus] Missing envs: BOT_TOKEN or OPENROUTER_API_KEY")

# ---------------- DB ----------------
DB = "verbus.db"

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
        conn.execute("""
        CREATE TABLE IF NOT EXISTS last_summary (
            chat_id INTEGER PRIMARY KEY,
            message_id INTEGER,
            created_at INTEGER NOT NULL
        );
        """)
        conn.execute("""
        CREATE TABLE IF NOT EXISTS auto_reply_stats (
            chat_id INTEGER PRIMARY KEY,
            last_reply_ts INTEGER DEFAULT 0,
            window_start_ts INTEGER DEFAULT 0,
            replies_in_window INTEGER DEFAULT 0
        );
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_messages_chat_time ON messages(chat_id, created_at);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_messages_chat_msgid ON messages(chat_id, message_id);")
        conn.commit()

def db_execute(sql, params=()):
    with closing(sqlite3.connect(DB)) as conn:
        cur = conn.execute(sql, params)
        conn.commit()
        return cur

def db_query(sql, params=()):
    with closing(sqlite3.connect(DB)) as conn:
        cur = conn.execute(sql, params)
        return cur.fetchall()

def now_ts() -> int:
    return int(datetime.now(timezone.utc).timestamp())

# ---------------- Helpers ----------------
QUESTION_PATTERNS = [
    r"\?",
    r"\bкто\b", r"\bчто\b", r"\bкак\b", r"\bпочему\b", r"\bзачем\b",
    r"\bкогда\b", r"\bгде\b", r"\bкакой\b", r"\bкакая\b", r"\bкакие\b",
    r"\bсколько\b", r"\bможно ли\b", r"\bесть ли\b",
    r"\bwho\b", r"\bwhat\b", r"\bhow\b", r"\bwhy\b", r"\bwhen\b", r"\bwhere\b"
]
QUESTION_RE = re.compile("|".join(QUESTION_PATTERNS), re.IGNORECASE)

def is_question(text: str) -> bool:
    if not text: return False
    if len(text) < 4096 and len(re.findall(r"https?://\S+", text)) > 2:
        return False
    return bool(QUESTION_RE.search(text))

def mentions_bot(text: str, bot_username: str | None) -> bool:
    if not text or not bot_username: return False
    return f"@{bot_username.lower()}" in text.lower()

def is_quiet_hours(local_dt: datetime) -> bool:
    return 0 <= local_dt.hour < 7  # 00:00–07:00

def tg_link(chat_id: int, message_id: int) -> str:
    s = str(chat_id)
    cid = s[4:] if s.startswith("-100") else s.lstrip("-")
    return f"https://t.me/c/{cid}/{message_id}"

def sanitize_html_whitelist(text: str) -> str:
    esc = _html.escape(text)
    esc = _re.sub(r"&lt;a href=&quot;([^&]*)&quot;&gt;(.*?)&lt;/a&gt;", r'<a href="\1">\2</a>', esc, flags=_re.DOTALL)
    esc = esc.replace("&lt;b&gt;", "<b>").replace("&lt;/b&gt;", "</b>")
    esc = esc.replace("&lt;i&gt;", "<i>").replace("&lt;/i&gt;", "</i>")
    esc = esc.replace("&lt;u&gt;", "<u>").replace("&lt;/u&gt;", "</u>")
    esc = esc.replace("&lt;code&gt;", "<code>").replace("&lt;/code&gt;", "</code>")
    return esc

def recent_chat_activity(chat_id: int, minutes: int) -> int:
    since = now_ts() - minutes * 60
    row = db_query("SELECT COUNT(*) FROM messages WHERE chat_id=? AND created_at>?", (chat_id, since))
    return row[0][0] if row else 0

def chat_recent_context(chat_id: int, limit: int = 30):
    rows = db_query(
        "SELECT username, text FROM messages WHERE chat_id=? AND text IS NOT NULL ORDER BY id DESC LIMIT ?;",
        (chat_id, limit)
    )
    return list(reversed(rows))

# ----- rare epithets (reduced frequency)
EPITHETS = [
    "к лешему", "к дьяволу", "к демонам", "будь он неладен", "будь оно проклято",
    "святая бессмыслица", "адская путаница", "мерзкая чепуха", "вот же напасть", "вот беда",
    "ни в какие ворота", "за гранью приличий", "бездна нелепости", "какого лешего",
    "голая софистика", "жалкая эквилибристика", "буря в стакане", "уродливый компромисс",
    "интеллектуальный бардак", "риторический мусор", "кособокая логика", "приторная наивность",
    "зудящая банальщина", "гниль рассуждений", "срамная мешанина", "смех и грех",
    "позор дедукции", "пыль в глаза", "сиропная банальность", "карнавал нелепости",
    "цирк без клоунов", "срамота логики", "грош цена аргументу", "та ещё катастрофа",
    "позорная муштра фактов", "зазеркальная нелепость", "свинцовая тупость идеи",
    "непрошеная чепуха", "жеваная банальщина", "бурлеск нелепости", "кислый софизм",
    "тоскливая демагогия", "мысленный хаос", "словесный дым", "скверный аргумент",
    "слепая уверенность", "упрямая нелепость", "грязная логическая каша", "нафталиновая мудрость"
]
_last_epithet = None
replies_since_epithet = 0  # кулдаун по количеству ответов

def maybe_pick_epithet(p: float = 0.18, min_gap: int = 4) -> str | None:
    """Редко и не чаще одного раза на min_gap ответов."""
    global _last_epithet, replies_since_epithet
    if replies_since_epithet < min_gap:
        return None
    if random.random() > p:
        return None
    pool = [e for e in EPITHETS if e != _last_epithet] or EPITHETS[:]
    choice = random.choice(pool)
    _last_epithet = choice
    replies_since_epithet = 0
    return choice

def bump_reply_counter():
    global replies_since_epithet
    replies_since_epithet += 1

def persona_prompt() -> str:
    return (
        "Ты — «Лорд Вербус»: аристократичный, язвительно-умный компаньон в духе Холмса (Downey Jr.). "
        "Пиши кратко. Сначала — СУТЬ/ОТВЕТ, затем — одна короткая колкость. "
        "Если ответа нет, скажи честно одним предложением и намекни, что нужно уточнить. "
        "Используй редкие изящные ругательства на ОБСТОЯТЕЛЬСТВА (не на людей) и не чаще одного за ответ, да и то редко. "
        "Не обращайся по имени и не используй @."
    )

# ---------------- OpenRouter ----------------
async def ai_reply(system_prompt: str, user_prompt: str, temperature: float = 0.7):
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
        async with session.post(
            "https://openrouter.ai/api/v1/chat/completions",
            headers=headers,
            json=body,
            timeout=60
        ) as r:
            data = await r.json()
            if r.status >= 400 or "error" in data:
                msg = data.get("error", {}).get("message", str(data))
                raise RuntimeError(f"OpenRouter error: {msg}")
            try:
                return data["choices"][0]["message"]["content"]
            except Exception:
                return data.get("output") or str(data)

# ---------------- Bot ----------------
bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()

# ---------- Commands setup ----------
async def setup_commands():
    base_cmds = [
        BotCommand(command="ping", description="Проверка, жив ли бот"),
        BotCommand(command="lord_summary", description="Саммари последних сообщений"),
        BotCommand(command="lord_search", description="Поиск по чату"),
    ]
    await bot.set_my_commands(base_cmds, scope=BotCommandScopeAllGroupChats())
    await bot.set_my_commands(base_cmds, scope=BotCommandScopeAllPrivateChats())

# ---------- SUMMARY ----------
def prev_summary_link(chat_id: int):
    prev = db_query("SELECT message_id FROM last_summary WHERE chat_id=?;", (chat_id,))
    return tg_link(chat_id, prev[0][0]) if prev and prev[0][0] else None

@dp.message(Command("lord_summary"))
async def cmd_summary(m: Message, command: CommandObject):
    # саммари всегда предпочтительно над «остроумным ответом»
    try:
        n = int((command.args or "").strip())
        n = max(50, min(800, n))
    except Exception:
        n = 300

    rows = db_query(
        "SELECT username, text, message_id FROM messages WHERE chat_id=? AND text IS NOT NULL ORDER BY id DESC LIMIT ?;",
        (m.chat.id, n)
    )
    if not rows:
        await m.reply("У меня пока нет сообщений для саммари.")
        return

    prev_link = prev_summary_link(m.chat.id)
    prev_line_html = f'<a href="{prev_link}">Предыдущий анализ</a>' if prev_link else "Предыдущий анализ (—)"

    enriched = []
    for u, t, mid in reversed(rows):
        link = tg_link(m.chat.id, mid) if mid else ""
        handle = ("@" + u) if u else "user"
        if link:
            enriched.append(f"{handle}: {t}  [link: {link}]")
        else:
            enriched.append(f"{handle}: {t}")
    dialog_block = "\n".join(enriched)

    system = (
        persona_prompt()
        + " Ты оформляешь отчёт по чату. Формат — HTML. Строго соблюдай шаблон. "
          "Никаких списков из 'Ключевых моментов', <h1>, <center> и т.п."
    )
    user = (
        f"{dialog_block}\n\n"
        "Сформируй ответ СТРОГО по этому шаблону (ровно в таком порядке):\n\n"
        f"{prev_line_html}\n\n"
        "✂️<b>Краткое содержание</b>:\n"
        "2–3 коротких предложения, обобщающих разговор. БЕЗ ссылок.\n\n"
        "Далее СТРОГО 2–4 тематических блока. Каждый блок РОВНО так:\n"
        "😄 <b>Короткое название темы</b>\n"
        "Короткий абзац (1–3 предложения) без дословных цитат, без списков. "
        "Внутри абзаца используй 1–3 встроенных гиперссылки <a href='URL'>…</a> на сообщения из входных данных "
        "(URL бери из соответствующих [link: URL]). Никого по имени не упоминай, не используй @.\n\n"
        "Заверши одной фразой от Лорда Вербуса — язвительно-умной; изящные восклицания допускаются редко."
    )

    try:
        reply = await ai_reply(system, user, temperature=0.2)
    except Exception as e:
        reply = f"Суммаризация временно недоступна: {e}"

    safe = sanitize_html_whitelist(reply)
    sent = await m.reply(safe)
    db_execute(
        "INSERT INTO last_summary(chat_id, message_id, created_at) VALUES(?, ?, ?) "
        "ON CONFLICT(chat_id) DO UPDATE SET message_id=excluded.message_id, created_at=excluded.created_at;",
        (m.chat.id, sent.message_id, now_ts())
    )

# ---------- SEARCH ----------
@dp.message(Command("lord_search"))
async def cmd_search(m: Message, command: CommandObject):
    q = (command.args or "").strip()
    if not q:
        await m.reply("Формат: <code>/lord_search печеньки в прошлом месяце</code> или <code>/lord_search дедлайн вчера</code>")
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
            return int(start.timestamp()), int(ref.timestamp())
        if "месяц" in ql or "30 дней" in ql:
            start = ref - timedelta(days=30)
            return int(start.timestamp()), int(ref.timestamp())
        return None, None

    since_ts, until_ts = parse_time_hint_ru(q)
    q_clean = re.sub(r"(вчера|сегодня|прошлой неделе|прошлая неделя|на прошлой неделе|прошлом месяце|прошлый месяц|неделю|7 дней|30 дней|месяц)", "", q, flags=re.IGNORECASE).strip()

    if since_ts and until_ts:
        rows = db_query(
            """
            SELECT m.id, m.username, m.text, m.created_at, m.message_id
            FROM messages m
            JOIN messages_fts f ON f.rowid = m.id
            WHERE m.chat_id=? AND m.created_at BETWEEN ? AND ? AND f.text MATCH ?
            ORDER BY m.id DESC LIMIT 20;
            """,
            (m.chat.id, since_ts, until_ts, q_clean or q)
        )
    else:
        rows = db_query(
            """
            SELECT m.id, m.username, m.text, m.created_at, m.message_id
            FROM messages m
            JOIN messages_fts f ON f.rowid = m.id
            WHERE m.chat_id=? AND f.text MATCH ?
            ORDER BY m.id DESC LIMIT 20;
            """,
            (m.chat.id, q_clean or q)
        )

    if not rows:
        await m.reply("Не нашёл. Попробуй изменить формулировку или указать период (например, «вчера», «в прошлом месяце»).")
        return

    def fmt(ts):
        dt = datetime.fromtimestamp(ts, tz=timezone.utc).astimezone()
        return dt.strftime("%d.%m %H:%M")

    lines = []
    for _id, u, t, ts, mid in rows[:10]:
        link = tg_link(m.chat.id, mid) if mid else None
        who = ("@" + u) if u else "user"
        if link:
            lines.append(f"• <b>{fmt(ts)}</b> — {who}: {sanitize_html_whitelist(t)}\n  Сообщение: <a href=\"{link}\">ссылка</a>")
        else:
            lines.append(f"• <b>{fmt(ts)}</b> — {who}: {sanitize_html_whitelist(t)}")
    await m.reply("Нашёл:\n" + "\n".join(lines))

# ---------- Dialog logic ----------

async def reply_to_mention(m: Message):
    if is_quiet_hours(datetime.now().astimezone()):
        return
    ctx = chat_recent_context(m.chat.id, limit=15)
    lines = [(("@" + u) if u else "user") + ": " + t for u, t in ctx]
    ctx_block = "\n".join(lines)

    epithet = maybe_pick_epithet()
    add = f"\nЕсли уместно, можно вставить ровно одно выражение (используй редко): «{epithet}»." if epithet else ""
    system = persona_prompt()
    user = (
        "Тебя упомянули в групповом чате.\n"
        "Правило: СНАЧАЛА дай ПРЯМОЙ КОРОТКИЙ ОТВЕТ по сути вопроса/реплики (1 фраза), "
        "ПОТОМ — одна короткая колкость.\n"
        "Если прямого ответа нет — скажи кратко, чего не хватает для ответа." + add +
        f"\n\nНедавний контекст:\n{ctx_block}\n\n"
        f"Сообщение:\n«{m.text}»"
    )
    try:
        reply = await ai_reply(system, user, temperature=0.68)
        await m.reply(sanitize_html_whitelist(reply))
    finally:
        bump_reply_counter()

async def reply_to_thread(m: Message):
    """Отвечаем, когда пользователь ответил реплаем на сообщение бота.
       Без троттлинга: отвечаем стабильно (кроме тихих часов)."""
    if is_quiet_hours(datetime.now().astimezone()):
        return

    ctx = chat_recent_context(m.chat.id, limit=15)
    lines = [(("@" + u) if u else "user") + ": " + t for u, t in ctx]
    ctx_block = "\n".join(lines)

    epithet = maybe_pick_epithet()
    add = f"\nЕсли уместно, можно вставить ровно одно выражение (используй редко): «{epithet}»." if epithet else ""
    system = persona_prompt()
    user = (
        "Пользователь ответил реплаем на твоё сообщение.\n"
        "Правило: СНАЧАЛА дай ПРЯМОЙ КОРОТКИЙ ОТВЕТ (1 фраза), ПОТОМ — одна короткая колкость.\n"
        "Если ответ невозможен — кратко укажи, чего не хватает." + add +
        f"\n\nНедавний контекст:\n{ctx_block}\n\n"
        f"Реплай:\n«{m.text}»"
    )
    try:
        reply = await ai_reply(system, user, temperature=0.7)
        await m.reply(sanitize_html_whitelist(reply))
    finally:
        bump_reply_counter()

async def maybe_reply(m: Message):
    """Уместное вмешательство — только на вопросы, при заметной активности."""
    if not m.chat or not m.from_user or not m.text:
        return
    me = await bot.get_me()
    if m.reply_to_message and m.reply_to_message.from_user and m.reply_to_message.from_user.id == me.id:
        return
    if mentions_bot(m.text, me.username):
        return
    if not is_question(m.text):
        return
    if recent_chat_activity(m.chat.id, minutes=5) < 5:
        return
    if is_quiet_hours(datetime.now().astimezone()):
        return

    ctx = chat_recent_context(m.chat.id, limit=20)
    lines = [(("@" + u) if u else "user") + ": " + t for u, t in ctx]
    ctx_block = "\n".join(lines[-20:])

    epithet = maybe_pick_epithet()
    add = f"\nЕсли уместно, можно вставить одно выражение (редко): «{epithet}»." if epithet else ""
    system = persona_prompt()
    user = (
        "Это фрагмент недавнего группового чата. Тебе задали вопрос (или близко к тому).\n"
        "Правило: СНАЧАЛА дай ПРЯМОЙ КОРОТКИЙ ОТВЕТ (1 фраза), ПОТОМ — одна короткая колкость. "
        "Если ответ невозможен — кратко объясни, чего не хватает." + add +
        f"\n\nКонтекст:\n{ctx_block}\n\nВопрос:\n«{m.text}»"
    )
    try:
        reply = await ai_reply(system, user, temperature=0.72)
        await m.reply(sanitize_html_whitelist(reply))
    finally:
        bump_reply_counter()

# ---------- Base handlers ----------
@dp.message(CommandStart())
async def cmd_start(m: Message):
    await m.reply("Лорд Вербус к вашим услугам. Команды: /ping, /lord_summary, /lord_search <запрос>")

@dp.message(Command("ping"))
async def cmd_ping(m: Message):
    await m.reply("pong")

# ВАЖНО: общий ловец **не мешает** командам
@dp.message(F.text)
async def catcher(m: Message):
    # логируем только не-команды
    if not m.text.startswith("/"):
        db_execute(
            "INSERT INTO messages(chat_id, user_id, username, text, created_at, message_id) VALUES (?, ?, ?, ?, ?, ?);",
            (m.chat.id, m.from_user.id if m.from_user else 0,
             m.from_user.username if m.from_user else None,
             m.text, now_ts(), m.message_id)
        )

    me = await bot.get_me()

    # если это КОМАНДА — вообще ничего не делаем здесь (пусть обработают command-хендлеры)
    if m.text.startswith("/"):
        return

    # если ответили на бота — отвечаем
    if m.reply_to_message and m.reply_to_message.from_user and m.reply_to_message.from_user.id == me.id:
        await reply_to_thread(m)
        return

    # если упомянули бота — отвечаем
    if mentions_bot(m.text or "", me.username):
        await reply_to_mention(m)
        return

    # иначе — уместное вмешательство (только на вопросы)
    await maybe_reply(m)

# ---------------- Main ----------------
async def main():
    init_db()
    await setup_commands()
    print("[Lord Verbus] Online ✅ Starting long polling…")
    await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())

if __name__ == "__main__":
    asyncio.run(main())
