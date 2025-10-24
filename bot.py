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

print("[ENV CHECK] BOT_TOKEN set?:", bool(BOT_TOKEN))
print("[ENV CHECK] OPENROUTER_API_KEY set?:", bool(OPENROUTER_API_KEY))
print("[ENV CHECK] OPENROUTER_SITE_URL set?:", bool(OPENROUTER_SITE_URL))
print("[ENV CHECK] OPENROUTER_APP_NAME set?:", bool(OPENROUTER_APP_NAME))
print("[ENV CHECK] OPENROUTER_MODEL:", MODEL)

if not BOT_TOKEN or not OPENROUTER_API_KEY:
    missing = []
    if not BOT_TOKEN: missing.append("BOT_TOKEN")
    if not OPENROUTER_API_KEY: missing.append("OPENROUTER_API_KEY")
    print(f"[Lord Verbus] Missing env: {', '.join(missing)}")
    raise SystemExit(1)

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
    # тихие часы 00:00–07:00
    return 0 <= local_dt.hour < 7

def tg_link(chat_id: int, message_id: int) -> str:
    s = str(chat_id)
    if s.startswith("-100"):
        cid = s[4:]
    else:
        cid = s.lstrip("-")
    return f"https://t.me/c/{cid}/{message_id}"

def sanitize_html_whitelist(text: str) -> str:
    esc = _html.escape(text)
    esc = _re.sub(r"&lt;a href=&quot;([^&]*)&quot;&gt;(.*?)&lt;/a&gt;",
                  r'<a href="\1">\2</a>', esc, flags=_re.DOTALL)
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

def can_autoreply(chat_id: int, cooldown_min: int = 10, per_hour_limit: int = 6) -> bool:
    now = now_ts()
    rows = db_query("SELECT last_reply_ts, window_start_ts, replies_in_window FROM auto_reply_stats WHERE chat_id=?;", (chat_id,))
    if not rows:
        return True
    last_ts, win_start, cnt = rows[0]
    if now - (last_ts or 0) < cooldown_min * 60:
        return False
    if now - (win_start or 0) >= 3600:
        return True
    return (cnt or 0) < per_hour_limit

def bump_autoreply(chat_id: int):
    now = now_ts()
    rows = db_query("SELECT window_start_ts, replies_in_window FROM auto_reply_stats WHERE chat_id=?;", (chat_id,))
    if not rows:
        db_execute("INSERT INTO auto_reply_stats(chat_id, last_reply_ts, window_start_ts, replies_in_window) VALUES(?, ?, ?, ?);",
                   (chat_id, now, now, 1))
        return
    win_start, cnt = rows[0]
    if now - (win_start or 0) >= 3600:
        db_execute("UPDATE auto_reply_stats SET last_reply_ts=?, window_start_ts=?, replies_in_window=? WHERE chat_id=?;",
                   (now, now, 1, chat_id))
    else:
        db_execute("UPDATE auto_reply_stats SET last_reply_ts=?, replies_in_window=? WHERE chat_id=?;",
                   (now, (cnt or 0) + 1, chat_id))

# ---------- Persona: «Лорд Вербус» в стиле Холмса (Downey Jr.) ----------
def persona_prompt() -> str:
    return (
        "Ты — «Лорд Вербус»: аристократичный, язвительно-умный компаньон. "
        "Стиль: сухое остроумие, дедукция, тонкий троллинг; временами уместный мягкий мат (редко) — "
        "не оскорбляй людей напрямую, ругайся на обстоятельства: «чёрт», «чёртов», «блин», «фигня» и т.п. "
        "Без грубости, без токсичности к личности, никаких дискриминационных выражений. "
        "Пиши коротко (1–2 фразы), по делу. Если нужна шутка — добавь лёгкую, как у гения, уставшего от глупости мира. "
        "Всегда отвечай на языке пользователя."
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

# ----- логируем любое обычное сообщение + реакция: упоминание → ответ; иначе — maybe_reply
@dp.message(F.text)
async def catcher(m: Message):
    # пишем в базу (не логируем чистые команды, чтобы не шуметь в поиске/саммари)
    if not m.text.startswith("/"):
        db_execute(
            "INSERT INTO messages(chat_id, user_id, username, text, created_at, message_id) VALUES (?, ?, ?, ?, ?, ?);",
            (
                m.chat.id,
                m.from_user.id if m.from_user else 0,
                m.from_user.username if m.from_user else None,
                m.text,
                now_ts(),
                m.message_id,
            )
        )

    # если это упоминание бота — отвечаем в характере
    try:
        me = await bot.get_me()
        if mentions_bot(m.text or "", me.username):
            await reply_to_mention(m)
            return
    except Exception:
        pass

    # иначе — попытаться «умно» вмешаться по вопросу (эвристика)
    if not m.text.startswith("/"):
        await maybe_reply(m)

@dp.message(CommandStart())
async def cmd_start(m: Message):
    await m.reply("Лорд Вербус к вашим услугам. Команды: /ping, /lord_summary, /lord_search <запрос>")

@dp.message(Command("ping"))
async def cmd_ping(m: Message):
    await m.reply("pong")

# ---------- SUMMARY (строгий формат + характер Лорда в финале) ----------
def prev_summary_link(chat_id: int):
    prev = db_query("SELECT message_id FROM last_summary WHERE chat_id=?;", (chat_id,))
    return tg_link(chat_id, prev[0][0]) if prev and prev[0][0] else None

@dp.message(Command("lord_summary"))
async def cmd_summary(m: Message, command: CommandObject):
    # по умолчанию 300, можно указать число
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
        + " Ты оформляешь отчёт по чату. Формат — HTML. Соблюдай шаблон строго. "
          "Никаких <center>, <h1> и прочих заголовков."
    )
    user = (
        f"{dialog_block}\n\n"
        "Сформируй ответ СТРОГО по этому шаблону (ровно в таком порядке):\n\n"
        f"{prev_line_html}\n\n"
        "✂️<b>Краткое содержание</b>:\n"
        "2–3 коротких предложения, обобщающих разговор. БЕЗ ссылок.\n\n"
        "Далее СТРОГО 2–4 тематических блока. Каждый блок РОВНО так:\n"
        "😄 <b>Короткое название темы</b>\n"
        "@username(ы) кратко описывают суть в 1–3 предложениях. Без прямых цитат. "
        "Обозначь 1–3 ключевых фразы встроенными ссылками <a href='URL'>…</a>, где URL берёшь из соответствующих [link: URL]. "
        "Никаких списков, буллетов и подзаголовков «Ключевые моменты» — только один абзац.\n\n"
        "Заверши одним предложением от Лорда Вербуса в его стиле (лёгкая язвительность, можно мягкое ругательство по ситуации, но без оскорблений людей).\n"
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

# ---------- Ответ на упоминание бота в его стиле ----------
async def reply_to_mention(m: Message):
    # тихие часы — молчим
    if is_quiet_hours(datetime.now().astimezone()):
        return

    # беру короткий контекст (последние 15 реплик)
    ctx = chat_recent_context(m.chat.id, limit=15)
    lines = []
    for u, t in ctx:
        handle = ("@" + u) if u else "user"
        lines.append(f"{handle}: {t}")
    ctx_block = "\n".join(lines)

    system = persona_prompt()
    user = (
        "Тебя упомянули в групповом чате. Ответь в своём стиле (1–2 фразы): умно, язвительно, но дружелюбно. "
        "Редко допустим мягкий мат по ситуации («чёрт», «блин»), без оскорблений людей.\n\n"
        f"Недавний контекст:\n{ctx_block}\n\n"
        f"Сообщение с упоминанием:\n@{m.from_user.username if m.from_user and m.from_user.username else 'user'}: {m.text}"
    )
    try:
        reply = await ai_reply(system, user, temperature=0.6)
        await m.reply(sanitize_html_whitelist(reply))
    except Exception:
        pass

# ---------- Smart event-based auto-reply ----------
async def maybe_reply(m: Message):
    if not m.chat or not m.from_user or not m.text:
        return
    if m.via_bot or m.forward_origin:
        return
    # если это ответ боту — не вмешиваемся
    me = await bot.get_me()
    if m.reply_to_message and m.reply_to_message.from_user and m.reply_to_message.from_user.id == me.id:
        return
    # вопрос не боту?
    if mentions_bot(m.text, me.username):
        return
    if not is_question(m.text):
        return
    # активность чата
    if recent_chat_activity(m.chat.id, minutes=5) < 5:
        return
    # тихий режим
    if is_quiet_hours(datetime.now().astimezone()):
        return
    # кулдаун/лимиты
    if not can_autoreply(m.chat.id, cooldown_min=10, per_hour_limit=6):
        return

    ctx = chat_recent_context(m.chat.id, limit=20)
    lines = []
    for u, t in ctx:
        handle = ("@" + u) if u else "user"
        lines.append(f"{handle}: {t}")
    ctx_block = "\n".join(lines[-20:])

    system = persona_prompt()
    user = (
        "Это фрагмент недавнего группового чата. Вмешайся уместно и ответь на вопрос пользователя. "
        "Пиши 1–2 короткие фразы, сухое остроумие, лёгкий троллинг. Мягкий мат разрешён по ситуации, "
        "но не оскорбляй людей.\n\n"
        f"Контекст:\n{ctx_block}\n\n"
        f"Вопрос:\n@{m.from_user.username if m.from_user and m.from_user.username else 'user'}: {m.text}"
    )
    try:
        reply = await ai_reply(system, user, temperature=0.7)
    except Exception:
        return

    try:
        await m.reply(sanitize_html_whitelist(reply))
        bump_autoreply(m.chat.id)
    except Exception:
        pass

# ---------- Commands setup ----------
async def setup_commands():
    base_cmds = [
        BotCommand(command="ping", description="Проверка, жив ли бот"),
        BotCommand(command="lord_summary", description="Саммари последних сообщений"),
        BotCommand(command="lord_search", description="Поиск по чату"),
    ]
    await bot.set_my_commands(base_cmds, scope=BotCommandScopeAllGroupChats())
    await bot.set_my_commands(base_cmds, scope=BotCommandScopeAllPrivateChats())

# ---------------- Main ----------------
async def main():
    init_db()
    await setup_commands()
    print("[Lord Verbus] Online ✅ Starting long polling…")
    await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())

if __name__ == "__main__":
    asyncio.run(main())
