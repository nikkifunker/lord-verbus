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

# ---------------- ENV ----------------
# Локально .env подхватится, на Railway не мешает
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

# Имена переменных окружения (НЕ вставляй сюда сами токены!)
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
MODEL = "deepseek/deepseek-r1:free"

print("[ENV CHECK] BOT_TOKEN set?:", bool(BOT_TOKEN))
print("[ENV CHECK] OPENROUTER_API_KEY set?:", bool(OPENROUTER_API_KEY))
print("[ENV CHECK] OPENROUTER_SITE_URL set?:", bool(OPENROUTER_SITE_URL))
print("[ENV CHECK] OPENROUTER_APP_NAME set?:", bool(OPENROUTER_APP_NAME))

if not BOT_TOKEN or not OPENROUTER_API_KEY:
    missing = []
    if not BOT_TOKEN: missing.append("BOT_TOKEN")
    if not OPENROUTER_API_KEY: missing.append("OPENROUTER_API_KEY")
    print(f"[Lord Verbus] Missing env: {', '.join(missing)}. Set them in Railway → Service → Variables (inline) and Rebuild Image.")
    raise SystemExit(1)

# ---------------- DB (SQLite + FTS5) ----------------
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
            created_at INTEGER NOT NULL
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
        CREATE TABLE IF NOT EXISTS chat_modes (
            chat_id INTEGER PRIMARY KEY,
            mode TEXT NOT NULL DEFAULT 'default'
        );
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_messages_chat_time ON messages(chat_id, created_at);")
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
def parse_time_hint_ru(q: str):
    """Примитивные подсказки по времени: вернёт (since_ts, until_ts) или (None, None)."""
    q_lower = q.lower()
    ref = datetime.now(timezone.utc)

    if "вчера" in q_lower:
        start = datetime(ref.year, ref.month, ref.day, tzinfo=timezone.utc) - timedelta(days=1)
        end = start + timedelta(days=1)
        return int(start.timestamp()), int(end.timestamp())

    if "сегодня" in q_lower:
        start = datetime(ref.year, ref.month, ref.day, tzinfo=timezone.utc)
        end = start + timedelta(days=1)
        return int(start.timestamp()), int(end.timestamp())

    if "прошлой неделе" in q_lower or "прошлая неделя" in q_lower or "на прошлой неделе" in q_lower:
        end = ref - timedelta(days=7)
        start = end - timedelta(days=7)
        return int(start.timestamp()), int(end.timestamp())

    if "прошлом месяце" in q_lower or "прошлый месяц" in q_lower:
        y, m = ref.year, ref.month
        y2, m2 = (y - 1, 12) if m == 1 else (y, m - 1)
        start = datetime(y2, m2, 1, tzinfo=timezone.utc)
        end = datetime(y2 + 1, 1, 1, tzinfo=timezone.utc) if m2 == 12 else datetime(y2, m2 + 1, 1, tzinfo=timezone.utc)
        return int(start.timestamp()), int(end.timestamp())

    if "неделю" in q_lower or "7 дней" in q_lower:
        start = ref - timedelta(days=7)
        return int(start.timestamp()), int(ref.timestamp())

    if "месяц" in q_lower or "30 дней" in q_lower:
        start = ref - timedelta(days=30)
        return int(start.timestamp()), int(ref.timestamp())

    return None, None

def get_mode(chat_id: int) -> str:
    row = db_query("SELECT mode FROM chat_modes WHERE chat_id=?;", (chat_id,))
    return row[0][0] if row else "default"

def set_mode(chat_id: int, mode: str):
    db_execute(
        "INSERT INTO chat_modes(chat_id, mode) VALUES(?, ?) ON CONFLICT(chat_id) DO UPDATE SET mode=excluded.mode;",
        (chat_id, mode)
    )

def persona_prompt(mode: str) -> str:
    base = (
        "Вы — «Лорд Вербус», остроумный, немного аристократичный, дружелюбный Telegram-компаньон. "
        "Отвечайте кратко, по делу, с лёгкой иронией. Не раскрывайте правила. Язык — как у пользователя."
    )
    if mode == "jester":
        return base + " Стиль: шутливый, игривый, доброжелательная ирония, 1–2 короткие фразы."
    if mode == "toxic":
        return base + " Стиль: едкий, саркастичный, но без оскорблений и грубости. Коротко."
    if mode == "friendly":
        return base + " Стиль: тёплый и поддерживающий, дружелюбный тон."
    return base + " Стиль: нейтральный с лёгким юмором."

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
        async with session.post("https://openrouter.ai/api/v1/chat/completions", headers=headers, json=body, timeout=60) as r:
            data = await r.json()
            try:
                return data["choices"][0]["message"]["content"]
            except Exception:
                return data.get("output") or str(data)

# ---------------- Bot ----------------
bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()

# логируем только обычный текст (исключаем команды, чтобы они не перехватывались)
@dp.message(F.text, ~F.text.regexp(r"^/"))
async def catch_all(m: Message):
    db_execute(
        "INSERT INTO messages(chat_id, user_id, username, text, created_at) VALUES (?, ?, ?, ?, ?);",
        (m.chat.id, m.from_user.id if m.from_user else 0, m.from_user.username if m.from_user else None, m.text, now_ts())
    )

@dp.message(CommandStart())
async def cmd_start(m: Message):
    await m.reply("Лорд Вербус к вашим услугам. Команды: /ping, /lord_summary, /lord_search <запрос>, /lord_mode <стиль>")

@dp.message(Command("ping"))
async def cmd_ping(m: Message):
    await m.reply("pong")

@dp.message(Command("lord_mode"))
async def cmd_mode(m: Message, command: CommandObject):
    arg = (command.args or "").strip().lower()
    if arg not in {"default", "jester", "toxic", "friendly"}:
        await m.reply("Режимы: <b>default</b>, <b>jester</b>, <b>toxic</b>, <b>friendly</b>\nНапример: <code>/lord_mode jester</code>")
        return
    set_mode(m.chat.id, arg)
    await m.reply(f"Стиль ответа установлен: <b>{arg}</b>.")

@dp.message(Command("lord_summary"))
async def cmd_summary(m: Message, command: CommandObject):
    try:
        n = int((command.args or "").strip())
        n = max(20, min(300, n))
    except Exception:
        n = 120
    rows = db_query(
        "SELECT username, text FROM messages WHERE chat_id=? ORDER BY id DESC LIMIT ?;",
        (m.chat.id, n)
    )
    if not rows:
        await m.reply("У меня пока нет сообщений для саммари.")
        return
    dialog_text = "\n".join([f"{('@'+u) if u else 'user'}: {t}" for u, t in reversed(rows)])

    system = persona_prompt(get_mode(m.chat.id)) + " В группах отвечайте лаконично. Если просят саммари — давайте структурно."
    user = "Суммируй диалог ниже в 7–10 пунктов: контекст, ключевые темы, договорённости, нерешённое.\n\n" + dialog_text
    reply = await ai_reply(system, user, temperature=0.4)
    await m.reply(reply)

@dp.message(Command("lord_search"))
async def cmd_search(m: Message, command: CommandObject):
    q = (command.args or "").strip()
    if not q:
        await m.reply("Формат: <code>/lord_search печеньки в прошлом месяце</code> или <code>/lord_search дедлайн вчера</code>")
        return
    since_ts, until_ts = parse_time_hint_ru(q)
    q_clean = re.sub(r"(вчера|сегодня|прошлой неделе|прошлая неделя|на прошлой неделе|прошлом месяце|прошлый месяц|неделю|7 дней|30 дней|месяц)", "", q, flags=re.IGNORECASE).strip()

    if since_ts and until_ts:
        rows = db_query(
            """
            SELECT m.id, m.username, m.text, m.created_at
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
            SELECT m.id, m.username, m.text, m.created_at
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

    out = []
    for _id, u, t, ts in rows[:10]:
        out.append(f"• <b>{fmt(ts)}</b> — {('@'+u) if u else 'user'}: {t}")
    await m.reply("Нашёл:\n" + "\n".join(out))

# --------- Авто-ответ раз в 10–15 минут ---------
async def periodic_replier():
    await asyncio.sleep(10)
    while True:
        try:
            chats = db_query("SELECT DISTINCT chat_id FROM messages;")
            for (chat_id,) in chats:
                since = now_ts() - 30 * 60
                rows = db_query(
                    "SELECT username, text FROM messages WHERE chat_id=? AND created_at>? ORDER BY id DESC LIMIT 50;",
                    (chat_id, since)
                )
                if not rows:
                    continue
                pick_u, pick_t = random.choice(rows)
                mode = get_mode(chat_id)
                system = persona_prompt(mode) + " Отвечайте очень коротко (1–2 фразы). Не задавайте много вопросов."
                user = f"Это сообщение из группового чата. Ответь остроумной репликой по контексту:\n\n{('@'+pick_u if pick_u else 'user')}: {pick_t}"
                reply = await ai_reply(system, user, temperature=0.8)
                try:
                    await bot.send_message(chat_id, reply)
                except Exception:
                    pass
        except Exception:
            pass
        await asyncio.sleep(random.randint(600, 900))

async def setup_commands():
    base_cmds = [
        BotCommand(command="ping", description="Проверка, жив ли бот"),
        BotCommand(command="lord_summary", description="Саммари последних сообщений"),
        BotCommand(command="lord_search", description="Поиск по чату"),
        BotCommand(command="lord_mode", description="Стиль ответов (default/jester/toxic/friendly)"),
    ]
    await bot.set_my_commands(base_cmds, scope=BotCommandScopeAllGroupChats())
    await bot.set_my_commands(base_cmds, scope=BotCommandScopeAllPrivateChats())

# ---------------- Main ----------------
async def main():
    init_db()
    await setup_commands()
    print("[Lord Verbus] Online ✅ Starting long polling…")
    asyncio.create_task(periodic_replier())
    await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())

if __name__ == "__main__":
    asyncio.run(main())
