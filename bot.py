import os
import asyncio
import random
import re
import sqlite3
from contextlib import closing
from datetime import datetime, timezone
import html as _html
import re as _re
import pathlib
import logging

import aiohttp
from aiogram import Bot, Dispatcher, F, Router
from aiogram.filters import Command, CommandStart, CommandObject
from aiogram.types import Message, BotCommand, BotCommandScopeAllGroupChats, BotCommandScopeAllPrivateChats
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties

# === achievements module (РОУТЕР, init и хук) ===
from achievements import router as ach_router, init_db as ach_init_db, on_text_hook as ach_on_text_hook

# =========================
# Логирование
# =========================
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
log = logging.getLogger("lord-verbus.bot")

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

# ВАЖНО: все «основные» хендлеры теперь в отдельном роутере,
# чтобы мы могли контролировать порядок подключения (achievements -> core)
core_router = Router(name="core")

# ID наблюдаемого пользователя (кружки отслеживаем у него)
WATCH_USER_ID = 447968194   # @daria_mango
# Кого упоминать/уведомлять
NOTIFY_USER_ID = 254160871  # @misukhanov
NOTIFY_USERNAME = "misukhanov"

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

def init_db_with_achievements():
    init_db()
    ach_init_db()

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
    r"\bкто\b", r"\bчто\b", r"\bкак\b", r"\bпочему\b", r"\bзачем\b",
    r"\bкогда\b", r"\bгде\b", r"\bкакой\b", r"\bкакая\b", r"\bкакие\b",
    r"\bсколько\b", r"\bможно ли\b", r"\bесть ли\b"
]
QUESTION_RE = re.compile("|".join(QUESTION_PATTERNS), re.IGNORECASE)

def is_question(text: str) -> bool:
    return bool(text and QUESTION_RE.search(text))

def mentions_bot(text: str, bot_username: str | None) -> bool:
    if not text or not bot_username:
        return False
    return f"@{bot_username.lower()}" in text.lower()

def sanitize_html_whitelist(text: str) -> str:
    allowed_tags = {
        "b", "strong", "i", "em", "u", "s", "del", "code", "pre",
        "a", "br", "blockquote", "span"
    }
    def repl(m):
        tag = m.group(1).lower().strip("/")
        if tag in allowed_tags:
            return m.group(0)
        return _html.escape(m.group(0))
    text = re.sub(r"<\s*/?\s*([a-zA-Z0-9]+)[^>]*>", repl, text)
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

# ---- target user resolver
async def resolve_target_user(m: Message) -> tuple[int | None, str | None, str | None]:
    if m.reply_to_message and m.reply_to_message.from_user:
        u = m.reply_to_message.from_user
        return u.id, (u.full_name or u.first_name), u.username
    if m.entities:
        for ent in m.entities:
            if ent.type == "text_mention" and ent.user:
                u = ent.user
                return u.id, (u.full_name or u.first_name), u.username
    if m.entities:
        for ent in m.entities:
            if ent.type == "mention":
                uname = (m.text or "")[ent.offset+1: ent.offset+ent.length]
                row = db_query("SELECT user_id, display_name, username FROM users WHERE LOWER(username)=LOWER(?) LIMIT 1;", (uname,))
                if row:
                    uid, dname, un = row[0]
                    return uid, dname, un
                return None, None, uname
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
# SUMMARY
# =========================
def prev_summary_link(chat_id: int) -> str | None:
    row = db_query("SELECT message_id FROM last_summary WHERE chat_id=? ORDER BY created_at DESC LIMIT 1;", (chat_id,))
    if not row:
        return None
    return tg_link(chat_id, row[0][0])

@core_router.message(Command("lord_summary"))
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
        "Стиль — нейтральный, информативный. HTML разрешён, структура жёсткая."
    )
    user = (
        f"Участники: {participants_html}\n\n{dialog_block}\n\n"
        "Дай отчёт по шаблону из трёх тем со ссылками на 2–5 слов, без слова «Источник»."
    )

    try:
        reply = await ai_reply(system, user, temperature=0.2)
    except Exception as e:
        reply = f"Суммаризация временно недоступна: {e}"

    await m.reply(sanitize_html_whitelist(reply))

# =========================
# Психологический портрет (3 абзаца)
# =========================
@core_router.message(Command("lord_psych"))
async def cmd_lord_psych(m: Message, command: CommandObject):
    target_id, display_name, uname = await resolve_target_user(m)
    if not target_id and not uname:
        await m.reply("Кого анализируем? Ответь командой на сообщение пользователя или укажи @username.")
        return

    rows = db_query(
        "SELECT text FROM messages WHERE chat_id=? AND (user_id=? OR username=?) AND text IS NOT NULL ORDER BY id DESC LIMIT 600;",
        (m.chat.id, target_id or -1, uname)
    )
    if not rows:
        await m.reply("Нет сообщений в базе по этому пользователю.")
        return

    texts = [re.sub(r"\s+", " ", (t or "")).strip() for (t,) in rows]
    joined = " \n".join(texts)[:8000]

    dname = (display_name or uname or "участник").strip()
    target_html = tg_mention(target_id or 0, dname, uname)

    system = (
        "Ты — «Лорд Вербус»: остроумный, язвительный аристократ, но без грубости. "
        "Дай психологический портрет по переписке. Ровно 3 абзаца. HTML минимум."
    )
    user = (
        f"Цель анализа: {target_html}\n\n"
        f"{joined}\n\n"
        "1) Вступление с именем жирным. 2) Портрет: стиль, мотиваторы, слепые зоны. 3) Короткий вердикт."
    )

    try:
        reply = strip_outer_quotes(await ai_reply(system, user, temperature=0.55))
    except Exception as e:
        reply = f"Портрет временно недоступен: {e}"

    await m.reply(sanitize_html_whitelist(reply))

# =========================
# Реплики / вмешательство
# =========================
EPITHETS = [
    "смелость есть, понимания нет — классика жанра",
    "аргумент звучит уверенно, как кот под дождём",
    "тут мысль пыталась быть острой, но сломала пятку",
    "уверенность уровня «я видел это в мемах»",
]

def maybe_pick_epithet(p: float = 0.2) -> str | None:
    if random.random() > p:
        return None
    return random.choice(EPITHETS)

@core_router.message(F.text)
async def on_text(m: Message):
    # команды игнорируем здесь (пусть их обрабатывают профильные хендлеры/роутеры)
    if m.text and m.text.startswith("/"):
        return

    # логируем
    if m.text:
        db_execute(
            "INSERT INTO messages(chat_id, user_id, username, text, created_at, message_id) VALUES (?, ?, ?, ?, ?, ?);",
            (m.chat.id, m.from_user.id if m.from_user else 0,
             m.from_user.username if m.from_user else None,
             m.text, now_ts(), m.message_id)
        )
        # обновим карточку пользователя
        if m.from_user:
            full_name = (m.from_user.full_name or "").strip() or (m.from_user.first_name or "")
            db_execute(
                "INSERT INTO users(user_id, display_name, username) VALUES(?, ?, ?) "
                "ON CONFLICT(user_id) DO UPDATE SET display_name=excluded.display_name, username=excluded.username;",
                (m.from_user.id, full_name, m.from_user.username)
            )

        # Хук ачивок — СЮДА, после логирования
        await ach_on_text_hook(m)

    # лёгкая авто-реплика по ключевым условиям (необязательно)
    me = await bot.get_me()
    if mentions_bot(m.text or "", me.username):
        epithet = maybe_pick_epithet()
        add = f" «{epithet}»" if epithet else ""
        await m.reply(sanitize_html_whitelist(f"Слышал. {add}"))

# =========================
# Уведомление о кружочках
# =========================
@core_router.message(F.video_note)
async def on_video_note_watch(m: Message):
    user = m.from_user
    if not user or user.id != WATCH_USER_ID:
        return
    who_html = tg_mention(user.id, user.full_name or user.first_name, user.username)
    link_html = ""
    if getattr(m.chat, "username", None):
        link_html = f" <a href='https://t.me/{m.chat.username}/{m.message_id}'>ссылка</a>"
    else:
        cid = str(m.chat.id)
        if cid.startswith("-100"):
            link_html = f" <a href='https://t.me/c/{cid[4:]}/{m.message_id}'>ссылка</a>"
    notify_html = tg_mention(NOTIFY_USER_ID, f"@{NOTIFY_USERNAME}", NOTIFY_USERNAME)
    if m.chat.type in ("group", "supergroup"):
        try:
            await m.reply(f"{notify_html}, {who_html} отправил видеокружок.{link_html}", disable_web_page_preview=True)
        except Exception:
            await m.reply(f"@{NOTIFY_USERNAME}, видеокружок от @{user.username or user.id}")

# =========================
# Команды списка в меню
# =========================
async def set_commands():
    commands_group = [
        BotCommand(command="lord_summary", description="Краткий отчёт по беседе"),
        BotCommand(command="lord_psych",  description="Психологический портрет"),
        BotCommand(command="my_achievements", description="Мои ачивки"),
        BotCommand(command="ach_top", description="Топ по ачивкам"),
    ]
    commands_private = [
        BotCommand(command="lord_summary", description="Краткий отчёт по беседе"),
        BotCommand(command="lord_psych",  description="Психологический портрет"),
        BotCommand(command="my_achievements", description="Мои ачивки"),
        BotCommand(command="ach_top", description="Топ по ачивкам"),
        BotCommand(command="start", description="Приветствие"),
    ]
    await bot.set_my_commands(commands_group, scope=BotCommandScopeAllGroupChats())
    await bot.set_my_commands(commands_private, scope=BotCommandScopeAllPrivateChats())

# =========================
# Диагностика: неизвестные команды (последним!)
# =========================
@core_router.message(F.text.startswith("/"))
async def unknown_command_echo(m: Message):
    # Этот хендлер сработает ТОЛЬКО если никакой другой командный хендлер не забрал апдейт.
    log.info("Unknown command reached core_router: %s", m.text)
    # Не палим админские команды. Просто молча игнорим.
    # Если хотите видеть явный ответ — раскомментируйте:
    # await m.reply("Команда не распознана.")

# =========================
# Start / Main
# =========================
@core_router.message(CommandStart())
async def start(m: Message):
    await m.reply(
        "Я — Лорд Вербус. Команды:\n"
        "• /lord_summary — краткий отчёт по беседе\n"
        "• /lord_psych — психологический портрет\n"
        "• /my_achievements — ваши ачивки\n"
        "• /ach_top — топ по ачивкам\n"
        "Говорите — вмешаюсь, если будет смысл."
    )

async def main():
    log.info("Starting bot…")
    init_db_with_achievements()
    # ПОРЯДОК ВАЖЕН: сначала ачивки, потом основной роутер
    dp.include_router(ach_router)
    dp.include_router(core_router)
    await set_commands()
    me = await bot.get_me()
    log.info("Bot started as @%s", me.username)
    await dp.start_polling(bot, allowed_updates=["message"])

if __name__ == "__main__":
    asyncio.run(main())
