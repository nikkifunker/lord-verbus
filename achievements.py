# achievements.py
import os
import json
import sqlite3
from contextlib import closing
from datetime import datetime, timezone
from typing import Any

from aiogram import Router
from aiogram.filters import Command, CommandObject
from aiogram.types import Message

# =========
# Конфиг
# =========
DB = os.getenv("DB_PATH", "bot.sqlite3")

# Список админов (user_id через запятую в переменной окружения ADMIN_IDS),
# напр. ADMIN_IDS="254160871,123456789"
ADMIN_IDS = {
    int(x) for x in os.getenv("ADMIN_IDS", "254160871").replace(" ", "").split(",") if x
}

router = Router(name="achievements")

# =========
# DB utils
# =========
def _conn():
    # Можно включить именованные строки при желании:
    # c = sqlite3.connect(DB)
    # c.row_factory = sqlite3.Row
    # return c
    return sqlite3.connect(DB)

def _exec(sql: str, params: tuple = ()):
    with closing(_conn()) as c:
        c.execute(sql, params)
        c.commit()

def _q(sql: str, params: tuple = ()) -> list[tuple]:
    with closing(_conn()) as c:
        cur = c.execute(sql, params)
        return cur.fetchall()

def _now_ts() -> int:
    return int(datetime.now(timezone.utc).timestamp())

# =========
# Init schema + миграция
# =========
def _table_has_column(table: str, col: str) -> bool:
    with closing(_conn()) as c:
        cur = c.execute(f"PRAGMA table_info({table});")
        cols = [r[1] for r in cur.fetchall()]  # name = index 1
        return col in cols

def _table_exists(table: str) -> bool:
    with closing(_conn()) as c:
        cur = c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?;", (table,))
        return cur.fetchone() is not None

def _migrate_achievements_schema():
    # Если таблицы нет — просто выходим, её создаст init_db ниже
    if not _table_exists("achievements"):
        return

    # Определим фактические колонки
    with closing(_conn()) as c:
        existing_cols = [r[1] for r in c.execute("PRAGMA table_info(achievements);").fetchall()]
    def has(col: str) -> bool:
        return col in existing_cols

    need_rebuild = False
    add_columns: list[str] = []

    # Критично: первичный ключ id. Если его нет — пересборка.
    if not has("id"):
        need_rebuild = True

    # Эти можно добавить ALTER'ом при необходимости
    if not has("condition_type"):
        add_columns.append("ALTER TABLE achievements ADD COLUMN condition_type TEXT")
    if not has("thresholds"):
        add_columns.append("ALTER TABLE achievements ADD COLUMN thresholds TEXT")
    if not has("target_ts"):
        add_columns.append("ALTER TABLE achievements ADD COLUMN target_ts INTEGER")
    if not has("active"):
        add_columns.append("ALTER TABLE achievements ADD COLUMN active INTEGER NOT NULL DEFAULT 1")
    if not has("extra_json"):
        add_columns.append("ALTER TABLE achievements ADD COLUMN extra_json TEXT")

    with closing(_conn()) as c:
        if need_rebuild:
            # Пересоберём таблицу через new->copy->drop->rename
            c.execute("""
                CREATE TABLE IF NOT EXISTS achievements_new (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    code TEXT UNIQUE NOT NULL,
                    title TEXT NOT NULL,
                    description TEXT NOT NULL,
                    kind TEXT NOT NULL CHECK(kind IN ('single','tiered')),
                    condition_type TEXT NOT NULL CHECK(condition_type IN ('messages','date','keyword')),
                    thresholds TEXT,
                    target_ts INTEGER,
                    active INTEGER NOT NULL DEFAULT 1,
                    extra_json TEXT
                );
            """)

            # Соберём SELECT под любую старую схему
            sel_id          = "id" if has("id") else "rowid"
            sel_code        = "code" if has("code") else "NULL"
            sel_title       = "title" if has("title") else "''"
            sel_description = "description" if has("description") else "''"
            sel_kind        = "kind" if has("kind") else "'single'"
            sel_cond        = "condition_type" if has("condition_type") else "'messages'"
            sel_thresholds  = "thresholds" if has("thresholds") else "NULL"
            sel_target_ts   = "target_ts" if has("target_ts") else "NULL"
            sel_active      = "active" if has("active") else "1"
            sel_extra       = "extra_json" if has("extra_json") else "NULL"

            # Переносим данные
            c.execute(f"""
                INSERT OR IGNORE INTO achievements_new
                (id, code, title, description, kind, condition_type, thresholds, target_ts, active, extra_json)
                SELECT
                    {sel_id} AS id,
                    {sel_code},
                    {sel_title},
                    {sel_description},
                    {sel_kind},
                    {sel_cond},
                    {sel_thresholds},
                    {sel_target_ts},
                    {sel_active},
                    {sel_extra}
                FROM achievements;
            """)
            c.execute("DROP TABLE achievements;")
            c.execute("ALTER TABLE achievements_new RENAME TO achievements;")
            c.commit()
        else:
            for ddl in add_columns:
                try:
                    c.execute(ddl + ";")
                except sqlite3.OperationalError:
                    pass
            c.commit()

def init_db():
    with closing(_conn()) as c:
        c.execute("PRAGMA journal_mode=WAL;")
        c.execute("PRAGMA synchronous=NORMAL;")
        # Создаём, если нет
        c.execute("""
        CREATE TABLE IF NOT EXISTS achievements (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT UNIQUE NOT NULL,
            title TEXT NOT NULL,
            description TEXT NOT NULL,
            kind TEXT NOT NULL CHECK(kind IN ('single','tiered')),
            condition_type TEXT NOT NULL CHECK(condition_type IN ('messages','date','keyword')),
            thresholds TEXT,
            target_ts INTEGER,
            active INTEGER NOT NULL DEFAULT 1,
            extra_json TEXT
        );
        """)
        c.execute("""
        CREATE TABLE IF NOT EXISTS user_stats (
            chat_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            messages_count INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY(chat_id, user_id)
        );
        """)
        c.execute("""
        CREATE TABLE IF NOT EXISTS user_achievements (
            chat_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            achievement_id INTEGER NOT NULL,
            tier INTEGER NOT NULL DEFAULT 1,
            unlocked_at INTEGER NOT NULL,
            PRIMARY KEY(chat_id, user_id, achievement_id, tier),
            FOREIGN KEY(achievement_id) REFERENCES achievements(id) ON DELETE CASCADE
        );
        """)
        c.commit()

    # После создания/проверок — миграция для старых БД
    _migrate_achievements_schema()

# =========
# Helpers
# =========
def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS

def _parse_thresholds(s: str) -> list[int]:
    # "100, 1000, 10000" -> [100,1000,10000]
    vals = []
    for p in (s or "").split(","):
        p = p.strip()
        if not p:
            continue
        n = int(p)
        if n <= 0:
            raise ValueError("Порог должен быть > 0")
        vals.append(n)
    vals = sorted(set(vals))
    if not vals:
        raise ValueError("Нужно указать хотя бы один порог")
    return vals

def _pretty_box(text: str) -> str:
    # Лёгкая «рамка» — совместима с Telegram HTML parse_mode
    line = "━━━━━━━━━━━━━━━━━━━━━━━━"
    return f"<b>{line}</b>\n{text}\n<b>{line}</b>"

def _calc_rarity(chat_id: int, achievement_id: int) -> float:
    # считем по вашей таблице messages
    total = _q("SELECT COUNT(DISTINCT user_id) FROM messages WHERE chat_id=?;", (chat_id,))[0][0] or 0
    have  = _q("SELECT COUNT(DISTINCT user_id) FROM user_achievements WHERE chat_id=? AND achievement_id=?;", (chat_id, achievement_id))[0][0] or 0
    if total == 0:
        return 0.0
    # редкость = 100 - доля получивших
    got_share = have / total
    rarity = max(0.0, 100.0 - got_share * 100.0)
    return round(rarity, 2)

async def _announce(m: Message, title: str, description: str, rarity: float, tier_label: str | None = None):
    tier_suffix = f"\n<i>Уровень:</i> <b>{tier_label}</b>" if tier_label else ""
    text = (
        f"<b>Поздравляю!</b> Добыта ачивка 🎖️\n\n"
        f"<b>{title}</b>\n"
        f"{description}\n"
        f"{tier_suffix}\n"
        f"\n<i>Редкость:</i> <b>{rarity}%</b>"
    )
    await m.answer(_pretty_box(text), disable_web_page_preview=True)

# =========
# Публичные хуки
# =========
async def on_text_hook(m: Message):
    """
    Вызывайте из вашего on_text сразу после логирования сообщения.
    Инкремент счётчиков и проверка триггеров.
    """
    if not m.from_user:
        return
    chat_id = m.chat.id
    user_id = m.from_user.id

    # 1) счётчик сообщений
    _exec("INSERT OR IGNORE INTO user_stats(chat_id, user_id) VALUES(?, ?);", (chat_id, user_id))
    _exec("UPDATE user_stats SET messages_count=messages_count+1 WHERE chat_id=? AND user_id=?;", (chat_id, user_id))

    # 2) активные ачивки
    achs = _q("""
        SELECT COALESCE(id,rowid) AS id, code, title, description, kind, condition_type, thresholds, target_ts, active, extra_json
        FROM achievements
        WHERE active=1;
    """)
    if not achs:
        return

    msg_cnt = _q("SELECT messages_count FROM user_stats WHERE chat_id=? AND user_id=?;", (chat_id, user_id))[0][0]
    text = (m.text or m.caption or "")

    for (aid, code, title, desc, kind, ctype, thresholds_json, target_ts, active, extra_json) in achs:
        # thresholds
        thresholds: list[int] = []
        if thresholds_json:
            try:
                thresholds = sorted(set(map(int, json.loads(thresholds_json))))
            except Exception:
                thresholds = []

        # messages
        if ctype == "messages":
            if kind == "single":
                limit = thresholds[0] if thresholds else None
                if limit and msg_cnt >= limit and not _user_has_tier(chat_id, user_id, aid, 1):
                    _unlock(chat_id, user_id, aid, 1)
                    await _announce(m, title, desc, _calc_rarity(chat_id, aid))
            else:
                for idx, limit in enumerate(thresholds, start=1):
                    if msg_cnt >= limit and not _user_has_tier(chat_id, user_id, aid, idx):
                        _unlock(chat_id, user_id, aid, idx)
                        await _announce(m, title, desc, _calc_rarity(chat_id, aid), tier_label=f"{idx}/{len(thresholds)}")

        # date
        elif ctype == "date" and target_ts:
            if _now_ts() >= int(target_ts) and not _user_has_tier(chat_id, user_id, aid, 1):
                _unlock(chat_id, user_id, aid, 1)
                await _announce(m, title, desc, _calc_rarity(chat_id, aid))

        # keyword
        elif ctype == "keyword":
            kw = None
            try:
                kw = json.loads(extra_json).get("keyword") if extra_json else None
            except Exception:
                kw = None
            if not kw:
                continue

            contains_now = kw.lower() in (text or "").lower()
            if not contains_now:
                continue

            # считаем исторические вхождения по таблице messages
            try:
                total = _q("""
                    SELECT COUNT(*) FROM messages
                    WHERE chat_id=? AND user_id=? AND LOWER(text) LIKE LOWER(?);
                """, (chat_id, user_id, f"%{kw}%"))[0][0]
            except Exception:
                total = 1  # fallback

            if kind == "single":
                limit = thresholds[0] if thresholds else None
                if limit and total >= limit and not _user_has_tier(chat_id, user_id, aid, 1):
                    _unlock(chat_id, user_id, aid, 1)
                    await _announce(m, title, desc, _calc_rarity(chat_id, aid))
            else:
                for idx, limit in enumerate(thresholds, start=1):
                    if total >= limit and not _user_has_tier(chat_id, user_id, aid, idx):
                        _unlock(chat_id, user_id, aid, idx)
                        await _announce(m, title, desc, _calc_rarity(chat_id, aid), tier_label=f"{idx}/{len(thresholds)}")

# =========
# Низкоуровневые операции
# =========
def _user_has_tier(chat_id: int, user_id: int, ach_id: int, tier: int) -> bool:
    row = _q(
        "SELECT 1 FROM user_achievements WHERE chat_id=? AND user_id=? AND achievement_id=? AND tier=? LIMIT 1;",
        (chat_id, user_id, ach_id, tier)
    )
    return bool(row)

def _unlock(chat_id: int, user_id: int, ach_id: int, tier: int):
    _exec(
        "INSERT INTO user_achievements(chat_id, user_id, achievement_id, tier, unlocked_at) VALUES(?,?,?,?,?)",
        (chat_id, user_id, ach_id, tier, _now_ts())
    )

def _find_achievement_by_code_or_id(code_or_id: str) -> tuple | None:
    sql = """
        SELECT COALESCE(id,rowid) AS id, code, title, description, kind, condition_type, thresholds, target_ts, active, extra_json
        FROM achievements
        WHERE {where}
        LIMIT 1;
    """
    if code_or_id.isdigit():
        rows = _q(sql.format(where="COALESCE(id,rowid)=?"), (int(code_or_id),))
    else:
        rows = _q(sql.format(where="LOWER(code)=LOWER(?)"), (code_or_id,))
    return rows[0] if rows else None

# =========
# Команды: админ
# =========
@router.message(Command("ach_add"))
async def cmd_ach_add(m: Message, command: CommandObject):
    """
    Форматы:
    1) messages + tiered:
       /ach_add code|Заголовок|Описание|tiered|messages|100,1000,10000
    2) messages + single:
       /ach_add code|Заголовок|Описание|single|messages|100
    3) date + single:
       /ach_add code|Заголовок|Описание|single|date|2025-12-31
    4) keyword + tiered:
       /ach_add code|Заголовок|Описание|tiered|keyword:WORD|1,3,5
    """
    if not m.from_user or not is_admin(m.from_user.id):
        return await m.reply("Недостаточно прав.")

    args = (command.args or "").split("|")
    if len(args) != 6:
        return await m.reply(
            "Формат:\n"
            "1) /ach_add code|title|description|tiered|messages|100,1000\n"
            "2) /ach_add code|title|description|single|messages|100\n"
            "3) /ach_add code|title|description|single|date|YYYY-MM-DD\n"
            "4) /ach_add code|title|description|tiered|keyword:WORD|1,3,5"
        )

    code, title, desc, kind, cond, data = [a.strip() for a in args]
    if kind not in ("single", "tiered"):
        return await m.reply("kind: single|tiered")

    cond_type = cond
    keyword = None
    if cond.startswith("keyword:"):
        cond_type = "keyword"
        keyword = cond.split(":", 1)[1].strip()
        if not keyword:
            return await m.reply("Укажите слово после keyword:, напр. keyword:testtest")

    if cond_type not in ("messages", "date", "keyword"):
        return await m.reply("condition: messages | date | keyword:<word>")

    thresholds_json = None
    target_ts = None
    extra_json = None

    try:
        if cond_type == "messages":
            thresholds = _parse_thresholds(data)
            if kind == "single" and len(thresholds) != 1:
                return await m.reply("Для single укажите ровно один порог.")
            thresholds_json = json.dumps(thresholds)
        elif cond_type == "date":
            y, mo, d = map(int, data.split("-"))
            dt = datetime(y, mo, d, 23, 59, 59, tzinfo=timezone.utc)
            target_ts = int(dt.timestamp())
        else:  # keyword
            thresholds = _parse_thresholds(data)
            if kind == "single" and len(thresholds) != 1:
                return await m.reply("Для single укажите ровно один порог.")
            thresholds_json = json.dumps(thresholds)
            extra_json = json.dumps({"keyword": keyword})
    except Exception as e:
        return await m.reply(f"Ошибка параметров: {e}")

    try:
        _exec("""
            INSERT INTO achievements(code,title,description,kind,condition_type,thresholds,target_ts,active,extra_json)
            VALUES(?,?,?,?,?,?,?,?,?);
        """, (code, title, desc, kind, cond_type, thresholds_json, target_ts, 1, extra_json))
        await m.reply(f"✅ Ачивка добавлена: <b>{title}</b> (code: <code>{code}</code>)")
    except sqlite3.IntegrityError:
        await m.reply("Ачивка с таким code уже существует.")
    except Exception as e:
        await m.reply(f"Ошибка: {e}")

@router.message(Command("ach_del"))
async def cmd_ach_del(m: Message, command: CommandObject):
    if not m.from_user or not is_admin(m.from_user.id):
        return await m.reply("Недостаточно прав.")
    arg = (command.args or "").strip()
    if not arg:
        return await m.reply("Укажите ID или code: /ach_del MSG100")
    ach = _find_achievement_by_code_or_id(arg)
    if not ach:
        return await m.reply("Не найдено.")
    _exec("DELETE FROM achievements WHERE id=?;", (ach[0],))
    await m.reply("Удалено.")

@router.message(Command("ach_list")))
async def cmd_ach_list(m: Message):
    if not m.from_user or not is_admin(m.from_user.id):
        return await m.reply("Недостаточно прав.")
    rows = _q("""
        SELECT COALESCE(id,rowid) AS id, code, title, kind, condition_type, thresholds, target_ts, active, extra_json
        FROM achievements
        ORDER BY id;
    """)
    if not rows:
        return await m.reply("Список пуст.")
    parts = []
    for (rid, code, title, kind, ctype, thr, ts, active, extra_json) in rows:
        data = []
        if ctype == "messages":
            data.append(f"thresholds={thr}")
        elif ctype == "date":
            data.append(f"date_ts={ts}")
        else:
            try:
                kw = json.loads(extra_json).get("keyword") if extra_json else None
            except Exception:
                kw = None
            data.append(f"keyword={kw}; thresholds={thr}")
        parts.append(f"#{rid} <b>{title}</b> (<code>{code}</code>) — {kind}/{ctype}, {'; '.join(data)}, active={active}")
    await m.reply("\n".join(parts), disable_web_page_preview=True)

@router.message(Command("ach_edit")))
async def cmd_ach_edit(m: Message, command: CommandObject):
    """
    /ach_edit code|field|value
    поля: title, description, thresholds, target_date(YYYY-MM-DD), kind(single|tiered), active(0/1)
    """
    if not m.from_user or not is_admin(m.from_user.id):
        return await m.reply("Недостаточно прав.")
    args = (command.args or "").split("|")
    if len(args) != 3:
        return await m.reply("Формат: /ach_edit code|field|value")
    code_or_id, field, value = [a.strip() for a in args]
    ach = _find_achievement_by_code_or_id(code_or_id)
    if not ach:
        return await m.reply("Ачивка не найдена.")

    rid = ach[0]
    try:
        if field == "title":
            _exec("UPDATE achievements SET title=? WHERE id=?", (value, rid))
        elif field == "description":
            _exec("UPDATE achievements SET description=? WHERE id=?", (value, rid))
        elif field == "thresholds":
            thr = json.dumps(_parse_thresholds(value))
            _exec("UPDATE achievements SET thresholds=? WHERE id=?", (thr, rid))
        elif field == "target_date":
            y, mo, d = map(int, value.split("-"))
            dt = datetime(y, mo, d, 23, 59, 59, tzinfo=timezone.utc)
            _exec("UPDATE achievements SET target_ts=? WHERE id=?", (int(dt.timestamp()), rid))
        elif field == "kind":
            if value not in ("single", "tiered"):
                return await m.reply("kind: single|tiered")
            _exec("UPDATE achievements SET kind=? WHERE id=?", (value, rid))
        elif field == "active":
            _exec("UPDATE achievements SET active=? WHERE id=?", (int(value), rid))
        else:
            return await m.reply("Неизвестное поле.")
        await m.reply("Готово.")
    except Exception as e:
        await m.reply(f"Ошибка: {e}")

@router.message(Command("ach_progress")))
async def cmd_ach_progress(m: Message, command: CommandObject):
    """
    /ach_progress code
    Показывает прогресс всех пользователей по указанной ачивке.
    Для messages: текущий счётчик и ближайший порог.
    """
    if not m.from_user or not is_admin(m.from_user.id):
        return await m.reply("Недостаточно прав.")
    code = (command.args or "").strip()
    if not code:
        return await m.reply("Укажите code или id ачивки.")

    ach = _find_achievement_by_code_or_id(code)
    if not ach:
        return await m.reply("Ачивка не найдена.")

    (aid, _, title, _, kind, ctype, thr_json, target_ts, active, extra_json) = ach
    try:
        thresholds = sorted(set(map(int, json.loads(thr_json)))) if thr_json else []
    except Exception:
        thresholds = []

    rows = _q("""
        SELECT s.user_id, s.messages_count
        FROM user_stats s
        WHERE s.chat_id=?
        ORDER BY s.messages_count DESC;
    """, (m.chat.id,))
    if not rows:
        return await m.reply("Пока нет данных по этому чату.")

    lines = [f"<b>Прогресс по</b> «{title}»:"]

    if ctype == "messages":
        for uid, msg_cnt in rows[:50]:
            taken = _q("""
                SELECT MAX(tier) FROM user_achievements
                WHERE chat_id=? AND user_id=? AND achievement_id=?;
            """, (m.chat.id, uid, aid))[0][0] or 0
            next_thr = None
            for i, t in enumerate(sorted(thresholds), start=1):
                if msg_cnt < t:
                    next_thr = t
                    break
            if next_thr is None and thresholds:
                status = f"все уровни ({taken}/{len(thresholds)})"
            else:
                status = f"{msg_cnt} / {next_thr or '-'}"
            lines.append(f"• user_id={uid}: {status}")
    elif ctype == "keyword":
        try:
            kw = json.loads(extra_json).get("keyword") if extra_json else None
        except Exception:
            kw = None
        lines.append(f"Тип: keyword, слово: <code>{kw}</code>")
    else:
        lines.append("Тип: date — выдаётся автоматически после наступления даты при любой активности.")
    await m.reply("\n".join(lines))

@router.message(Command("ach_reset")))
async def cmd_ach_reset(m: Message, command: CommandObject):
    """
    /ach_reset code @user (или user_id)
    """
    if not m.from_user or not is_admin(m.from_user.id):
        return await m.reply("Недостаточно прав.")
    args = (command.args or "").split()
    if len(args) < 2:
        return await m.reply("Формат: /ach_reset code @username|user_id")
    code = args[0].strip()
    ach = _find_achievement_by_code_or_id(code)
    if not ach:
        return await m.reply("Ачивка не найдена.")

    u = args[1].strip()
    if u.startswith("@"):
        row = _q("SELECT user_id FROM users WHERE LOWER(username)=LOWER(?) LIMIT 1;", (u[1:],))
        if not row:
            return await m.reply("Пользователь не найден в базе.")
        user_id = row[0][0]
    else:
        try:
            user_id = int(u)
        except Exception:
            return await m.reply("Некорректный user.")
    _exec("DELETE FROM user_achievements WHERE chat_id=? AND user_id=? AND achievement_id=?;",
          (m.chat.id, user_id, ach[0]))
    await m.reply("Сброшено.")

# =========
# Команды: для всех
# =========
@router.message(Command("my_achievements")))
async def cmd_my_achievements(m: Message):
    if not m.from_user:
        return
    rows = _q("""
        SELECT a.title, a.description, a.kind, a.condition_type, ua.tier, ua.unlocked_at
        FROM user_achievements ua
        JOIN achievements a ON a.id=ua.achievement_id
        WHERE ua.chat_id=? AND ua.user_id=?
        ORDER BY ua.unlocked_at DESC;
    """, (m.chat.id, m.from_user.id))
    if not rows:
        return await m.reply("У вас пока нет ачивок.")
    parts = []
    for title, desc, kind, ctype, tier, ts in rows[:30]:
        when = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")
        level = f" | Уровень {tier}" if kind == "tiered" else ""
        parts.append(f"• <b>{title}</b>{level} — {desc}  <i>({when})</i>")
    await m.reply("\n".join(parts), disable_web_page_preview=True)

@router.message(Command("ach_top")))
async def cmd_ach_top(m: Message):
    rows = _q("""
        SELECT user_id, COUNT(*) AS cnt
        FROM user_achievements
        WHERE chat_id=?
        GROUP BY user_id
        ORDER BY cnt DESC, user_id ASC
        LIMIT 20;
    """, (m.chat.id,))
    if not rows:
        return await m.reply("Пока никто не получил ачивок.")
    lines = ["<b>Топ по ачивкам</b>:"]
    place = 1
    for uid, cnt in rows:
        lines.append(f"{place}. user_id={uid} — {cnt}")
        place += 1
    await m.reply("\n".join(lines))
