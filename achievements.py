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
ADMIN_IDS = {int(x) for x in os.getenv("ADMIN_IDS", "320872593").replace(" ", "").split(",") if x}

router = Router(name="achievements")

# =========
# DB utils
# =========
def _conn():
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
# Init schema + миграции (все таблицы)
# =========
def _table_exists(name: str) -> bool:
    with closing(_conn()) as c:
        cur = c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?;", (name,))
        return cur.fetchone() is not None

def _table_cols(name: str) -> list[str]:
    if not _table_exists(name):
        return []
    with closing(_conn()) as c:
        cur = c.execute(f"PRAGMA table_info({name});")
        return [r[1] for r in cur.fetchall()]  # name = index 1

def _rebuild_achievements_if_needed():
    cols = _table_cols("achievements")
    need_rebuild = ("id" not in cols)
    with closing(_conn()) as c:
        if need_rebuild:
            # создаём новую таблицу со всеми нужными полями
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
            # безопасный SELECT из старой
            sel = {
                "id":       "id" if "id" in cols else "rowid",
                "code":     "code" if "code" in cols else "NULL",
                "title":    "title" if "title" in cols else "''",
                "desc":     "description" if "description" in cols else "''",
                "kind":     "kind" if "kind" in cols else "'single'",
                "ctype":    "condition_type" if "condition_type" in cols else "'messages'",
                "thr":      "thresholds" if "thresholds" in cols else "NULL",
                "ts":       "target_ts" if "target_ts" in cols else "NULL",
                "active":   "active" if "active" in cols else "1",
                "extra":    "extra_json" if "extra_json" in cols else "NULL",
            }
            if cols:
                c.execute(f"""
                    INSERT OR IGNORE INTO achievements_new
                    (id, code, title, description, kind, condition_type, thresholds, target_ts, active, extra_json)
                    SELECT {sel['id']}, {sel['code']}, {sel['title']}, {sel['desc']},
                           {sel['kind']}, {sel['ctype']}, {sel['thr']}, {sel['ts']}, {sel['active']}, {sel['extra']}
                    FROM achievements;
                """)
                c.execute("DROP TABLE achievements;")
            c.execute("ALTER TABLE achievements_new RENAME TO achievements;")
            c.commit()
        else:
            # дозаводим недостающие поля через ALTER
            need = {
                "condition_type": "ALTER TABLE achievements ADD COLUMN condition_type TEXT",
                "thresholds":     "ALTER TABLE achievements ADD COLUMN thresholds TEXT",
                "target_ts":      "ALTER TABLE achievements ADD COLUMN target_ts INTEGER",
                "active":         "ALTER TABLE achievements ADD COLUMN active INTEGER NOT NULL DEFAULT 1",
                "extra_json":     "ALTER TABLE achievements ADD COLUMN extra_json TEXT",
            }
            for col, ddl in need.items():
                if col not in cols:
                    try: c.execute(ddl + ";")
                    except sqlite3.OperationalError: pass
            c.commit()

def _rebuild_user_stats_if_needed():
    cols = _table_cols("user_stats")
    # нужная схема: chat_id, user_id, messages_count
    need_rebuild = not cols or any(c not in cols for c in ("chat_id","user_id","messages_count"))
    with closing(_conn()) as c:
        if need_rebuild:
            c.execute("""
                CREATE TABLE IF NOT EXISTS user_stats_new (
                    chat_id INTEGER NOT NULL,
                    user_id INTEGER NOT NULL,
                    messages_count INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY(chat_id, user_id)
                );
            """)
            if cols:
                # попробуем скопировать, если есть пересекающиеся поля
                sel_chat = "chat_id" if "chat_id" in cols else "0"
                sel_user = "user_id" if "user_id" in cols else "user_id" if "id" not in cols else "0"
                sel_cnt  = "messages_count" if "messages_count" in cols else "0"
                try:
                    c.execute(f"""
                        INSERT OR IGNORE INTO user_stats_new (chat_id, user_id, messages_count)
                        SELECT {sel_chat}, {sel_user}, {sel_cnt} FROM user_stats;
                    """)
                except sqlite3.OperationalError:
                    pass
                c.execute("DROP TABLE user_stats;")
            c.execute("ALTER TABLE user_stats_new RENAME TO user_stats;")
            c.commit()

def _rebuild_user_achievements_if_needed():
    cols = _table_cols("user_achievements")
    # нужная схема: chat_id, user_id, achievement_id, tier, unlocked_at
    need_rebuild = not cols or any(c not in cols for c in ("chat_id","user_id","achievement_id","tier","unlocked_at"))
    with closing(_conn()) as c:
        if need_rebuild:
            c.execute("""
                CREATE TABLE IF NOT EXISTS user_achievements_new (
                    chat_id INTEGER NOT NULL,
                    user_id INTEGER NOT NULL,
                    achievement_id INTEGER NOT NULL,
                    tier INTEGER NOT NULL DEFAULT 1,
                    unlocked_at INTEGER NOT NULL,
                    PRIMARY KEY(chat_id, user_id, achievement_id, tier),
                    FOREIGN KEY(achievement_id) REFERENCES achievements(id) ON DELETE CASCADE
                );
            """)
            if cols:
                sel_chat = "chat_id" if "chat_id" in cols else "0"
                sel_user = "user_id" if "user_id" in cols else "user_id" if "id" not in cols else "0"
                sel_ach  = "achievement_id" if "achievement_id" in cols else "achievement_id" if "ach_id" in cols else "0"
                sel_tier = "tier" if "tier" in cols else "1"
                sel_time = "unlocked_at" if "unlocked_at" in cols else "strftime('%s','now')"
                try:
                    c.execute(f"""
                        INSERT OR IGNORE INTO user_achievements_new
                        (chat_id, user_id, achievement_id, tier, unlocked_at)
                        SELECT {sel_chat}, {sel_user}, {sel_ach}, {sel_tier}, {sel_time}
                        FROM user_achievements;
                    """)
                except sqlite3.OperationalError:
                    pass
                c.execute("DROP TABLE user_achievements;")
            c.execute("ALTER TABLE user_achievements_new RENAME TO user_achievements;")
            c.commit()

def init_db():
    # базовое создание (если первый запуск)
    with closing(_conn()) as c:
        c.execute("PRAGMA journal_mode=WAL;")
        c.execute("PRAGMA synchronous=NORMAL;")
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
    # миграции для любых старых схем
    _rebuild_achievements_if_needed()
    _rebuild_user_stats_if_needed()
    _rebuild_user_achievements_if_needed()

# =========
# Helpers
# =========
def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS

def _parse_thresholds(s: str) -> list[int]:
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
    line = "━━━━━━━━━━━━━━━━━━━━━━━━"
    return f"<b>{line}</b>\n{text}\n<b>{line}</b>"

def _calc_rarity(chat_id: int, achievement_id: int) -> float:
    total = _q("SELECT COUNT(DISTINCT user_id) FROM messages WHERE chat_id=?;", (chat_id,))[0][0] or 0
    have  = _q("SELECT COUNT(DISTINCT user_id) FROM user_achievements WHERE chat_id=? AND achievement_id=?;", (chat_id, achievement_id))[0][0] or 0
    if total == 0:
        return 0.0
    got_share = have / total
    return round(max(0.0, 100.0 - got_share * 100.0), 2)

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

# =========
# Публичный хук (вызывать после логирования сообщения)
# =========
async def on_text_hook(m: Message):
    if not m.from_user:
        return
    chat_id = m.chat.id
    user_id = m.from_user.id

    # учёт сообщений
    _exec("INSERT OR IGNORE INTO user_stats(chat_id, user_id) VALUES(?, ?);", (chat_id, user_id))
    _exec("UPDATE user_stats SET messages_count=messages_count+1 WHERE chat_id=? AND user_id=?;", (chat_id, user_id))

    # активные ачивки
    achs = _q("""
        SELECT COALESCE(id,rowid) AS id, code, title, description, kind, condition_type, thresholds, target_ts, active, extra_json
        FROM achievements
        WHERE active=1;
    """)
    if not achs:
        return

    msg_cnt = _q("SELECT messages_count FROM user_stats WHERE chat_id=? AND user_id=?;", (chat_id, user_id))[0][0]
    text = (m.text or m.caption or "")

    for aid, code, title, desc, kind, ctype, thr_json, target_ts, active, extra_json in achs:
        # thresholds
        try:
            thresholds = sorted(set(map(int, json.loads(thr_json)))) if thr_json else []
        except Exception:
            thresholds = []

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

        elif ctype == "date" and target_ts:
            if _now_ts() >= int(target_ts) and not _user_has_tier(chat_id, user_id, aid, 1):
                _unlock(chat_id, user_id, aid, 1)
                await _announce(m, title, desc, _calc_rarity(chat_id, aid))

        elif ctype == "keyword":
            kw = None
            try:
                kw = json.loads(extra_json).get("keyword") if extra_json else None
            except Exception:
                kw = None
            if not kw:
                continue
            if kw.lower() not in (text or "").lower():
                continue
            # считаем исторические вхождения
            try:
                total = _q("""
                    SELECT COUNT(*) FROM messages
                    WHERE chat_id=? AND user_id=? AND LOWER(text) LIKE LOWER(?);
                """, (chat_id, user_id, f"%{kw}%"))[0][0]
            except Exception:
                total = 1
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
# Поиск ачивки
# =========
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
    1) /ach_add code|title|description|tiered|messages|100,1000,10000
    2) /ach_add code|title|description|single|messages|100
    3) /ach_add code|title|description|single|date|YYYY-MM-DD
    4) /ach_add code|title|description|tiered|keyword:WORD|1,3,5
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

@router.message(Command("ach_list"))
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
    for rid, code, title, kind, ctype, thr, ts, active, extra_json in rows:
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

@router.message(Command("ach_edit"))
async def cmd_ach_edit(m: Message, command: CommandObject):
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

@router.message(Command("ach_progress"))
async def cmd_ach_progress(m: Message, command: CommandObject):
    if not m.from_user or not is_admin(m.from_user.id):
        return await m.reply("Недостаточно прав.")
    code = (command.args or "").strip()
    if not code:
        return await m.reply("Укажите code или id ачивки.")
    ach = _find_achievement_by_code_or_id(code)
    if not ach:
        return await m.reply("Ачивка не найдена.")
    aid, _, title, _, kind, ctype, thr_json, target_ts, active, extra_json = ach
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
            status = f"все уровни ({taken}/{len(thresholds)})" if next_thr is None and thresholds else f"{msg_cnt} / {next_thr or '-'}"
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

@router.message(Command("ach_reset"))
async def cmd_ach_reset(m: Message, command: CommandObject):
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
    _exec("DELETE FROM user_achievements WHERE chat_id=? AND user_id=? AND achievement_id=?;", (m.chat.id, user_id, ach[0]))
    await m.reply("Сброшено.")

# =========
# Команды: для всех
# =========
@router.message(Command("my_achievements"))
async def cmd_my_achievements(m: Message):
    if not m.from_user:
        return
    rows = _q("""
        SELECT a.title, a.description, a.kind, a.condition_type, ua.tier, ua.unlocked_at
        FROM user_achievements AS ua
        JOIN achievements AS a ON a.id = ua.achievement_id
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

@router.message(Command("ach_top"))
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
    for i, (uid, cnt) in enumerate(rows, start=1):
        lines.append(f"{i}. user_id={uid} — {cnt}")
    await m.reply("\n".join(lines))
