# achievements.py
import os
import json
import sqlite3
from contextlib import closing
from datetime import datetime, timezone
from html import escape
from typing import Any

from aiogram import Router
from aiogram.filters import Command, CommandObject
from aiogram.types import Message

from utils.achievements_format import format_achievement_message
from utils.sender import send_achievement_award

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


def _table_exists_conn(conn: sqlite3.Connection, name: str) -> bool:
    cur = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?;", (name,))
    return cur.fetchone() is not None


def _table_cols_conn(conn: sqlite3.Connection, name: str) -> set[str]:
    if not _table_exists_conn(conn, name):
        return set()
    cur = conn.execute(f"PRAGMA table_info({name});")
    return {row[1] for row in cur.fetchall()}


def _delete_optional_records(
    conn: sqlite3.Connection,
    table: str,
    column_values: dict[str, Any],
) -> int:
    columns = _table_cols_conn(conn, table)
    if not columns:
        return 0
    filtered = [(col, val) for col, val in column_values.items() if val is not None and col in columns]
    if not filtered:
        return 0
    where = " AND ".join(f"{col}=?" for col, _ in filtered)
    cur = conn.execute(f"DELETE FROM {table} WHERE {where};", tuple(val for _, val in filtered))
    return cur.rowcount


def _remove_progress_records(
    conn: sqlite3.Connection,
    *,
    ach_code: str,
    ach_id: int,
    chat_id: int | None,
    user_id: int | None,
) -> int:
    targets = (
        "achievements_progress",
        "achievement_progress",
        "achievements_states",
        "achievement_states",
        "achievements_awards",
    )
    total = 0
    for table in targets:
        total += _delete_optional_records(
            conn,
            table,
            {
                "chat_id": chat_id,
                "user_id": user_id,
                "achievement_id": ach_id,
                "ach_id": ach_id,
                "achievement_code": ach_code,
                "ach_code": ach_code,
                "code": ach_code,
            },
        )
    return total


def _truncate_table(conn: sqlite3.Connection, table: str) -> int:
    if not _table_exists_conn(conn, table):
        return 0
    cur = conn.execute(f"DELETE FROM {table};")
    return cur.rowcount


def _get_progress_value_conn(
    conn: sqlite3.Connection,
    chat_id: int,
    user_id: int,
    ach_id: int,
) -> int:
    cur = conn.execute(
        "SELECT progress FROM achievement_progress WHERE chat_id=? AND user_id=? AND achievement_id=?;",
        (chat_id, user_id, ach_id),
    )
    row = cur.fetchone()
    return int(row[0]) if row and row[0] is not None else 0


def _get_progress_value(chat_id: int, user_id: int, ach_id: int) -> int:
    with closing(_conn()) as conn:
        return _get_progress_value_conn(conn, chat_id, user_id, ach_id)


def _increment_progress(chat_id: int, user_id: int, ach_id: int, delta: int) -> int:
    if delta <= 0:
        return _get_progress_value(chat_id, user_id, ach_id)
    now = _now_ts()
    with closing(_conn()) as conn:
        conn.execute("PRAGMA foreign_keys=ON;")
        conn.execute(
            """
            INSERT OR IGNORE INTO achievement_progress(chat_id, user_id, achievement_id, progress, updated_at)
            VALUES(?,?,?,?,?);
            """,
            (chat_id, user_id, ach_id, 0, now),
        )
        conn.execute(
            """
            UPDATE achievement_progress
            SET progress = progress + ?, updated_at=?
            WHERE chat_id=? AND user_id=? AND achievement_id=?;
            """,
            (delta, now, chat_id, user_id, ach_id),
        )
        conn.commit()
        return _get_progress_value_conn(conn, chat_id, user_id, ach_id)


def _fetch_user_profiles(user_ids: set[int]) -> dict[int, tuple[str | None, str | None]]:
    if not user_ids or not _table_exists("users"):
        return {}
    cols = set(_table_cols("users"))
    if "user_id" not in cols:
        return {}
    placeholders = ",".join("?" for _ in user_ids)
    sel_display = "display_name" if "display_name" in cols else "username" if "username" in cols else "NULL"
    sel_username = "username" if "username" in cols else "NULL"
    rows = _q(
        f"SELECT user_id, {sel_display}, {sel_username} FROM users WHERE user_id IN ({placeholders});",
        tuple(user_ids),
    )
    return {int(uid): (display_name, username) for uid, display_name, username in rows}


def _format_user_mention(user_id: int, profiles: dict[int, tuple[str | None, str | None]] | None = None) -> str:
    display = None
    username = None
    if profiles and user_id in profiles:
        display, username = profiles[user_id]
    elif _table_exists("users"):
        cols = set(_table_cols("users"))
        if "user_id" in cols:
            sel_display = "display_name" if "display_name" in cols else "username" if "username" in cols else "NULL"
            sel_username = "username" if "username" in cols else "NULL"
            order = " ORDER BY updated_at DESC" if "updated_at" in cols else ""
            rows = _q(
                f"SELECT {sel_display}, {sel_username} FROM users WHERE user_id=?{order} LIMIT 1;",
                (user_id,),
            )
        else:
            rows = []
        if rows:
            display, username = rows[0]
    name = (display or username or str(user_id)).strip() or str(user_id)
    return f'<a href="tg://user?id={user_id}">{escape(name)}</a>'


def delete_user_achievement(chat_id: int, user_id: int, ach_code: str) -> int:
    with closing(_conn()) as conn:
        conn.execute("PRAGMA foreign_keys=ON;")
        conn.execute("BEGIN")
        try:
            ach_row = conn.execute(
                "SELECT COALESCE(id,rowid) FROM achievements WHERE LOWER(code)=LOWER(?) LIMIT 1;",
                (ach_code,),
            ).fetchone()
            if not ach_row:
                conn.rollback()
                return 0
            ach_id = int(ach_row[0])
            total = 0
            total += _delete_optional_records(
                conn,
                "user_achievements",
                {
                    "chat_id": chat_id,
                    "user_id": user_id,
                    "achievement_id": ach_id,
                    "ach_id": ach_id,
                    "achievement_code": ach_code,
                    "ach_code": ach_code,
                    "code": ach_code,
                },
            )
            total += _remove_progress_records(
                conn,
                ach_code=ach_code,
                ach_id=ach_id,
                chat_id=chat_id,
                user_id=user_id,
            )
            conn.commit()
            return total
        except Exception:
            conn.rollback()
            raise


def global_reset_achievements() -> dict[str, int]:
    with closing(_conn()) as conn:
        conn.execute("PRAGMA foreign_keys=ON;")
        conn.execute("BEGIN")
        try:
            stats: dict[str, int] = {}
            stats["user_achievements"] = _truncate_table(conn, "user_achievements")
            stats["achievement_progress"] = _truncate_table(conn, "achievement_progress")
            for table in (
                "achievements_progress",
                "achievements_states",
                "achievement_states",
                "achievements_awards",
            ):
                stats[table] = _truncate_table(conn, table)
            stats["achievements"] = _truncate_table(conn, "achievements")
            conn.commit()
            return stats
        except Exception:
            conn.rollback()
            raise


def reset_user_achievement_progress(chat_id: int, user_id: int, ach_code: str) -> int:
    with closing(_conn()) as conn:
        conn.execute("PRAGMA foreign_keys=ON;")
        conn.execute("BEGIN")
        try:
            ach_row = conn.execute(
                "SELECT COALESCE(id,rowid) FROM achievements WHERE LOWER(code)=LOWER(?) LIMIT 1;",
                (ach_code,),
            ).fetchone()
            if not ach_row:
                conn.rollback()
                return 0
            ach_id = int(ach_row[0])
            removed = _remove_progress_records(
                conn,
                ach_code=ach_code,
                ach_id=ach_id,
                chat_id=chat_id,
                user_id=user_id,
            )
            conn.commit()
            return removed
        except Exception:
            conn.rollback()
            raise


def delete_achievement_globally(ach_code: str) -> int:
    with closing(_conn()) as conn:
        conn.execute("PRAGMA foreign_keys=ON;")
        conn.execute("BEGIN")
        try:
            ach_row = conn.execute(
                "SELECT COALESCE(id,rowid) FROM achievements WHERE LOWER(code)=LOWER(?) LIMIT 1;",
                (ach_code,),
            ).fetchone()
            if not ach_row:
                conn.rollback()
                return 0
            ach_id = int(ach_row[0])
            total = 0
            total += _delete_optional_records(
                conn,
                "user_achievements",
                {
                    "achievement_id": ach_id,
                    "ach_id": ach_id,
                    "achievement_code": ach_code,
                    "ach_code": ach_code,
                    "code": ach_code,
                },
            )
            total += _remove_progress_records(
                conn,
                ach_code=ach_code,
                ach_id=ach_id,
                chat_id=None,
                user_id=None,
            )
            total += _delete_optional_records(
                conn,
                "achievements",
                {"id": ach_id, "code": ach_code},
            )
            conn.commit()
            return total
        except Exception:
            conn.rollback()
            raise

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


def _rebuild_achievement_progress_if_needed():
    cols = _table_cols("achievement_progress")
    need_rebuild = not cols or any(
        c not in cols for c in ("chat_id", "user_id", "achievement_id", "progress", "updated_at")
    )
    with closing(_conn()) as c:
        if need_rebuild:
            c.execute(
                """
                CREATE TABLE IF NOT EXISTS achievement_progress_new (
                    chat_id INTEGER NOT NULL,
                    user_id INTEGER NOT NULL,
                    achievement_id INTEGER NOT NULL,
                    progress INTEGER NOT NULL DEFAULT 0,
                    updated_at INTEGER NOT NULL,
                    PRIMARY KEY(chat_id, user_id, achievement_id),
                    FOREIGN KEY(achievement_id) REFERENCES achievements(id) ON DELETE CASCADE
                );
                """
            )
            if cols:
                available = set(cols)
                try:
                    select_cols = []
                    for col in ("chat_id", "user_id", "achievement_id", "progress", "updated_at"):
                        if col in available:
                            select_cols.append(col)
                        elif col == "updated_at":
                            select_cols.append("strftime('%s','now')")
                        else:
                            select_cols.append("0")
                    c.execute(
                        """
                        INSERT OR IGNORE INTO achievement_progress_new
                        (chat_id, user_id, achievement_id, progress, updated_at)
                        SELECT {chat_id}, {user_id}, {achievement_id}, {progress}, {updated_at}
                        FROM achievement_progress;
                        """.format(
                            chat_id=select_cols[0],
                            user_id=select_cols[1],
                            achievement_id=select_cols[2],
                            progress=select_cols[3],
                            updated_at=select_cols[4],
                        )
                    )
                except sqlite3.OperationalError:
                    pass
                c.execute("DROP TABLE achievement_progress;")
            c.execute("ALTER TABLE achievement_progress_new RENAME TO achievement_progress;")
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
        c.execute("""
        CREATE TABLE IF NOT EXISTS achievement_progress (
            chat_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            achievement_id INTEGER NOT NULL,
            progress INTEGER NOT NULL DEFAULT 0,
            updated_at INTEGER NOT NULL,
            PRIMARY KEY(chat_id, user_id, achievement_id),
            FOREIGN KEY(achievement_id) REFERENCES achievements(id) ON DELETE CASCADE
        );
        """)
        c.commit()
    # миграции для любых старых схем
    _rebuild_achievements_if_needed()
    _rebuild_user_stats_if_needed()
    _rebuild_user_achievements_if_needed()
    _rebuild_achievement_progress_if_needed()

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

async def _announce(
    m: Message,
    code: str,
    title: str,
    description: str,
    rarity: float,
    level: int | None = None,
):
    if not m.from_user:
        return
    text = format_achievement_message(
        user_id=m.from_user.id,
        user_name=m.from_user.full_name,
        ach_title=title,
        level=level,
        description=description,
    )
    text = f"{text}\n<i>Редкость:</i> <b>{rarity}%</b>"
    await send_achievement_award(m.bot, m.chat.id, text)

def _user_has_tier(chat_id: int, user_id: int, ach_id: int, tier: int) -> bool:
    row = _q(
        "SELECT 1 FROM user_achievements WHERE chat_id=? AND user_id=? AND achievement_id=? AND tier=? LIMIT 1;",
        (chat_id, user_id, ach_id, tier)
    )
    return bool(row)

def _user_max_tier(chat_id: int, user_id: int, ach_id: int) -> int:
    row = _q(
        "SELECT COALESCE(MAX(tier), 0) FROM user_achievements WHERE chat_id=? AND user_id=? AND achievement_id=?;",
        (chat_id, user_id, ach_id)
    )
    return int(row[0][0] or 0)

def _next_tier_to_award(thresholds: list[int], total: int, current_tier: int) -> int | None:
    """
    Возвращает СЛЕДУЮЩИЙ (ровно +1) уровень, если его порог уже достигнут.
    Если текущий 0 и total >= thresholds[0] -> вернёт 1.
    Если текущий 1 и total >= thresholds[1] -> вернёт 2.
    И т.д. Иначе None.
    """
    if not thresholds:
        return None
    # индексы уровней: 1..N
    next_idx = current_tier + 1
    if 1 <= next_idx <= len(thresholds) and total >= thresholds[next_idx - 1]:
        return next_idx
    return None

def _unlock(chat_id: int, user_id: int, ach_id: int, tier: int):
    _exec(
        "INSERT INTO user_achievements(chat_id, user_id, achievement_id, tier, unlocked_at) VALUES(?,?,?,?,?)",
        (chat_id, user_id, ach_id, tier, _now_ts())
    )

# =========
# Публичный хук (вызывать после логирования сообщения)
# =========
async def on_text_hook(m: Message):
    """
    Вызывайте из вашего on_text сразу после логирования сообщения.
    Инкремент счётчиков и проверка триггеров.
    ВАЖНО: выдаётся только ОДИН следующий уровень за одно сообщение.
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

    text = (m.text or m.caption or "")

    for (aid, code, title, desc, kind, ctype, thresholds_json, target_ts, active, extra_json) in achs:
        # thresholds
        try:
            thresholds = sorted(set(map(int, json.loads(thresholds_json)))) if thresholds_json else []
        except Exception:
            thresholds = []

        # --- SINGLE-STEP режим: выдаём ровно один следующий уровень ---
        curr_tier = _user_max_tier(chat_id, user_id, aid)

        # messages
        if ctype == "messages":
            total = _increment_progress(chat_id, user_id, aid, 1)
            next_tier = _next_tier_to_award(thresholds, total, curr_tier) if kind == "tiered" else (1 if thresholds and total >= thresholds[0] and curr_tier == 0 else None)
            if next_tier:
                _unlock(chat_id, user_id, aid, next_tier)
                rarity = _calc_rarity(chat_id, aid)
                level = next_tier if kind == "tiered" else None
                await _announce(
                    m,
                    code,
                    title,
                    desc,
                    rarity,
                    level=level,
                )

        # date (разовая)
        elif ctype == "date" and target_ts:
            if curr_tier == 0 and _now_ts() >= int(target_ts):
                _unlock(chat_id, user_id, aid, 1)
                await _announce(
                    m,
                    code,
                    title,
                    desc,
                    _calc_rarity(chat_id, aid),
                )

        # keyword
        elif ctype == "keyword":
            kw = None
            try:
                kw = json.loads(extra_json).get("keyword") if extra_json else None
            except Exception:
                kw = None
            if not kw:
                continue

            # текущий меседж должен содержать ключевое слово (чтобы не выдавать «вхолостую»)
            contains_now = kw.lower() in (text or "").lower()
            if not contains_now:
                continue

            # «прогресс» — число сообщений пользователя с ключевым словом
            total = _increment_progress(chat_id, user_id, aid, 1)

            next_tier = _next_tier_to_award(thresholds, total, curr_tier) if kind == "tiered" else (1 if thresholds and total >= thresholds[0] and curr_tier == 0 else None)
            if next_tier:
                _unlock(chat_id, user_id, aid, next_tier)
                rarity = _calc_rarity(chat_id, aid)
                level = next_tier if kind == "tiered" else None
                await _announce(
                    m,
                    code,
                    title,
                    desc,
                    rarity,
                    level=level,
                )

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
    deleted = delete_achievement_globally(ach[1])
    await m.reply(
        "Удалено." if deleted else "Не найдено данных."
        + (f" Очищено записей: {deleted}." if deleted else "")
    )


@router.message(Command("ach_globalreset"))
async def cmd_ach_globalreset(m: Message):
    if not m.from_user or not is_admin(m.from_user.id):
        return await m.reply("Недостаточно прав.")
    stats = global_reset_achievements()
    lines = ["<b>Глобальный сброс выполнен.</b>"]
    label_map = {
        "achievements": "achievements",
        "user_achievements": "user_achievements",
        "achievement_progress": "achievement_progress",
        "achievements_progress": "achievements_progress",
        "achievements_states": "achievements_states",
        "achievement_states": "achievement_states",
        "achievements_awards": "achievements_awards",
    }
    for key in label_map:
        if key in stats:
            lines.append(f"{label_map[key]}: {stats[key]}")
    await m.reply(
        "\n".join(lines),
        disable_web_page_preview=True,
        parse_mode="HTML",
    )

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

    lines = [f"<b>Прогресс по</b> «{title}»:"]

    if ctype == "messages":
        rows = _q(
            """
            SELECT user_id, progress
            FROM achievement_progress
            WHERE achievement_id=? AND chat_id=?
            ORDER BY progress DESC, user_id ASC;
            """,
            (aid, m.chat.id),
        )
        if not rows:
            lines.append("Данных по прогрессу пока нет.")
        else:
            profiles = _fetch_user_profiles({int(uid) for uid, _ in rows[:50]})
            for uid, progress_val in rows[:50]:
                uid = int(uid)
                taken = _q(
                    """
                    SELECT MAX(tier) FROM user_achievements
                    WHERE chat_id=? AND user_id=? AND achievement_id=?;
                    """,
                    (m.chat.id, uid, aid),
                )[0][0] or 0
                next_thr = None
                for idx, threshold in enumerate(sorted(thresholds), start=1):
                    if progress_val < threshold:
                        next_thr = threshold
                        break
                status = (
                    f"все уровни ({taken}/{len(thresholds)})"
                    if next_thr is None and thresholds
                    else f"{progress_val} / {next_thr or '-'}"
                )
                mention = _format_user_mention(uid, profiles)
                lines.append(f"• {mention}: {status}")
    elif ctype == "keyword":
        try:
            kw = json.loads(extra_json).get("keyword") if extra_json else None
        except Exception:
            kw = None
        rows = _q(
            """
            SELECT user_id, progress
            FROM achievement_progress
            WHERE achievement_id=? AND chat_id=?
            ORDER BY progress DESC, user_id ASC;
            """,
            (aid, m.chat.id),
        )
        lines.append(f"Тип: keyword, слово: <code>{kw}</code>")
        if rows:
            profiles = _fetch_user_profiles({int(uid) for uid, _ in rows[:50]})
            for uid, progress_val in rows[:50]:
                mention = _format_user_mention(int(uid), profiles)
                lines.append(f"• {mention}: {progress_val}")
        else:
            lines.append("Данных по прогрессу пока нет.")
    else:
        lines.append("Тип: date — выдаётся автоматически после наступления даты при любой активности.")
    await m.reply("\n".join(lines), disable_web_page_preview=True)


@router.message(Command("ach_globalview"))
async def cmd_ach_globalview(m: Message):
    if not m.from_user or not is_admin(m.from_user.id):
        return await m.reply("Недостаточно прав.")
    achs = _q(
        """
        SELECT COALESCE(id,rowid) AS id, code, title, kind, condition_type
        FROM achievements
        ORDER BY id;
        """
    )
    if not achs:
        return await m.reply("Ачивок нет в базе.")
    blocks: list[str] = []
    for aid, code, title, kind, ctype in achs:
        block_lines = [
            f"<b>#{aid}</b> — <b>{escape(title)}</b> (<code>{escape(code)}</code>)",
            f"Тип: {kind}/{ctype}",
        ]
        progress_rows = _q(
            """
            SELECT chat_id, user_id, progress
            FROM achievement_progress
            WHERE achievement_id=?
            ORDER BY progress DESC, user_id ASC
            LIMIT 50;
            """,
            (aid,),
        )
        awards_rows = _q(
            """
            SELECT chat_id, user_id, MAX(tier) AS max_tier
            FROM user_achievements
            WHERE achievement_id=?
            GROUP BY chat_id, user_id;
            """,
            (aid,),
        )
        progress_map = {(int(chat), int(user)): int(progress) for chat, user, progress in progress_rows}
        awards_map = {(int(chat), int(user)): int(tier) for chat, user, tier in awards_rows}
        keys = set(progress_map.keys()) | set(awards_map.keys())
        if not keys:
            block_lines.append("  • записей не найдено")
        else:
            user_ids = {uid for _, uid in keys}
            profiles = _fetch_user_profiles(user_ids)
            for chat_user in sorted(
                keys,
                key=lambda cu: (-progress_map.get(cu, 0), cu[0], cu[1]),
            ):
                chat_id, user_id = chat_user
                mention = _format_user_mention(user_id, profiles)
                progress_val = progress_map.get(chat_user, 0)
                tier_val = awards_map.get(chat_user)
                tier_text = f", уровни: {tier_val}" if tier_val else ""
                block_lines.append(
                    f"  • чат <code>{chat_id}</code>: {mention} — прогресс: {progress_val}{tier_text}"
                )
            if len(keys) >= 50:
                block_lines.append("  • … (обрезано до 50 записей)")
        blocks.append("\n".join(block_lines))
    await m.reply(
        "\n\n".join(blocks),
        disable_web_page_preview=True,
        parse_mode="HTML",
    )


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
    progress_removed = reset_user_achievement_progress(m.chat.id, user_id, ach[1])
    awards_removed = delete_user_achievement(m.chat.id, user_id, ach[1])
    await m.reply(
        "Сброшено. "
        + f"Удалено наград: {awards_removed}. "
        + f"Записей прогресса очищено: {progress_removed}."
    )

# =========
# Команды: для всех
# =========
@router.message(Command(commands=["my_achievements", "my_ach"]))
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
    await m.reply(
        "\n".join(parts),
        disable_web_page_preview=True,
        parse_mode="HTML",
    )

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
    profiles = _fetch_user_profiles({uid for uid, _ in rows})
    for i, (uid, cnt) in enumerate(rows, start=1):
        mention = _format_user_mention(uid, profiles)
        lines.append(f"{i}. {mention} — {cnt}")
    await m.reply("\n".join(lines), parse_mode="HTML")
