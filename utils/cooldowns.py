import os
import sqlite3
from contextlib import closing
from datetime import datetime, timezone
from typing import Optional

DB = os.getenv("DB_PATH", "bot.sqlite3")


def _conn() -> sqlite3.Connection:
    return sqlite3.connect(DB)


def _now_ts() -> int:
    return int(datetime.now(timezone.utc).timestamp())


def _user_key(user_id: Optional[int]) -> int:
    return int(user_id) if user_id is not None else 0


def set_cooldown(scope: str, chat_id: int, user_id: Optional[int], ttl_sec: int) -> None:
    expires_at = _now_ts() + max(0, int(ttl_sec))
    with closing(_conn()) as conn:
        conn.execute(
            """
            INSERT INTO bot_cooldowns(scope, chat_id, user_id, user_key, expires_at)
            VALUES(?, ?, ?, ?, ?)
            ON CONFLICT(scope, chat_id, user_key) DO UPDATE SET expires_at=excluded.expires_at
            """,
            (scope, chat_id, user_id, _user_key(user_id), expires_at),
        )
        conn.commit()


def is_on_cooldown(scope: str, chat_id: int, user_id: Optional[int]) -> bool:
    now = _now_ts()
    with closing(_conn()) as conn:
        cur = conn.execute(
            """
            SELECT 1 FROM bot_cooldowns
            WHERE scope=? AND chat_id=? AND user_key=? AND expires_at > ?
            LIMIT 1;
            """,
            (scope, chat_id, _user_key(user_id), now),
        )
        return cur.fetchone() is not None


def clear_expired_cooldowns() -> int:
    now = _now_ts()
    with closing(_conn()) as conn:
        cur = conn.execute("DELETE FROM bot_cooldowns WHERE expires_at <= ?;", (now,))
        conn.commit()
        return cur.rowcount
