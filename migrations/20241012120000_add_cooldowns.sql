CREATE TABLE IF NOT EXISTS bot_cooldowns (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    scope TEXT NOT NULL,
    chat_id INTEGER NOT NULL,
    user_id INTEGER,
    user_key INTEGER NOT NULL,
    expires_at INTEGER NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS uniq_bot_cooldowns_scope_chat_user
    ON bot_cooldowns (scope, chat_id, user_key);

CREATE INDEX IF NOT EXISTS idx_bot_cooldowns_expires_at
    ON bot_cooldowns (expires_at);
