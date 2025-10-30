CREATE TABLE IF NOT EXISTS user_metrics (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    chat_id INTEGER NOT NULL,
    user_id INTEGER NOT NULL,
    metric TEXT NOT NULL,
    count INTEGER NOT NULL DEFAULT 0,
    updated_at INTEGER NOT NULL DEFAULT (strftime('%s','now')),
    UNIQUE(chat_id, user_id, metric)
);
CREATE INDEX IF NOT EXISTS idx_user_metrics_metric ON user_metrics(metric);
