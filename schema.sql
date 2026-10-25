CREATE TABLE IF NOT EXISTS snapshots (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 collector_id TEXT NOT NULL,
 collected_at TEXT NOT NULL,
 received_at TEXT NOT NULL,
 payload TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS snapshots_collector ON snapshots(collector_id,id DESC);
CREATE INDEX IF NOT EXISTS snapshots_received ON snapshots(received_at);
