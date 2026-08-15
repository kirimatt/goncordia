CREATE TABLE IF NOT EXISTS goncordia_leaders (
    name       TEXT PRIMARY KEY,
    worker_id  TEXT NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL
);
