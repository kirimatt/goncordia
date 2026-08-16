CREATE TABLE IF NOT EXISTS goncordia_schedule_cursors (
    id        TEXT PRIMARY KEY,
    cursor_at TIMESTAMPTZ NOT NULL
);
