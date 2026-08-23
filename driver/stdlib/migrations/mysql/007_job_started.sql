ALTER TABLE goncordia_jobs ADD COLUMN started_at DATETIME(6);

CREATE INDEX idx_goncordia_jobs_ready_order
    ON goncordia_jobs (queue, state, priority DESC, run_at, created_at, id);
