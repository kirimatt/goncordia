ALTER TABLE goncordia_jobs ADD COLUMN IF NOT EXISTS started_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_goncordia_jobs_ready_order
    ON goncordia_jobs (queue, state, priority DESC, run_at, created_at, id);
