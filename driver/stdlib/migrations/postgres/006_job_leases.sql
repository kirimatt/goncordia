ALTER TABLE goncordia_jobs
    ADD COLUMN IF NOT EXISTS lease_expires_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_goncordia_jobs_running_lease
    ON goncordia_jobs (queue, lease_expires_at)
    WHERE state = 'running';
