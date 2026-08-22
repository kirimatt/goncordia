ALTER TABLE goncordia_jobs
    ADD COLUMN lease_expires_at DATETIME(6) NULL;

CREATE INDEX idx_goncordia_jobs_running_lease
    ON goncordia_jobs (queue, state, lease_expires_at);
