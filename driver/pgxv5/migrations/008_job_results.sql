CREATE TABLE IF NOT EXISTS goncordia_job_results (
    job_id     BIGINT      PRIMARY KEY REFERENCES goncordia_jobs (id) ON DELETE CASCADE,
    value      BYTEA       NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);
