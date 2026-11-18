-- Retention reads expired terminal jobs through this index.
CREATE INDEX IF NOT EXISTS idx_goncordia_jobs_finalized ON goncordia_jobs (state, finalized_at);
