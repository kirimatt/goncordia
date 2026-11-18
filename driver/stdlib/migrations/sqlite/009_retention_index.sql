-- Retention reads expired terminal jobs and results through these indexes.
CREATE INDEX IF NOT EXISTS idx_goncordia_jobs_finalized ON goncordia_jobs (state, finalized_at);
CREATE INDEX IF NOT EXISTS idx_goncordia_job_results_updated ON goncordia_job_results (updated_at);
