-- Retention reads expired results through this index.
CREATE INDEX idx_goncordia_job_results_updated ON goncordia_job_results (updated_at);
