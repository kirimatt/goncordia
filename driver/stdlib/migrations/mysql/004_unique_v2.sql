ALTER TABLE goncordia_jobs DROP INDEX goncordia_jobs_unique_key;
CREATE UNIQUE INDEX goncordia_jobs_unique_key ON goncordia_jobs (unique_key);
