DROP INDEX IF EXISTS goncordia_jobs_unique_key;

CREATE UNIQUE INDEX goncordia_jobs_unique_key
    ON goncordia_jobs (unique_key)
    WHERE unique_key IS NOT NULL;
