CREATE TABLE IF NOT EXISTS goncordia_job_results (
    job_id     BIGINT      NOT NULL PRIMARY KEY,
    value      LONGBLOB    NOT NULL,
    updated_at DATETIME(6) NOT NULL
);
