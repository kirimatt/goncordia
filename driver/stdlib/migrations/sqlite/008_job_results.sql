CREATE TABLE IF NOT EXISTS goncordia_job_results (
    job_id     INTEGER  PRIMARY KEY,
    value      BLOB     NOT NULL,
    updated_at DATETIME NOT NULL
);
