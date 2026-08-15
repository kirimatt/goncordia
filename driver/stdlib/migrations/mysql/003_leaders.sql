CREATE TABLE IF NOT EXISTS goncordia_leaders (
    name       VARCHAR(255) PRIMARY KEY,
    worker_id  VARCHAR(255) NOT NULL,
    expires_at DATETIME(6) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
