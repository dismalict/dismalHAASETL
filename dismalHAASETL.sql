CREATE TABLE alerting.cnc_alerts (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    machine_name VARCHAR(50) NOT NULL,
    timestamp DATETIME NOT NULL,
    mode VARCHAR(50),
    run_status VARCHAR(50),
    program VARCHAR(255),
    m30counter1 INT,
    active_alarms TEXT,
    emergency_stop VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY unique_machine_timestamp (machine_name, timestamp)
);
