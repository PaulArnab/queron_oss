CREATE DATABASE IF NOT EXISTS LOOMDB;

CREATE USER IF NOT EXISTS 'loom_user'@'%' IDENTIFIED BY 'LoomMariaPass123!';
CREATE USER IF NOT EXISTS 'uri_user'@'%' IDENTIFIED BY 'UriMariaPass123!';
CREATE USER IF NOT EXISTS 'env_user'@'%' IDENTIFIED BY 'EnvMariaPass123!';
CREATE USER IF NOT EXISTS 'tls_user'@'%' IDENTIFIED BY 'TlsMariaPass123!' REQUIRE SSL;
CREATE USER IF NOT EXISTS 'mtls_user'@'%' IDENTIFIED BY 'MtlsMariaPass123!' REQUIRE SUBJECT '/CN=mtls_user';
CREATE USER IF NOT EXISTS 'socket_user'@'localhost' IDENTIFIED VIA unix_socket;

GRANT ALL PRIVILEGES ON LOOMDB.* TO 'loom_user'@'%';
GRANT ALL PRIVILEGES ON LOOMDB.* TO 'uri_user'@'%';
GRANT ALL PRIVILEGES ON LOOMDB.* TO 'env_user'@'%';
GRANT ALL PRIVILEGES ON LOOMDB.* TO 'tls_user'@'%';
GRANT ALL PRIVILEGES ON LOOMDB.* TO 'mtls_user'@'%';
GRANT ALL PRIVILEGES ON LOOMDB.* TO 'socket_user'@'localhost';

USE LOOMDB;

CREATE TABLE IF NOT EXISTS policy (
    policy_id INT PRIMARY KEY,
    policy_number VARCHAR(32) NOT NULL,
    customer_name VARCHAR(120) NOT NULL,
    premium_amount DECIMAL(12, 2) NOT NULL,
    active_flag BOOLEAN NOT NULL,
    effective_date DATE NOT NULL,
    updated_at DATETIME NOT NULL
);

INSERT INTO policy (
    policy_id,
    policy_number,
    customer_name,
    premium_amount,
    active_flag,
    effective_date,
    updated_at
) VALUES
    (1, 'POL-MDB-001', 'Avery Stone', 1200.50, TRUE, '2026-01-01', '2026-04-01 10:15:00'),
    (2, 'POL-MDB-002', 'Morgan Lee', 875.25, TRUE, '2026-02-15', '2026-04-02 11:30:00'),
    (3, 'POL-MDB-003', 'Riley Chen', 430.00, FALSE, '2026-03-20', '2026-04-03 12:45:00')
ON DUPLICATE KEY UPDATE
    policy_number = VALUES(policy_number),
    customer_name = VALUES(customer_name),
    premium_amount = VALUES(premium_amount),
    active_flag = VALUES(active_flag),
    effective_date = VALUES(effective_date),
    updated_at = VALUES(updated_at);

CREATE TABLE IF NOT EXISTS policy_egress (
    policy_id INT PRIMARY KEY,
    policy_number VARCHAR(32) NOT NULL,
    premium_amount DECIMAL(12, 2) NOT NULL,
    loaded_at DATETIME NULL
);

FLUSH PRIVILEGES;

