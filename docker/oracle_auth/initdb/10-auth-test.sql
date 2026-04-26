ALTER SESSION SET CONTAINER=FREEPDB1;

CREATE USER loom_user IDENTIFIED BY "LoomOraclePass123!" QUOTA UNLIMITED ON USERS;
CREATE USER uri_user IDENTIFIED BY "UriOraclePass123!" QUOTA UNLIMITED ON USERS;
CREATE USER env_user IDENTIFIED BY "EnvOraclePass123!" QUOTA UNLIMITED ON USERS;

GRANT CONNECT, RESOURCE TO loom_user;
GRANT CONNECT, RESOURCE TO uri_user;
GRANT CONNECT, RESOURCE TO env_user;

GRANT CREATE SESSION, CREATE TABLE, CREATE VIEW, CREATE SEQUENCE, CREATE PROCEDURE TO loom_user;
GRANT CREATE SESSION, CREATE TABLE, CREATE VIEW, CREATE SEQUENCE, CREATE PROCEDURE TO uri_user;
GRANT CREATE SESSION, CREATE TABLE, CREATE VIEW, CREATE SEQUENCE, CREATE PROCEDURE TO env_user;

CREATE TABLE loom_user.policy (
    policy_id NUMBER(10) PRIMARY KEY,
    policy_number VARCHAR2(32) NOT NULL,
    customer_name VARCHAR2(120) NOT NULL,
    premium_amount NUMBER(12, 2) NOT NULL,
    active_flag NUMBER(1) NOT NULL,
    effective_date DATE NOT NULL,
    updated_at TIMESTAMP NOT NULL
);

INSERT INTO loom_user.policy (
    policy_id,
    policy_number,
    customer_name,
    premium_amount,
    active_flag,
    effective_date,
    updated_at
) VALUES (
    1,
    'POL-ORA-001',
    'Avery Stone',
    1200.50,
    1,
    DATE '2026-01-01',
    TIMESTAMP '2026-04-01 10:15:00'
);

INSERT INTO loom_user.policy (
    policy_id,
    policy_number,
    customer_name,
    premium_amount,
    active_flag,
    effective_date,
    updated_at
) VALUES (
    2,
    'POL-ORA-002',
    'Morgan Lee',
    875.25,
    1,
    DATE '2026-02-15',
    TIMESTAMP '2026-04-02 11:30:00'
);

INSERT INTO loom_user.policy (
    policy_id,
    policy_number,
    customer_name,
    premium_amount,
    active_flag,
    effective_date,
    updated_at
) VALUES (
    3,
    'POL-ORA-003',
    'Riley Chen',
    430.00,
    0,
    DATE '2026-03-20',
    TIMESTAMP '2026-04-03 12:45:00'
);

CREATE TABLE loom_user.policy_egress (
    policy_id NUMBER(10) PRIMARY KEY,
    policy_number VARCHAR2(32) NOT NULL,
    premium_amount NUMBER(12, 2) NOT NULL,
    loaded_at TIMESTAMP NULL
);

GRANT SELECT, INSERT, UPDATE, DELETE ON loom_user.policy TO uri_user;
GRANT SELECT, INSERT, UPDATE, DELETE ON loom_user.policy_egress TO uri_user;
GRANT SELECT, INSERT, UPDATE, DELETE ON loom_user.policy TO env_user;
GRANT SELECT, INSERT, UPDATE, DELETE ON loom_user.policy_egress TO env_user;

COMMIT;
