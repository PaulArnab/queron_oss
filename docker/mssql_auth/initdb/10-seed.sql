IF DB_ID('$(DB_NAME)') IS NULL
BEGIN
    EXEC ('CREATE DATABASE [$(DB_NAME)]');
END;
GO

USE [$(DB_NAME)];
GO

IF NOT EXISTS (SELECT 1 FROM sys.sql_logins WHERE name = '$(APP_USER)')
BEGIN
    EXEC ('CREATE LOGIN [$(APP_USER)] WITH PASSWORD = ''$(APP_PASSWORD)'', CHECK_POLICY = OFF');
END;
GO

IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = '$(APP_USER)')
BEGIN
    EXEC ('CREATE USER [$(APP_USER)] FOR LOGIN [$(APP_USER)]');
END;
GO

ALTER ROLE db_owner ADD MEMBER [$(APP_USER)];
GO

IF OBJECT_ID('dbo.policy', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.policy (
        policy_id INT NOT NULL PRIMARY KEY,
        policy_number NVARCHAR(32) NOT NULL,
        product_line NVARCHAR(32) NOT NULL,
        written_premium DECIMAL(18,2) NOT NULL,
        risk_state NVARCHAR(8) NOT NULL
    );
END;
GO

IF NOT EXISTS (SELECT 1 FROM dbo.policy)
BEGIN
    INSERT INTO dbo.policy (policy_id, policy_number, product_line, written_premium, risk_state)
    VALUES
        (1, 'POL-1001', 'AUTO', 1250.00, 'TX'),
        (2, 'POL-1002', 'HOME', 3425.50, 'CA'),
        (3, 'POL-1003', 'AUTO', 890.25, 'WA');
END;
GO
