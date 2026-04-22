DO
$$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'mtls_user') THEN
        CREATE ROLE mtls_user LOGIN;
    END IF;
END
$$;
