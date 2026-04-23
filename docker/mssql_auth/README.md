# MSSQL Auth Test Image

This image is meant for Queron MSSQL connector testing.

Supported test surface from this image:
- SQL auth (`basic`)
- TLS with server encryption enabled
- TLS with trust-server-certificate client behavior

Not realistically testable in this Linux container:
- Windows SSO / Integrated Authentication

## Build

```powershell
docker build -t queron/mssql-auth-test .\docker\mssql_auth
```

## Run

```powershell
docker run --name queron-mssql-auth-test `
  -e ACCEPT_EULA=Y `
  -e MSSQL_SA_PASSWORD=LoomSqlSaPass123! `
  -e MSSQL_DATABASE=LOOMDB `
  -e MSSQL_APP_USER=loom_user `
  -e MSSQL_APP_PASSWORD=LoomSqlPass123! `
  -p 51433:1433 `
  queron/mssql-auth-test
```

## Test DB

- database: `LOOMDB`
- SQL login: `loom_user`
- password: `LoomSqlPass123!`
- table: `dbo.policy`

## Notes

- The container forces TLS on the SQL Server listener.
- For local connector tests, client-side `trust_server_certificate=true` is expected.
