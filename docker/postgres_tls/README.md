# Postgres TLS Test Image

Separate Postgres image for Queron TLS/mTLS testing.

Base image:
- `postgres@sha256:9b5bd946f3a507db72c55959700e517463e8d5dbb6f7eb30d920d5bcf6951431`

What it adds:
- server TLS enabled by default
- generated test CA, server cert, client cert
- optional client-cert enforcement with `POSTGRES_CLIENTCERT_MODE=require`
- bootstrap role: `mtls_user`

## Build

```powershell
docker build -t queron/postgres-tls-test .\docker\postgres_tls
```

## Run: TLS only

```powershell
docker run --name queron-postgres-tls `
  -e POSTGRES_PASSWORD=password123 `
  -e POSTGRES_DB=postgres `
  -p 55432:5432 `
  queron/postgres-tls-test
```

## Run: mTLS required

```powershell
docker run --name queron-postgres-mtls `
  -e POSTGRES_PASSWORD=password123 `
  -e POSTGRES_DB=postgres `
  -e POSTGRES_CLIENTCERT_MODE=require `
  -p 55433:5432 `
  queron/postgres-tls-test
```

## Export certs

```powershell
docker cp queron-postgres-tls:/tls/ca.crt .\ca.crt
docker cp queron-postgres-mtls:/tls/ca.crt .\ca.crt
docker cp queron-postgres-mtls:/tls/client.crt .\client.crt
docker cp queron-postgres-mtls:/tls/client.key .\client.key
```

## Queron examples

TLS:

```python
queron.PostgresBinding(
    host="localhost",
    port=55432,
    database="postgres",
    username="postgres",
    password="password123",
    auth_mode="tls",
    sslmode="verify-full",
    sslrootcert=r"C:\path\to\ca.crt",
)
```

mTLS:

```python
queron.PostgresBinding(
    host="localhost",
    port=55433,
    database="postgres",
    username="mtls_user",
    auth_mode="mtls",
    sslmode="verify-full",
    sslrootcert=r"C:\path\to\ca.crt",
    sslcert=r"C:\path\to\client.crt",
    sslkey=r"C:\path\to\client.key",
)
```
