# MariaDB Auth Test Image

This image is for Queron MariaDB connector development.

Base image:
- `mariadb:11.4@sha256:3b4dfcc32247eb07adbebec0793afae2a8eafa6860ec523ee56af4d3dec42f7f`

Supported test surface:
- basic username/password auth
- full URI auth
- password-from-env config shape
- TLS-required user auth
- client-certificate auth
- local Unix socket auth inside Linux/container environments

## Build

```powershell
docker build -t queron/mariadb-auth-test .\docker\mariadb_auth
```

## Run

```powershell
docker run --name queron-mariadb-auth-test `
  -e MARIADB_ROOT_PASSWORD=RootMariaPass123! `
  -e MARIADB_DATABASE=LOOMDB `
  -p 53306:3306 `
  queron/mariadb-auth-test
```

## Test DB

- database: `LOOMDB`
- table: `policy`
- egress target table: `policy_egress`

Users:

| Shape | User | Password |
| --- | --- | --- |
| basic host/port | `loom_user` | `LoomMariaPass123!` |
| URI | `uri_user` | `UriMariaPass123!` |
| env password | `env_user` | `EnvMariaPass123!` |
| TLS required | `tls_user` | `TlsMariaPass123!` |
| mTLS/client cert | `mtls_user` | `MtlsMariaPass123!` |
| Unix socket | `socket_user` | none, socket plugin |

## Export certs

```powershell
docker cp queron-mariadb-auth-test:/tls/ca.crt .\mariadb-ca.crt
docker cp queron-mariadb-auth-test:/tls/client.crt .\mariadb-client.crt
docker cp queron-mariadb-auth-test:/tls/client.key .\mariadb-client.key
```

## Config Shapes

Basic:

```yaml
type: mariadb
host: localhost
port: 53306
database: LOOMDB
username: loom_user
password: LoomMariaPass123!
```

URI:

```yaml
type: mariadb
uri: mariadb://uri_user:UriMariaPass123!@localhost:53306/LOOMDB
```

Password from env:

```powershell
$env:QUERON_MARIADB_PASSWORD="EnvMariaPass123!"
```

```yaml
type: mariadb
host: localhost
port: 53306
database: LOOMDB
username: env_user
password_env: QUERON_MARIADB_PASSWORD
```

TLS:

```yaml
type: mariadb
host: localhost
port: 53306
database: LOOMDB
username: tls_user
password: TlsMariaPass123!
ssl_ca: C:\path\to\mariadb-ca.crt
```

mTLS:

```yaml
type: mariadb
host: localhost
port: 53306
database: LOOMDB
username: mtls_user
password: MtlsMariaPass123!
ssl_ca: C:\path\to\mariadb-ca.crt
ssl_cert: C:\path\to\mariadb-client.crt
ssl_key: C:\path\to\mariadb-client.key
```

Unix socket auth is mainly for Linux/container-side tests where the client can access the MariaDB socket.
