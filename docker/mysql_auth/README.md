# MySQL Auth Test Image

This image is for Queron MySQL connector development.

Base image:
- `mysql:8.4@sha256:43bd9764df60666fb2ba2cf8217dd17b3d2414c150005d2fdd07a0cd73d3b7f5`
- Current resolved version when added: MySQL 8.4.9

Supported test surface:
- basic username/password auth
- full URI auth
- password-from-env config shape
- TLS-required user auth
- client-certificate auth
- local Unix socket transport inside Linux/container environments

## Build

```powershell
docker build -t queron/mysql-auth-test .\docker\mysql_auth
```

## Run

```powershell
docker run --name queron-mysql-auth-test `
  -e MYSQL_ROOT_PASSWORD=RootMysqlPass123! `
  -e MYSQL_DATABASE=LOOMDB `
  -p 53307:3306 `
  queron/mysql-auth-test
```

## Test DB

- database: `LOOMDB`
- table: `policy`
- egress target table: `policy_egress`

Users:

| Shape | User | Password |
| --- | --- | --- |
| basic host/port | `loom_user` | `LoomMysqlPass123!` |
| URI | `uri_user` | `UriMysqlPass123!` |
| env password | `env_user` | `EnvMysqlPass123!` |
| TLS required | `tls_user` | `TlsMysqlPass123!` |
| mTLS/client cert | `mtls_user` | `MtlsMysqlPass123!` |
| Unix socket transport | `socket_user` | `SocketMysqlPass123!` |

## Export Certs

```powershell
docker cp queron-mysql-auth-test:/tls/ca.crt .\mysql-ca.crt
docker cp queron-mysql-auth-test:/tls/client.crt .\mysql-client.crt
docker cp queron-mysql-auth-test:/tls/client.key .\mysql-client.key
```

## Config Shapes

Basic:

```yaml
type: mysql
host: localhost
port: 53307
database: LOOMDB
username: loom_user
password: LoomMysqlPass123!
```

URI:

```yaml
type: mysql
uri: mysql://uri_user:UriMysqlPass123!@localhost:53307/LOOMDB
```

Password from env:

```powershell
$env:QUERON_MYSQL_PASSWORD="EnvMysqlPass123!"
```

```yaml
type: mysql
host: localhost
port: 53307
database: LOOMDB
username: env_user
password_env: QUERON_MYSQL_PASSWORD
```

TLS:

```yaml
type: mysql
host: localhost
port: 53307
database: LOOMDB
username: tls_user
password: TlsMysqlPass123!
ssl_ca: C:\path\to\mysql-ca.crt
```

mTLS:

```yaml
type: mysql
host: localhost
port: 53307
database: LOOMDB
username: mtls_user
password: MtlsMysqlPass123!
ssl_ca: C:\path\to\mysql-ca.crt
ssl_cert: C:\path\to\mysql-client.crt
ssl_key: C:\path\to\mysql-client.key
```

Unix socket transport is mainly for Linux/container-side tests where the client can access the MySQL socket.
