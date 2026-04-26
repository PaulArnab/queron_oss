# Oracle Auth Test Image

This image is for Queron Oracle connector development.

Base image:
- `gvenzl/oracle-free:23-slim-faststart`

Supported local test surface:
- basic username/password auth
- raw DSN auth
- full connect descriptor auth
- TNS alias with `config_dir`
- password-from-env config shape

Deferred:
- wallet/mTLS
- external/Kerberos auth
- proxy auth
- OCI IAM/token auth

## Build

```powershell
docker build -t queron/oracle-auth-test .\docker\oracle_auth
```

## Run

```powershell
docker run --name queron-oracle-auth-test `
  -e ORACLE_PASSWORD=RootOraclePass123! `
  -p 51521:1521 `
  queron/oracle-auth-test
```

Wait until logs say the database is ready.

```powershell
docker logs -f queron-oracle-auth-test
```

## Test DB

- service: `FREEPDB1`
- host DSN: `localhost:51521/FREEPDB1`
- TNS alias: `QUERON_ORACLE_LOCAL`
- table: `loom_user.policy`
- egress target table: `loom_user.policy_egress`

Users:

| Shape | User | Password |
| --- | --- | --- |
| basic host/port/service | `loom_user` | `LoomOraclePass123!` |
| raw DSN / URI-style config | `uri_user` | `UriOraclePass123!` |
| env password | `env_user` | `EnvOraclePass123!` |

## Validate

Install the native driver if needed:

```powershell
python -m pip install oracledb
```

Run:

```powershell
python .\docker\oracle_auth\verify_oracle_auth.py
```

Expected output:

```text
basic: 3
uri-style-dsn: 3
descriptor: 3
tns-alias: 3
password-env-shape: 3
```

## Config Shapes

Basic:

```yaml
type: oracle
host: localhost
port: 51521
service_name: FREEPDB1
username: loom_user
password: LoomOraclePass123!
```

Raw DSN:

```yaml
type: oracle
dsn: localhost:51521/FREEPDB1
username: uri_user
password: UriOraclePass123!
```

Connect descriptor:

```yaml
type: oracle
dsn: (DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=localhost)(PORT=51521))(CONNECT_DATA=(SERVICE_NAME=FREEPDB1)))
username: loom_user
password: LoomOraclePass123!
```

TNS alias:

```yaml
type: oracle
tns_alias: QUERON_ORACLE_LOCAL
config_dir: C:\Queron_code\open_source\docker\oracle_auth\tns
username: loom_user
password: LoomOraclePass123!
```

Password from env:

```powershell
$env:QUERON_ORACLE_PASSWORD="EnvOraclePass123!"
```

```yaml
type: oracle
host: localhost
port: 51521
service_name: FREEPDB1
username: env_user
password_env: QUERON_ORACLE_PASSWORD
```
