# DB2 Auth Test Image

Separate DB2 image for Queron auth testing.

Purpose:
- keep the existing `loom-db2` container untouched
- add a second SSL listener on port `50001`
- generate GSKit server and client material for later DB2 TLS and certificate-auth tests

## Phases

1. Image and DB2 SSL bootstrap
- done here
- DB2 keeps normal TCP on `50000`
- DB2 SSL is enabled on `50001`

2. Queron DB2 connector auth fields
- add explicit `basic` / `tls` / `mtls or certificate` config shape

3. Queron DB2 connection-string builder
- translate Queron auth config into DB2 CLI keywords

4. Runtime verification
- test one-way TLS
- test certificate auth if the Python driver path supports the needed CLI keywords

## Build

```powershell
docker build -t queron/db2-auth-test .\docker\db2_auth
```

## Run

```powershell
docker run --name queron-db2-auth-test `
  --privileged `
  --security-opt label=disable `
  -e LICENSE=accept `
  -e DB2INST1_PASSWORD=LoomDb2Pass123! `
  -e DBNAME=LOOMDB `
  -e TO_CREATE_SAMPLEDB=false `
  -p 50000:50000 `
  -p 50001:50001 `
  queron/db2-auth-test
```

## Generated SSL assets

Inside the container:
- `/database/config/ssl/server.arm`
- `/database/config/ssl/server.kdb`
- `/database/config/ssl/server.sth`
- `/database/config/ssl/client.kdb`
- `/database/config/ssl/client.sth`
- `/database/config/ssl/client.arm`
- `/database/config/ssl/db2cli.ini`

Copy them out:

```powershell
docker cp queron-db2-auth-test:/database/config/ssl .\db2_ssl
```

## Notes

- DB2 SSL server config uses:
  - `SSL_SVR_KEYDB`
  - `SSL_SVR_STASH`
  - `SSL_SVR_LABEL`
  - `SSL_SVCENAME`
  - `DB2COMM=SSL,TCPIP`
- `db2cli.ini` is generated as a reference file for later client testing.
- This image prepares certificate-auth artifacts, but Queron DB2 client auth is not wired yet.

## Queron direction

Planned connector auth modes:
- `basic`
- `tls`
- `certificate` or `mtls` depending on the DB2 driver capability we confirm
