#!/usr/bin/env bash
set -euo pipefail

SA_PASSWORD="${MSSQL_SA_PASSWORD:-}"
if [[ -z "${SA_PASSWORD}" ]]; then
  echo "MSSQL_SA_PASSWORD is required." >&2
  exit 1
fi

MSSQL_DATABASE="${MSSQL_DATABASE:-LOOMDB}"
MSSQL_APP_USER="${MSSQL_APP_USER:-loom_user}"
MSSQL_APP_PASSWORD="${MSSQL_APP_PASSWORD:-LoomSqlPass123!}"
MSSQL_ENABLE_TLS="${MSSQL_ENABLE_TLS:-1}"
MSSQL_TLS_HOST="${MSSQL_TLS_HOST:-localhost}"

if [[ "${MSSQL_ENABLE_TLS}" == "1" ]]; then
  mkdir -p /var/opt/mssql/certs
  if [[ ! -f /var/opt/mssql/certs/server.key || ! -f /var/opt/mssql/certs/server.crt ]]; then
    openssl req -x509 -nodes -newkey rsa:2048 \
      -subj "/CN=${MSSQL_TLS_HOST}" \
      -keyout /var/opt/mssql/certs/server.key \
      -out /var/opt/mssql/certs/server.crt \
      -days 3650
    chmod 600 /var/opt/mssql/certs/server.key
    chmod 644 /var/opt/mssql/certs/server.crt
  fi
  /opt/mssql/bin/mssql-conf set network.tlscert /var/opt/mssql/certs/server.crt
  /opt/mssql/bin/mssql-conf set network.tlskey /var/opt/mssql/certs/server.key
  /opt/mssql/bin/mssql-conf set network.forceencryption 1
fi

/opt/mssql/bin/sqlservr &
MSSQL_PID_RUNTIME=$!

cleanup() {
  if kill -0 "${MSSQL_PID_RUNTIME}" >/dev/null 2>&1; then
    kill "${MSSQL_PID_RUNTIME}" >/dev/null 2>&1 || true
    wait "${MSSQL_PID_RUNTIME}" || true
  fi
}
trap cleanup EXIT

for _ in $(seq 1 60); do
  if sqlcmd -C -S localhost,"${MSSQL_TCP_PORT:-1433}" -U sa -P "${SA_PASSWORD}" -Q "SELECT 1" >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

sqlcmd -C -S localhost,"${MSSQL_TCP_PORT:-1433}" -U sa -P "${SA_PASSWORD}" -v \
  DB_NAME="${MSSQL_DATABASE}" \
  APP_USER="${MSSQL_APP_USER}" \
  APP_PASSWORD="${MSSQL_APP_PASSWORD}" \
  -i /tmp/queron-mssql-init.sql

wait "${MSSQL_PID_RUNTIME}"
