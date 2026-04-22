#!/bin/bash
set -euo pipefail

CLIENTCERT_MODE="${POSTGRES_CLIENTCERT_MODE:-off}"
if [[ "$CLIENTCERT_MODE" != "off" && "$CLIENTCERT_MODE" != "require" ]]; then
  echo "Unsupported POSTGRES_CLIENTCERT_MODE: $CLIENTCERT_MODE" >&2
  exit 1
fi

mkdir -p /var/lib/postgresql/generated
PG_HBA_PATH="/var/lib/postgresql/generated/pg_hba.conf"
cat > "$PG_HBA_PATH" <<'EOF'
local   all             all                                     trust
host    all             all             127.0.0.1/32            scram-sha-256
host    all             all             ::1/128                 scram-sha-256
EOF

if [[ "$CLIENTCERT_MODE" == "require" ]]; then
  cat >> "$PG_HBA_PATH" <<'EOF'
hostnossl all           all             0.0.0.0/0               reject
hostssl   all           all             0.0.0.0/0               cert clientcert=verify-full
hostssl   all           all             ::0/0                   cert clientcert=verify-full
EOF
else
  cat >> "$PG_HBA_PATH" <<'EOF'
host     all            all             0.0.0.0/0               scram-sha-256
host     all            all             ::0/0                   scram-sha-256
EOF
fi

exec /usr/local/bin/docker-entrypoint.sh "$@" \
  -c ssl=on \
  -c ssl_cert_file=/tls/server.crt \
  -c ssl_key_file=/tls/server.key \
  -c ssl_ca_file=/tls/ca.crt \
  -c hba_file="$PG_HBA_PATH"
