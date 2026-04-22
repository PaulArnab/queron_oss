#!/bin/bash
set -euo pipefail

if [[ "${DB2_SSL_ENABLE:-true}" != "true" ]]; then
  echo "(*) DB2 SSL auth test setup disabled."
  exit 0
fi

DB2INSTANCE="${DB2INSTANCE:-db2inst1}"
INST_GROUP="${INST_GROUP:-db2iadm1}"
DBNAME="${DBNAME:-LOOMDB}"
DB2_SSL_PORT="${DB2_SSL_PORT:-50001}"
DB2_SSL_DIR="${DB2_SSL_DIR:-/database/config/ssl}"
DB2_SSL_PASSWORD="${DB2_SSL_PASSWORD:-LoomDb2Ssl123!}"
DB2_SSL_SERVER_LABEL="${DB2_SSL_SERVER_LABEL:-db2-server}"
DB2_SSL_CLIENT_LABEL="${DB2_SSL_CLIENT_LABEL:-db2-client}"

GSK="/opt/ibm/db2/V11.5/gskit/bin/gsk8capicmd_64"
SERVER_KEYDB="${DB2_SSL_DIR}/server.kdb"
SERVER_STH="${DB2_SSL_DIR}/server.sth"
SERVER_ARM="${DB2_SSL_DIR}/server.arm"
CLIENT_KEYDB="${DB2_SSL_DIR}/client.kdb"
CLIENT_STH="${DB2_SSL_DIR}/client.sth"
CLIENT_ARM="${DB2_SSL_DIR}/client.arm"
CLIENT_INI="${DB2_SSL_DIR}/db2cli.ini"

echo "(*) Configuring DB2 TLS test material in ${DB2_SSL_DIR}"
mkdir -p "${DB2_SSL_DIR}"
chown -R "${DB2INSTANCE}:${INST_GROUP}" "${DB2_SSL_DIR}"

su - "${DB2INSTANCE}" -c "mkdir -p '${DB2_SSL_DIR}'"

if [[ ! -f "${SERVER_KEYDB}" ]]; then
  su - "${DB2INSTANCE}" -c "\"${GSK}\" -keydb -create -db '${SERVER_KEYDB}' -pw '${DB2_SSL_PASSWORD}' -stash"
  su - "${DB2INSTANCE}" -c "\"${GSK}\" -cert -create -db '${SERVER_KEYDB}' -pw '${DB2_SSL_PASSWORD}' -label '${DB2_SSL_SERVER_LABEL}' -dn 'CN=localhost,O=Queron,OU=DB2 Auth Test,L=Local,ST=Local,C=US' -default_cert yes"
  su - "${DB2INSTANCE}" -c "\"${GSK}\" -cert -extract -db '${SERVER_KEYDB}' -pw '${DB2_SSL_PASSWORD}' -label '${DB2_SSL_SERVER_LABEL}' -target '${SERVER_ARM}' -format ascii"
fi

if [[ ! -f "${CLIENT_KEYDB}" ]]; then
  su - "${DB2INSTANCE}" -c "\"${GSK}\" -keydb -create -db '${CLIENT_KEYDB}' -pw '${DB2_SSL_PASSWORD}' -stash"
  su - "${DB2INSTANCE}" -c "\"${GSK}\" -cert -create -db '${CLIENT_KEYDB}' -pw '${DB2_SSL_PASSWORD}' -label '${DB2_SSL_CLIENT_LABEL}' -dn 'CN=db2client,O=Queron,OU=DB2 Auth Test,L=Local,ST=Local,C=US' -default_cert yes"
  su - "${DB2INSTANCE}" -c "\"${GSK}\" -cert -extract -db '${CLIENT_KEYDB}' -pw '${DB2_SSL_PASSWORD}' -label '${DB2_SSL_CLIENT_LABEL}' -target '${CLIENT_ARM}' -format ascii"
fi

su - "${DB2INSTANCE}" -c "\"${GSK}\" -cert -add -db '${CLIENT_KEYDB}' -pw '${DB2_SSL_PASSWORD}' -label '${DB2_SSL_SERVER_LABEL}' -file '${SERVER_ARM}' -format ascii" || true
su - "${DB2INSTANCE}" -c "\"${GSK}\" -cert -add -db '${SERVER_KEYDB}' -pw '${DB2_SSL_PASSWORD}' -label '${DB2_SSL_CLIENT_LABEL}' -file '${CLIENT_ARM}' -format ascii" || true

cat > "${CLIENT_INI}" <<EOF
[LOOMDB_SSL]
Database=${DBNAME}
Protocol=TCPIP
Hostname=localhost
ServiceName=${DB2_SSL_PORT}
Security=SSL
SSLServerCertificate=${SERVER_ARM}

[LOOMDB_CERT]
Database=${DBNAME}
Protocol=TCPIP
Hostname=localhost
ServiceName=${DB2_SSL_PORT}
Security=SSL
Authentication=CERTIFICATE
SSLServerCertificate=${SERVER_ARM}
SSLClientKeystoredb=${CLIENT_KEYDB}
SSLClientKeystash=${CLIENT_STH}
SSLClientLabel=${DB2_SSL_CLIENT_LABEL}
EOF

chown "${DB2INSTANCE}:${INST_GROUP}" "${CLIENT_INI}"

su - "${DB2INSTANCE}" -c ". sqllib/db2profile; db2 update dbm cfg using SSL_SVR_KEYDB '${SERVER_KEYDB}'"
su - "${DB2INSTANCE}" -c ". sqllib/db2profile; db2 update dbm cfg using SSL_SVR_STASH '${SERVER_STH}'"
su - "${DB2INSTANCE}" -c ". sqllib/db2profile; db2 update dbm cfg using SSL_SVR_LABEL '${DB2_SSL_SERVER_LABEL}'"
su - "${DB2INSTANCE}" -c ". sqllib/db2profile; db2 update dbm cfg using SSL_SVCENAME '${DB2_SSL_PORT}'"
su - "${DB2INSTANCE}" -c ". sqllib/db2profile; db2set DB2COMM=SSL,TCPIP"
su - "${DB2INSTANCE}" -c ". sqllib/db2profile; db2stop force"
su - "${DB2INSTANCE}" -c ". sqllib/db2profile; db2start"

echo "(*) DB2 TLS auth test setup complete."
echo "(*) TLS port: ${DB2_SSL_PORT}"
echo "(*) Server certificate: ${SERVER_ARM}"
echo "(*) Client keystore: ${CLIENT_KEYDB}"
