FROM mysql:8.4@sha256:43bd9764df60666fb2ba2cf8217dd17b3d2414c150005d2fdd07a0cd73d3b7f5

USER root

RUN command -v openssl >/dev/null 2>&1 \
    || microdnf install -y openssl \
    || yum install -y openssl

RUN mkdir -p /tls /docker-entrypoint-initdb.d \
    && openssl genrsa -out /tls/ca.key 4096 \
    && openssl req -x509 -new -nodes -key /tls/ca.key -sha256 -days 3650 \
        -subj "/CN=Queron MySQL Test CA" \
        -out /tls/ca.crt \
    && openssl genrsa -out /tls/server.key 2048 \
    && openssl req -new -key /tls/server.key \
        -subj "/CN=localhost" \
        -out /tls/server.csr \
    && printf "subjectAltName=DNS:localhost,DNS:mysql-auth,IP:127.0.0.1\n" > /tls/server.ext \
    && openssl x509 -req -in /tls/server.csr -CA /tls/ca.crt -CAkey /tls/ca.key -CAcreateserial \
        -out /tls/server.crt -days 3650 -sha256 \
        -extfile /tls/server.ext \
    && openssl genrsa -out /tls/client.key 2048 \
    && openssl req -new -key /tls/client.key \
        -subj "/CN=mtls_user" \
        -out /tls/client.csr \
    && openssl x509 -req -in /tls/client.csr -CA /tls/ca.crt -CAkey /tls/ca.key -CAcreateserial \
        -out /tls/client.crt -days 3650 -sha256 \
    && chmod 600 /tls/server.key /tls/client.key /tls/ca.key \
    && chmod 644 /tls/server.crt /tls/client.crt /tls/ca.crt \
    && chown -R mysql:mysql /tls

COPY mysql-auth.cnf /etc/mysql/conf.d/mysql-auth.cnf
COPY initdb/10-auth-test.sql /docker-entrypoint-initdb.d/10-auth-test.sql
