FROM mariadb:11.4@sha256:3b4dfcc32247eb07adbebec0793afae2a8eafa6860ec523ee56af4d3dec42f7f

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends openssl \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /tls /docker-entrypoint-initdb.d \
    && openssl genrsa -out /tls/ca.key 4096 \
    && openssl req -x509 -new -nodes -key /tls/ca.key -sha256 -days 3650 \
        -subj "/CN=Queron MariaDB Test CA" \
        -out /tls/ca.crt \
    && openssl genrsa -out /tls/server.key 2048 \
    && openssl req -new -key /tls/server.key \
        -subj "/CN=localhost" \
        -out /tls/server.csr \
    && printf "subjectAltName=DNS:localhost,DNS:mariadb-auth,IP:127.0.0.1\n" > /tls/server.ext \
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

COPY mariadb-auth.cnf /etc/mysql/conf.d/mariadb-auth.cnf
COPY initdb/10-auth-test.sql /docker-entrypoint-initdb.d/10-auth-test.sql
