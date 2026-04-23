FROM mcr.microsoft.com/mssql/server:2022-latest

USER root

RUN apt-get update \
    && ACCEPT_EULA=Y apt-get install -y --no-install-recommends curl gnupg2 apt-transport-https ca-certificates openssl unixodbc \
    && curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
    && curl https://packages.microsoft.com/config/ubuntu/22.04/prod.list > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y --no-install-recommends mssql-tools18 \
    && ln -s /opt/mssql-tools18/bin/sqlcmd /usr/local/bin/sqlcmd \
    && rm -rf /var/lib/apt/lists/*

COPY entrypoint.sh /usr/local/bin/queron-mssql-entrypoint.sh
COPY initdb/10-seed.sql /tmp/queron-mssql-init.sql

RUN chmod +x /usr/local/bin/queron-mssql-entrypoint.sh

ENV MSSQL_PID=Developer
ENV MSSQL_TCP_PORT=1433
ENV MSSQL_DATABASE=LOOMDB
ENV MSSQL_APP_USER=loom_user
ENV MSSQL_APP_PASSWORD=LoomSqlPass123!
ENV MSSQL_ENABLE_TLS=1
ENV MSSQL_TLS_HOST=localhost

ENTRYPOINT ["/usr/local/bin/queron-mssql-entrypoint.sh"]
