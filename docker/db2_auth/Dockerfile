FROM ibmcom/db2:latest

USER root

RUN mkdir -p /var/custom /opt/queron

COPY 10-configure-db2-tls.sh /var/custom/10-configure-db2-tls.sh
COPY README.md /opt/queron/db2_auth_README.md

RUN chmod 755 /var/custom/10-configure-db2-tls.sh

EXPOSE 50000 50001
