# NiFi 1.28 + Postgres JDBC + Python (for the REST-API flow installer)
# We install the Postgres driver into NiFi's lib/ so the PutDatabaseRecord
# processor can connect to our quakenet `postgres` service out of the box.
FROM apache/nifi:1.28.1

USER root

#nstall python3 and curl/jq for the REST-API flow installer.
RUN apt-get update -y && apt-get install -y --no-install-recommends \
        python3 \
        python3-pip \
        python3-requests \
        curl \
        jq \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Postgres JDBC driver for PutDatabaseRecord onnifi
ARG PG_JDBC_VERSION=42.7.4
RUN curl -fsSL -o /opt/nifi/nifi-current/lib/postgresql.jar \
      "https://repo1.maven.org/maven2/org/postgresql/postgresql/${PG_JDBC_VERSION}/postgresql-${PG_JDBC_VERSION}.jar" \
    && chmod 0644 /opt/nifi/nifi-current/lib/postgresql.jar

#project flow installer + entrypoint wrapper.
COPY nifi/scripts/start.sh /opt/quakeflow/start.sh
COPY nifi/scripts/install_flow.py /opt/quakeflow/install_flow.py
RUN sed -i 's/\r$//' /opt/quakeflow/start.sh /opt/quakeflow/install_flow.py \
    && chmod +x /opt/quakeflow/start.sh

USER nifi

# Run our wrapper which 
#(1) spawns the flow installer in the background,
#2 hands off to the official NiFi entrypoint.
ENTRYPOINT ["/opt/quakeflow/start.sh"]
