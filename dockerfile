# ==========================
# Builder stage - download connectors
# ==========================
FROM flink:1.19-scala_2.12-java17 AS builder

USER root

# Install wget to download connectors
RUN apt-get update && \
    apt-get install -y --no-install-recommends wget && \
    mkdir -p /tmp/connectors && \
    wget -q -P /tmp/connectors/ \
        https://repo1.maven.org/maven2/com/ververica/flink-sql-connector-mysql-cdc/3.0.1/flink-sql-connector-mysql-cdc-3.0.1.jar \
        https://repo1.maven.org/maven2/com/ververica/flink-sql-connector-postgres-cdc/3.0.1/flink-sql-connector-postgres-cdc-3.0.1.jar \
        https://jdbc.postgresql.org/download/postgresql-42.7.4.jar \
        https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/3.2.0-1.19/flink-connector-jdbc-3.2.0-1.19.jar

# ==========================
# Final stage - ARM64
# ==========================
FROM flink:1.19-scala_2.12-java17

USER root

# Install Python dev tools, build tools, and full JDK for ARM64
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        python3 python3-pip python3-dev build-essential openjdk-17-jdk-headless wget && \
    rm -rf /var/lib/apt/lists/*

# Upgrade pip and install PyFlink

RUN pip3 install --no-cache-dir --upgrade pip setuptools wheel
RUN pip3 install --no-cache-dir apache-flink==1.19.0 pipreqs

# Copy connectors from builder stage
COPY --from=builder /tmp/connectors/*.jar /opt/flink/lib/

# Set working directory
WORKDIR /opt/flink/workspace

# Copy and install Python requirements if present
COPY --chown=flink:flink requirements.txt* ./
RUN if [ -f requirements.txt ]; then \
        pip3 install --no-cache-dir -r requirements.txt; \
    fi

# Copy application code
COPY --chown=flink:flink . .

# Switch back to flink user
USER flink

# Keep container running for interactive testing
CMD ["python3", "main.py"]
