#!/usr/bin/env bash
set -euo pipefail

# =============================================================================
# Submit Flink SQL job: Kafka -> Iceberg streaming pipeline
# =============================================================================
# This script waits for dependencies to be reachable, then submits the
# create_tables.sql and insert_jobs.sql files via Flink SQL Client.
# =============================================================================

SQL_DIR="/opt/flink/sql"

# -----------------------------------------------------------------------------
# Wait for a TCP endpoint to become reachable
# -----------------------------------------------------------------------------
wait_for() {
    local name="$1" host="$2" port="$3"
    echo "Waiting for ${name} at ${host}:${port}..."
    while ! bash -c "echo > /dev/tcp/${host}/${port}" 2>/dev/null; do
        sleep 2
    done
    echo "  ${name} is reachable."
}

# -----------------------------------------------------------------------------
# Wait for all dependencies
# -----------------------------------------------------------------------------
wait_for "Kafka"        kafka        9092
wait_for "Iceberg REST" iceberg-rest 8181
wait_for "Flink JM"     localhost    8081

echo ""
echo "All dependencies are reachable. Submitting Flink SQL job..."
echo ""

# -----------------------------------------------------------------------------
# Submit SQL via embedded SQL Client
# -----------------------------------------------------------------------------
cat "${SQL_DIR}/create_tables.sql" "${SQL_DIR}/insert_jobs.sql" \
    | /opt/flink/bin/sql-client.sh embedded

echo ""
echo "Flink SQL job submitted successfully."
