#!/usr/bin/env bash
# =============================================================================
# clear-data.sh — Purge all data and restart Flink jobs from a clean slate
#
# Stops Flink jobs, deletes and recreates Kafka topics, wipes the MinIO
# warehouse (Iceberg data files, checkpoints, savepoints), resets the Iceberg
# catalog, and restarts Flink jobs with no prior state.
#
# Usage: clear-data.sh <application|session>
# =============================================================================

set -euo pipefail

FLINK_MODE="${1:-}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
NAMESPACE="flink"

case "$FLINK_MODE" in
  application|session) ;;
  *)
    cat <<'USAGE'
Usage: clear-data.sh <application|session>

Purge all data and restart Flink jobs from a clean slate:
  1. Stop Flink jobs
  2. Delete and recreate Kafka topics
  3. Wipe MinIO warehouse (data files, checkpoints, savepoints)
  4. Reset Iceberg catalog and recreate tables
  5. Restart Flink jobs (stateless — no savepoint recovery)

NOTE: The data generator is NOT stopped. If it is running, new events will
      flow into the recreated topics immediately.
USAGE
    exit 1
    ;;
esac

TOPICS=(bid-requests bid-responses impressions clicks)

# -------------------------------------------------------------------------
# [1/5] Stop Flink jobs
# -------------------------------------------------------------------------
echo "==> [1/5] Stopping Flink jobs..."

if [[ "$FLINK_MODE" == "application" ]]; then
  for name in flink-ingestion flink-aggregation flink-funnel; do
    echo "    Deleting FlinkDeployment '$name'..."
    kubectl -n "$NAMESPACE" delete flinkdeployment "$name" --ignore-not-found --timeout=60s
  done
  echo "    Waiting for Flink pods to terminate..."
  kubectl -n "$NAMESPACE" wait --for=delete pod -l component=taskmanager --timeout=120s 2>/dev/null || true
  kubectl -n "$NAMESPACE" wait --for=delete pod -l component=jobmanager --timeout=120s 2>/dev/null || true
else
  SESSION_POD=$(kubectl -n "$NAMESPACE" get pods \
    -l "app=flink-session,component=jobmanager" \
    --field-selector=status.phase=Running \
    -o jsonpath='{.items[0].metadata.name}' 2>/dev/null) || true

  if [[ -n "$SESSION_POD" ]]; then
    JOB_IDS=$(kubectl -n "$NAMESPACE" exec "$SESSION_POD" -- \
      /opt/flink/bin/flink list -r 2>/dev/null | grep -oE '[a-f0-9]{32}') || true
    for jid in $JOB_IDS; do
      kubectl -n "$NAMESPACE" exec "$SESSION_POD" -- \
        wget -qO /dev/null --method=PATCH "http://localhost:8081/jobs/$jid?mode=cancel" 2>/dev/null || true
      echo "    Cancelled job $jid"
    done
  else
    echo "    No running session cluster found — skipping job cancellation."
  fi
fi

# -------------------------------------------------------------------------
# [2/5] Purge Kafka topics
# -------------------------------------------------------------------------
echo "==> [2/5] Purging Kafka topics..."

for topic in "${TOPICS[@]}"; do
  docker exec kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 --delete --topic "$topic" 2>/dev/null || true
done
echo "    Deleted topics: ${TOPICS[*]}"

for topic in "${TOPICS[@]}"; do
  docker exec kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 --create --topic "$topic" \
    --partitions 3 --replication-factor 1 --if-not-exists
done
echo "    Recreated topics: ${TOPICS[*]}"

# -------------------------------------------------------------------------
# [3/5] Wipe MinIO warehouse data
# -------------------------------------------------------------------------
echo "==> [3/5] Wiping MinIO warehouse data..."

docker exec minio mc alias set local http://localhost:9000 admin password 2>/dev/null
docker exec minio mc rm --recursive --force local/warehouse/ 2>/dev/null || true
docker exec minio mc mb --ignore-existing local/warehouse
echo "    MinIO warehouse bucket cleared."

# -------------------------------------------------------------------------
# [4/5] Reset Iceberg catalog and recreate tables
# -------------------------------------------------------------------------
echo "==> [4/5] Resetting Iceberg catalog..."

# The REST catalog stores table metadata in SQLite inside the container
# (/tmp/iceberg_rest.db) with no volume mount. Force-recreating the
# container resets the database to a clean state.
docker compose up -d --force-recreate iceberg-rest

echo "    Waiting for Iceberg REST catalog..."
max_attempts=12
attempt=0
while [ $attempt -lt $max_attempts ]; do
  attempt=$((attempt + 1))
  if curl -sf "http://localhost:8181/v1/config" > /dev/null 2>&1; then
    echo "    Iceberg REST catalog is ready."
    break
  fi
  if [ $attempt -eq $max_attempts ]; then
    echo "    ERROR: Iceberg REST catalog not ready after ${max_attempts} attempts."
    exit 1
  fi
  sleep 5
done

echo "    Recreating Iceberg tables..."
VENV_PYTHON="$SCRIPT_DIR/../.venv/bin/python"
if [ ! -x "$VENV_PYTHON" ]; then
  echo "    Virtual environment not found. Creating .venv..."
  python3 -m venv "$SCRIPT_DIR/../.venv"
  "$VENV_PYTHON" -m pip install --quiet -r "$SCRIPT_DIR/../iceberg/requirements.txt"
fi
"$VENV_PYTHON" "$SCRIPT_DIR/../iceberg/apply_tables.py"

# -------------------------------------------------------------------------
# [5/5] Restart Flink jobs
# -------------------------------------------------------------------------
echo "==> [5/5] Restarting Flink jobs ($FLINK_MODE mode)..."

if [[ "$FLINK_MODE" == "application" ]]; then
  OVERLAY_DIR="$PROJECT_ROOT/k8s/flink/overlays/application-mode"
  kubectl apply -f "$OVERLAY_DIR/flink-ingestion.yaml"
  kubectl apply -f "$OVERLAY_DIR/flink-aggregation.yaml"
  kubectl apply -f "$OVERLAY_DIR/flink-funnel.yaml"

  echo "    Waiting for Flink pods..."
  kubectl wait --for=condition=Ready pod -l app=flink-ingestion -n "$NAMESPACE" --timeout=180s 2>/dev/null || true
  kubectl wait --for=condition=Ready pod -l app=flink-aggregation -n "$NAMESPACE" --timeout=180s 2>/dev/null || true
  kubectl wait --for=condition=Ready pod -l app=flink-funnel -n "$NAMESPACE" --timeout=180s 2>/dev/null || true
else
  bash "$SCRIPT_DIR/redeploy-sql.sh" all
fi

echo ""
echo "==> Data cleared and Flink jobs restarted ($FLINK_MODE mode)."
