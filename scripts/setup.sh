#!/usr/bin/env bash
set -euo pipefail

# =============================================================================
# AdTech Data Playground - Setup
# =============================================================================
# Run this script AFTER `docker compose up -d` to initialize:
#   1. Kafka topics (bid-requests, bid-responses, impressions, clicks)
#   2. MinIO bucket (warehouse)
#   3. Iceberg tables via PyIceberg (iceberg/tables/*.yml)
#   4. Flink streaming jobs on Kubernetes (application or session mode)
#   5. Trino connectivity verification
#   6. CloudBeaver readiness check
#   7. Superset readiness check and dashboard setup
#
# Environment variables:
#   FLINK_MODE  - "application" (default) or "session"
# =============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FLINK_MODE="${FLINK_MODE:-application}"

SCHEMA_REGISTRY_URL="http://localhost:8085"

# -----------------------------------------------------------------------------
# Task 0: Wait for Schema Registry
# -----------------------------------------------------------------------------
echo "==> Waiting for Schema Registry to become ready..."

max_attempts=30
attempt=0
while [ $attempt -lt $max_attempts ]; do
  attempt=$((attempt + 1))
  if curl -sf "${SCHEMA_REGISTRY_URL}/subjects" > /dev/null 2>&1; then
    echo "    Schema Registry is ready."
    break
  fi
  if [ $attempt -eq $max_attempts ]; then
    echo "    WARNING: Schema Registry not ready after ${max_attempts} attempts."
    echo "    Check: docker compose ps schema-registry"
    exit 1
  fi
  echo "    Waiting for Schema Registry... (attempt ${attempt}/${max_attempts})"
  sleep 5
done

echo "==> Setting Schema Registry compatibility to BACKWARD..."
curl -sf -X PUT "${SCHEMA_REGISTRY_URL}/config" \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{"compatibility":"BACKWARD"}' > /dev/null 2>&1 \
  && echo "    Compatibility set to BACKWARD." \
  || echo "    WARNING: Could not set compatibility mode."

# -----------------------------------------------------------------------------
# Task 1: Create Kafka topics
# -----------------------------------------------------------------------------
for topic in bid-requests bid-responses impressions clicks; do
  echo "==> Creating Kafka topic '${topic}' (3 partitions, replication-factor 1)..."
  docker exec kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 \
    --create \
    --topic "${topic}" \
    --partitions 3 \
    --replication-factor 1 \
    --if-not-exists
  echo "    Kafka topic '${topic}' is ready."
done

# -----------------------------------------------------------------------------
# Task 2: Create MinIO bucket
# -----------------------------------------------------------------------------
echo "==> Configuring MinIO client alias and creating 'warehouse' bucket..."

docker exec minio mc alias set local http://localhost:9000 admin password
docker exec minio mc mb --ignore-existing local/warehouse

echo "    MinIO bucket 'warehouse' is ready."

# -----------------------------------------------------------------------------
# Task 3: Create Iceberg tables via PyIceberg
# Table schemas are defined in iceberg/tables/*.yml and applied using PyIceberg.
# Tables must exist before Flink starts so INSERT INTO targets are available.
# -----------------------------------------------------------------------------
echo "==> Waiting for Iceberg REST catalog to become ready..."

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
  echo "    Waiting for Iceberg REST catalog... (attempt ${attempt}/${max_attempts})"
  sleep 5
done

echo "==> Creating Iceberg tables from YAML definitions..."
VENV_PYTHON="$SCRIPT_DIR/../.venv/bin/python"
if [ ! -x "$VENV_PYTHON" ]; then
  echo "    Virtual environment not found. Creating .venv..."
  python3 -m venv "$SCRIPT_DIR/../.venv"
fi
"$VENV_PYTHON" -m pip install --quiet -r "$SCRIPT_DIR/../iceberg/requirements.txt"
"$VENV_PYTHON" -m pip install --quiet -e "$SCRIPT_DIR/../mock-data-gen"
"$VENV_PYTHON" "$SCRIPT_DIR/../iceberg/apply_tables.py"

# -----------------------------------------------------------------------------
# Task 3.5: Seed dimension tables with static mock data
# Dimension data is written directly to Iceberg via PyIceberg.
# Must run after tables are created (Task 3) and before Flink starts (Task 4).
# -----------------------------------------------------------------------------
echo "==> Seeding dimension tables with mock data..."
"$VENV_PYTHON" "$SCRIPT_DIR/../iceberg/seed_dimensions.py"

# -----------------------------------------------------------------------------
# Task 4: Deploy Flink streaming jobs on Kubernetes
# -----------------------------------------------------------------------------
echo "==> Deploying Flink streaming jobs on Kubernetes (${FLINK_MODE} mode)..."
bash "$SCRIPT_DIR/../k8s/scripts/setup-k8s.sh" --mode "$FLINK_MODE"

# -----------------------------------------------------------------------------
# Task 5: Verify Trino connectivity
# -----------------------------------------------------------------------------
echo "==> Verifying Trino can query the Iceberg catalog..."

max_attempts=12
attempt=0
while [ $attempt -lt $max_attempts ]; do
  attempt=$((attempt + 1))
  tables=$(docker exec trino trino --catalog iceberg --schema db --execute "SHOW TABLES" 2>/dev/null || true)
  found=true
  for t in bid_requests bid_responses impressions clicks bid_requests_enriched hourly_impressions_by_geo rolling_metrics_by_bidder hourly_funnel_by_publisher dq_rejected_events dq_event_quality_hourly bid_landscape_hourly realtime_serving_metrics_1m funnel_leakage_hourly dim_agency dim_advertiser dim_campaign dim_line_item dim_strategy dim_creative dim_bidder dim_publisher dim_deal dim_geo dim_device_type dim_device_os dim_browser materialization_watermarks; do
    if ! echo "$tables" | grep -q "$t"; then
      found=false
      break
    fi
  done
  if [ "$found" = true ]; then
    echo "    Trino verified: all 27 tables are visible."
    break
  fi
  if [ $attempt -eq $max_attempts ]; then
    echo "    WARNING: Trino could not find all tables after ${max_attempts} attempts."
    echo "    Check that the Trino service is healthy: docker compose ps trino"
    break
  fi
  echo "    Waiting for Trino... (attempt ${attempt}/${max_attempts})"
  sleep 5
done

echo "==> Creating Trino views..."
bash "$SCRIPT_DIR/../trino/apply_views.sh"

# -----------------------------------------------------------------------------
# Task 6: Wait for CloudBeaver
# -----------------------------------------------------------------------------
echo "==> Waiting for CloudBeaver to become ready..."

max_attempts=30
attempt=0
while [ $attempt -lt $max_attempts ]; do
  attempt=$((attempt + 1))
  if curl -sf http://localhost:8978 > /dev/null 2>&1; then
    echo "    CloudBeaver is ready."
    break
  fi
  if [ $attempt -eq $max_attempts ]; then
    echo "    WARNING: CloudBeaver not ready after ${max_attempts} attempts."
    echo "    Check: docker compose ps cloudbeaver"
    break
  fi
  echo "    Waiting for CloudBeaver... (attempt ${attempt}/${max_attempts})"
  sleep 5
done

# -----------------------------------------------------------------------------
# Task 7: Wait for Superset and set up dashboards
# -----------------------------------------------------------------------------
echo "==> Waiting for Superset to become ready..."

max_attempts=60
attempt=0
while [ $attempt -lt $max_attempts ]; do
  attempt=$((attempt + 1))
  if curl -sf http://localhost:8088/health > /dev/null 2>&1; then
    echo "    Superset is ready."
    break
  fi
  if [ $attempt -eq $max_attempts ]; then
    echo "    WARNING: Superset not ready after ${max_attempts} attempts."
    echo "    Check: docker compose ps superset"
    break
  fi
  echo "    Waiting for Superset... (attempt ${attempt}/${max_attempts})"
  sleep 15
done

echo "==> Setting up Superset dashboards..."
docker compose exec -T superset python /app/setup-dashboards.py
echo "    Superset dashboards configured."

# -----------------------------------------------------------------------------
# Done
# -----------------------------------------------------------------------------
echo ""
echo "==> Setup complete. Infrastructure is ready:"
echo "    - Schema Registry: Avro schema governance (http://localhost:8085)"
echo "    - Kafka topics:    bid-requests, bid-responses, impressions, clicks (3 partitions each)"
echo "    - MinIO bucket:    s3://warehouse"
echo "    - Iceberg tables:  27 tables created by PyIceberg (13 dimension + 14 fact/aggregate)"
echo "    - Flink jobs:      Running on K8s (${FLINK_MODE} mode)"
if [ "$FLINK_MODE" = "application" ]; then
echo "    - Flink UI:        http://localhost:8081 (ingestion)"
echo "                       http://localhost:8082 (aggregation)"
echo "                       http://localhost:8083 (funnel)"
else
echo "    - Flink UI:        http://localhost:8081"
fi
echo "    - Argo CD UI:      https://localhost:8443 (admin / password)"
echo "    - Trino:           Query engine ready (http://localhost:8082)"
echo "    - Airflow:         ETL orchestration (http://localhost:8080)"
echo "    - CloudBeaver:     Web SQL IDE ready (http://localhost:8978)"
echo "    - Superset:        Dashboards ready (http://localhost:8088)"

# Start port-forwards
pkill -f "kubectl port-forward.*-n flink" 2>/dev/null || true
pkill -f "kubectl port-forward.*-n argocd" 2>/dev/null || true
if [ "$FLINK_MODE" = "application" ]; then
  kubectl port-forward svc/flink-ingestion-rest -n flink 8081:8081 &>/dev/null &
  kubectl port-forward svc/flink-aggregation-rest -n flink 8082:8081 &>/dev/null &
  kubectl port-forward svc/flink-funnel-rest -n flink 8083:8081 &>/dev/null &
else
  kubectl port-forward svc/flink-session-rest -n flink 8081:8081 &>/dev/null &
fi
kubectl port-forward svc/argocd-server -n argocd 8443:443 &>/dev/null &

echo ""
echo "Port-forwards running in background:"
if [ "$FLINK_MODE" = "application" ]; then
  echo "    - Flink Ingestion:  http://localhost:8081"
  echo "    - Flink Aggregation: http://localhost:8082"
  echo "    - Flink Funnel:     http://localhost:8083"
else
  echo "    - Flink Session:    http://localhost:8081"
fi
echo "    - Argo CD:          https://localhost:8443"
echo ""
echo "Press Ctrl+C to stop port-forwards."
wait
