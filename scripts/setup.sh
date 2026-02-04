#!/usr/bin/env bash
set -euo pipefail

# =============================================================================
# AdTech Data Lake Streaming Platform - Setup
# =============================================================================
# Run this script AFTER `docker compose up -d` to initialize:
#   1. Kafka topic (bid-requests)
#   2. MinIO bucket (warehouse)
#   3. Iceberg namespace and table (db.bid_requests)
#   4. Flink streaming job (Kafka -> Iceberg)
# =============================================================================

ICEBERG_REST_URL="http://localhost:8181"

# -----------------------------------------------------------------------------
# Task 1: Create Kafka topic
# -----------------------------------------------------------------------------
echo "==> Creating Kafka topic 'bid-requests' (3 partitions, replication-factor 1)..."

docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --topic bid-requests \
  --partitions 3 \
  --replication-factor 1 \
  --if-not-exists

echo "    Kafka topic 'bid-requests' is ready."

# -----------------------------------------------------------------------------
# Task 2: Create MinIO bucket
# -----------------------------------------------------------------------------
echo "==> Configuring MinIO client alias and creating 'warehouse' bucket..."

docker exec minio mc alias set local http://localhost:9000 admin password
docker exec minio mc mb --ignore-existing local/warehouse

echo "    MinIO bucket 'warehouse' is ready."

# -----------------------------------------------------------------------------
# Task 3: Create Iceberg namespace and table via REST API
# -----------------------------------------------------------------------------

# -- 3a: Create namespace 'db' --
echo "==> Creating Iceberg namespace 'db'..."

ns_http_code=$(curl -s -o /dev/null -w "%{http_code}" \
  -X POST "${ICEBERG_REST_URL}/v1/namespaces" \
  -H "Content-Type: application/json" \
  -d '{"namespace": ["db"]}')

if [ "$ns_http_code" -eq 200 ]; then
  echo "    Namespace 'db' created successfully."
elif [ "$ns_http_code" -eq 409 ]; then
  echo "    Namespace 'db' already exists, skipping."
else
  echo "    ERROR: Failed to create namespace 'db' (HTTP ${ns_http_code})."
  exit 1
fi

# -- 3b: Create table 'bid_requests' --
echo "==> Creating Iceberg table 'db.bid_requests'..."

table_payload='{
  "name": "bid_requests",
  "schema": {
    "type": "struct",
    "fields": [
      {"id": 1, "name": "request_id", "type": "string", "required": false},
      {"id": 2, "name": "imp_id", "type": "string", "required": false},
      {"id": 3, "name": "imp_banner_w", "type": "int", "required": false},
      {"id": 4, "name": "imp_banner_h", "type": "int", "required": false},
      {"id": 5, "name": "imp_bidfloor", "type": "double", "required": false},
      {"id": 6, "name": "site_id", "type": "string", "required": false},
      {"id": 7, "name": "site_domain", "type": "string", "required": false},
      {"id": 8, "name": "site_cat", "type": {"type": "list", "element-id": 22, "element": "string", "element-required": false}, "required": false},
      {"id": 9, "name": "publisher_id", "type": "string", "required": false},
      {"id": 10, "name": "device_type", "type": "int", "required": false},
      {"id": 11, "name": "device_os", "type": "string", "required": false},
      {"id": 12, "name": "device_geo_country", "type": "string", "required": false},
      {"id": 13, "name": "device_geo_region", "type": "string", "required": false},
      {"id": 14, "name": "user_id", "type": "string", "required": false},
      {"id": 15, "name": "auction_type", "type": "int", "required": false},
      {"id": 16, "name": "tmax", "type": "int", "required": false},
      {"id": 17, "name": "currency", "type": "string", "required": false},
      {"id": 18, "name": "is_coppa", "type": "boolean", "required": false},
      {"id": 19, "name": "is_gdpr", "type": "boolean", "required": false},
      {"id": 20, "name": "event_timestamp", "type": "timestamptz", "required": false},
      {"id": 21, "name": "received_at", "type": "timestamptz", "required": false}
    ]
  },
  "partition-spec": {
    "spec-id": 0,
    "fields": [
      {"source-id": 20, "transform": "day", "name": "event_timestamp_day", "field-id": 1000},
      {"source-id": 12, "transform": "identity", "name": "device_geo_country", "field-id": 1001}
    ]
  },
  "write-order": {
    "order-id": 0,
    "fields": []
  },
  "properties": {
    "format-version": "2"
  }
}'

tbl_http_code=$(curl -s -o /dev/null -w "%{http_code}" \
  -X POST "${ICEBERG_REST_URL}/v1/namespaces/db/tables" \
  -H "Content-Type: application/json" \
  -d "${table_payload}")

if [ "$tbl_http_code" -eq 200 ]; then
  echo "    Table 'db.bid_requests' created successfully."
elif [ "$tbl_http_code" -eq 409 ]; then
  echo "    Table 'db.bid_requests' already exists, skipping."
else
  echo "    ERROR: Failed to create table 'db.bid_requests' (HTTP ${tbl_http_code})."
  exit 1
fi

# -----------------------------------------------------------------------------
# Task 4: Submit Flink streaming job
# -----------------------------------------------------------------------------
echo "==> Submitting Flink streaming job (Kafka -> Iceberg)..."

docker compose exec -T flink-jobmanager bash /opt/flink/submit-sql-job.sh

echo "    Flink streaming job submitted."

# -----------------------------------------------------------------------------
# Done
# -----------------------------------------------------------------------------
echo ""
echo "==> Setup complete. Infrastructure is ready:"
echo "    - Kafka topic:    bid-requests (3 partitions)"
echo "    - MinIO bucket:   s3://warehouse"
echo "    - Iceberg table:  db.bid_requests (21 fields, partitioned by day(event_timestamp) + device_geo_country)"
echo "    - Flink job:      Streaming Kafka -> Iceberg (check http://localhost:8081)"
