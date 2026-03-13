#!/usr/bin/env bash
set -euo pipefail

# =============================================================================
# AdTech Data Lake Streaming Platform - Setup
# =============================================================================
# Run this script AFTER `docker compose up -d` to initialize:
#   1. Kafka topics (bid-requests, bid-responses, impressions, clicks)
#   2. MinIO bucket (warehouse)
#   3. Iceberg namespace and tables:
#      - Core: db.bid_requests, bid_responses, impressions, clicks
#      - Enriched/metrics: db.bid_requests_enriched, hourly_impressions_by_geo, rolling_metrics_by_bidder, hourly_funnel_by_publisher
#      - Quality/serving: db.dq_rejected_events, dq_event_quality_hourly, bid_landscape_hourly, realtime_serving_metrics_1m, funnel_leakage_hourly
#   4. Flink streaming jobs on Kubernetes (application or session mode)
#   5. Trino connectivity verification
#
# Environment variables:
#   FLINK_MODE  - "application" (default) or "session"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FLINK_MODE="${FLINK_MODE:-application}"
#   6. CloudBeaver readiness check
#   7. Superset readiness check and dashboard setup
# =============================================================================

ICEBERG_REST_URL="http://localhost:8181"
SCHEMA_REGISTRY_URL="http://localhost:8082"

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

# -- 3c: Create table 'bid_responses' --
echo "==> Creating Iceberg table 'db.bid_responses'..."

bid_responses_payload='{
  "name": "bid_responses",
  "schema": {
    "type": "struct",
    "fields": [
      {"id": 1, "name": "response_id", "type": "string", "required": false},
      {"id": 2, "name": "request_id", "type": "string", "required": false},
      {"id": 3, "name": "seat", "type": "string", "required": false},
      {"id": 4, "name": "bid_id", "type": "string", "required": false},
      {"id": 5, "name": "imp_id", "type": "string", "required": false},
      {"id": 6, "name": "bid_price", "type": "double", "required": false},
      {"id": 7, "name": "creative_id", "type": "string", "required": false},
      {"id": 8, "name": "deal_id", "type": "string", "required": false},
      {"id": 9, "name": "ad_domain", "type": "string", "required": false},
      {"id": 10, "name": "currency", "type": "string", "required": false},
      {"id": 11, "name": "event_timestamp", "type": "timestamptz", "required": false}
    ]
  },
  "partition-spec": {
    "spec-id": 0,
    "fields": [
      {"source-id": 11, "transform": "day", "name": "event_timestamp_day", "field-id": 1000}
    ]
  },
  "write-order": {"order-id": 0, "fields": []},
  "properties": {"format-version": "2"}
}'

br_http_code=$(curl -s -o /dev/null -w "%{http_code}" \
  -X POST "${ICEBERG_REST_URL}/v1/namespaces/db/tables" \
  -H "Content-Type: application/json" \
  -d "${bid_responses_payload}")

if [ "$br_http_code" -eq 200 ]; then
  echo "    Table 'db.bid_responses' created successfully."
elif [ "$br_http_code" -eq 409 ]; then
  echo "    Table 'db.bid_responses' already exists, skipping."
else
  echo "    ERROR: Failed to create table 'db.bid_responses' (HTTP ${br_http_code})."
  exit 1
fi

# -- 3d: Create table 'impressions' --
echo "==> Creating Iceberg table 'db.impressions'..."

impressions_payload='{
  "name": "impressions",
  "schema": {
    "type": "struct",
    "fields": [
      {"id": 1, "name": "impression_id", "type": "string", "required": false},
      {"id": 2, "name": "request_id", "type": "string", "required": false},
      {"id": 3, "name": "response_id", "type": "string", "required": false},
      {"id": 4, "name": "imp_id", "type": "string", "required": false},
      {"id": 5, "name": "bidder_id", "type": "string", "required": false},
      {"id": 6, "name": "win_price", "type": "double", "required": false},
      {"id": 7, "name": "win_currency", "type": "string", "required": false},
      {"id": 8, "name": "creative_id", "type": "string", "required": false},
      {"id": 9, "name": "ad_domain", "type": "string", "required": false},
      {"id": 10, "name": "event_timestamp", "type": "timestamptz", "required": false}
    ]
  },
  "partition-spec": {
    "spec-id": 0,
    "fields": [
      {"source-id": 10, "transform": "day", "name": "event_timestamp_day", "field-id": 1000}
    ]
  },
  "write-order": {"order-id": 0, "fields": []},
  "properties": {"format-version": "2"}
}'

imp_http_code=$(curl -s -o /dev/null -w "%{http_code}" \
  -X POST "${ICEBERG_REST_URL}/v1/namespaces/db/tables" \
  -H "Content-Type: application/json" \
  -d "${impressions_payload}")

if [ "$imp_http_code" -eq 200 ]; then
  echo "    Table 'db.impressions' created successfully."
elif [ "$imp_http_code" -eq 409 ]; then
  echo "    Table 'db.impressions' already exists, skipping."
else
  echo "    ERROR: Failed to create table 'db.impressions' (HTTP ${imp_http_code})."
  exit 1
fi

# -- 3e: Create table 'clicks' --
echo "==> Creating Iceberg table 'db.clicks'..."

clicks_payload='{
  "name": "clicks",
  "schema": {
    "type": "struct",
    "fields": [
      {"id": 1, "name": "click_id", "type": "string", "required": false},
      {"id": 2, "name": "request_id", "type": "string", "required": false},
      {"id": 3, "name": "impression_id", "type": "string", "required": false},
      {"id": 4, "name": "imp_id", "type": "string", "required": false},
      {"id": 5, "name": "bidder_id", "type": "string", "required": false},
      {"id": 6, "name": "creative_id", "type": "string", "required": false},
      {"id": 7, "name": "click_url", "type": "string", "required": false},
      {"id": 8, "name": "event_timestamp", "type": "timestamptz", "required": false}
    ]
  },
  "partition-spec": {
    "spec-id": 0,
    "fields": [
      {"source-id": 8, "transform": "day", "name": "event_timestamp_day", "field-id": 1000}
    ]
  },
  "write-order": {"order-id": 0, "fields": []},
  "properties": {"format-version": "2"}
}'

clk_http_code=$(curl -s -o /dev/null -w "%{http_code}" \
  -X POST "${ICEBERG_REST_URL}/v1/namespaces/db/tables" \
  -H "Content-Type: application/json" \
  -d "${clicks_payload}")

if [ "$clk_http_code" -eq 200 ]; then
  echo "    Table 'db.clicks' created successfully."
elif [ "$clk_http_code" -eq 409 ]; then
  echo "    Table 'db.clicks' already exists, skipping."
else
  echo "    ERROR: Failed to create table 'db.clicks' (HTTP ${clk_http_code})."
  exit 1
fi

# -- 3f: Create table 'bid_requests_enriched' (with device classification and traffic flags) --
echo "==> Creating Iceberg table 'db.bid_requests_enriched'..."

enriched_payload='{
  "name": "bid_requests_enriched",
  "schema": {
    "type": "struct",
    "fields": [
      {"id": 1, "name": "request_id", "type": "string", "required": false},
      {"id": 2, "name": "imp_id", "type": "string", "required": false},
      {"id": 3, "name": "imp_banner_w", "type": "int", "required": false},
      {"id": 4, "name": "imp_banner_h", "type": "int", "required": false},
      {"id": 5, "name": "imp_bidfloor", "type": "double", "required": false},
      {"id": 6, "name": "imp_bidfloor_usd", "type": "double", "required": false},
      {"id": 7, "name": "imp_bidfloorcur", "type": "string", "required": false},
      {"id": 8, "name": "site_id", "type": "string", "required": false},
      {"id": 9, "name": "site_domain", "type": "string", "required": false},
      {"id": 10, "name": "app_id", "type": "string", "required": false},
      {"id": 11, "name": "app_bundle", "type": "string", "required": false},
      {"id": 12, "name": "publisher_id", "type": "string", "required": false},
      {"id": 13, "name": "device_type", "type": "int", "required": false},
      {"id": 14, "name": "device_os", "type": "string", "required": false},
      {"id": 15, "name": "device_ip", "type": "string", "required": false},
      {"id": 16, "name": "device_geo_country", "type": "string", "required": false},
      {"id": 17, "name": "device_geo_region", "type": "string", "required": false},
      {"id": 18, "name": "device_category", "type": "string", "required": false},
      {"id": 19, "name": "user_id", "type": "string", "required": false},
      {"id": 20, "name": "auction_type", "type": "int", "required": false},
      {"id": 21, "name": "currency", "type": "string", "required": false},
      {"id": 22, "name": "is_coppa", "type": "boolean", "required": false},
      {"id": 23, "name": "is_gdpr", "type": "boolean", "required": false},
      {"id": 24, "name": "is_test_traffic", "type": "boolean", "required": false},
      {"id": 25, "name": "is_private_ip", "type": "boolean", "required": false},
      {"id": 26, "name": "event_timestamp", "type": "timestamptz", "required": false},
      {"id": 27, "name": "received_at", "type": "timestamptz", "required": false}
    ]
  },
  "partition-spec": {
    "spec-id": 0,
    "fields": [
      {"source-id": 26, "transform": "day", "name": "event_timestamp_day", "field-id": 1000},
      {"source-id": 18, "transform": "identity", "name": "device_category", "field-id": 1001}
    ]
  },
  "write-order": {"order-id": 0, "fields": []},
  "properties": {"format-version": "2"}
}'

enr_http_code=$(curl -s -o /dev/null -w "%{http_code}" \
  -X POST "${ICEBERG_REST_URL}/v1/namespaces/db/tables" \
  -H "Content-Type: application/json" \
  -d "${enriched_payload}")

if [ "$enr_http_code" -eq 200 ]; then
  echo "    Table 'db.bid_requests_enriched' created successfully."
elif [ "$enr_http_code" -eq 409 ]; then
  echo "    Table 'db.bid_requests_enriched' already exists, skipping."
else
  echo "    ERROR: Failed to create table 'db.bid_requests_enriched' (HTTP ${enr_http_code})."
  exit 1
fi

# -- 3g: Create table 'hourly_impressions_by_geo' (upsert aggregation table) --
echo "==> Creating Iceberg table 'db.hourly_impressions_by_geo'..."

geo_agg_payload='{
  "name": "hourly_impressions_by_geo",
  "schema": {
    "type": "struct",
    "schema-id": 0,
    "identifier-field-ids": [1, 2],
    "fields": [
      {"id": 1, "name": "window_start", "type": "timestamp", "required": true},
      {"id": 2, "name": "device_geo_country", "type": "string", "required": true},
      {"id": 3, "name": "impression_count", "type": "long", "required": false},
      {"id": 4, "name": "total_revenue", "type": "double", "required": false},
      {"id": 5, "name": "avg_win_price", "type": "double", "required": false}
    ]
  },
  "partition-spec": {
    "spec-id": 0,
    "fields": [
      {"source-id": 1, "transform": "day", "name": "window_day", "field-id": 1000}
    ]
  },
  "write-order": {"order-id": 0, "fields": []},
  "properties": {"format-version": "2", "write.upsert.enabled": "true"}
}'

geo_http_code=$(curl -s -o /dev/null -w "%{http_code}" \
  -X POST "${ICEBERG_REST_URL}/v1/namespaces/db/tables" \
  -H "Content-Type: application/json" \
  -d "${geo_agg_payload}")

if [ "$geo_http_code" -eq 200 ]; then
  echo "    Table 'db.hourly_impressions_by_geo' created successfully."
elif [ "$geo_http_code" -eq 409 ]; then
  echo "    Table 'db.hourly_impressions_by_geo' already exists, skipping."
else
  echo "    ERROR: Failed to create table 'db.hourly_impressions_by_geo' (HTTP ${geo_http_code})."
  exit 1
fi

# -- 3h: Create table 'rolling_metrics_by_bidder' (upsert aggregation table) --
echo "==> Creating Iceberg table 'db.rolling_metrics_by_bidder'..."

bidder_agg_payload='{
  "name": "rolling_metrics_by_bidder",
  "schema": {
    "type": "struct",
    "schema-id": 0,
    "identifier-field-ids": [1, 3],
    "fields": [
      {"id": 1, "name": "window_start", "type": "timestamp", "required": true},
      {"id": 2, "name": "window_end", "type": "timestamp", "required": false},
      {"id": 3, "name": "bidder_id", "type": "string", "required": true},
      {"id": 4, "name": "win_count", "type": "long", "required": false},
      {"id": 5, "name": "revenue", "type": "double", "required": false},
      {"id": 6, "name": "avg_cpm", "type": "double", "required": false}
    ]
  },
  "partition-spec": {
    "spec-id": 0,
    "fields": [
      {"source-id": 1, "transform": "day", "name": "window_day", "field-id": 1000}
    ]
  },
  "write-order": {"order-id": 0, "fields": []},
  "properties": {"format-version": "2", "write.upsert.enabled": "true"}
}'

bidder_http_code=$(curl -s -o /dev/null -w "%{http_code}" \
  -X POST "${ICEBERG_REST_URL}/v1/namespaces/db/tables" \
  -H "Content-Type: application/json" \
  -d "${bidder_agg_payload}")

if [ "$bidder_http_code" -eq 200 ]; then
  echo "    Table 'db.rolling_metrics_by_bidder' created successfully."
elif [ "$bidder_http_code" -eq 409 ]; then
  echo "    Table 'db.rolling_metrics_by_bidder' already exists, skipping."
else
  echo "    ERROR: Failed to create table 'db.rolling_metrics_by_bidder' (HTTP ${bidder_http_code})."
  exit 1
fi

# -- 3i: Create table 'hourly_funnel_by_publisher' (upsert aggregation table) --
echo "==> Creating Iceberg table 'db.hourly_funnel_by_publisher'..."

funnel_payload='{
  "name": "hourly_funnel_by_publisher",
  "schema": {
    "type": "struct",
    "schema-id": 0,
    "identifier-field-ids": [1, 2],
    "fields": [
      {"id": 1, "name": "window_start", "type": "timestamp", "required": true},
      {"id": 2, "name": "publisher_id", "type": "string", "required": true},
      {"id": 3, "name": "bid_requests", "type": "long", "required": false},
      {"id": 4, "name": "bid_responses", "type": "long", "required": false},
      {"id": 5, "name": "impressions", "type": "long", "required": false},
      {"id": 6, "name": "clicks", "type": "long", "required": false},
      {"id": 7, "name": "fill_rate", "type": "double", "required": false},
      {"id": 8, "name": "win_rate", "type": "double", "required": false},
      {"id": 9, "name": "ctr", "type": "double", "required": false}
    ]
  },
  "partition-spec": {
    "spec-id": 0,
    "fields": [
      {"source-id": 1, "transform": "day", "name": "window_day", "field-id": 1000}
    ]
  },
  "write-order": {"order-id": 0, "fields": []},
  "properties": {"format-version": "2", "write.upsert.enabled": "true"}
}'

funnel_http_code=$(curl -s -o /dev/null -w "%{http_code}" \
  -X POST "${ICEBERG_REST_URL}/v1/namespaces/db/tables" \
  -H "Content-Type: application/json" \
  -d "${funnel_payload}")

if [ "$funnel_http_code" -eq 200 ]; then
  echo "    Table 'db.hourly_funnel_by_publisher' created successfully."
elif [ "$funnel_http_code" -eq 409 ]; then
  echo "    Table 'db.hourly_funnel_by_publisher' already exists, skipping."
else
  echo "    ERROR: Failed to create table 'db.hourly_funnel_by_publisher' (HTTP ${funnel_http_code})."
  exit 1
fi

# -- 3j: Create table 'dq_rejected_events' (append quality table) --
echo "==> Creating Iceberg table 'db.dq_rejected_events'..."

dq_rejected_payload='{
  "name": "dq_rejected_events",
  "schema": {
    "type": "struct",
    "fields": [
      {"id": 1, "name": "request_id", "type": "string", "required": false},
      {"id": 2, "name": "imp_id", "type": "string", "required": false},
      {"id": 3, "name": "publisher_id", "type": "string", "required": false},
      {"id": 4, "name": "device_ip", "type": "string", "required": false},
      {"id": 5, "name": "reject_reason", "type": "string", "required": false},
      {"id": 6, "name": "event_timestamp", "type": "timestamptz", "required": false}
    ]
  },
  "partition-spec": {
    "spec-id": 0,
    "fields": [
      {"source-id": 6, "transform": "day", "name": "event_timestamp_day", "field-id": 1000}
    ]
  },
  "write-order": {"order-id": 0, "fields": []},
  "properties": {"format-version": "2"}
}'

dq_rejected_http_code=$(curl -s -o /dev/null -w "%{http_code}" \
  -X POST "${ICEBERG_REST_URL}/v1/namespaces/db/tables" \
  -H "Content-Type: application/json" \
  -d "${dq_rejected_payload}")

if [ "$dq_rejected_http_code" -eq 200 ]; then
  echo "    Table 'db.dq_rejected_events' created successfully."
elif [ "$dq_rejected_http_code" -eq 409 ]; then
  echo "    Table 'db.dq_rejected_events' already exists, skipping."
else
  echo "    ERROR: Failed to create table 'db.dq_rejected_events' (HTTP ${dq_rejected_http_code})."
  exit 1
fi

# -- 3k: Create table 'dq_event_quality_hourly' (upsert quality metrics table) --
echo "==> Creating Iceberg table 'db.dq_event_quality_hourly'..."

dq_quality_payload='{
  "name": "dq_event_quality_hourly",
  "schema": {
    "type": "struct",
    "schema-id": 0,
    "identifier-field-ids": [1],
    "fields": [
      {"id": 1, "name": "window_start", "type": "timestamp", "required": true},
      {"id": 2, "name": "total_bid_requests", "type": "long", "required": false},
      {"id": 3, "name": "unique_bid_requests", "type": "long", "required": false},
      {"id": 4, "name": "duplicate_bid_requests", "type": "long", "required": false},
      {"id": 5, "name": "duplicate_bid_request_rate", "type": "double", "required": false},
      {"id": 6, "name": "total_bid_responses", "type": "long", "required": false},
      {"id": 7, "name": "unique_bid_responses", "type": "long", "required": false},
      {"id": 8, "name": "duplicate_bid_responses", "type": "long", "required": false},
      {"id": 9, "name": "duplicate_bid_response_rate", "type": "double", "required": false},
      {"id": 10, "name": "total_wins", "type": "long", "required": false},
      {"id": 11, "name": "unique_wins", "type": "long", "required": false},
      {"id": 12, "name": "duplicate_wins", "type": "long", "required": false},
      {"id": 13, "name": "duplicate_win_rate", "type": "double", "required": false},
      {"id": 14, "name": "total_clicks", "type": "long", "required": false},
      {"id": 15, "name": "unique_clicks", "type": "long", "required": false},
      {"id": 16, "name": "duplicate_clicks", "type": "long", "required": false},
      {"id": 17, "name": "duplicate_click_rate", "type": "double", "required": false},
      {"id": 18, "name": "invalid_bid_requests", "type": "long", "required": false},
      {"id": 19, "name": "invalid_bid_request_rate", "type": "double", "required": false},
      {"id": 20, "name": "total_events_all", "type": "long", "required": false},
      {"id": 21, "name": "duplicate_events_all", "type": "long", "required": false},
      {"id": 22, "name": "duplicate_rate_all", "type": "double", "required": false}
    ]
  },
  "partition-spec": {
    "spec-id": 0,
    "fields": [
      {"source-id": 1, "transform": "day", "name": "window_day", "field-id": 1000}
    ]
  },
  "write-order": {"order-id": 0, "fields": []},
  "properties": {"format-version": "2", "write.upsert.enabled": "true"}
}'

dq_quality_http_code=$(curl -s -o /dev/null -w "%{http_code}" \
  -X POST "${ICEBERG_REST_URL}/v1/namespaces/db/tables" \
  -H "Content-Type: application/json" \
  -d "${dq_quality_payload}")

if [ "$dq_quality_http_code" -eq 200 ]; then
  echo "    Table 'db.dq_event_quality_hourly' created successfully."
elif [ "$dq_quality_http_code" -eq 409 ]; then
  echo "    Table 'db.dq_event_quality_hourly' already exists. Recreating to apply latest schema..."
  delete_http_code=$(curl -s -o /dev/null -w "%{http_code}" \
    -X DELETE "${ICEBERG_REST_URL}/v1/namespaces/db/tables/dq_event_quality_hourly")
  if [ "$delete_http_code" -ne 204 ] && [ "$delete_http_code" -ne 404 ]; then
    echo "    ERROR: Failed to delete table 'db.dq_event_quality_hourly' (HTTP ${delete_http_code})."
    exit 1
  fi
  dq_quality_http_code=$(curl -s -o /dev/null -w "%{http_code}" \
    -X POST "${ICEBERG_REST_URL}/v1/namespaces/db/tables" \
    -H "Content-Type: application/json" \
    -d "${dq_quality_payload}")
  if [ "$dq_quality_http_code" -eq 200 ]; then
    echo "    Table 'db.dq_event_quality_hourly' recreated successfully."
  else
    echo "    ERROR: Failed to recreate table 'db.dq_event_quality_hourly' (HTTP ${dq_quality_http_code})."
    exit 1
  fi
else
  echo "    ERROR: Failed to create table 'db.dq_event_quality_hourly' (HTTP ${dq_quality_http_code})."
  exit 1
fi

# -- 3l: Create table 'bid_landscape_hourly' (upsert auction metrics table) --
echo "==> Creating Iceberg table 'db.bid_landscape_hourly'..."

bid_landscape_payload='{
  "name": "bid_landscape_hourly",
  "schema": {
    "type": "struct",
    "schema-id": 0,
    "identifier-field-ids": [1, 2],
    "fields": [
      {"id": 1, "name": "window_start", "type": "timestamp", "required": true},
      {"id": 2, "name": "publisher_id", "type": "string", "required": true},
      {"id": 3, "name": "request_count", "type": "long", "required": false},
      {"id": 4, "name": "total_bids", "type": "long", "required": false},
      {"id": 5, "name": "bids_per_request", "type": "double", "required": false},
      {"id": 6, "name": "avg_bid_price", "type": "double", "required": false},
      {"id": 7, "name": "max_bid_price", "type": "double", "required": false}
    ]
  },
  "partition-spec": {
    "spec-id": 0,
    "fields": [
      {"source-id": 1, "transform": "day", "name": "window_day", "field-id": 1000}
    ]
  },
  "write-order": {"order-id": 0, "fields": []},
  "properties": {"format-version": "2", "write.upsert.enabled": "true"}
}'

bid_landscape_http_code=$(curl -s -o /dev/null -w "%{http_code}" \
  -X POST "${ICEBERG_REST_URL}/v1/namespaces/db/tables" \
  -H "Content-Type: application/json" \
  -d "${bid_landscape_payload}")

if [ "$bid_landscape_http_code" -eq 200 ]; then
  echo "    Table 'db.bid_landscape_hourly' created successfully."
elif [ "$bid_landscape_http_code" -eq 409 ]; then
  echo "    Table 'db.bid_landscape_hourly' already exists, skipping."
else
  echo "    ERROR: Failed to create table 'db.bid_landscape_hourly' (HTTP ${bid_landscape_http_code})."
  exit 1
fi

# -- 3m: Create table 'realtime_serving_metrics_1m' (upsert realtime table) --
echo "==> Creating Iceberg table 'db.realtime_serving_metrics_1m'..."

realtime_metrics_payload='{
  "name": "realtime_serving_metrics_1m",
  "schema": {
    "type": "struct",
    "schema-id": 0,
    "identifier-field-ids": [1, 2],
    "fields": [
      {"id": 1, "name": "window_start", "type": "timestamp", "required": true},
      {"id": 2, "name": "bidder_id", "type": "string", "required": true},
      {"id": 3, "name": "impressions", "type": "long", "required": false},
      {"id": 4, "name": "clicks", "type": "long", "required": false},
      {"id": 5, "name": "revenue", "type": "double", "required": false},
      {"id": 6, "name": "ctr", "type": "double", "required": false}
    ]
  },
  "partition-spec": {
    "spec-id": 0,
    "fields": [
      {"source-id": 1, "transform": "day", "name": "window_day", "field-id": 1000}
    ]
  },
  "write-order": {"order-id": 0, "fields": []},
  "properties": {"format-version": "2", "write.upsert.enabled": "true"}
}'

realtime_metrics_http_code=$(curl -s -o /dev/null -w "%{http_code}" \
  -X POST "${ICEBERG_REST_URL}/v1/namespaces/db/tables" \
  -H "Content-Type: application/json" \
  -d "${realtime_metrics_payload}")

if [ "$realtime_metrics_http_code" -eq 200 ]; then
  echo "    Table 'db.realtime_serving_metrics_1m' created successfully."
elif [ "$realtime_metrics_http_code" -eq 409 ]; then
  echo "    Table 'db.realtime_serving_metrics_1m' already exists, skipping."
else
  echo "    ERROR: Failed to create table 'db.realtime_serving_metrics_1m' (HTTP ${realtime_metrics_http_code})."
  exit 1
fi

# -- 3n: Create table 'funnel_leakage_hourly' (upsert leakage table) --
echo "==> Creating Iceberg table 'db.funnel_leakage_hourly'..."

funnel_leakage_payload='{
  "name": "funnel_leakage_hourly",
  "schema": {
    "type": "struct",
    "schema-id": 0,
    "identifier-field-ids": [1, 2],
    "fields": [
      {"id": 1, "name": "window_start", "type": "timestamp", "required": true},
      {"id": 2, "name": "publisher_id", "type": "string", "required": true},
      {"id": 3, "name": "requests_no_response", "type": "long", "required": false},
      {"id": 4, "name": "responses_no_impression", "type": "long", "required": false},
      {"id": 5, "name": "impressions_no_click", "type": "long", "required": false},
      {"id": 6, "name": "response_leakage_rate", "type": "double", "required": false},
      {"id": 7, "name": "impression_leakage_rate", "type": "double", "required": false},
      {"id": 8, "name": "click_leakage_rate", "type": "double", "required": false}
    ]
  },
  "partition-spec": {
    "spec-id": 0,
    "fields": [
      {"source-id": 1, "transform": "day", "name": "window_day", "field-id": 1000}
    ]
  },
  "write-order": {"order-id": 0, "fields": []},
  "properties": {"format-version": "2", "write.upsert.enabled": "true"}
}'

funnel_leakage_http_code=$(curl -s -o /dev/null -w "%{http_code}" \
  -X POST "${ICEBERG_REST_URL}/v1/namespaces/db/tables" \
  -H "Content-Type: application/json" \
  -d "${funnel_leakage_payload}")

if [ "$funnel_leakage_http_code" -eq 200 ]; then
  echo "    Table 'db.funnel_leakage_hourly' created successfully."
elif [ "$funnel_leakage_http_code" -eq 409 ]; then
  echo "    Table 'db.funnel_leakage_hourly' already exists, skipping."
else
  echo "    ERROR: Failed to create table 'db.funnel_leakage_hourly' (HTTP ${funnel_leakage_http_code})."
  exit 1
fi

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
  for t in bid_requests bid_responses impressions clicks bid_requests_enriched hourly_impressions_by_geo rolling_metrics_by_bidder hourly_funnel_by_publisher dq_rejected_events dq_event_quality_hourly bid_landscape_hourly realtime_serving_metrics_1m funnel_leakage_hourly; do
    if ! echo "$tables" | grep -q "$t"; then
      found=false
      break
    fi
  done
  if [ "$found" = true ]; then
    echo "    Trino verified: all 13 tables are visible."
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
echo "    - Schema Registry: Avro schema governance (http://localhost:8082)"
echo "    - Kafka topics:    bid-requests, bid-responses, impressions, clicks (3 partitions each)"
echo "    - MinIO bucket:    s3://warehouse"
echo "    - Iceberg tables:  db.bid_requests, db.bid_responses, db.impressions, db.clicks"
echo "                       db.bid_requests_enriched (with device classification)"
echo "                       db.hourly_impressions_by_geo (upsert aggregation)"
echo "                       db.rolling_metrics_by_bidder (upsert aggregation)"
echo "                       db.hourly_funnel_by_publisher (upsert aggregation)"
echo "                       db.dq_rejected_events (rejected traffic stream)"
echo "                       db.dq_event_quality_hourly (quality KPIs)"
echo "                       db.bid_landscape_hourly (auction density/price KPIs)"
echo "                       db.realtime_serving_metrics_1m (low-latency serving metrics)"
echo "                       db.funnel_leakage_hourly (stage leakage KPIs)"
ARGOCD_PASSWORD=$(kubectl -n argocd get secret argocd-initial-admin-secret -o jsonpath='{.data.password}' 2>/dev/null | base64 -d 2>/dev/null || echo "unknown")
echo "    - Flink jobs:      Running on K8s (${FLINK_MODE} mode)"
echo "    - Flink UI:        http://localhost:8081"
echo "    - Argo CD UI:      https://localhost:8443 (admin / ${ARGOCD_PASSWORD})"
echo "    - Trino:           Query engine ready (http://localhost:8080)"
echo "    - CloudBeaver:     Web SQL IDE ready (http://localhost:8978)"
echo "    - Superset:        Dashboards ready (http://localhost:8088)"
