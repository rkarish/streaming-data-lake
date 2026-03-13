#!/bin/bash
set -euo pipefail

KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-host.docker.internal:39092}"
SCHEMA_REGISTRY="${SCHEMA_REGISTRY:-http://host.docker.internal:8082}"
ICEBERG_REST="${ICEBERG_REST:-http://host.docker.internal:8181}"
MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://host.docker.internal:9000}"

for f in /opt/flink/sql/*.sql; do
  sed -i \
    -e "s|kafka:9092|${KAFKA_BOOTSTRAP}|g" \
    -e "s|http://schema-registry:8081|${SCHEMA_REGISTRY}|g" \
    -e "s|http://iceberg-rest:8181|${ICEBERG_REST}|g" \
    -e "s|http://minio:9000|${MINIO_ENDPOINT}|g" \
    "$f"
done

exec "$@"
