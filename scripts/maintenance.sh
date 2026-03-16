#!/usr/bin/env bash
set -euo pipefail

# =============================================================================
# AdTech Data Playground - Table Maintenance
# =============================================================================
# Runs Iceberg table maintenance operations via Trino:
#   1. Compaction (optimize small files)
#   2. Snapshot expiry (remove old snapshots)
#   3. Orphan file cleanup (remove unreferenced files)
#
# Prerequisites: docker compose up -d && bash scripts/setup.sh
# =============================================================================

TRINO="docker exec trino trino --catalog iceberg --schema db"

for table in \
  bid_requests \
  bid_responses \
  impressions \
  clicks \
  bid_requests_enriched \
  hourly_impressions_by_geo \
  rolling_metrics_by_bidder \
  hourly_funnel_by_publisher \
  dq_rejected_events \
  dq_event_quality_hourly \
  bid_landscape_hourly \
  realtime_serving_metrics_1m \
  funnel_leakage_hourly; do
  echo "==> Starting Iceberg table maintenance for 'db.${table}'..."

  echo ""
  echo "    [1/3] Compacting small files (target: 128MB)..."
  ${TRINO} --execute \
    "ALTER TABLE ${table} EXECUTE optimize(file_size_threshold => '128MB')"
  echo "    Compaction complete."

  echo ""
  echo "    [2/3] Expiring snapshots older than 7 days..."
  ${TRINO} --execute \
    "ALTER TABLE ${table} EXECUTE expire_snapshots(retention_threshold => '7d')"
  echo "    Snapshot expiry complete."

  echo ""
  echo "    [3/3] Removing orphan files older than 7 days..."
  ${TRINO} --execute \
    "ALTER TABLE ${table} EXECUTE remove_orphan_files(retention_threshold => '7d')"
  echo "    Orphan file cleanup complete."

  echo ""
done

echo "==> Table maintenance complete for all tables."
