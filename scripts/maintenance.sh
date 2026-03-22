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
  funnel_leakage_hourly \
  dim_agency \
  dim_advertiser \
  dim_campaign \
  dim_line_item \
  dim_strategy \
  dim_creative \
  dim_bidder \
  dim_publisher \
  dim_deal \
  dim_geo \
  dim_device_type \
  dim_device_os \
  dim_browser \
  materialization_watermarks \
  mat_bid_requests \
  mat_bid_responses \
  mat_impressions \
  mat_clicks \
  mat_agg_metrics_by_bidder \
  mat_agg_bid_landscape \
  mat_agg_serving_metrics \
  mat_agg_funnel_by_publisher \
  mat_agg_funnel_leakage \
  mat_agg_impressions_by_geo \
  mat_full_funnel; do
  # Skip tables that don't exist yet (e.g., mat_* before first materialization run)
  if ! ${TRINO} --execute "DESCRIBE ${table}" &>/dev/null; then
    echo "==> Skipping 'db.${table}' (table does not exist)"
    continue
  fi

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
