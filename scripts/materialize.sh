#!/usr/bin/env bash
set -euo pipefail

# =============================================================================
# Incremental materialization of dimension-enriched views.
#
# Creates physical Iceberg tables (mat_*) that mirror the Trino views (v_*),
# pre-joining fact data with SCD Type 2 dimension attributes. Supports:
#   - Late-arriving data repair within a lookback window (e.g. after backfill)
#   - New fact data (appended since last run)
#   - Dimension changes (re-materializes affected rows)
#
# First run does a full materialization. Subsequent runs are incremental.
#
# Environment variables:
#   LOOKBACK_HOURS  Hours to look back for late-arriving data (default: 0 = disabled).
#                   Set this after running a backfill to repair data that arrived
#                   with event timestamps older than the current watermark.
#
# NOTE: This script is a temporary orchestration mechanism. The materialization
# workflow will be moved to Apache Airflow for scheduled, observable execution.
#
# Prerequisites: docker compose up -d && bash scripts/setup.sh
# =============================================================================

TRINO="docker exec trino trino --catalog iceberg --schema db"
LOOKBACK_HOURS="${LOOKBACK_HOURS:-0}"

# Run a Trino query and return the result (trimmed, no quotes)
trino_query() {
  docker exec trino trino --catalog iceberg --schema db \
    --execute "$1" 2>/dev/null | tr -d '"' | xargs
}

# ---------------------------------------------------------------------------
# Materialization definitions
# Each entry: mat_table|view_name|view_ts_col|fact_table|fact_ts_col|dim_fk_checks
#
# view_ts_col: timestamp column name in the view/materialized table
# fact_ts_col: timestamp column name in the source fact table (for new-data detection)
# dim_fk_checks: semicolon-separated list of fk_col:dim_table:dim_pk
# ---------------------------------------------------------------------------
MATERIALIZATIONS=(
  "mat_bid_requests|v_event_enriched_bid_requests|event_timestamp|bid_requests|event_timestamp|publisher_id:dim_publisher:publisher_id;device_type:dim_device_type:device_type_code;device_os:dim_device_os:os_name"
  "mat_bid_responses|v_event_enriched_bid_responses|event_timestamp|bid_responses|event_timestamp|seat:dim_bidder:bidder_id;creative_id:dim_creative:creative_id;strategy_id:dim_strategy:strategy_id;line_item_id:dim_line_item:line_item_id;campaign_id:dim_campaign:campaign_id;advertiser_id:dim_advertiser:advertiser_id;agency_id:dim_agency:agency_id;deal_id:dim_deal:deal_id"
  "mat_impressions|v_event_enriched_impressions|event_timestamp|impressions|event_timestamp|bidder_id:dim_bidder:bidder_id;creative_id:dim_creative:creative_id"
  "mat_clicks|v_event_enriched_clicks|event_timestamp|clicks|event_timestamp|bidder_id:dim_bidder:bidder_id;creative_id:dim_creative:creative_id"
  "mat_full_funnel|v_event_enriched_full_funnel|request_timestamp|bid_requests|event_timestamp|publisher_id:dim_publisher:publisher_id;seat:dim_bidder:bidder_id;creative_id:dim_creative:creative_id;strategy_id:dim_strategy:strategy_id;line_item_id:dim_line_item:line_item_id;campaign_id:dim_campaign:campaign_id;advertiser_id:dim_advertiser:advertiser_id;agency_id:dim_agency:agency_id;deal_id:dim_deal:deal_id"
  "mat_agg_metrics_by_bidder|v_agg_metrics_by_bidder|hour_start|impressions|event_timestamp|bidder_id:dim_bidder:bidder_id"
  "mat_agg_bid_landscape|v_agg_bid_landscape|hour_start|bid_responses|event_timestamp|publisher_id:dim_publisher:publisher_id"
  "mat_agg_serving_metrics|v_agg_serving_metrics|hour_start|impressions|event_timestamp|bidder_id:dim_bidder:bidder_id"
  "mat_agg_funnel_by_publisher|v_agg_funnel_by_publisher|hour_start|bid_requests|event_timestamp|publisher_id:dim_publisher:publisher_id"
  "mat_agg_funnel_leakage|v_agg_funnel_leakage|hour_start|bid_requests|event_timestamp|publisher_id:dim_publisher:publisher_id"
  "mat_agg_impressions_by_geo|v_agg_impressions_by_geo|hour_start|impressions|event_timestamp|device_geo_country:dim_geo:country_code"
)

echo "==> Incremental materialization of dimension-enriched views"
if [ "$LOOKBACK_HOURS" -gt 0 ]; then
  echo "    Lookback window: ${LOOKBACK_HOURS} hour(s)"
fi
echo ""

# ---------------------------------------------------------------------------
# Ensure watermark table exists
# ---------------------------------------------------------------------------
${TRINO} --execute "
  CREATE TABLE IF NOT EXISTS iceberg.db.materialization_watermarks (
    table_name VARCHAR,
    last_materialized_at TIMESTAMP(6) WITH TIME ZONE
  )
" 2>/dev/null || true

errors=0
for entry in "${MATERIALIZATIONS[@]}"; do
  IFS='|' read -r mat_table view_name view_ts_col fact_table fact_ts_col dim_checks <<< "$entry"

  echo "--- ${mat_table} ---"

  # Check if materialized table exists
  table_exists=$(trino_query "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'db' AND table_name = '${mat_table}'")

  if [ "$table_exists" = "0" ]; then
    echo "    Creating table and running full materialization..."
    if ${TRINO} --execute "CREATE TABLE iceberg.db.${mat_table} AS SELECT * FROM iceberg.db.${view_name}" 2>/dev/null; then
      row_count=$(trino_query "SELECT count(*) FROM iceberg.db.${mat_table}")
      echo "    OK    Full load: ${row_count} rows"
      ${TRINO} --execute "
        DELETE FROM iceberg.db.materialization_watermarks WHERE table_name = '${mat_table}'
      " 2>/dev/null || true
      ${TRINO} --execute "
        INSERT INTO iceberg.db.materialization_watermarks
        SELECT '${mat_table}', MAX(${fact_ts_col}) FROM iceberg.db.${fact_table}
      " 2>/dev/null
    else
      echo "    FAIL  Could not create ${mat_table}"
      errors=$((errors + 1))
    fi
    continue
  fi

  # Get last watermark
  watermark=$(trino_query "SELECT CAST(last_materialized_at AS VARCHAR) FROM iceberg.db.materialization_watermarks WHERE table_name = '${mat_table}'")

  if [ -z "$watermark" ]; then
    echo "    No watermark found, running full materialization..."
    ${TRINO} --execute "DELETE FROM iceberg.db.${mat_table}" 2>/dev/null || true
    if ${TRINO} --execute "INSERT INTO iceberg.db.${mat_table} SELECT * FROM iceberg.db.${view_name}" 2>/dev/null; then
      row_count=$(trino_query "SELECT count(*) FROM iceberg.db.${mat_table}")
      echo "    OK    Full load: ${row_count} rows"
      ${TRINO} --execute "
        INSERT INTO iceberg.db.materialization_watermarks
        SELECT '${mat_table}', MAX(${fact_ts_col}) FROM iceberg.db.${fact_table}
      " 2>/dev/null
    else
      echo "    FAIL  Full load failed"
      errors=$((errors + 1))
    fi
    continue
  fi

  echo "    Watermark: ${watermark}"

  # --- Pass 0: Late-arrival repair (lookback window) ---
  pass0_repaired=false
  if [ "$LOOKBACK_HOURS" -gt 0 ]; then
    lookback_start=$(trino_query "
      SELECT CAST(TIMESTAMP '${watermark}' - INTERVAL '${LOOKBACK_HOURS}' HOUR AS VARCHAR)
    ")

    # Skip lookback repair for Flink aggregate tables — their windows are
    # immutable once closed, so late arrivals cannot affect them.
    # Only raw fact tables and cross-fact joins need lookback repair.
    needs_repair=false
    if [ "$fact_ts_col" = "window_start" ]; then
      echo "    Skipping lookback (Flink aggregate, windows are immutable)"
    else
      fact_count=$(trino_query "
        SELECT count(*) FROM iceberg.db.${fact_table}
        WHERE ${fact_ts_col} >= TIMESTAMP '${lookback_start}'
          AND ${fact_ts_col} <= TIMESTAMP '${watermark}'
      ")
      mat_count=$(trino_query "
        SELECT count(*) FROM iceberg.db.${mat_table}
        WHERE ${view_ts_col} >= TIMESTAMP '${lookback_start}'
          AND ${view_ts_col} <= TIMESTAMP '${watermark}'
      ")
      if [ "${fact_count:-0}" != "${mat_count:-0}" ]; then
        needs_repair=true
        echo "    Late arrivals detected in lookback window: ${fact_count} fact rows vs ${mat_count} mat rows"
      fi
    fi

    if [ "$needs_repair" = true ]; then
      ${TRINO} --execute "
        DELETE FROM iceberg.db.${mat_table}
        WHERE ${view_ts_col} >= TIMESTAMP '${lookback_start}'
          AND ${view_ts_col} <= TIMESTAMP '${watermark}'
      " 2>/dev/null
      ${TRINO} --execute "
        INSERT INTO iceberg.db.${mat_table}
        SELECT * FROM iceberg.db.${view_name}
        WHERE ${view_ts_col} >= TIMESTAMP '${lookback_start}'
          AND ${view_ts_col} <= TIMESTAMP '${watermark}'
      " 2>/dev/null
      repaired_count=$(trino_query "
        SELECT count(*) FROM iceberg.db.${mat_table}
        WHERE ${view_ts_col} >= TIMESTAMP '${lookback_start}'
          AND ${view_ts_col} <= TIMESTAMP '${watermark}'
      ")
      echo "    Repaired lookback window [${lookback_start} .. ${watermark}]: ${repaired_count} rows"
      pass0_repaired=true
    fi
  fi

  # --- Pass 1: Handle dimension changes ---
  dim_changed_count=0
  IFS=';' read -ra checks <<< "$dim_checks"
  for check in "${checks[@]}"; do
    IFS=':' read -r fk_col dim_table dim_pk <<< "$check"

    changed=$(trino_query "
      SELECT count(*) FROM iceberg.db.${dim_table}
      WHERE valid_from > TIMESTAMP '${watermark}'
    ")

    if [ "$changed" != "0" ] && [ -n "$changed" ]; then
      echo "    Dimension change: ${dim_table} (${changed} new versions)"

      # Delete materialized rows affected by the dimension change
      # If Pass 0 already repaired the lookback window, only repair older rows
      if [ "$pass0_repaired" = true ]; then
        dim_repair_upper="AND ${view_ts_col} < TIMESTAMP '${lookback_start}'"
      else
        dim_repair_upper=""
      fi

      ${TRINO} --execute "
        DELETE FROM iceberg.db.${mat_table}
        WHERE ${fk_col} IN (
          SELECT ${dim_pk} FROM iceberg.db.${dim_table}
          WHERE valid_from > TIMESTAMP '${watermark}'
        )
        ${dim_repair_upper}
      " 2>/dev/null

      # Re-insert affected rows from the view (with fresh dimension values)
      ${TRINO} --execute "
        INSERT INTO iceberg.db.${mat_table}
        SELECT * FROM iceberg.db.${view_name}
        WHERE ${fk_col} IN (
          SELECT ${dim_pk} FROM iceberg.db.${dim_table}
          WHERE valid_from > TIMESTAMP '${watermark}'
        )
        AND ${view_ts_col} <= TIMESTAMP '${watermark}'
          ${dim_repair_upper}
      " 2>/dev/null

      dim_changed_count=$((dim_changed_count + 1))
    fi
  done

  if [ "$dim_changed_count" -gt 0 ]; then
    echo "    Repaired ${dim_changed_count} dimension change(s)"
  fi

  # --- Pass 2: Append new fact data ---
  new_rows=$(trino_query "
    SELECT count(*) FROM iceberg.db.${fact_table}
    WHERE ${fact_ts_col} > TIMESTAMP '${watermark}'
  ")

  if [ "$new_rows" != "0" ] && [ -n "$new_rows" ]; then
    ${TRINO} --execute "
      INSERT INTO iceberg.db.${mat_table}
      SELECT * FROM iceberg.db.${view_name}
      WHERE ${view_ts_col} > TIMESTAMP '${watermark}'
    " 2>/dev/null
    echo "    Appended ${new_rows} new fact rows"
  else
    echo "    No new fact data"
  fi

  # --- Pass 3: Repair incomplete funnel rows (full funnel only) ---
  # When downstream events (responses, impressions, clicks) arrive after a
  # request was already materialized, the mat row has stale NULL values.
  # Delete and re-insert those rows with fresh join results.
  if [ "$mat_table" = "mat_full_funnel" ]; then
    repair_responses=$(trino_query "
      SELECT count(*) FROM iceberg.db.mat_full_funnel m
      WHERE m.request_timestamp <= TIMESTAMP '${watermark}'
        AND m.has_response = false
        AND EXISTS (SELECT 1 FROM iceberg.db.bid_responses r WHERE r.request_id = m.request_id)
    ")
    repair_impressions=$(trino_query "
      SELECT count(*) FROM iceberg.db.mat_full_funnel m
      WHERE m.request_timestamp <= TIMESTAMP '${watermark}'
        AND m.has_impression = false AND m.has_response = true
        AND EXISTS (
          SELECT 1 FROM iceberg.db.impressions i
          JOIN iceberg.db.bid_responses r ON r.response_id = i.response_id
          WHERE r.request_id = m.request_id)
    ")
    repair_clicks=$(trino_query "
      SELECT count(*) FROM iceberg.db.mat_full_funnel m
      WHERE m.request_timestamp <= TIMESTAMP '${watermark}'
        AND m.has_click = false AND m.has_impression = true
        AND EXISTS (
          SELECT 1 FROM iceberg.db.clicks c
          JOIN iceberg.db.impressions i ON i.impression_id = c.impression_id
          JOIN iceberg.db.bid_responses r ON r.response_id = i.response_id
          WHERE r.request_id = m.request_id)
    ")
    repair_count=$(( ${repair_responses:-0} + ${repair_impressions:-0} + ${repair_clicks:-0} ))

    if [ "$repair_count" -gt 0 ]; then
      echo "    Stale rows: ${repair_responses:-0} responses, ${repair_impressions:-0} impressions, ${repair_clicks:-0} clicks"
      # Collect request_ids that need repair
      ${TRINO} --execute "
        DELETE FROM iceberg.db.mat_full_funnel
        WHERE request_timestamp <= TIMESTAMP '${watermark}'
          AND (
            (has_response = false AND EXISTS (
              SELECT 1 FROM iceberg.db.bid_responses r WHERE r.request_id = mat_full_funnel.request_id))
            OR (has_impression = false AND has_response = true AND EXISTS (
              SELECT 1 FROM iceberg.db.impressions i
              JOIN iceberg.db.bid_responses r ON r.response_id = i.response_id
              WHERE r.request_id = mat_full_funnel.request_id))
            OR (has_click = false AND has_impression = true AND EXISTS (
              SELECT 1 FROM iceberg.db.clicks c
              JOIN iceberg.db.impressions i ON i.impression_id = c.impression_id
              JOIN iceberg.db.bid_responses r ON r.response_id = i.response_id
              WHERE r.request_id = mat_full_funnel.request_id))
          )
      " 2>/dev/null

      ${TRINO} --execute "
        INSERT INTO iceberg.db.mat_full_funnel
        SELECT v.* FROM iceberg.db.v_event_enriched_full_funnel v
        WHERE v.request_id IN (
          SELECT br.request_id FROM iceberg.db.bid_requests br
          WHERE br.event_timestamp <= TIMESTAMP '${watermark}'
            AND (
              EXISTS (SELECT 1 FROM iceberg.db.bid_responses r
                WHERE r.request_id = br.request_id
                AND r.event_timestamp > TIMESTAMP '${watermark}')
              OR EXISTS (SELECT 1 FROM iceberg.db.impressions i
                JOIN iceberg.db.bid_responses r ON r.response_id = i.response_id
                WHERE r.request_id = br.request_id
                AND i.event_timestamp > TIMESTAMP '${watermark}')
              OR EXISTS (SELECT 1 FROM iceberg.db.clicks c
                JOIN iceberg.db.impressions i ON i.impression_id = c.impression_id
                JOIN iceberg.db.bid_responses r ON r.response_id = i.response_id
                WHERE r.request_id = br.request_id
                AND c.event_timestamp > TIMESTAMP '${watermark}')
            )
        )
      " 2>/dev/null

      echo "    Repaired ${repair_count} incomplete funnel rows (${repair_responses:-0} responses, ${repair_impressions:-0} impressions, ${repair_clicks:-0} clicks)"
    fi
  fi

  # --- Update watermark ---
  ${TRINO} --execute "
    DELETE FROM iceberg.db.materialization_watermarks WHERE table_name = '${mat_table}'
  " 2>/dev/null || true
  ${TRINO} --execute "
    INSERT INTO iceberg.db.materialization_watermarks
    SELECT '${mat_table}', MAX(${fact_ts_col}) FROM iceberg.db.${fact_table}
  " 2>/dev/null

  echo "    OK"
done

echo ""
echo "==> Materialization complete (${errors} errors)"
exit $((errors > 0 ? 1 : 0))
