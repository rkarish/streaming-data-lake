#!/usr/bin/env bash
set -euo pipefail

# =============================================================================
# AdTech Data Lake Streaming Platform - Sample Analytical Queries
# =============================================================================
# Runs example queries against the Iceberg tables via Trino.
# Includes single-table analytics and full-funnel join queries.
# Prerequisites: docker compose up -d && bash scripts/setup.sh
# =============================================================================

TRINO="docker exec trino trino --catalog iceberg --schema db"

run_query() {
  local title="$1"
  local sql="$2"
  echo ""
  echo "==> ${title}"
  echo "---"
  ${TRINO} --execute "${sql}"
  echo ""
}

# ---- 1. Request volume by country ----
run_query "Request volume by country (top 10)" \
  "SELECT device_geo_country, COUNT(*) AS request_count
   FROM bid_requests
   GROUP BY device_geo_country
   ORDER BY request_count DESC
   LIMIT 10"

# ---- 2. Average bid floor by country/region ----
run_query "Average bid floor by country and region (top 10)" \
  "SELECT device_geo_country, device_geo_region,
          ROUND(AVG(imp_bidfloor), 4) AS avg_bidfloor,
          COUNT(*) AS request_count
   FROM bid_requests
   GROUP BY device_geo_country, device_geo_region
   ORDER BY avg_bidfloor DESC
   LIMIT 10"

# ---- 3. Bid floor distribution by ad size ----
run_query "Bid floor distribution by ad size" \
  "SELECT imp_banner_w, imp_banner_h,
          ROUND(MIN(imp_bidfloor), 4) AS min_floor,
          ROUND(AVG(imp_bidfloor), 4) AS avg_floor,
          ROUND(MAX(imp_bidfloor), 4) AS max_floor,
          COUNT(*) AS request_count
   FROM bid_requests
   GROUP BY imp_banner_w, imp_banner_h
   ORDER BY request_count DESC
   LIMIT 10"

# ---- 4. Device OS and type breakdown ----
run_query "Device OS and type breakdown" \
  "SELECT device_os, device_type, COUNT(*) AS request_count
   FROM bid_requests
   GROUP BY device_os, device_type
   ORDER BY request_count DESC"

# ---- 5. Hourly request volume ----
run_query "Hourly request volume" \
  "SELECT date_trunc('hour', event_timestamp) AS hour,
          COUNT(*) AS request_count
   FROM bid_requests
   GROUP BY date_trunc('hour', event_timestamp)
   ORDER BY hour DESC
   LIMIT 24"

# ---- 6. Auction type distribution ----
run_query "Auction type distribution" \
  "SELECT auction_type,
          CASE auction_type
            WHEN 1 THEN 'First Price'
            WHEN 2 THEN 'Second Price'
            ELSE 'Other'
          END AS auction_name,
          COUNT(*) AS request_count
   FROM bid_requests
   GROUP BY auction_type
   ORDER BY request_count DESC"

# ---- 7. GDPR/COPPA flag distribution ----
run_query "GDPR and COPPA flag distribution" \
  "SELECT is_gdpr, is_coppa, COUNT(*) AS request_count
   FROM bid_requests
   GROUP BY is_gdpr, is_coppa
   ORDER BY request_count DESC"

# ---- 8. Iceberg snapshot metadata (time travel) ----
run_query "Iceberg snapshot history" \
  "SELECT snapshot_id, parent_id, committed_at, operation, summary
   FROM iceberg.db.\"bid_requests\$snapshots\"
   ORDER BY committed_at DESC
   LIMIT 10"

# =============================================================================
# Funnel Analytics Queries (cross-table JOINs)
# =============================================================================

# ---- 9. Fill rate by country ----
run_query "Fill rate by country (bid_requests LEFT JOIN bid_responses)" \
  "SELECT br.device_geo_country,
          COUNT(br.request_id) AS requests,
          COUNT(resp.request_id) AS responses,
          ROUND(CAST(COUNT(resp.request_id) AS DOUBLE) / COUNT(br.request_id) * 100, 2) AS fill_rate_pct
   FROM bid_requests br
   LEFT JOIN bid_responses resp ON br.request_id = resp.request_id
   GROUP BY br.device_geo_country
   ORDER BY requests DESC
   LIMIT 10"

# ---- 10. Win rate by bidder ----
run_query "Win rate by bidder (bid_responses LEFT JOIN impressions)" \
  "SELECT resp.seat AS bidder,
          COUNT(resp.response_id) AS bids,
          COUNT(imp.impression_id) AS wins,
          ROUND(CAST(COUNT(imp.impression_id) AS DOUBLE) / COUNT(resp.response_id) * 100, 2) AS win_rate_pct
   FROM bid_responses resp
   LEFT JOIN impressions imp ON resp.response_id = imp.response_id
   GROUP BY resp.seat
   ORDER BY bids DESC"

# ---- 11. CTR by creative ----
run_query "CTR by creative (impressions LEFT JOIN clicks)" \
  "SELECT imp.creative_id,
          COUNT(imp.impression_id) AS impressions,
          COUNT(clk.click_id) AS clicks,
          ROUND(CAST(COUNT(clk.click_id) AS DOUBLE) / COUNT(imp.impression_id) * 100, 2) AS ctr_pct
   FROM impressions imp
   LEFT JOIN clicks clk ON imp.impression_id = clk.impression_id
   GROUP BY imp.creative_id
   ORDER BY impressions DESC
   LIMIT 10"

# ---- 12. Revenue by publisher ----
run_query "Revenue by publisher (bid_requests JOIN impressions)" \
  "SELECT br.publisher_id,
          COUNT(imp.impression_id) AS wins,
          ROUND(SUM(imp.win_price), 2) AS total_revenue,
          ROUND(AVG(imp.win_price), 4) AS avg_win_price
   FROM bid_requests br
   JOIN impressions imp ON br.request_id = imp.request_id
   GROUP BY br.publisher_id
   ORDER BY total_revenue DESC
   LIMIT 10"

# ---- 13. Full funnel summary ----
run_query "Full funnel (4-table LEFT JOIN chain)" \
  "SELECT COUNT(DISTINCT br.request_id) AS requests,
          COUNT(DISTINCT resp.request_id) AS responses,
          COUNT(DISTINCT imp.impression_id) AS impressions,
          COUNT(DISTINCT clk.click_id) AS clicks,
          ROUND(CAST(COUNT(DISTINCT resp.request_id) AS DOUBLE) / COUNT(DISTINCT br.request_id) * 100, 2) AS fill_rate_pct,
          ROUND(CAST(COUNT(DISTINCT imp.impression_id) AS DOUBLE) / NULLIF(COUNT(DISTINCT resp.request_id), 0) * 100, 2) AS win_rate_pct,
          ROUND(CAST(COUNT(DISTINCT clk.click_id) AS DOUBLE) / NULLIF(COUNT(DISTINCT imp.impression_id), 0) * 100, 2) AS ctr_pct
   FROM bid_requests br
   LEFT JOIN bid_responses resp ON br.request_id = resp.request_id
   LEFT JOIN impressions imp ON br.request_id = imp.request_id
   LEFT JOIN clicks clk ON imp.impression_id = clk.impression_id"

# ---- 14. Average bid-to-win spread ----
run_query "Average bid-to-win spread (bid_responses JOIN impressions)" \
  "SELECT resp.seat AS bidder,
          ROUND(AVG(resp.bid_price), 4) AS avg_bid_price,
          ROUND(AVG(imp.win_price), 4) AS avg_win_price,
          ROUND(AVG(resp.bid_price - imp.win_price), 4) AS avg_spread
   FROM bid_responses resp
   JOIN impressions imp ON resp.response_id = imp.response_id
   GROUP BY resp.seat
   ORDER BY avg_spread DESC"

echo "==> All queries completed."
