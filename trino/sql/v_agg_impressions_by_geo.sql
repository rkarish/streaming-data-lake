CREATE OR REPLACE VIEW iceberg.db.v_agg_impressions_by_geo AS
SELECT
    date_trunc('hour', imp.event_timestamp) AS hour_start,
    br.device_geo_country,
    dg.country_name,
    COUNT(*) AS impression_count,
    SUM(imp.win_price) AS total_revenue,
    AVG(imp.win_price) AS avg_win_price
FROM iceberg.db.impressions imp
LEFT JOIN iceberg.db.bid_requests br ON imp.request_id = br.request_id
LEFT JOIN (
    SELECT country_code, MAX(country_name) AS country_name
    FROM iceberg.db.dim_geo
    WHERE is_current = true
    GROUP BY country_code
) dg
    ON br.device_geo_country = dg.country_code
GROUP BY
    date_trunc('hour', imp.event_timestamp),
    br.device_geo_country, dg.country_name;
