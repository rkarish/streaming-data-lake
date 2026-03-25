CREATE OR REPLACE VIEW iceberg.db.v_agg_serving_metrics AS
SELECT
    date_trunc('hour', imp.event_timestamp) AS hour_start,
    imp.bidder_id,
    db.bidder_name,
    COUNT(*) AS impressions,
    COALESCE(SUM(cl.click_count), 0) AS clicks,
    SUM(imp.win_price) AS revenue,
    CASE
        WHEN COUNT(*) > 0
        THEN CAST(COALESCE(SUM(cl.click_count), 0) AS DOUBLE) / CAST(COUNT(*) AS DOUBLE)
        ELSE 0.0
    END AS ctr
FROM iceberg.db.impressions imp
LEFT JOIN (
    SELECT impression_id, COUNT(*) AS click_count
    FROM iceberg.db.clicks
    GROUP BY impression_id
) cl
    ON imp.impression_id = cl.impression_id
LEFT JOIN iceberg.db.dim_bidder db
    ON imp.bidder_id = db.bidder_id AND db.is_current = true
GROUP BY
    date_trunc('hour', imp.event_timestamp),
    imp.bidder_id, db.bidder_name;
