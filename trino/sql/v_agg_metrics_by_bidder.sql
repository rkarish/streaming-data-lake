CREATE OR REPLACE VIEW iceberg.db.v_agg_metrics_by_bidder AS
SELECT
    date_trunc('hour', imp.event_timestamp) AS hour_start,
    imp.bidder_id,
    db.bidder_name,
    db.domain AS bidder_domain,
    COUNT(*) AS win_count,
    SUM(imp.win_price) AS revenue,
    AVG(imp.win_price) AS avg_cpm
FROM iceberg.db.impressions imp
LEFT JOIN iceberg.db.dim_bidder db
    ON imp.bidder_id = db.bidder_id AND db.is_current = true
GROUP BY
    date_trunc('hour', imp.event_timestamp),
    imp.bidder_id, db.bidder_name, db.domain;
