CREATE OR REPLACE VIEW iceberg.db.v_realtime_agg_serving_metrics_1m AS
SELECT
    m.window_start,
    m.bidder_id,
    db.bidder_name,
    m.impressions,
    m.clicks,
    m.revenue,
    m.ctr
FROM iceberg.db.realtime_serving_metrics_1m m
LEFT JOIN iceberg.db.dim_bidder db
    ON m.bidder_id = db.bidder_id AND db.is_current = true;
