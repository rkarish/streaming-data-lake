CREATE OR REPLACE VIEW iceberg.db.v_realtime_agg_rolling_metrics_by_bidder AS
SELECT
    m.window_start,
    m.window_end,
    m.bidder_id,
    db.bidder_name,
    db.domain AS bidder_domain,
    m.win_count,
    m.revenue,
    m.avg_cpm
FROM iceberg.db.rolling_metrics_by_bidder m
LEFT JOIN iceberg.db.dim_bidder db
    ON m.bidder_id = db.bidder_id AND db.is_current = true;
