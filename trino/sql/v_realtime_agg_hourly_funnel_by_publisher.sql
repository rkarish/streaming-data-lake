CREATE OR REPLACE VIEW iceberg.db.v_realtime_agg_hourly_funnel_by_publisher AS
SELECT
    f.window_start,
    f.publisher_id,
    dp.publisher_name,
    dp.vertical AS publisher_vertical,
    dp.tier AS publisher_tier,
    f.bid_requests,
    f.bid_responses,
    f.impressions,
    f.clicks,
    f.fill_rate,
    f.win_rate,
    f.ctr
FROM iceberg.db.hourly_funnel_by_publisher f
LEFT JOIN iceberg.db.dim_publisher dp
    ON f.publisher_id = dp.publisher_id AND dp.is_current = true;
