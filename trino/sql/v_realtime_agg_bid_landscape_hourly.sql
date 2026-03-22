CREATE OR REPLACE VIEW iceberg.db.v_realtime_agg_bid_landscape_hourly AS
SELECT
    bl.window_start,
    bl.publisher_id,
    dp.publisher_name,
    dp.vertical AS publisher_vertical,
    bl.request_count,
    bl.total_bids,
    bl.bids_per_request,
    bl.avg_bid_price,
    bl.max_bid_price
FROM iceberg.db.bid_landscape_hourly bl
LEFT JOIN iceberg.db.dim_publisher dp
    ON bl.publisher_id = dp.publisher_id AND dp.is_current = true;
