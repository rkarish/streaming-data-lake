CREATE OR REPLACE VIEW iceberg.db.v_agg_bid_landscape AS
SELECT
    date_trunc('hour', resp.event_timestamp) AS hour_start,
    br.publisher_id,
    dp.publisher_name,
    dp.vertical AS publisher_vertical,
    COUNT(DISTINCT resp.request_id) AS request_count,
    COUNT(*) AS total_bids,
    CASE
        WHEN COUNT(DISTINCT resp.request_id) > 0
        THEN CAST(COUNT(*) AS DOUBLE) / CAST(COUNT(DISTINCT resp.request_id) AS DOUBLE)
        ELSE 0.0
    END AS bids_per_request,
    AVG(resp.bid_price) AS avg_bid_price,
    MAX(resp.bid_price) AS max_bid_price
FROM iceberg.db.bid_responses resp
LEFT JOIN iceberg.db.bid_requests br
    ON resp.request_id = br.request_id
LEFT JOIN iceberg.db.dim_publisher dp
    ON br.publisher_id = dp.publisher_id AND dp.is_current = true
GROUP BY
    date_trunc('hour', resp.event_timestamp),
    br.publisher_id, dp.publisher_name, dp.vertical;
