CREATE OR REPLACE VIEW iceberg.db.v_agg_funnel_by_publisher AS
SELECT
    date_trunc('hour', br.event_timestamp) AS hour_start,
    br.publisher_id,
    dp.publisher_name,
    dp.vertical AS publisher_vertical,
    dp.tier AS publisher_tier,
    COUNT(DISTINCT br.request_id) AS bid_requests,
    COUNT(DISTINCT resp.response_id) AS bid_responses,
    COUNT(DISTINCT imp.impression_id) AS impressions,
    COUNT(DISTINCT cl.click_id) AS clicks,
    CASE
        WHEN COUNT(DISTINCT br.request_id) > 0
        THEN CAST(COUNT(DISTINCT resp.response_id) AS DOUBLE)
            / CAST(COUNT(DISTINCT br.request_id) AS DOUBLE)
        ELSE 0.0
    END AS fill_rate,
    CASE
        WHEN COUNT(DISTINCT resp.response_id) > 0
        THEN CAST(COUNT(DISTINCT imp.impression_id) AS DOUBLE)
            / CAST(COUNT(DISTINCT resp.response_id) AS DOUBLE)
        ELSE 0.0
    END AS win_rate,
    CASE
        WHEN COUNT(DISTINCT imp.impression_id) > 0
        THEN CAST(COUNT(DISTINCT cl.click_id) AS DOUBLE)
            / CAST(COUNT(DISTINCT imp.impression_id) AS DOUBLE)
        ELSE 0.0
    END AS ctr
FROM iceberg.db.bid_requests br
LEFT JOIN iceberg.db.bid_responses resp ON br.request_id = resp.request_id
LEFT JOIN iceberg.db.impressions imp ON resp.response_id = imp.response_id
LEFT JOIN iceberg.db.clicks cl ON imp.impression_id = cl.impression_id
LEFT JOIN iceberg.db.dim_publisher dp
    ON br.publisher_id = dp.publisher_id AND dp.is_current = true
GROUP BY
    date_trunc('hour', br.event_timestamp),
    br.publisher_id, dp.publisher_name, dp.vertical, dp.tier;
