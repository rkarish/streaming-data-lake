-- Flink SQL DML: Streaming aggregations to Iceberg tables
-- These jobs use windowing and write to upsert-enabled tables with primary keys
-- Run as a separate Flink job from the main insert_jobs.sql for independent lifecycle

-- Pipeline name for deterministic job identification (used by redeploy-sql.sh)
SET 'pipeline.name' = 'adtech-aggregation';

-- Checkpoint configuration for aggregation jobs (separate checkpoint directory)
SET 'execution.checkpointing.interval' = '60s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET 'state.checkpoints.dir' = 's3://warehouse/checkpoints/aggregations/';

-- Configure state TTL for windowed operations (24 hours, cleans up old state)
SET 'table.exec.state.ttl' = '86400000';

EXECUTE STATEMENT SET
BEGIN

-- Hourly impressions by geo: interval join with hour-bucketed aggregation
-- Joins impressions with bid_requests to get actual device geo country
-- Uses FLOOR(TO HOUR) instead of TUMBLE because time attributes lose their
-- watermark property after an interval join, preventing TUMBLE from firing.
-- The upsert sink continuously updates aggregates per (hour, country) key.
INSERT INTO iceberg_catalog.db.hourly_impressions_by_geo
SELECT
    FLOOR(imp.`event_ts` TO HOUR) AS window_start,
    br.`device_geo_country` AS device_geo_country,
    COUNT(*) AS impression_count,
    SUM(imp.`win_price`) AS total_revenue,
    AVG(imp.`win_price`) AS avg_win_price
FROM (
    SELECT
        `impression_id`,
        `request_id`,
        MAX(`win_price`) AS `win_price`,
        MAX(`event_ts`) AS `event_ts`
    FROM kafka_impressions
    GROUP BY
        `impression_id`,
        `request_id`
) imp
INNER JOIN (
    SELECT
        `id` AS request_id,
        MIN(`device`.`geo`.`country`) AS device_geo_country,
        MAX(`event_ts`) AS `event_ts`
    FROM kafka_bid_requests
    GROUP BY
        `id`
) br
    ON imp.`request_id` = br.`request_id`
    AND br.`event_ts` BETWEEN imp.`event_ts` - INTERVAL '10' SECOND AND imp.`event_ts`
GROUP BY
    FLOOR(imp.`event_ts` TO HOUR),
    br.`device_geo_country`;

-- Rolling 5-minute metrics by bidder: sliding window (1-min hop, 5-min size)
-- Provides near-real-time metrics for dashboards, updated every minute
-- Uses explicit Flink sink table with upsert-enabled for proper Iceberg writes
INSERT INTO iceberg_catalog.db.rolling_metrics_by_bidder
SELECT
    HOP_START(`event_ts`, INTERVAL '1' MINUTE, INTERVAL '5' MINUTES) AS window_start,
    HOP_END(`event_ts`, INTERVAL '1' MINUTE, INTERVAL '5' MINUTES) AS window_end,
    `bidder_id`,
    COUNT(*) AS win_count,
    SUM(`win_price`) AS revenue,
    AVG(`win_price`) AS avg_cpm
FROM (
    SELECT
        `impression_id`,
        `bidder_id`,
        MAX(`win_price`) AS `win_price`,
        MAX(`event_ts`) AS `event_ts`
    FROM kafka_impressions
    GROUP BY
        `impression_id`,
        `bidder_id`
) dedup_impressions
GROUP BY
    HOP(`event_ts`, INTERVAL '1' MINUTE, INTERVAL '5' MINUTES),
    `bidder_id`;

-- Hourly data quality metrics with duplicate breakdown by event type.
INSERT INTO iceberg_catalog.db.dq_event_quality_hourly
SELECT
    w.window_start,
    COALESCE(req.total_bid_requests, 0) AS total_bid_requests,
    COALESCE(req.unique_bid_requests, 0) AS unique_bid_requests,
    COALESCE(req.total_bid_requests, 0) - COALESCE(req.unique_bid_requests, 0) AS duplicate_bid_requests,
    CASE
        WHEN COALESCE(req.total_bid_requests, 0) > 0
        THEN CAST(COALESCE(req.total_bid_requests, 0) - COALESCE(req.unique_bid_requests, 0) AS DOUBLE)
            / CAST(COALESCE(req.total_bid_requests, 0) AS DOUBLE)
        ELSE 0.0
    END AS duplicate_bid_request_rate,
    COALESCE(resp.total_bid_responses, 0) AS total_bid_responses,
    COALESCE(resp.unique_bid_responses, 0) AS unique_bid_responses,
    COALESCE(resp.total_bid_responses, 0) - COALESCE(resp.unique_bid_responses, 0) AS duplicate_bid_responses,
    CASE
        WHEN COALESCE(resp.total_bid_responses, 0) > 0
        THEN CAST(COALESCE(resp.total_bid_responses, 0) - COALESCE(resp.unique_bid_responses, 0) AS DOUBLE)
            / CAST(COALESCE(resp.total_bid_responses, 0) AS DOUBLE)
        ELSE 0.0
    END AS duplicate_bid_response_rate,
    COALESCE(win.total_wins, 0) AS total_wins,
    COALESCE(win.unique_wins, 0) AS unique_wins,
    COALESCE(win.total_wins, 0) - COALESCE(win.unique_wins, 0) AS duplicate_wins,
    CASE
        WHEN COALESCE(win.total_wins, 0) > 0
        THEN CAST(COALESCE(win.total_wins, 0) - COALESCE(win.unique_wins, 0) AS DOUBLE)
            / CAST(COALESCE(win.total_wins, 0) AS DOUBLE)
        ELSE 0.0
    END AS duplicate_win_rate,
    COALESCE(clk.total_clicks, 0) AS total_clicks,
    COALESCE(clk.unique_clicks, 0) AS unique_clicks,
    COALESCE(clk.total_clicks, 0) - COALESCE(clk.unique_clicks, 0) AS duplicate_clicks,
    CASE
        WHEN COALESCE(clk.total_clicks, 0) > 0
        THEN CAST(COALESCE(clk.total_clicks, 0) - COALESCE(clk.unique_clicks, 0) AS DOUBLE)
            / CAST(COALESCE(clk.total_clicks, 0) AS DOUBLE)
        ELSE 0.0
    END AS duplicate_click_rate,
    COALESCE(req.invalid_bid_requests, 0) AS invalid_bid_requests,
    CASE
        WHEN COALESCE(req.total_bid_requests, 0) > 0
        THEN CAST(COALESCE(req.invalid_bid_requests, 0) AS DOUBLE)
            / CAST(COALESCE(req.total_bid_requests, 0) AS DOUBLE)
        ELSE 0.0
    END AS invalid_bid_request_rate,
    COALESCE(req.total_bid_requests, 0) + COALESCE(resp.total_bid_responses, 0)
        + COALESCE(win.total_wins, 0) + COALESCE(clk.total_clicks, 0) AS total_events_all,
    (COALESCE(req.total_bid_requests, 0) - COALESCE(req.unique_bid_requests, 0))
        + (COALESCE(resp.total_bid_responses, 0) - COALESCE(resp.unique_bid_responses, 0))
        + (COALESCE(win.total_wins, 0) - COALESCE(win.unique_wins, 0))
        + (COALESCE(clk.total_clicks, 0) - COALESCE(clk.unique_clicks, 0)) AS duplicate_events_all,
    CASE
        WHEN (COALESCE(req.total_bid_requests, 0) + COALESCE(resp.total_bid_responses, 0)
            + COALESCE(win.total_wins, 0) + COALESCE(clk.total_clicks, 0)) > 0
        THEN CAST(
            (COALESCE(req.total_bid_requests, 0) - COALESCE(req.unique_bid_requests, 0))
            + (COALESCE(resp.total_bid_responses, 0) - COALESCE(resp.unique_bid_responses, 0))
            + (COALESCE(win.total_wins, 0) - COALESCE(win.unique_wins, 0))
            + (COALESCE(clk.total_clicks, 0) - COALESCE(clk.unique_clicks, 0))
            AS DOUBLE
        ) / CAST(
            COALESCE(req.total_bid_requests, 0) + COALESCE(resp.total_bid_responses, 0)
            + COALESCE(win.total_wins, 0) + COALESCE(clk.total_clicks, 0)
            AS DOUBLE
        )
        ELSE 0.0
    END AS duplicate_rate_all
FROM (
    SELECT window_start FROM (
        SELECT FLOOR(`event_ts` TO HOUR) AS window_start FROM kafka_bid_requests GROUP BY FLOOR(`event_ts` TO HOUR)
        UNION
        SELECT FLOOR(`event_ts` TO HOUR) AS window_start FROM kafka_bid_responses GROUP BY FLOOR(`event_ts` TO HOUR)
        UNION
        SELECT FLOOR(`event_ts` TO HOUR) AS window_start FROM kafka_impressions GROUP BY FLOOR(`event_ts` TO HOUR)
        UNION
        SELECT FLOOR(`event_ts` TO HOUR) AS window_start FROM kafka_clicks GROUP BY FLOOR(`event_ts` TO HOUR)
    ) all_windows
) w
LEFT JOIN (
    SELECT
        totals.window_start,
        totals.total_bid_requests,
        totals.unique_bid_requests,
        COALESCE(invalid.invalid_bid_requests, 0) AS invalid_bid_requests
    FROM (
        SELECT
            FLOOR(`event_ts` TO HOUR) AS window_start,
            COUNT(*) AS total_bid_requests,
            COUNT(DISTINCT `id`) AS unique_bid_requests
        FROM kafka_bid_requests
        GROUP BY FLOOR(`event_ts` TO HOUR)
    ) totals
    LEFT JOIN (
        SELECT
            FLOOR(br.`event_ts` TO HOUR) AS window_start,
            COUNT(DISTINCT CASE
                WHEN COALESCE(br.`site`.`publisher`.`id`, br.`app`.`publisher`.`id`) < 0
                    OR br.`device`.`ip` LIKE '10.%'
                    OR br.`device`.`ip` LIKE '192.168.%'
                    OR br.`device`.`ip` LIKE '172.16.%'
                    OR br.`device`.`ip` LIKE '172.17.%'
                    OR br.`device`.`ip` LIKE '172.18.%'
                    OR br.`device`.`ip` LIKE '172.19.%'
                    OR br.`device`.`ip` LIKE '172.2_.%'
                    OR br.`device`.`ip` LIKE '172.30.%'
                    OR br.`device`.`ip` LIKE '172.31.%'
                    OR imp_bidfloor <= 0
                THEN br.`id`
                ELSE NULL
            END) AS invalid_bid_requests
        FROM kafka_bid_requests br
        CROSS JOIN UNNEST(br.`imp`) AS imp_t(`imp_id`, `imp_banner`, `imp_bidfloor`, `imp_bidfloorcur`, `imp_secure`)
        GROUP BY FLOOR(br.`event_ts` TO HOUR)
    ) invalid
        ON totals.window_start = invalid.window_start
) req
    ON w.window_start = req.window_start
LEFT JOIN (
    SELECT
        FLOOR(`event_ts` TO HOUR) AS window_start,
        COUNT(*) AS total_bid_responses,
        COUNT(DISTINCT `id`) AS unique_bid_responses
    FROM kafka_bid_responses
    GROUP BY FLOOR(`event_ts` TO HOUR)
) resp
    ON w.window_start = resp.window_start
LEFT JOIN (
    SELECT
        FLOOR(`event_ts` TO HOUR) AS window_start,
        COUNT(*) AS total_wins,
        COUNT(DISTINCT `impression_id`) AS unique_wins
    FROM kafka_impressions
    GROUP BY FLOOR(`event_ts` TO HOUR)
) win
    ON w.window_start = win.window_start
LEFT JOIN (
    SELECT
        FLOOR(`event_ts` TO HOUR) AS window_start,
        COUNT(*) AS total_clicks,
        COUNT(DISTINCT `click_id`) AS unique_clicks
    FROM kafka_clicks
    GROUP BY FLOOR(`event_ts` TO HOUR)
) clk
    ON w.window_start = clk.window_start;

-- Hourly auction landscape metrics by publisher.
-- Uses full seatbid[].bid[] expansion to preserve bid cardinality.
INSERT INTO iceberg_catalog.db.bid_landscape_hourly
SELECT
    FLOOR(resp.`event_ts` TO HOUR) AS window_start,
    COALESCE(br.`site`.`publisher`.`id`, br.`app`.`publisher`.`id`, 0) AS publisher_id,
    COUNT(DISTINCT resp.`request_id`) AS request_count,
    COUNT(*) AS total_bids,
    CASE
        WHEN COUNT(DISTINCT resp.`request_id`) > 0
        THEN CAST(COUNT(*) AS DOUBLE) / CAST(COUNT(DISTINCT resp.`request_id`) AS DOUBLE)
        ELSE 0.0
    END AS bids_per_request,
    AVG(resp.`bid_price`) AS avg_bid_price,
    MAX(resp.`bid_price`) AS max_bid_price
FROM (
    SELECT
        bid_id,
        request_id,
        MAX(bid_price) AS bid_price,
        MAX(event_ts) AS event_ts
    FROM (
        SELECT
            resp.`ext`.`request_id` AS request_id,
            bid_id,
            bid_price,
            resp.`event_ts` AS event_ts
        FROM kafka_bid_responses resp
        CROSS JOIN UNNEST(resp.`seatbid`) AS seatbid_t(`seat`, `seat_bids`)
        CROSS JOIN UNNEST(seat_bids) AS bid_t(`bid_id`, `bid_impid`, `bid_price`, `bid_adid`, `bid_crid`, `bid_adomain`, `bid_dealid`, `bid_w`, `bid_h`, `bid_campaign_id`, `bid_line_item_id`, `bid_strategy_id`, `bid_advertiser_id`, `bid_agency_id`)
    ) expanded_resp
    GROUP BY
        bid_id,
        request_id
) resp
LEFT JOIN kafka_bid_requests br
    ON br.`id` = resp.`request_id`
    AND br.`event_ts` BETWEEN resp.`event_ts` - INTERVAL '10' SECOND AND resp.`event_ts` + INTERVAL '5' SECOND
GROUP BY
    FLOOR(resp.`event_ts` TO HOUR),
    COALESCE(br.`site`.`publisher`.`id`, br.`app`.`publisher`.`id`, 0);

-- One-minute serving metrics for low-latency monitoring.
INSERT INTO iceberg_catalog.db.realtime_serving_metrics_1m
SELECT
    FLOOR(imp.`event_ts` TO MINUTE) AS window_start,
    imp.`bidder_id`,
    COUNT(*) AS impressions,
    COUNT(DISTINCT cl.`click_id`) AS clicks,
    SUM(imp.`win_price`) AS revenue,
    CASE
        WHEN COUNT(*) > 0
        THEN CAST(COUNT(DISTINCT cl.`click_id`) AS DOUBLE) / CAST(COUNT(*) AS DOUBLE)
        ELSE 0.0
    END AS ctr
FROM (
    SELECT
        `impression_id`,
        `bidder_id`,
        MAX(`win_price`) AS `win_price`,
        MAX(`event_ts`) AS `event_ts`
    FROM kafka_impressions
    GROUP BY
        `impression_id`,
        `bidder_id`
) imp
LEFT JOIN (
    SELECT
        `click_id`,
        `impression_id`,
        MAX(`event_ts`) AS `event_ts`
    FROM kafka_clicks
    GROUP BY
        `click_id`,
        `impression_id`
) cl
    ON imp.`impression_id` = cl.`impression_id`
    AND cl.`event_ts` BETWEEN imp.`event_ts` AND imp.`event_ts` + INTERVAL '60' SECOND
GROUP BY
    FLOOR(imp.`event_ts` TO MINUTE),
    imp.`bidder_id`;

END;
