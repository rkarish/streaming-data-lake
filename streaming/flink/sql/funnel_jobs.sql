-- Flink SQL DML: Funnel metrics aggregation via 4-way interval join
-- Joins bid_requests -> bid_responses -> impressions -> clicks to compute
-- full-funnel conversion metrics per publisher per hour.
-- Run as a separate Flink job for independent lifecycle and checkpointing.

-- Checkpoint configuration for funnel jobs (separate checkpoint directory)
SET 'execution.checkpointing.interval' = '60s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET 'state.checkpoints.dir' = 's3://warehouse/checkpoints/funnel/';

-- Configure state TTL for interval joins (24 hours, cleans up old state)
SET 'table.exec.state.ttl' = '86400000';

-- 4-way LEFT interval join: bid_requests -> bid_responses -> impressions -> clicks
-- Interval bounds reflect the expected latency between each stage:
--   request -> response: within 5 seconds
--   response -> impression: within 10 seconds
--   impression -> click: within 60 seconds
-- Uses FLOOR(TO HOUR) instead of TUMBLE because time attributes lose their
-- watermark property after interval joins, preventing TUMBLE from firing.
-- The upsert sink continuously updates funnel metrics per (hour, publisher) key.
INSERT INTO iceberg_hourly_funnel_by_publisher
SELECT
    FLOOR(br.`event_ts` TO HOUR) AS window_start,
    COALESCE(br.`site`.`publisher`.`id`, br.`app`.`publisher`.`id`) AS publisher_id,
    COUNT(DISTINCT br.`id`) AS bid_requests,
    COUNT(DISTINCT resp.`id`) AS bid_responses,
    COUNT(DISTINCT imp.`impression_id`) AS impressions,
    COUNT(DISTINCT cl.`click_id`) AS clicks,
    -- Fill rate: fraction of requests that received a response
    CASE
        WHEN COUNT(DISTINCT br.`id`) > 0
        THEN CAST(COUNT(DISTINCT resp.`id`) AS DOUBLE) / CAST(COUNT(DISTINCT br.`id`) AS DOUBLE)
        ELSE 0.0
    END AS fill_rate,
    -- Win rate: fraction of responses that won an impression
    CASE
        WHEN COUNT(DISTINCT resp.`id`) > 0
        THEN CAST(COUNT(DISTINCT imp.`impression_id`) AS DOUBLE) / CAST(COUNT(DISTINCT resp.`id`) AS DOUBLE)
        ELSE 0.0
    END AS win_rate,
    -- CTR: fraction of impressions that received a click
    CASE
        WHEN COUNT(DISTINCT imp.`impression_id`) > 0
        THEN CAST(COUNT(DISTINCT cl.`click_id`) AS DOUBLE) / CAST(COUNT(DISTINCT imp.`impression_id`) AS DOUBLE)
        ELSE 0.0
    END AS ctr
FROM kafka_bid_requests br
LEFT JOIN kafka_bid_responses resp
    ON br.`id` = resp.`ext`.`request_id`
    AND resp.`event_ts` BETWEEN br.`event_ts` AND br.`event_ts` + INTERVAL '5' SECOND
LEFT JOIN kafka_impressions imp
    ON resp.`id` = imp.`response_id`
    AND imp.`event_ts` BETWEEN resp.`event_ts` AND resp.`event_ts` + INTERVAL '10' SECOND
LEFT JOIN kafka_clicks cl
    ON imp.`impression_id` = cl.`impression_id`
    AND cl.`event_ts` BETWEEN imp.`event_ts` AND imp.`event_ts` + INTERVAL '60' SECOND
GROUP BY
    FLOOR(br.`event_ts` TO HOUR),
    COALESCE(br.`site`.`publisher`.`id`, br.`app`.`publisher`.`id`);
