-- Flink SQL DML: Streaming aggregations to Iceberg tables
-- These jobs use windowing and write to upsert-enabled tables with primary keys
-- Run as a separate Flink job from the main insert_jobs.sql for independent lifecycle

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
INSERT INTO iceberg_hourly_impressions_by_geo
SELECT
    FLOOR(imp.`event_ts` TO HOUR) AS window_start,
    br.`device`.`geo`.`country` AS device_geo_country,
    COUNT(*) AS impression_count,
    SUM(imp.`win_price`) AS total_revenue,
    AVG(imp.`win_price`) AS avg_win_price
FROM kafka_impressions imp
INNER JOIN kafka_bid_requests br
    ON imp.`request_id` = br.`id`
    AND br.`event_ts` BETWEEN imp.`event_ts` - INTERVAL '10' SECOND AND imp.`event_ts`
GROUP BY
    FLOOR(imp.`event_ts` TO HOUR),
    br.`device`.`geo`.`country`;

-- Rolling 5-minute metrics by bidder: sliding window (1-min hop, 5-min size)
-- Provides near-real-time metrics for dashboards, updated every minute
-- Uses explicit Flink sink table with upsert-enabled for proper Iceberg writes
INSERT INTO iceberg_rolling_metrics_by_bidder
SELECT
    HOP_START(`event_ts`, INTERVAL '1' MINUTE, INTERVAL '5' MINUTES) AS window_start,
    HOP_END(`event_ts`, INTERVAL '1' MINUTE, INTERVAL '5' MINUTES) AS window_end,
    `bidder_id`,
    COUNT(*) AS win_count,
    SUM(`win_price`) AS revenue,
    AVG(`win_price`) AS avg_cpm
FROM kafka_impressions
GROUP BY
    HOP(`event_ts`, INTERVAL '1' MINUTE, INTERVAL '5' MINUTES),
    `bidder_id`;

END;
