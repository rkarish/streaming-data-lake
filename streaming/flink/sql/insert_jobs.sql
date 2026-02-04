-- Flink SQL DML: Streaming insert from Kafka into Iceberg

-- Checkpoint configuration
SET 'execution.checkpointing.interval' = '60s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET 'state.checkpoints.dir' = 's3://warehouse/checkpoints/';

-- Insert bid_requests: flatten nested JSON into Iceberg table
INSERT INTO iceberg_catalog.db.bid_requests
SELECT
    `id` AS request_id,
    `imp`[1].`id` AS imp_id,
    `imp`[1].`banner`.`w` AS imp_banner_w,
    `imp`[1].`banner`.`h` AS imp_banner_h,
    `imp`[1].`bidfloor` AS imp_bidfloor,
    `site`.`id` AS site_id,
    `site`.`domain` AS site_domain,
    `site`.`cat` AS site_cat,
    `site`.`publisher`.`id` AS publisher_id,
    `device`.`devicetype` AS device_type,
    `device`.`os` AS device_os,
    `device`.`geo`.`country` AS device_geo_country,
    `device`.`geo`.`region` AS device_geo_region,
    `user`.`id` AS user_id,
    `at` AS auction_type,
    `tmax` AS tmax,
    `cur`[1] AS currency,
    CASE WHEN `regs`.`coppa` = 1 THEN TRUE ELSE FALSE END AS is_coppa,
    CASE WHEN `regs`.`ext`.`gdpr` = 1 THEN TRUE ELSE FALSE END AS is_gdpr,
    CAST(
        TO_TIMESTAMP(SUBSTRING(`event_timestamp`, 1, 26), 'yyyy-MM-dd''T''HH:mm:ss.SSSSSS')
        AS TIMESTAMP_LTZ(6)
    ) AS event_timestamp,
    CAST(
        TO_TIMESTAMP(SUBSTRING(`received_at`, 1, 26), 'yyyy-MM-dd''T''HH:mm:ss.SSSSSS')
        AS TIMESTAMP_LTZ(6)
    ) AS received_at
FROM kafka_bid_requests;
