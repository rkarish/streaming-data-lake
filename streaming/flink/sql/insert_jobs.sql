-- Flink SQL DML: Streaming inserts from Kafka into Iceberg

-- Checkpoint configuration
SET 'execution.checkpointing.interval' = '60s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET 'state.checkpoints.dir' = 's3://warehouse/checkpoints/';

EXECUTE STATEMENT SET
BEGIN

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

-- Insert bid_responses: flatten nested seatbid[].bid[] into Iceberg table
INSERT INTO iceberg_catalog.db.bid_responses
SELECT
    `id` AS response_id,
    `ext`.`request_id` AS request_id,
    `seatbid`[1].`seat` AS seat,
    `seatbid`[1].`bid`[1].`id` AS bid_id,
    `seatbid`[1].`bid`[1].`impid` AS imp_id,
    `seatbid`[1].`bid`[1].`price` AS bid_price,
    `seatbid`[1].`bid`[1].`crid` AS creative_id,
    `seatbid`[1].`bid`[1].`dealid` AS deal_id,
    `seatbid`[1].`bid`[1].`adomain`[1] AS ad_domain,
    `cur` AS currency,
    CAST(
        TO_TIMESTAMP(SUBSTRING(`event_timestamp`, 1, 26), 'yyyy-MM-dd''T''HH:mm:ss.SSSSSS')
        AS TIMESTAMP_LTZ(6)
    ) AS event_timestamp
FROM kafka_bid_responses;

-- Insert impressions: flat structure, 1:1 mapping
INSERT INTO iceberg_catalog.db.impressions
SELECT
    `impression_id`,
    `request_id`,
    `response_id`,
    `imp_id`,
    `bidder_id`,
    `win_price`,
    `win_currency`,
    `creative_id`,
    `ad_domain`,
    CAST(
        TO_TIMESTAMP(SUBSTRING(`event_timestamp`, 1, 26), 'yyyy-MM-dd''T''HH:mm:ss.SSSSSS')
        AS TIMESTAMP_LTZ(6)
    ) AS event_timestamp
FROM kafka_impressions;

-- Insert clicks: flat structure, 1:1 mapping
INSERT INTO iceberg_catalog.db.clicks
SELECT
    `click_id`,
    `request_id`,
    `impression_id`,
    `imp_id`,
    `bidder_id`,
    `creative_id`,
    `click_url`,
    CAST(
        TO_TIMESTAMP(SUBSTRING(`event_timestamp`, 1, 26), 'yyyy-MM-dd''T''HH:mm:ss.SSSSSS')
        AS TIMESTAMP_LTZ(6)
    ) AS event_timestamp
FROM kafka_clicks;

END;
