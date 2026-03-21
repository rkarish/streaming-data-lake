-- Flink SQL DML: Streaming inserts from Kafka into Iceberg

-- Pipeline name for deterministic job identification (used by redeploy-sql.sh)
SET 'pipeline.name' = 'adtech-ingestion';

-- Checkpoint configuration
SET 'execution.checkpointing.interval' = '60s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET 'state.checkpoints.dir' = 's3://warehouse/checkpoints/';

EXECUTE STATEMENT SET
BEGIN

-- Insert bid_requests: flatten nested arrays into Iceberg table
-- Supports both site and app traffic (mutually exclusive)
-- Filters out test publishers (negative IDs) and private IPs for clean analytics
INSERT INTO iceberg_catalog.db.bid_requests
SELECT
    `id` AS request_id,
    imp_id AS imp_id,
    imp_banner.`w` AS imp_banner_w,
    imp_banner.`h` AS imp_banner_h,
    imp_bidfloor AS imp_bidfloor,
    COALESCE(`site`.`id`, `app`.`id`) AS site_id,
    COALESCE(`site`.`domain`, `app`.`bundle`) AS site_domain,
    COALESCE(`site`.`cat`, `app`.`cat`) AS site_cat,
    COALESCE(`site`.`publisher`.`id`, `app`.`publisher`.`id`) AS publisher_id,
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
FROM kafka_bid_requests
CROSS JOIN UNNEST(`imp`) AS imp_t(`imp_id`, `imp_banner`, `imp_bidfloor`, `imp_bidfloorcur`, `imp_secure`)
WHERE
    -- Exclude test publishers
    COALESCE(`site`.`publisher`.`id`, `app`.`publisher`.`id`) > 0
    -- Exclude private IPs (RFC1918 ranges)
    AND `device`.`ip` NOT LIKE '10.%'
    AND `device`.`ip` NOT LIKE '192.168.%'
    AND `device`.`ip` NOT LIKE '172.16.%'
    AND `device`.`ip` NOT LIKE '172.17.%'
    AND `device`.`ip` NOT LIKE '172.18.%'
    AND `device`.`ip` NOT LIKE '172.19.%'
    AND `device`.`ip` NOT LIKE '172.2_.%'
    AND `device`.`ip` NOT LIKE '172.30.%'
    AND `device`.`ip` NOT LIKE '172.31.%'
    -- Ensure valid bid floor
    AND imp_bidfloor > 0;

-- Insert bid_requests_enriched: includes device classification, currency normalization, and traffic flags
-- This table captures ALL traffic (including test and invalid) for analysis
INSERT INTO iceberg_catalog.db.bid_requests_enriched
SELECT
    `id` AS request_id,
    imp_id AS imp_id,
    imp_banner.`w` AS imp_banner_w,
    imp_banner.`h` AS imp_banner_h,
    imp_bidfloor AS imp_bidfloor,
    -- Currency normalization: convert bid floor to USD using static exchange rates
    CASE imp_bidfloorcur
        WHEN 'EUR' THEN imp_bidfloor * 1.08
        WHEN 'GBP' THEN imp_bidfloor * 1.25
        WHEN 'JPY' THEN imp_bidfloor * 0.0067
        ELSE imp_bidfloor
    END AS imp_bidfloor_usd,
    imp_bidfloorcur AS imp_bidfloorcur,
    `site`.`id` AS site_id,
    `site`.`domain` AS site_domain,
    `app`.`id` AS app_id,
    `app`.`bundle` AS app_bundle,
    COALESCE(`site`.`publisher`.`id`, `app`.`publisher`.`id`) AS publisher_id,
    `device`.`devicetype` AS device_type,
    `device`.`os` AS device_os,
    `device`.`ip` AS device_ip,
    `device`.`geo`.`country` AS device_geo_country,
    `device`.`geo`.`region` AS device_geo_region,
    -- Device classification based on device type and site/app presence
    CASE
        WHEN `device`.`devicetype` = 7 THEN 'CTV'
        WHEN `device`.`devicetype` IN (1, 4) AND `app`.`id` IS NOT NULL THEN 'Mobile App'
        WHEN `device`.`devicetype` IN (1, 4) AND `site`.`id` IS NOT NULL THEN 'Mobile Web'
        WHEN `device`.`devicetype` = 2 THEN 'Desktop'
        ELSE 'Unknown'
    END AS device_category,
    `user`.`id` AS user_id,
    `at` AS auction_type,
    `cur`[1] AS currency,
    CASE WHEN `regs`.`coppa` = 1 THEN TRUE ELSE FALSE END AS is_coppa,
    CASE WHEN `regs`.`ext`.`gdpr` = 1 THEN TRUE ELSE FALSE END AS is_gdpr,
    -- Test traffic flag: negative publisher IDs indicate test traffic
    CASE
        WHEN COALESCE(`site`.`publisher`.`id`, `app`.`publisher`.`id`) < 0 THEN TRUE
        ELSE FALSE
    END AS is_test_traffic,
    -- Private IP flag: RFC1918 private IP ranges
    CASE
        WHEN `device`.`ip` LIKE '10.%'
            OR `device`.`ip` LIKE '192.168.%'
            OR `device`.`ip` LIKE '172.16.%'
            OR `device`.`ip` LIKE '172.17.%'
            OR `device`.`ip` LIKE '172.18.%'
            OR `device`.`ip` LIKE '172.19.%'
            OR `device`.`ip` LIKE '172.2_.%'
            OR `device`.`ip` LIKE '172.30.%'
            OR `device`.`ip` LIKE '172.31.%'
        THEN TRUE
        ELSE FALSE
    END AS is_private_ip,
    CAST(
        TO_TIMESTAMP(SUBSTRING(`event_timestamp`, 1, 26), 'yyyy-MM-dd''T''HH:mm:ss.SSSSSS')
        AS TIMESTAMP_LTZ(6)
    ) AS event_timestamp,
    CAST(
        TO_TIMESTAMP(SUBSTRING(`received_at`, 1, 26), 'yyyy-MM-dd''T''HH:mm:ss.SSSSSS')
        AS TIMESTAMP_LTZ(6)
    ) AS received_at
FROM kafka_bid_requests
CROSS JOIN UNNEST(`imp`) AS imp_t(`imp_id`, `imp_banner`, `imp_bidfloor`, `imp_bidfloorcur`, `imp_secure`);

-- Insert dq_rejected_events: persist rejected request-impression rows with reasons
INSERT INTO iceberg_catalog.db.dq_rejected_events
SELECT
    `id` AS request_id,
    imp_id AS imp_id,
    COALESCE(`site`.`publisher`.`id`, `app`.`publisher`.`id`) AS publisher_id,
    `device`.`ip` AS device_ip,
    CASE
        WHEN COALESCE(`site`.`publisher`.`id`, `app`.`publisher`.`id`) < 0 THEN 'TEST_PUBLISHER'
        WHEN `device`.`ip` LIKE '10.%'
            OR `device`.`ip` LIKE '192.168.%'
            OR `device`.`ip` LIKE '172.16.%'
            OR `device`.`ip` LIKE '172.17.%'
            OR `device`.`ip` LIKE '172.18.%'
            OR `device`.`ip` LIKE '172.19.%'
            OR `device`.`ip` LIKE '172.2_.%'
            OR `device`.`ip` LIKE '172.30.%'
            OR `device`.`ip` LIKE '172.31.%'
        THEN 'PRIVATE_IP'
        WHEN imp_bidfloor <= 0 THEN 'NON_POSITIVE_BIDFLOOR'
        ELSE 'UNKNOWN'
    END AS reject_reason,
    CAST(
        TO_TIMESTAMP(SUBSTRING(`event_timestamp`, 1, 26), 'yyyy-MM-dd''T''HH:mm:ss.SSSSSS')
        AS TIMESTAMP_LTZ(6)
    ) AS event_timestamp
FROM kafka_bid_requests
CROSS JOIN UNNEST(`imp`) AS imp_t(`imp_id`, `imp_banner`, `imp_bidfloor`, `imp_bidfloorcur`, `imp_secure`)
WHERE
    COALESCE(`site`.`publisher`.`id`, `app`.`publisher`.`id`) < 0
    OR `device`.`ip` LIKE '10.%'
    OR `device`.`ip` LIKE '192.168.%'
    OR `device`.`ip` LIKE '172.16.%'
    OR `device`.`ip` LIKE '172.17.%'
    OR `device`.`ip` LIKE '172.18.%'
    OR `device`.`ip` LIKE '172.19.%'
    OR `device`.`ip` LIKE '172.2_.%'
    OR `device`.`ip` LIKE '172.30.%'
    OR `device`.`ip` LIKE '172.31.%'
    OR imp_bidfloor <= 0;

-- Insert bid_responses: flatten nested seatbid[].bid[] into Iceberg table
INSERT INTO iceberg_catalog.db.bid_responses
SELECT
    `id` AS response_id,
    `ext`.`request_id` AS request_id,
    seat AS seat,
    bid_id AS bid_id,
    bid_impid AS imp_id,
    bid_price AS bid_price,
    bid_crid AS creative_id,
    bid_dealid AS deal_id,
    bid_adomain[1] AS ad_domain,
    bid_campaign_id AS campaign_id,
    bid_line_item_id AS line_item_id,
    bid_strategy_id AS strategy_id,
    bid_advertiser_id AS advertiser_id,
    bid_agency_id AS agency_id,
    `cur` AS currency,
    CAST(
        TO_TIMESTAMP(SUBSTRING(`event_timestamp`, 1, 26), 'yyyy-MM-dd''T''HH:mm:ss.SSSSSS')
        AS TIMESTAMP_LTZ(6)
    ) AS event_timestamp
FROM kafka_bid_responses
CROSS JOIN UNNEST(`seatbid`) AS seatbid_t(`seat`, `seat_bids`)
CROSS JOIN UNNEST(seat_bids) AS bid_t(`bid_id`, `bid_impid`, `bid_price`, `bid_adid`, `bid_crid`, `bid_adomain`, `bid_dealid`, `bid_w`, `bid_h`, `bid_campaign_id`, `bid_line_item_id`, `bid_strategy_id`, `bid_advertiser_id`, `bid_agency_id`);

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
