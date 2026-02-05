-- Flink SQL DDL: Register catalogs and source tables

-- 1. Register Iceberg REST catalog
CREATE CATALOG iceberg_catalog WITH (
    'type' = 'iceberg',
    'catalog-type' = 'rest',
    'uri' = 'http://iceberg-rest:8181',
    'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
    's3.endpoint' = 'http://minio:9000',
    's3.path-style-access' = 'true',
    'warehouse' = 's3://warehouse/'
);

-- 2. Kafka source table for bid_requests (OpenRTB 2.6 JSON)
CREATE TEMPORARY TABLE kafka_bid_requests (
    `id` STRING,
    `imp` ARRAY<ROW<
        `id` STRING,
        `banner` ROW<`w` INT, `h` INT, `pos` INT>,
        `bidfloor` DOUBLE,
        `bidfloorcur` STRING,
        `secure` INT
    >>,
    `site` ROW<
        `id` STRING,
        `domain` STRING,
        `cat` ARRAY<STRING>,
        `page` STRING,
        `publisher` ROW<`id` STRING, `name` STRING>
    >,
    `app` ROW<
        `id` STRING,
        `bundle` STRING,
        `storeurl` STRING,
        `cat` ARRAY<STRING>,
        `publisher` ROW<`id` STRING, `name` STRING>
    >,
    `device` ROW<
        `ua` STRING,
        `ip` STRING,
        `geo` ROW<
            `lat` DOUBLE,
            `lon` DOUBLE,
            `country` STRING,
            `region` STRING
        >,
        `devicetype` INT,
        `os` STRING,
        `osv` STRING
    >,
    `user` ROW<
        `id` STRING,
        `buyeruid` STRING
    >,
    `at` INT,
    `tmax` INT,
    `cur` ARRAY<STRING>,
    `source` ROW<
        `fd` INT,
        `tid` STRING
    >,
    `regs` ROW<
        `coppa` INT,
        `ext` ROW<`gdpr` INT>
    >,
    `event_timestamp` STRING,
    `received_at` STRING,
    -- Computed column: parse ISO timestamp string to TIMESTAMP(3)
    `event_ts` AS TO_TIMESTAMP(SUBSTRING(`event_timestamp`, 1, 26), 'yyyy-MM-dd''T''HH:mm:ss.SSSSSS'),
    -- Watermark for event-time windowing (5 second tolerance for late data)
    WATERMARK FOR `event_ts` AS `event_ts` - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'bid-requests',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'flink-bid-requests',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
);

-- 3. Kafka source table for bid_responses (OpenRTB 2.6 JSON)
CREATE TEMPORARY TABLE kafka_bid_responses (
    `id` STRING,
    `seatbid` ARRAY<ROW<
        `seat` STRING,
        `bid` ARRAY<ROW<
            `id` STRING,
            `impid` STRING,
            `price` DOUBLE,
            `adid` STRING,
            `crid` STRING,
            `adomain` ARRAY<STRING>,
            `dealid` STRING,
            `w` INT,
            `h` INT
        >>
    >>,
    `bidid` STRING,
    `cur` STRING,
    `ext` ROW<`request_id` STRING>,
    `event_timestamp` STRING,
    -- Computed column: parse ISO timestamp string to TIMESTAMP(3)
    `event_ts` AS TO_TIMESTAMP(SUBSTRING(`event_timestamp`, 1, 26), 'yyyy-MM-dd''T''HH:mm:ss.SSSSSS'),
    -- Watermark for event-time windowing (5 second tolerance for late data)
    WATERMARK FOR `event_ts` AS `event_ts` - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'bid-responses',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'flink-bid-responses',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
);

-- 4. Kafka source table for impressions (flat JSON)
-- Includes event_ts computed column and watermark for window aggregations
CREATE TEMPORARY TABLE kafka_impressions (
    `impression_id` STRING,
    `request_id` STRING,
    `response_id` STRING,
    `imp_id` STRING,
    `bidder_id` STRING,
    `win_price` DOUBLE,
    `win_currency` STRING,
    `creative_id` STRING,
    `ad_domain` STRING,
    `event_timestamp` STRING,
    -- Computed column: parse ISO timestamp string to TIMESTAMP(3)
    `event_ts` AS TO_TIMESTAMP(SUBSTRING(`event_timestamp`, 1, 26), 'yyyy-MM-dd''T''HH:mm:ss.SSSSSS'),
    -- Watermark for event-time windowing (5 second tolerance for late data)
    WATERMARK FOR `event_ts` AS `event_ts` - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'impressions',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'flink-impressions',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
);

-- 5. Kafka source table for clicks (flat JSON)
CREATE TEMPORARY TABLE kafka_clicks (
    `click_id` STRING,
    `request_id` STRING,
    `impression_id` STRING,
    `imp_id` STRING,
    `bidder_id` STRING,
    `creative_id` STRING,
    `click_url` STRING,
    `event_timestamp` STRING,
    -- Computed column: parse ISO timestamp string to TIMESTAMP(3)
    `event_ts` AS TO_TIMESTAMP(SUBSTRING(`event_timestamp`, 1, 26), 'yyyy-MM-dd''T''HH:mm:ss.SSSSSS'),
    -- Watermark for event-time windowing (5 second tolerance for late data)
    WATERMARK FOR `event_ts` AS `event_ts` - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'clicks',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'flink-clicks',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
);

-- 6. Iceberg sink table for hourly_impressions_by_geo (upsert mode)
-- Explicit sink definition required for Flink-Iceberg upsert writes
-- PK: (window_start, device_geo_country) matches Iceberg identifier-field-ids
CREATE TABLE iceberg_hourly_impressions_by_geo (
    `window_start` TIMESTAMP(3),
    `device_geo_country` STRING,
    `impression_count` BIGINT,
    `total_revenue` DOUBLE,
    `avg_win_price` DOUBLE,
    PRIMARY KEY (`window_start`, `device_geo_country`) NOT ENFORCED
) WITH (
    'connector' = 'iceberg',
    'catalog-type' = 'rest',
    'uri' = 'http://iceberg-rest:8181',
    'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
    's3.endpoint' = 'http://minio:9000',
    's3.path-style-access' = 'true',
    'warehouse' = 's3://warehouse/',
    'catalog-name' = 'iceberg_catalog',
    'catalog-database' = 'db',
    'catalog-table' = 'hourly_impressions_by_geo',
    'upsert-enabled' = 'true'
);

-- 7. Iceberg sink table for rolling_metrics_by_bidder (upsert mode)
-- Explicit sink definition required for Flink-Iceberg upsert writes
-- PK: (window_start, bidder_id) matches Iceberg identifier-field-ids
CREATE TABLE iceberg_rolling_metrics_by_bidder (
    `window_start` TIMESTAMP(3),
    `window_end` TIMESTAMP(3),
    `bidder_id` STRING,
    `win_count` BIGINT,
    `revenue` DOUBLE,
    `avg_cpm` DOUBLE,
    PRIMARY KEY (`window_start`, `bidder_id`) NOT ENFORCED
) WITH (
    'connector' = 'iceberg',
    'catalog-type' = 'rest',
    'uri' = 'http://iceberg-rest:8181',
    'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
    's3.endpoint' = 'http://minio:9000',
    's3.path-style-access' = 'true',
    'warehouse' = 's3://warehouse/',
    'catalog-name' = 'iceberg_catalog',
    'catalog-database' = 'db',
    'catalog-table' = 'rolling_metrics_by_bidder',
    'upsert-enabled' = 'true'
);

-- 8. Iceberg sink table for hourly_funnel_by_publisher (upsert mode)
-- Funnel metrics: bid_requests -> bid_responses -> impressions -> clicks
-- PK: (window_start, publisher_id) matches Iceberg identifier-field-ids
CREATE TABLE iceberg_hourly_funnel_by_publisher (
    `window_start` TIMESTAMP(3),
    `publisher_id` STRING,
    `bid_requests` BIGINT,
    `bid_responses` BIGINT,
    `impressions` BIGINT,
    `clicks` BIGINT,
    `fill_rate` DOUBLE,
    `win_rate` DOUBLE,
    `ctr` DOUBLE,
    PRIMARY KEY (`window_start`, `publisher_id`) NOT ENFORCED
) WITH (
    'connector' = 'iceberg',
    'catalog-type' = 'rest',
    'uri' = 'http://iceberg-rest:8181',
    'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
    's3.endpoint' = 'http://minio:9000',
    's3.path-style-access' = 'true',
    'warehouse' = 's3://warehouse/',
    'catalog-name' = 'iceberg_catalog',
    'catalog-database' = 'db',
    'catalog-table' = 'hourly_funnel_by_publisher',
    'upsert-enabled' = 'true'
);
