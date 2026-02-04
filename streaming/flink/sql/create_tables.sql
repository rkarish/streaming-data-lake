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
    `received_at` STRING
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
    `event_timestamp` STRING
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
    `event_timestamp` STRING
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
    `event_timestamp` STRING
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
