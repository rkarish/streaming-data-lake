-- Trino views that join fact tables with SCD Type 2 dimension tables.
-- All dimension joins use is_current = true for the latest dimension state.
-- For time-travel reporting, query fact tables directly and join on
-- valid_from/valid_to ranges instead.

-- Bid requests enriched with publisher and geo dimensions
CREATE OR REPLACE VIEW iceberg.db.v_bid_requests AS
SELECT
    br.request_id,
    br.imp_id,
    br.imp_banner_w,
    br.imp_banner_h,
    br.imp_bidfloor,
    br.site_id,
    br.site_domain,
    br.publisher_id,
    dp.publisher_name,
    dp.vertical AS publisher_vertical,
    dp.tier AS publisher_tier,
    br.device_type,
    ddt.device_type_name,
    ddt.form_factor,
    ddt.is_mobile,
    br.device_os,
    dos.os_vendor,
    br.device_geo_country,
    br.device_geo_region,
    dg.country_name,
    dg.region_name,
    dg.timezone,
    br.user_id,
    br.auction_type,
    br.tmax,
    br.currency,
    br.is_coppa,
    br.is_gdpr,
    br.event_timestamp,
    br.received_at
FROM iceberg.db.bid_requests br
LEFT JOIN iceberg.db.dim_publisher dp
    ON br.publisher_id = dp.publisher_id AND dp.is_current = true
LEFT JOIN iceberg.db.dim_device_type ddt
    ON br.device_type = ddt.device_type_code AND ddt.is_current = true
LEFT JOIN iceberg.db.dim_device_os dos
    ON br.device_os = dos.os_name AND dos.is_current = true
LEFT JOIN iceberg.db.dim_geo dg
    ON br.device_geo_country = dg.country_code
    AND br.device_geo_region = dg.region_code
    AND dg.is_current = true;

-- Bid responses with full DSP hierarchy and deal dimensions
CREATE OR REPLACE VIEW iceberg.db.v_bid_responses AS
SELECT
    br.response_id,
    br.request_id,
    br.seat,
    db.bidder_name,
    br.bid_id,
    br.imp_id,
    br.bid_price,
    br.creative_id,
    dc.creative_name,
    dc.format AS creative_format,
    dc.width AS creative_width,
    dc.height AS creative_height,
    br.strategy_id,
    ds.strategy_name,
    ds.targeting_type,
    ds.channel,
    br.line_item_id,
    dl.line_item_name,
    dl.budget,
    dl.bid_strategy,
    br.campaign_id,
    dcmp.campaign_name,
    dcmp.objective AS campaign_objective,
    br.advertiser_id,
    da.advertiser_name,
    da.industry,
    br.agency_id,
    dag.agency_name,
    dag.holding_company,
    br.deal_id,
    dd.deal_name,
    dd.deal_type,
    dd.floor_price AS deal_floor_price,
    br.ad_domain,
    br.currency,
    br.event_timestamp
FROM iceberg.db.bid_responses br
LEFT JOIN iceberg.db.dim_bidder db
    ON br.seat = db.bidder_id AND db.is_current = true
LEFT JOIN iceberg.db.dim_creative dc
    ON br.creative_id = dc.creative_id AND dc.is_current = true
LEFT JOIN iceberg.db.dim_strategy ds
    ON br.strategy_id = ds.strategy_id AND ds.is_current = true
LEFT JOIN iceberg.db.dim_line_item dl
    ON br.line_item_id = dl.line_item_id AND dl.is_current = true
LEFT JOIN iceberg.db.dim_campaign dcmp
    ON br.campaign_id = dcmp.campaign_id AND dcmp.is_current = true
LEFT JOIN iceberg.db.dim_advertiser da
    ON br.advertiser_id = da.advertiser_id AND da.is_current = true
LEFT JOIN iceberg.db.dim_agency dag
    ON br.agency_id = dag.agency_id AND dag.is_current = true
LEFT JOIN iceberg.db.dim_deal dd
    ON br.deal_id = dd.deal_id AND dd.is_current = true;

-- Impressions with bidder and creative dimensions
CREATE OR REPLACE VIEW iceberg.db.v_impressions AS
SELECT
    imp.impression_id,
    imp.request_id,
    imp.response_id,
    imp.imp_id,
    imp.bidder_id,
    db.bidder_name,
    imp.win_price,
    imp.win_currency,
    imp.creative_id,
    dc.creative_name,
    dc.format AS creative_format,
    imp.ad_domain,
    imp.event_timestamp
FROM iceberg.db.impressions imp
LEFT JOIN iceberg.db.dim_bidder db
    ON imp.bidder_id = db.bidder_id AND db.is_current = true
LEFT JOIN iceberg.db.dim_creative dc
    ON imp.creative_id = dc.creative_id AND dc.is_current = true;

-- Clicks with bidder and creative dimensions
CREATE OR REPLACE VIEW iceberg.db.v_clicks AS
SELECT
    cl.click_id,
    cl.request_id,
    cl.impression_id,
    cl.imp_id,
    cl.bidder_id,
    db.bidder_name,
    cl.creative_id,
    dc.creative_name,
    dc.format AS creative_format,
    cl.click_url,
    cl.event_timestamp
FROM iceberg.db.clicks cl
LEFT JOIN iceberg.db.dim_bidder db
    ON cl.bidder_id = db.bidder_id AND db.is_current = true
LEFT JOIN iceberg.db.dim_creative dc
    ON cl.creative_id = dc.creative_id AND dc.is_current = true;

-- Hourly funnel by publisher with publisher dimension
CREATE OR REPLACE VIEW iceberg.db.v_hourly_funnel_by_publisher AS
SELECT
    f.window_start,
    f.publisher_id,
    dp.publisher_name,
    dp.vertical AS publisher_vertical,
    dp.tier AS publisher_tier,
    f.bid_requests,
    f.bid_responses,
    f.impressions,
    f.clicks,
    f.fill_rate,
    f.win_rate,
    f.ctr
FROM iceberg.db.hourly_funnel_by_publisher f
LEFT JOIN iceberg.db.dim_publisher dp
    ON f.publisher_id = dp.publisher_id AND dp.is_current = true;

-- Rolling metrics by bidder with bidder dimension
CREATE OR REPLACE VIEW iceberg.db.v_rolling_metrics_by_bidder AS
SELECT
    m.window_start,
    m.window_end,
    m.bidder_id,
    db.bidder_name,
    db.domain AS bidder_domain,
    m.win_count,
    m.revenue,
    m.avg_cpm
FROM iceberg.db.rolling_metrics_by_bidder m
LEFT JOIN iceberg.db.dim_bidder db
    ON m.bidder_id = db.bidder_id AND db.is_current = true;

-- Bid landscape hourly with publisher dimension
CREATE OR REPLACE VIEW iceberg.db.v_bid_landscape_hourly AS
SELECT
    bl.window_start,
    bl.publisher_id,
    dp.publisher_name,
    dp.vertical AS publisher_vertical,
    bl.request_count,
    bl.total_bids,
    bl.bids_per_request,
    bl.avg_bid_price,
    bl.max_bid_price
FROM iceberg.db.bid_landscape_hourly bl
LEFT JOIN iceberg.db.dim_publisher dp
    ON bl.publisher_id = dp.publisher_id AND dp.is_current = true;

-- Realtime serving metrics with bidder dimension
CREATE OR REPLACE VIEW iceberg.db.v_realtime_serving_metrics_1m AS
SELECT
    m.window_start,
    m.bidder_id,
    db.bidder_name,
    m.impressions,
    m.clicks,
    m.revenue,
    m.ctr
FROM iceberg.db.realtime_serving_metrics_1m m
LEFT JOIN iceberg.db.dim_bidder db
    ON m.bidder_id = db.bidder_id AND db.is_current = true;

-- Full funnel: row-level join across all 4 stages with all dimensions.
-- LEFT JOINs preserve rows at each stage so drop-off is visible:
--   request with no response = no fill
--   response with no impression = no win
--   impression with no click = no click-through
CREATE OR REPLACE VIEW iceberg.db.v_full_funnel AS
SELECT
    -- Request fields
    br.request_id,
    br.imp_id,
    br.imp_banner_w,
    br.imp_banner_h,
    br.imp_bidfloor,
    br.site_id,
    br.site_domain,
    br.publisher_id,
    dp.publisher_name,
    dp.vertical AS publisher_vertical,
    dp.tier AS publisher_tier,
    br.device_type,
    ddt.device_type_name,
    ddt.is_mobile,
    br.device_os,
    dos.os_vendor,
    br.device_geo_country,
    br.device_geo_region,
    dg.country_name,
    dg.region_name,
    br.user_id,
    br.auction_type,
    br.currency,
    br.is_coppa,
    br.is_gdpr,
    br.event_timestamp AS request_timestamp,
    -- Response fields (NULL if no response)
    resp.response_id,
    resp.seat,
    dbid.bidder_name,
    resp.bid_price,
    resp.creative_id,
    dc.creative_name,
    dc.format AS creative_format,
    resp.strategy_id,
    ds.strategy_name,
    ds.targeting_type,
    resp.line_item_id,
    dl.line_item_name,
    resp.campaign_id,
    dcmp.campaign_name,
    dcmp.objective AS campaign_objective,
    resp.advertiser_id,
    da.advertiser_name,
    da.industry,
    resp.agency_id,
    dag.agency_name,
    dag.holding_company,
    resp.deal_id,
    dd.deal_name,
    dd.deal_type,
    resp.ad_domain,
    resp.event_timestamp AS response_timestamp,
    -- Impression fields (NULL if no win)
    imp.impression_id,
    imp.win_price,
    imp.win_currency,
    imp.event_timestamp AS impression_timestamp,
    -- Click fields (NULL if no click)
    cl.click_id,
    cl.click_url,
    cl.event_timestamp AS click_timestamp,
    -- Funnel stage flags
    CASE WHEN resp.response_id IS NOT NULL THEN true ELSE false END AS has_response,
    CASE WHEN imp.impression_id IS NOT NULL THEN true ELSE false END AS has_impression,
    CASE WHEN cl.click_id IS NOT NULL THEN true ELSE false END AS has_click
FROM iceberg.db.bid_requests br
-- Stage 2: Response
LEFT JOIN iceberg.db.bid_responses resp
    ON br.request_id = resp.request_id
-- Stage 3: Impression
LEFT JOIN iceberg.db.impressions imp
    ON resp.response_id = imp.response_id
-- Stage 4: Click
LEFT JOIN iceberg.db.clicks cl
    ON imp.impression_id = cl.impression_id
-- Request dimensions
LEFT JOIN iceberg.db.dim_publisher dp
    ON br.publisher_id = dp.publisher_id AND dp.is_current = true
LEFT JOIN iceberg.db.dim_device_type ddt
    ON br.device_type = ddt.device_type_code AND ddt.is_current = true
LEFT JOIN iceberg.db.dim_device_os dos
    ON br.device_os = dos.os_name AND dos.is_current = true
LEFT JOIN iceberg.db.dim_geo dg
    ON br.device_geo_country = dg.country_code
    AND br.device_geo_region = dg.region_code
    AND dg.is_current = true
-- Response dimensions (DSP hierarchy)
LEFT JOIN iceberg.db.dim_bidder dbid
    ON resp.seat = dbid.bidder_id AND dbid.is_current = true
LEFT JOIN iceberg.db.dim_creative dc
    ON resp.creative_id = dc.creative_id AND dc.is_current = true
LEFT JOIN iceberg.db.dim_strategy ds
    ON resp.strategy_id = ds.strategy_id AND ds.is_current = true
LEFT JOIN iceberg.db.dim_line_item dl
    ON resp.line_item_id = dl.line_item_id AND dl.is_current = true
LEFT JOIN iceberg.db.dim_campaign dcmp
    ON resp.campaign_id = dcmp.campaign_id AND dcmp.is_current = true
LEFT JOIN iceberg.db.dim_advertiser da
    ON resp.advertiser_id = da.advertiser_id AND da.is_current = true
LEFT JOIN iceberg.db.dim_agency dag
    ON resp.agency_id = dag.agency_id AND dag.is_current = true
LEFT JOIN iceberg.db.dim_deal dd
    ON resp.deal_id = dd.deal_id AND dd.is_current = true;
