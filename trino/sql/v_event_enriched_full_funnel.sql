-- Full funnel: row-level join across all 4 stages with all dimensions.
-- LEFT JOINs preserve rows at each stage so drop-off is visible:
--   request with no response = no fill
--   response with no impression = no win
--   impression with no click = no click-through
CREATE OR REPLACE VIEW iceberg.db.v_event_enriched_full_funnel AS
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
