CREATE OR REPLACE VIEW iceberg.db.v_event_enriched_bid_responses AS
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
