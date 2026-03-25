CREATE OR REPLACE VIEW iceberg.db.v_event_enriched_clicks AS
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
