CREATE OR REPLACE VIEW iceberg.db.v_event_enriched_impressions AS
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
