CREATE OR REPLACE VIEW iceberg.db.v_event_enriched_bid_requests AS
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
