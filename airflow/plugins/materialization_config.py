from dataclasses import dataclass, field


@dataclass(frozen=True)
class DimCheck:
    fk_col: str
    dim_table: str
    dim_pk: str


@dataclass(frozen=True)
class MaterializationEntry:
    mat_table: str
    view_name: str
    view_ts_col: str
    fact_table: str
    fact_ts_col: str
    dim_checks: list[DimCheck] = field(default_factory=list)
    has_funnel_repair: bool = False


MATERIALIZATIONS: list[MaterializationEntry] = [
    MaterializationEntry(
        mat_table="mat_bid_requests",
        view_name="v_event_enriched_bid_requests",
        view_ts_col="event_timestamp",
        fact_table="bid_requests",
        fact_ts_col="event_timestamp",
        dim_checks=[
            DimCheck("publisher_id", "dim_publisher", "publisher_id"),
            DimCheck("device_type", "dim_device_type", "device_type_code"),
            DimCheck("device_os", "dim_device_os", "os_name"),
        ],
    ),
    MaterializationEntry(
        mat_table="mat_bid_responses",
        view_name="v_event_enriched_bid_responses",
        view_ts_col="event_timestamp",
        fact_table="bid_responses",
        fact_ts_col="event_timestamp",
        dim_checks=[
            DimCheck("seat", "dim_bidder", "bidder_id"),
            DimCheck("creative_id", "dim_creative", "creative_id"),
            DimCheck("strategy_id", "dim_strategy", "strategy_id"),
            DimCheck("line_item_id", "dim_line_item", "line_item_id"),
            DimCheck("campaign_id", "dim_campaign", "campaign_id"),
            DimCheck("advertiser_id", "dim_advertiser", "advertiser_id"),
            DimCheck("agency_id", "dim_agency", "agency_id"),
            DimCheck("deal_id", "dim_deal", "deal_id"),
        ],
    ),
    MaterializationEntry(
        mat_table="mat_impressions",
        view_name="v_event_enriched_impressions",
        view_ts_col="event_timestamp",
        fact_table="impressions",
        fact_ts_col="event_timestamp",
        dim_checks=[
            DimCheck("bidder_id", "dim_bidder", "bidder_id"),
            DimCheck("creative_id", "dim_creative", "creative_id"),
        ],
    ),
    MaterializationEntry(
        mat_table="mat_clicks",
        view_name="v_event_enriched_clicks",
        view_ts_col="event_timestamp",
        fact_table="clicks",
        fact_ts_col="event_timestamp",
        dim_checks=[
            DimCheck("bidder_id", "dim_bidder", "bidder_id"),
            DimCheck("creative_id", "dim_creative", "creative_id"),
        ],
    ),
    MaterializationEntry(
        mat_table="mat_full_funnel",
        view_name="v_event_enriched_full_funnel",
        view_ts_col="request_timestamp",
        fact_table="bid_requests",
        fact_ts_col="event_timestamp",
        dim_checks=[
            DimCheck("publisher_id", "dim_publisher", "publisher_id"),
            DimCheck("seat", "dim_bidder", "bidder_id"),
            DimCheck("creative_id", "dim_creative", "creative_id"),
            DimCheck("strategy_id", "dim_strategy", "strategy_id"),
            DimCheck("line_item_id", "dim_line_item", "line_item_id"),
            DimCheck("campaign_id", "dim_campaign", "campaign_id"),
            DimCheck("advertiser_id", "dim_advertiser", "advertiser_id"),
            DimCheck("agency_id", "dim_agency", "agency_id"),
            DimCheck("deal_id", "dim_deal", "deal_id"),
        ],
        has_funnel_repair=True,
    ),
    MaterializationEntry(
        mat_table="mat_agg_metrics_by_bidder",
        view_name="v_agg_metrics_by_bidder",
        view_ts_col="hour_start",
        fact_table="v_agg_metrics_by_bidder",
        fact_ts_col="hour_start",
        dim_checks=[DimCheck("bidder_id", "dim_bidder", "bidder_id")],
    ),
    MaterializationEntry(
        mat_table="mat_agg_bid_landscape",
        view_name="v_agg_bid_landscape",
        view_ts_col="hour_start",
        fact_table="v_agg_bid_landscape",
        fact_ts_col="hour_start",
        dim_checks=[DimCheck("publisher_id", "dim_publisher", "publisher_id")],
    ),
    MaterializationEntry(
        mat_table="mat_agg_serving_metrics",
        view_name="v_agg_serving_metrics",
        view_ts_col="hour_start",
        fact_table="v_agg_serving_metrics",
        fact_ts_col="hour_start",
        dim_checks=[DimCheck("bidder_id", "dim_bidder", "bidder_id")],
    ),
    MaterializationEntry(
        mat_table="mat_agg_funnel_by_publisher",
        view_name="v_agg_funnel_by_publisher",
        view_ts_col="hour_start",
        fact_table="v_agg_funnel_by_publisher",
        fact_ts_col="hour_start",
        dim_checks=[DimCheck("publisher_id", "dim_publisher", "publisher_id")],
    ),
    MaterializationEntry(
        mat_table="mat_agg_funnel_leakage",
        view_name="v_agg_funnel_leakage",
        view_ts_col="hour_start",
        fact_table="v_agg_funnel_leakage",
        fact_ts_col="hour_start",
        dim_checks=[DimCheck("publisher_id", "dim_publisher", "publisher_id")],
    ),
    MaterializationEntry(
        mat_table="mat_agg_impressions_by_geo",
        view_name="v_agg_impressions_by_geo",
        view_ts_col="hour_start",
        fact_table="v_agg_impressions_by_geo",
        fact_ts_col="hour_start",
        dim_checks=[DimCheck("device_geo_country", "dim_geo", "country_code")],
    ),
]
