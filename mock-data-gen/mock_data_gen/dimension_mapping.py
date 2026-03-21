"""
Deterministic DSP hierarchy and reference dimension data.

This module builds the complete set of dimension entities used by both
the Iceberg dimension seeder (seed_dimensions.py) and the Kafka event
generator (schemas.py).  Every function is pure and deterministic --
no randomness, no external dependencies beyond the standard library.

Hierarchy:
    agency -> advertiser -> campaign -> line_item -> strategy -> creative

Cardinalities:
    5 agencies, 20 advertisers, 60 campaigns, 120 line items,
    180 strategies, 50 creatives, 5 bidders

Reference dimensions:
    30 publishers, 19 deals, 24 geos, 4 device types,
    4 device OS entries, 6 browsers
"""

from datetime import date

_CURRENT_YEAR = date.today().year

# SCD Type 2 defaults applied to every dimension row
SCD_VALID_FROM = "2020-01-01T00:00:00+00:00"
SCD_VALID_TO = None
SCD_IS_CURRENT = True

# ---------------------------------------------------------------------------
# ID pools (must match schemas.py)
# ---------------------------------------------------------------------------
CREATIVE_IDS = list(range(1, 51))
BIDDER_IDS = [1, 2, 3, 4, 5]
DEAL_IDS = list(range(1, 20))
PUBLISHER_IDS = list(range(1, 31))

# ---------------------------------------------------------------------------
# Static name pools
# ---------------------------------------------------------------------------
AGENCY_NAMES = [
    (1, "Omnicom Digital", "Omnicom Group", "New York"),
    (2, "Publicis Groupe", "Publicis", "Paris"),
    (3, "WPP GroupM", "WPP plc", "London"),
    (4, "Dentsu International", "Dentsu Group", "Tokyo"),
    (5, "IPG Mediabrands", "Interpublic Group", "New York"),
]

INDUSTRIES = [
    "Automotive", "CPG", "Financial Services", "Healthcare",
    "Retail", "Technology", "Telecom", "Travel",
    "Entertainment", "Education", "Real Estate", "Insurance",
    "Food & Beverage", "Apparel", "Electronics", "Gaming",
    "Pharma", "Energy", "Logistics", "SaaS",
]

ADVERTISER_DOMAINS = [
    "acmecars.com", "brightfoods.com", "capitalbank.com", "deltahealth.com",
    "eagleretail.com", "fusiontech.com", "globaltelecom.com", "horizontravel.com",
    "infinityent.com", "jadelearn.com", "keystonereal.com", "libertyins.com",
    "maxbev.com", "nextwear.com", "omegaelec.com", "pixelgames.com",
    "quantumpharma.com", "renewenergy.com", "swiftship.com", "terracloud.com",
]

CAMPAIGN_OBJECTIVES = ["awareness", "consideration", "conversion", "retargeting"]

BID_STRATEGIES = ["max_clicks", "target_cpa", "target_roas", "max_conversions"]

STRATEGY_TARGETING = ["contextual", "behavioral", "demographic", "geo-targeted"]

STRATEGY_CHANNELS = ["display", "video", "mobile", "cross-device"]

CREATIVE_FORMATS = ["banner", "video", "native", "rich_media"]

BANNER_SIZES = [(300, 250), (728, 90), (160, 600), (320, 50), (970, 250)]

BIDDER_MAP = {
    1: ("AlphaExchange DSP", "alphaexchange.io", "US"),
    2: ("BetaMedia Bidder", "betamedia.com", "EU"),
    3: ("GammaAds Platform", "gammaads.net", "US"),
    4: ("DeltaBid Network", "deltabid.co", "APAC"),
    5: ("EpsilonRTB Engine", "epsilonrtb.com", "EU"),
}

PUBLISHER_VERTICALS = [
    "News", "Entertainment", "Sports", "Tech", "Finance",
    "Lifestyle", "Health", "Education", "Travel", "Automotive",
]

PUBLISHER_TIERS = ["premium", "mid-tier", "long-tail"]

DEAL_TYPES = ["preferred", "private_auction", "programmatic_guaranteed"]

GEO_COUNTRIES = {
    "USA": "United States",
    "GBR": "United Kingdom",
    "DEU": "Germany",
    "CAN": "Canada",
    "AUS": "Australia",
    "FRA": "France",
}

GEO_REGIONS = {
    "USA": [("CA", "California", "America/Los_Angeles"),
            ("NY", "New York", "America/New_York"),
            ("TX", "Texas", "America/Chicago"),
            ("FL", "Florida", "America/New_York")],
    "GBR": [("ENG", "England", "Europe/London"),
            ("SCT", "Scotland", "Europe/London"),
            ("WLS", "Wales", "Europe/London"),
            ("NIR", "Northern Ireland", "Europe/London")],
    "DEU": [("BY", "Bavaria", "Europe/Berlin"),
            ("NW", "North Rhine-Westphalia", "Europe/Berlin"),
            ("BE", "Berlin", "Europe/Berlin"),
            ("HH", "Hamburg", "Europe/Berlin")],
    "CAN": [("ON", "Ontario", "America/Toronto"),
            ("BC", "British Columbia", "America/Vancouver"),
            ("QC", "Quebec", "America/Montreal"),
            ("AB", "Alberta", "America/Edmonton")],
    "AUS": [("NSW", "New South Wales", "Australia/Sydney"),
            ("VIC", "Victoria", "Australia/Melbourne"),
            ("QLD", "Queensland", "Australia/Brisbane"),
            ("WA", "Western Australia", "Australia/Perth")],
    "FRA": [("IDF", "Ile-de-France", "Europe/Paris"),
            ("PAC", "Provence-Alpes-Cote d'Azur", "Europe/Paris"),
            ("ARA", "Auvergne-Rhone-Alpes", "Europe/Paris"),
            ("OCC", "Occitanie", "Europe/Paris")],
}

DEVICE_TYPES = [
    (1, "Mobile/Tablet", "tablet", True),
    (2, "Personal Computer", "desktop", False),
    (4, "Phone", "phone", True),
    (7, "Set Top Box", "stb", False),
]

DEVICE_OS_LIST = [
    ("iOS", "Apple", "Darwin"),
    ("Android", "Google", "Linux"),
    ("Windows", "Microsoft", "NT"),
    ("macOS", "Apple", "Darwin"),
]

BROWSERS = [
    (1, "Chrome", "Google", "Blink"),
    (2, "Safari", "Apple", "WebKit"),
    (3, "Firefox", "Mozilla", "Gecko"),
    (4, "Edge", "Microsoft", "Blink"),
    (5, "Samsung Internet", "Samsung", "Blink"),
    (6, "Opera", "Opera Software", "Blink"),
]


def _scd_fields() -> dict:
    """Return the shared SCD Type 2 columns for every dimension row."""
    return {
        "valid_from": SCD_VALID_FROM,
        "valid_to": SCD_VALID_TO,
        "is_current": SCD_IS_CURRENT,
    }


# ---------------------------------------------------------------------------
# DSP hierarchy builder
# ---------------------------------------------------------------------------

def build_dsp_hierarchy() -> dict:
    """Build the deterministic buy-side DSP hierarchy.

    Returns a dict with keys:
        agencies, advertisers, campaigns, line_items, strategies,
        creatives, bidders
    Each value is a list of dicts representing dimension rows.
    """
    agencies = []
    advertisers = []
    campaigns = []
    line_items = []
    strategies = []
    creatives = []

    adv_idx = 0
    cmp_idx = 0
    li_idx = 0
    str_idx = 0

    for ag_id, ag_name, holding, hq in AGENCY_NAMES:
        agencies.append({
            "agency_id": ag_id,
            "agency_name": ag_name,
            "holding_company": holding,
            "headquarters": hq,
            **_scd_fields(),
        })

        # 4 advertisers per agency
        for _ in range(4):
            adv_id = adv_idx + 1
            advertisers.append({
                "advertiser_id": adv_id,
                "agency_id": ag_id,
                "advertiser_name": f"{INDUSTRIES[adv_idx]} Brand",
                "industry": INDUSTRIES[adv_idx],
                "domain": ADVERTISER_DOMAINS[adv_idx],
                **_scd_fields(),
            })

            # 3 campaigns per advertiser
            for c in range(3):
                cmp_id = cmp_idx + 1
                objective = CAMPAIGN_OBJECTIVES[cmp_idx % len(CAMPAIGN_OBJECTIVES)]
                # Stagger start dates deterministically
                start_month = (cmp_idx % 12) + 1
                campaigns.append({
                    "campaign_id": cmp_id,
                    "advertiser_id": adv_id,
                    "campaign_name": f"{INDUSTRIES[adv_idx]} {objective.title()} Q{(c % 4) + 1}",
                    "objective": objective,
                    "start_date": f"{_CURRENT_YEAR}-{start_month:02d}-01",
                    "end_date": f"{_CURRENT_YEAR + 1 if start_month >= 11 else _CURRENT_YEAR}-{((start_month + 2) % 12) + 1:02d}-28",
                    **_scd_fields(),
                })

                # 2 line items per campaign
                for l in range(2):
                    li_id = li_idx + 1
                    bid_strategy = BID_STRATEGIES[li_idx % len(BID_STRATEGIES)]
                    budget = 5000 + (li_idx * 250) % 20000
                    line_items.append({
                        "line_item_id": li_id,
                        "campaign_id": cmp_id,
                        "line_item_name": f"LI {objective.title()} {bid_strategy.replace('_', ' ').title()} {l + 1}",
                        "budget": float(budget),
                        "bid_strategy": bid_strategy,
                        **_scd_fields(),
                    })

                    # ~1.5 strategies per line item (alternating 1 and 2)
                    n_strategies = 2 if li_idx % 2 == 0 else 1
                    for s in range(n_strategies):
                        st_id = str_idx + 1
                        targeting = STRATEGY_TARGETING[str_idx % len(STRATEGY_TARGETING)]
                        channel = STRATEGY_CHANNELS[str_idx % len(STRATEGY_CHANNELS)]
                        strategies.append({
                            "strategy_id": st_id,
                            "line_item_id": li_id,
                            "strategy_name": f"{targeting.title()} {channel.title()} Strategy",
                            "targeting_type": targeting,
                            "channel": channel,
                            **_scd_fields(),
                        })
                        str_idx += 1

                    li_idx += 1
                cmp_idx += 1
            adv_idx += 1

    # Assign 50 creatives across the 180 strategies (round-robin)
    for cr_i, cr_id in enumerate(CREATIVE_IDS):
        parent_strategy = strategies[cr_i % len(strategies)]
        fmt = CREATIVE_FORMATS[cr_i % len(CREATIVE_FORMATS)]
        w, h = BANNER_SIZES[cr_i % len(BANNER_SIZES)]
        creatives.append({
            "creative_id": cr_id,
            "strategy_id": parent_strategy["strategy_id"],
            "creative_name": f"{fmt.title()} {w}x{h} #{cr_i + 1}",
            "format": fmt,
            "width": w,
            "height": h,
            "landing_page_url": f"https://landing.example.com/{fmt}/{cr_id}",
            **_scd_fields(),
        })

    # Bidders (one per seat)
    bidders = []
    for seat_id in BIDDER_IDS:
        name, domain, region = BIDDER_MAP[seat_id]
        bidders.append({
            "bidder_id": seat_id,
            "bidder_name": name,
            "domain": domain,
            "exchange_seat": seat_id,
            "region": region,
            **_scd_fields(),
        })

    return {
        "agencies": agencies,
        "advertisers": advertisers,
        "campaigns": campaigns,
        "line_items": line_items,
        "strategies": strategies,
        "creatives": creatives,
        "bidders": bidders,
    }


# ---------------------------------------------------------------------------
# Reference dimensions builder
# ---------------------------------------------------------------------------

def build_reference_dimensions() -> dict:
    """Build supply-side and reference dimension data.

    Returns a dict with keys:
        publishers, deals, geos, device_types, device_os_list, browsers
    Each value is a list of dicts representing dimension rows.
    """
    # Publishers (30)
    publishers = []
    for i, pub_id in enumerate(PUBLISHER_IDS):
        vertical = PUBLISHER_VERTICALS[i % len(PUBLISHER_VERTICALS)]
        tier = PUBLISHER_TIERS[i % len(PUBLISHER_TIERS)]
        publishers.append({
            "publisher_id": pub_id,
            "publisher_name": f"{vertical} Publisher {i + 1}",
            "domain": f"{vertical.lower()}{i + 1}.example.com",
            "vertical": vertical,
            "tier": tier,
            **_scd_fields(),
        })

    # Deals (19, deal-1 through deal-19)
    deals = []
    for i, deal_id in enumerate(DEAL_IDS):
        deal_type = DEAL_TYPES[i % len(DEAL_TYPES)]
        buyer_seat = BIDDER_IDS[i % len(BIDDER_IDS)]
        seller_id = PUBLISHER_IDS[i % len(PUBLISHER_IDS)]
        floor_price = round(1.0 + (i * 0.5), 2)
        deals.append({
            "deal_id": deal_id,
            "deal_name": f"{deal_type.replace('_', ' ').title()} Deal {i + 1}",
            "deal_type": deal_type,
            "floor_price": floor_price,
            "buyer_seat": buyer_seat,
            "seller_id": seller_id,
            **_scd_fields(),
        })

    # Geos (6 countries x 4 regions = 24)
    geos = []
    geo_idx = 0
    for country_code, country_name in GEO_COUNTRIES.items():
        for region_code, region_name, tz in GEO_REGIONS[country_code]:
            geo_idx += 1
            geos.append({
                "geo_key": geo_idx,
                "country_code": country_code,
                "country_name": country_name,
                "region_code": region_code,
                "region_name": region_name,
                "timezone": tz,
                **_scd_fields(),
            })

    # Device types (4)
    device_types = []
    for code, name, form_factor, is_mobile in DEVICE_TYPES:
        device_types.append({
            "device_type_code": code,
            "device_type_name": name,
            "form_factor": form_factor,
            "is_mobile": is_mobile,
            **_scd_fields(),
        })

    # Device OS (4)
    device_os_list = []
    for os_name, vendor, family in DEVICE_OS_LIST:
        device_os_list.append({
            "os_name": os_name,
            "os_vendor": vendor,
            "os_family": family,
            **_scd_fields(),
        })

    # Browsers (6)
    browsers = []
    for br_id, br_name, vendor, engine in BROWSERS:
        browsers.append({
            "browser_id": br_id,
            "browser_name": br_name,
            "vendor": vendor,
            "engine": engine,
            **_scd_fields(),
        })

    return {
        "publishers": publishers,
        "deals": deals,
        "geos": geos,
        "device_types": device_types,
        "device_os_list": device_os_list,
        "browsers": browsers,
    }


# ---------------------------------------------------------------------------
# Creative lookup for the event generator
# ---------------------------------------------------------------------------

def build_creative_lookup() -> dict:
    """Build a mapping from creative_id to its full DSP hierarchy path.

    Returns:
        dict mapping creative_id -> {strategy_id, line_item_id,
        campaign_id, advertiser_id, agency_id}
    """
    hierarchy = build_dsp_hierarchy()

    # Index strategies, line_items, campaigns, advertisers by their PK
    strategy_map = {s["strategy_id"]: s for s in hierarchy["strategies"]}
    li_map = {li["line_item_id"]: li for li in hierarchy["line_items"]}
    cmp_map = {c["campaign_id"]: c for c in hierarchy["campaigns"]}
    adv_map = {a["advertiser_id"]: a for a in hierarchy["advertisers"]}

    lookup = {}
    for cr in hierarchy["creatives"]:
        st = strategy_map[cr["strategy_id"]]
        li = li_map[st["line_item_id"]]
        cmp = cmp_map[li["campaign_id"]]
        adv = adv_map[cmp["advertiser_id"]]
        lookup[cr["creative_id"]] = {
            "strategy_id": st["strategy_id"],
            "line_item_id": li["line_item_id"],
            "campaign_id": cmp["campaign_id"],
            "advertiser_id": adv["advertiser_id"],
            "agency_id": adv["agency_id"],
        }

    return lookup
