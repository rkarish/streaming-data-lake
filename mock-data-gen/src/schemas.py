import random
from datetime import datetime, timezone, timedelta

from faker import Faker

fake = Faker()

# ---------------------------------------------------------------------------
# Weighted distributions
# ---------------------------------------------------------------------------

BANNER_SIZES = [(300, 250), (728, 90), (160, 600), (320, 50), (970, 250)]
BANNER_SIZE_WEIGHTS = [0.40, 0.20, 0.15, 0.15, 0.10]

DEVICE_TYPES = [2, 1, 4]  # mobile/tablet, desktop, phone
DEVICE_TYPE_WEIGHTS = [0.60, 0.30, 0.10]

OS_CHOICES = ["iOS", "Android", "Windows", "macOS"]
OS_WEIGHTS = [0.35, 0.35, 0.20, 0.10]

OS_VERSIONS = {
    "iOS": ["16.0", "16.5", "17.0", "17.1", "17.2", "18.0"],
    "Android": ["12", "13", "14", "15"],
    "Windows": ["10", "11"],
    "macOS": ["13.0", "14.0", "15.0"],
}

GEO_COUNTRIES = ["USA", "GBR", "DEU", "CAN", "AUS", "FRA"]
GEO_COUNTRY_WEIGHTS = [0.50, 0.15, 0.10, 0.10, 0.10, 0.05]

GEO_REGIONS = {
    "USA": ["CA", "NY", "TX", "FL", "IL", "WA"],
    "GBR": ["ENG", "SCT"],
    "DEU": ["BY", "NW", "BE"],
    "CAN": ["ON", "BC", "QC"],
    "AUS": ["NSW", "VIC", "QLD"],
    "FRA": ["IDF", "PAC", "ARA"],
}

GEO_COORDS = {
    "USA": {"lat": (25.0, 48.0), "lon": (-124.0, -71.0)},
    "GBR": {"lat": (50.0, 58.0), "lon": (-8.0, 2.0)},
    "DEU": {"lat": (47.0, 55.0), "lon": (5.0, 15.0)},
    "CAN": {"lat": (42.0, 60.0), "lon": (-141.0, -52.0)},
    "AUS": {"lat": (-44.0, -10.0), "lon": (113.0, 154.0)},
    "FRA": {"lat": (42.0, 51.0), "lon": (-5.0, 9.0)},
}

IAB_CATEGORIES = [f"IAB{i}" for i in range(1, 11)]

TMAX_CHOICES = [100, 120, 150, 200, 300]


def generate_bid_request() -> dict:
    """Generate a single OpenRTB 2.6 BidRequest dictionary."""

    # Banner
    w, h = random.choices(BANNER_SIZES, weights=BANNER_SIZE_WEIGHTS, k=1)[0]
    banner_pos = random.randint(0, 3)

    # Device
    device_type = random.choices(DEVICE_TYPES, weights=DEVICE_TYPE_WEIGHTS, k=1)[0]
    os_name = random.choices(OS_CHOICES, weights=OS_WEIGHTS, k=1)[0]
    os_version = random.choice(OS_VERSIONS[os_name])

    # Geo
    country = random.choices(GEO_COUNTRIES, weights=GEO_COUNTRY_WEIGHTS, k=1)[0]
    region = random.choice(GEO_REGIONS[country])
    coords = GEO_COORDS[country]
    lat = round(random.uniform(*coords["lat"]), 4)
    lon = round(random.uniform(*coords["lon"]), 4)

    # Site / Publisher
    site_id = f"site-{random.randint(100, 999)}"
    pub_id = f"pub-{random.randint(100, 999)}"
    pub_name = fake.company()
    domain = fake.domain_name()
    page_path = fake.uri_path()
    page_url = f"https://{domain}/{page_path}"

    # Categories (1-3 random IAB categories)
    num_cats = random.randint(1, 3)
    categories = random.sample(IAB_CATEGORIES, k=num_cats)

    # Auction
    auction_type = random.choices([1, 2], weights=[0.70, 0.30], k=1)[0]
    tmax = random.choice(TMAX_CHOICES)
    bidfloor = round(random.uniform(0.01, 5.00), 2)

    # Regulatory
    coppa = random.choices([0, 1], weights=[0.95, 0.05], k=1)[0]
    gdpr = random.choices([0, 1], weights=[0.70, 0.30], k=1)[0]

    # Timestamps
    event_ts = datetime.now(timezone.utc)
    received_at = event_ts + timedelta(milliseconds=random.randint(1, 50))

    return {
        "id": fake.uuid4(),
        "imp": [
            {
                "id": "1",
                "banner": {"w": w, "h": h, "pos": banner_pos},
                "bidfloor": bidfloor,
                "bidfloorcur": "USD",
                "secure": 1,
            }
        ],
        "site": {
            "id": site_id,
            "domain": domain,
            "cat": categories,
            "page": page_url,
            "publisher": {"id": pub_id, "name": pub_name},
        },
        "device": {
            "ua": fake.user_agent(),
            "ip": fake.ipv4(),
            "geo": {
                "lat": lat,
                "lon": lon,
                "country": country,
                "region": region,
            },
            "devicetype": device_type,
            "os": os_name,
            "osv": os_version,
        },
        "user": {
            "id": fake.uuid4(),
            "buyeruid": fake.uuid4(),
        },
        "at": auction_type,
        "tmax": tmax,
        "cur": ["USD"],
        "source": {
            "fd": 1,
            "tid": fake.uuid4(),
        },
        "regs": {
            "coppa": coppa,
            "ext": {"gdpr": gdpr},
        },
        "event_timestamp": event_ts.isoformat(),
        "received_at": received_at.isoformat(),
    }


# ---------------------------------------------------------------------------
# Bid-response data
# ---------------------------------------------------------------------------

BIDDER_SEATS = ["seat-alpha", "seat-beta", "seat-gamma", "seat-delta", "seat-epsilon"]
AD_DOMAINS = ["ads.example.com", "media.adnetwork.io", "serve.bidder.co", "cdn.adx.net"]
CREATIVE_IDS = [f"cr-{i}" for i in range(1000, 1050)]
DEAL_IDS = [f"deal-{i}" for i in range(1, 20)]


def generate_bid_response(bid_request: dict) -> dict:
    """Generate an OpenRTB 2.6 BidResponse correlated to the given bid_request."""
    req_ts = datetime.fromisoformat(bid_request["event_timestamp"])
    response_ts = req_ts + timedelta(milliseconds=random.randint(5, 80))

    imp = bid_request["imp"][0]
    bidfloor = imp["bidfloor"]
    price = round(bidfloor + random.uniform(0.01, 3.00), 2)

    bid_obj = {
        "id": fake.uuid4(),
        "impid": imp["id"],
        "price": price,
        "adid": fake.uuid4(),
        "crid": random.choice(CREATIVE_IDS),
        "adomain": [random.choice(AD_DOMAINS)],
        "w": imp["banner"]["w"],
        "h": imp["banner"]["h"],
    }

    if random.random() < 0.10:
        bid_obj["dealid"] = random.choice(DEAL_IDS)

    return {
        "id": fake.uuid4(),
        "seatbid": [
            {
                "seat": random.choice(BIDDER_SEATS),
                "bid": [bid_obj],
            }
        ],
        "bidid": fake.uuid4(),
        "cur": "USD",
        "ext": {
            "request_id": bid_request["id"],
        },
        "event_timestamp": response_ts.isoformat(),
    }


def generate_impression(bid_request: dict, bid_response: dict) -> dict:
    """Generate a win-notice / impression event correlated to a bid request and response."""
    req_ts = datetime.fromisoformat(bid_request["event_timestamp"])
    imp_ts = req_ts + timedelta(milliseconds=random.randint(100, 500))

    bid_obj = bid_response["seatbid"][0]["bid"][0]
    bid_price = bid_obj["price"]
    win_price = round(random.uniform(bid_request["imp"][0]["bidfloor"], bid_price), 2)

    return {
        "impression_id": fake.uuid4(),
        "request_id": bid_request["id"],
        "response_id": bid_response["id"],
        "imp_id": bid_obj["impid"],
        "bidder_id": bid_response["seatbid"][0]["seat"],
        "win_price": win_price,
        "win_currency": "USD",
        "creative_id": bid_obj["crid"],
        "ad_domain": bid_obj["adomain"][0],
        "event_timestamp": imp_ts.isoformat(),
    }


def generate_click(bid_request: dict, impression: dict) -> dict:
    """Generate a click event correlated to an impression."""
    imp_ts = datetime.fromisoformat(impression["event_timestamp"])
    click_ts = imp_ts + timedelta(seconds=random.randint(1, 10))

    return {
        "click_id": fake.uuid4(),
        "request_id": bid_request["id"],
        "impression_id": impression["impression_id"],
        "imp_id": impression["imp_id"],
        "bidder_id": impression["bidder_id"],
        "creative_id": impression["creative_id"],
        "click_url": f"https://{impression['ad_domain']}/click/{fake.uuid4()}",
        "event_timestamp": click_ts.isoformat(),
    }
