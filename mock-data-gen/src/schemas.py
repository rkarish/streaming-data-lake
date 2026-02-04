"""
OpenRTB 2.6 event schema generators.

This module generates realistic mock data for the ad-tech event funnel:
  1. Bid Requests - SSP sends auction opportunity to DSPs
  2. Bid Responses - DSP responds with a bid price and creative
  3. Impressions - Ad is displayed (win notice)
  4. Clicks - User clicks on the ad

All events are correlated via request_id, enabling funnel analytics.
Data distributions are weighted to reflect real-world programmatic advertising patterns.
"""

import random
from datetime import datetime, timezone, timedelta

from faker import Faker

fake = Faker()

# ---------------------------------------------------------------------------
# Weighted distributions for realistic data generation
# ---------------------------------------------------------------------------

# Common IAB banner ad sizes (width, height) with market share weights
# 300x250 (medium rectangle) is the most common display ad format
BANNER_SIZES = [(300, 250), (728, 90), (160, 600), (320, 50), (970, 250)]
BANNER_SIZE_WEIGHTS = [0.40, 0.20, 0.15, 0.15, 0.10]

# OpenRTB device type codes: 1=Mobile/Tablet, 2=Personal Computer, 4=Phone
# Mobile dominates modern programmatic traffic
DEVICE_TYPES = [2, 1, 4]
DEVICE_TYPE_WEIGHTS = [0.60, 0.30, 0.10]

# Operating system distribution reflecting mobile-heavy traffic
OS_CHOICES = ["iOS", "Android", "Windows", "macOS"]
OS_WEIGHTS = [0.35, 0.35, 0.20, 0.10]

# Recent OS versions for each platform
OS_VERSIONS = {
    "iOS": ["16.0", "16.5", "17.0", "17.1", "17.2", "18.0"],
    "Android": ["12", "13", "14", "15"],
    "Windows": ["10", "11"],
    "macOS": ["13.0", "14.0", "15.0"],
}

# ISO 3166-1 alpha-3 country codes with US-heavy distribution (typical for US-based SSPs)
GEO_COUNTRIES = ["USA", "GBR", "DEU", "CAN", "AUS", "FRA"]
GEO_COUNTRY_WEIGHTS = [0.50, 0.15, 0.10, 0.10, 0.10, 0.05]

# Major regions/states within each country
GEO_REGIONS = {
    "USA": ["CA", "NY", "TX", "FL", "IL", "WA"],
    "GBR": ["ENG", "SCT"],
    "DEU": ["BY", "NW", "BE"],
    "CAN": ["ON", "BC", "QC"],
    "AUS": ["NSW", "VIC", "QLD"],
    "FRA": ["IDF", "PAC", "ARA"],
}

# Latitude/longitude bounding boxes for generating realistic coordinates per country
GEO_COORDS = {
    "USA": {"lat": (25.0, 48.0), "lon": (-124.0, -71.0)},
    "GBR": {"lat": (50.0, 58.0), "lon": (-8.0, 2.0)},
    "DEU": {"lat": (47.0, 55.0), "lon": (5.0, 15.0)},
    "CAN": {"lat": (42.0, 60.0), "lon": (-141.0, -52.0)},
    "AUS": {"lat": (-44.0, -10.0), "lon": (113.0, 154.0)},
    "FRA": {"lat": (42.0, 51.0), "lon": (-5.0, 9.0)},
}

# IAB content taxonomy categories (IAB1 = Arts & Entertainment, IAB2 = Automotive, etc.)
IAB_CATEGORIES = [f"IAB{i}" for i in range(1, 11)]

# Maximum time (ms) the bidder has to respond to the bid request
TMAX_CHOICES = [100, 120, 150, 200, 300]


def generate_bid_request() -> dict:
    """
    Generate a single OpenRTB 2.6 BidRequest dictionary.

    A bid request represents an ad auction opportunity sent from an SSP (Supply-Side Platform)
    to DSPs (Demand-Side Platforms). It contains information about:
    - The ad slot (impression) including size and minimum price
    - The website/app where the ad will appear
    - The user's device and location
    - Auction parameters and regulatory compliance flags

    Returns:
        dict: A complete OpenRTB 2.6 BidRequest object with realistic mock data
    """

    # Select banner ad dimensions using weighted distribution (300x250 most common)
    w, h = random.choices(BANNER_SIZES, weights=BANNER_SIZE_WEIGHTS, k=1)[0]
    # Ad position: 0=unknown, 1=above fold, 2=below fold, 3=header
    banner_pos = random.randint(0, 3)

    # Generate device characteristics with mobile-heavy distribution
    device_type = random.choices(DEVICE_TYPES, weights=DEVICE_TYPE_WEIGHTS, k=1)[0]
    os_name = random.choices(OS_CHOICES, weights=OS_WEIGHTS, k=1)[0]
    os_version = random.choice(OS_VERSIONS[os_name])

    # Generate geographic location with US-heavy distribution
    country = random.choices(GEO_COUNTRIES, weights=GEO_COUNTRY_WEIGHTS, k=1)[0]
    region = random.choice(GEO_REGIONS[country])
    coords = GEO_COORDS[country]
    # Generate random lat/lon within the country's bounding box
    lat = round(random.uniform(*coords["lat"]), 4)
    lon = round(random.uniform(*coords["lon"]), 4)

    # Generate publisher/site information using Faker for realistic names and URLs
    site_id = f"site-{random.randint(100, 999)}"
    pub_id = f"pub-{random.randint(100, 999)}"
    pub_name = fake.company()
    domain = fake.domain_name()
    page_path = fake.uri_path()
    page_url = f"https://{domain}/{page_path}"

    # Assign 1-3 random IAB content categories to the page
    num_cats = random.randint(1, 3)
    categories = random.sample(IAB_CATEGORIES, k=num_cats)

    # Auction settings
    # at=1: First-price auction (winner pays their bid)
    # at=2: Second-price auction (winner pays second-highest bid + $0.01)
    auction_type = random.choices([1, 2], weights=[0.70, 0.30], k=1)[0]
    tmax = random.choice(TMAX_CHOICES)  # Max response time in milliseconds
    bidfloor = round(random.uniform(0.01, 5.00), 2)  # Minimum CPM in USD

    # Regulatory compliance flags
    # COPPA: Children's Online Privacy Protection Act (US)
    coppa = random.choices([0, 1], weights=[0.95, 0.05], k=1)[0]
    # GDPR: General Data Protection Regulation (EU)
    gdpr = random.choices([0, 1], weights=[0.70, 0.30], k=1)[0]

    # Timestamps: event_timestamp is when the request was created,
    # received_at simulates a small network delay
    event_ts = datetime.now(timezone.utc)
    received_at = event_ts + timedelta(milliseconds=random.randint(1, 50))

    # Construct the complete OpenRTB 2.6 BidRequest object
    return {
        "id": fake.uuid4(),  # Unique request identifier
        "imp": [
            {
                "id": "1",  # Impression ID within this request
                "banner": {"w": w, "h": h, "pos": banner_pos},
                "bidfloor": bidfloor,  # Minimum bid price (CPM)
                "bidfloorcur": "USD",
                "secure": 1,  # Require HTTPS creative
            }
        ],
        "site": {
            "id": site_id,
            "domain": domain,
            "cat": categories,  # IAB content categories
            "page": page_url,
            "publisher": {"id": pub_id, "name": pub_name},
        },
        "device": {
            "ua": fake.user_agent(),  # Browser user agent string
            "ip": fake.ipv4(),  # User's IP address
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
            "id": fake.uuid4(),  # SSP's user ID
            "buyeruid": fake.uuid4(),  # DSP's user ID (cookie sync)
        },
        "at": auction_type,  # Auction type (1=first-price, 2=second-price)
        "tmax": tmax,  # Max response time in ms
        "cur": ["USD"],  # Accepted currencies
        "source": {
            "fd": 1,  # Entity responsible for final decision (1=exchange)
            "tid": fake.uuid4(),  # Transaction ID for the entire auction
        },
        "regs": {
            "coppa": coppa,  # 1 if COPPA applies
            "ext": {"gdpr": gdpr},  # 1 if GDPR applies
        },
        "event_timestamp": event_ts.isoformat(),
        "received_at": received_at.isoformat(),
    }


# ---------------------------------------------------------------------------
# Bid Response data pools
# ---------------------------------------------------------------------------

# Bidder seat IDs represent different DSPs/advertisers in the auction
BIDDER_SEATS = ["seat-alpha", "seat-beta", "seat-gamma", "seat-delta", "seat-epsilon"]

# Advertiser domains for ad.txt/sellers.json compliance
AD_DOMAINS = ["ads.example.com", "media.adnetwork.io", "serve.bidder.co", "cdn.adx.net"]

# Creative IDs representing different ad creatives in the bidder's inventory
CREATIVE_IDS = [f"cr-{i}" for i in range(1000, 1050)]

# Deal IDs for Private Marketplace (PMP) deals between publishers and advertisers
DEAL_IDS = [f"deal-{i}" for i in range(1, 20)]


def generate_bid_response(bid_request: dict) -> dict:
    """
    Generate an OpenRTB 2.6 BidResponse correlated to the given bid_request.

    A bid response is sent by a DSP in reply to a bid request, containing:
    - The bid price (CPM) the advertiser is willing to pay
    - The creative (ad) to display if the bid wins
    - The advertiser's domain for transparency
    - Optional deal ID for PMP transactions

    The response reuses the request_id and imp_id from the parent bid request
    to enable correlation and funnel analytics.

    Args:
        bid_request: The parent BidRequest dictionary to correlate with

    Returns:
        dict: A complete OpenRTB 2.6 BidResponse object
    """
    # Response timestamp is slightly after the request (simulating DSP processing time)
    req_ts = datetime.fromisoformat(bid_request["event_timestamp"])
    response_ts = req_ts + timedelta(milliseconds=random.randint(5, 80))

    # Extract impression details from the request
    imp = bid_request["imp"][0]
    bidfloor = imp["bidfloor"]
    # Bid price is above the floor (bidders won't bid below the minimum)
    price = round(bidfloor + random.uniform(0.01, 3.00), 2)

    # Construct the bid object (the actual offer)
    bid_obj = {
        "id": fake.uuid4(),  # Unique bid ID
        "impid": imp["id"],  # References the impression being bid on
        "price": price,  # Bid price in CPM
        "adid": fake.uuid4(),  # Advertiser's ad ID
        "crid": random.choice(CREATIVE_IDS),  # Creative ID
        "adomain": [random.choice(AD_DOMAINS)],  # Advertiser domain(s)
        "w": imp["banner"]["w"],  # Creative width (matches request)
        "h": imp["banner"]["h"],  # Creative height (matches request)
    }

    # 10% of bids are part of a Private Marketplace deal
    if random.random() < 0.10:
        bid_obj["dealid"] = random.choice(DEAL_IDS)

    return {
        "id": fake.uuid4(),  # Unique response ID
        "seatbid": [
            {
                "seat": random.choice(BIDDER_SEATS),  # Bidder/DSP identifier
                "bid": [bid_obj],  # Array of bids (typically one per impression)
            }
        ],
        "bidid": fake.uuid4(),  # Bidder-generated response ID
        "cur": "USD",  # Currency of the bid price
        "ext": {
            "request_id": bid_request["id"],  # Correlation to parent request
        },
        "event_timestamp": response_ts.isoformat(),
    }


def generate_impression(bid_request: dict, bid_response: dict) -> dict:
    """
    Generate a win-notice / impression event correlated to a bid request and response.

    An impression event (also called a "win notice") is fired when:
    1. The bid wins the auction
    2. The ad creative is successfully rendered in the user's browser

    The win_price (clearing price) may be lower than the bid_price depending on
    the auction type (second-price auctions clear at the second-highest bid).

    This event is critical for billing - advertisers pay based on impressions served.

    Args:
        bid_request: The original BidRequest dictionary
        bid_response: The winning BidResponse dictionary

    Returns:
        dict: An impression event with correlation IDs and win price
    """
    # Impression occurs after the ad is rendered (100-500ms after the request)
    req_ts = datetime.fromisoformat(bid_request["event_timestamp"])
    imp_ts = req_ts + timedelta(milliseconds=random.randint(100, 500))

    # Extract winning bid details
    bid_obj = bid_response["seatbid"][0]["bid"][0]
    bid_price = bid_obj["price"]

    # Win price (clearing price) is between the floor and the bid price
    # In second-price auctions, this would be the second-highest bid + $0.01
    win_price = round(random.uniform(bid_request["imp"][0]["bidfloor"], bid_price), 2)

    return {
        "impression_id": fake.uuid4(),  # Unique impression identifier
        "request_id": bid_request["id"],  # Correlation to original request
        "response_id": bid_response["id"],  # Correlation to winning response
        "imp_id": bid_obj["impid"],  # Impression slot ID
        "bidder_id": bid_response["seatbid"][0]["seat"],  # Winning bidder
        "win_price": win_price,  # Actual price paid (CPM)
        "win_currency": "USD",
        "creative_id": bid_obj["crid"],  # Creative that was displayed
        "ad_domain": bid_obj["adomain"][0],  # Advertiser domain
        "event_timestamp": imp_ts.isoformat(),
    }


def generate_click(bid_request: dict, impression: dict) -> dict:
    """
    Generate a click event correlated to an impression.

    A click event is fired when a user clicks on a displayed ad. Clicks are
    relatively rare (typical CTR is 0.1-2%) but valuable for:
    - Performance measurement (CTR, conversion tracking)
    - CPC (cost-per-click) billing models
    - Fraud detection (click patterns)

    Args:
        bid_request: The original BidRequest dictionary
        impression: The parent impression event dictionary

    Returns:
        dict: A click event with correlation IDs and click URL
    """
    # Click occurs 1-10 seconds after the impression (user viewing the ad)
    imp_ts = datetime.fromisoformat(impression["event_timestamp"])
    click_ts = imp_ts + timedelta(seconds=random.randint(1, 10))

    return {
        "click_id": fake.uuid4(),  # Unique click identifier
        "request_id": bid_request["id"],  # Correlation to original request
        "impression_id": impression["impression_id"],  # Correlation to impression
        "imp_id": impression["imp_id"],  # Impression slot ID
        "bidder_id": impression["bidder_id"],  # Advertiser who served the ad
        "creative_id": impression["creative_id"],  # Creative that was clicked
        "click_url": f"https://{impression['ad_domain']}/click/{fake.uuid4()}",  # Landing page URL
        "event_timestamp": click_ts.isoformat(),
    }
