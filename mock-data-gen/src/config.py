"""
Configuration module for the mock data generator.

All settings are loaded from environment variables with sensible defaults.
The Config dataclass is frozen (immutable) to prevent accidental modification.
"""

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Config:
    """
    Immutable configuration for the mock data generator.

    Attributes:
        kafka_bootstrap_servers: Kafka broker address (host:port)
        events_per_second: Target throughput for bid requests per second
        topic_*: Kafka topic names for each event type in the funnel
        bid_response_rate: Probability (0-1) that a bid request receives a response
        win_rate: Probability (0-1) that a bid response wins the auction
        click_rate: Probability (0-1) that an impression generates a click
    """

    # Kafka connection settings
    kafka_bootstrap_servers: str = os.environ.get(
        "KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"
    )

    # Throughput control
    events_per_second: int = int(os.environ.get("EVENTS_PER_SECOND", "10"))

    # Kafka topic names for the 4-stage event funnel
    topic_bid_requests: str = os.environ.get("TOPIC_BID_REQUESTS", "bid-requests")
    topic_bid_responses: str = os.environ.get("TOPIC_BID_RESPONSES", "bid-responses")
    topic_impressions: str = os.environ.get("TOPIC_IMPRESSIONS", "impressions")
    topic_clicks: str = os.environ.get("TOPIC_CLICKS", "clicks")

    # Funnel conversion rates (probability that each stage proceeds to the next)
    # bid_request -> bid_response: ~60% fill rate (typical for programmatic)
    bid_response_rate: float = float(os.environ.get("BID_RESPONSE_RATE", "0.60"))
    # bid_response -> impression: ~15% win rate (auction clearing)
    win_rate: float = float(os.environ.get("WIN_RATE", "0.15"))
    # impression -> click: ~5% CTR (click-through rate)
    click_rate: float = float(os.environ.get("CLICK_RATE", "0.05"))

    # Traffic variation rates for stream transformations testing
    # Rate of test publisher IDs (test-*) for traffic filtering tests
    test_publisher_rate: float = float(os.environ.get("TEST_PUBLISHER_RATE", "0.05"))
    # Rate of RFC1918 private IPs for invalid traffic filtering tests
    invalid_ip_rate: float = float(os.environ.get("INVALID_IP_RATE", "0.02"))
    # Rate of app traffic vs site traffic for device classification tests
    app_traffic_rate: float = float(os.environ.get("APP_TRAFFIC_RATE", "0.30"))
    # Rate of non-USD currencies for currency normalization tests
    non_usd_currency_rate: float = float(os.environ.get("NON_USD_CURRENCY_RATE", "0.10"))

    # Backfill mode: generate historical events spanning this many hours before now.
    # Events are sent at max speed (no rate limiting) with timestamps spread evenly
    # across the backfill window, then the generator switches to real-time mode.
    # Set to 0 to disable backfill (default).
    backfill_hours: int = int(os.environ.get("BACKFILL_HOURS", "0"))


# Singleton instance used throughout the application
config = Config()
