import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Config:
    kafka_bootstrap_servers: str = os.environ.get(
        "KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"
    )
    events_per_second: int = int(os.environ.get("EVENTS_PER_SECOND", "10"))
    topic_bid_requests: str = os.environ.get("TOPIC_BID_REQUESTS", "bid-requests")
    topic_bid_responses: str = os.environ.get("TOPIC_BID_RESPONSES", "bid-responses")
    topic_impressions: str = os.environ.get("TOPIC_IMPRESSIONS", "impressions")
    topic_clicks: str = os.environ.get("TOPIC_CLICKS", "clicks")
    bid_response_rate: float = float(os.environ.get("BID_RESPONSE_RATE", "0.60"))
    win_rate: float = float(os.environ.get("WIN_RATE", "0.15"))
    click_rate: float = float(os.environ.get("CLICK_RATE", "0.02"))


config = Config()
