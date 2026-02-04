import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Config:
    kafka_bootstrap_servers: str = os.environ.get(
        "KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"
    )
    events_per_second: int = int(os.environ.get("EVENTS_PER_SECOND", "10"))
    topic_bid_requests: str = os.environ.get("TOPIC_BID_REQUESTS", "bid-requests")


config = Config()
