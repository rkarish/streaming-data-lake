import json
import logging
import random
import time

from kafka import KafkaProducer

from src.config import config
from src.schemas import (
    generate_bid_request,
    generate_bid_response,
    generate_click,
    generate_impression,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)


def create_producer() -> KafkaProducer:
    """Create a KafkaProducer with exponential backoff retry on connection failure."""
    delay = 1.0
    max_delay = 30.0
    max_retries = 10

    for attempt in range(1, max_retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=config.kafka_bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8"),
            )
            logger.info(
                "Connected to Kafka at %s", config.kafka_bootstrap_servers
            )
            return producer
        except Exception as exc:
            if attempt == max_retries:
                logger.error(
                    "Failed to connect to Kafka after %d attempts", max_retries
                )
                raise
            logger.warning(
                "Kafka connection attempt %d/%d failed: %s. "
                "Retrying in %.1fs...",
                attempt,
                max_retries,
                exc,
                delay,
            )
            time.sleep(delay)
            delay = min(delay * 2, max_delay)

    # Unreachable, but satisfies type checkers
    raise RuntimeError("Failed to connect to Kafka")


def main() -> None:
    """Main producer loop with rate-limited funnel event generation."""
    producer = create_producer()
    eps = config.events_per_second
    interval = 1.0 / eps

    logger.info(
        "Starting funnel generator: %d bid-requests/sec "
        "(response_rate=%.2f, win_rate=%.2f, click_rate=%.2f)",
        eps,
        config.bid_response_rate,
        config.win_rate,
        config.click_rate,
    )

    counts = {"bid_requests": 0, "bid_responses": 0, "impressions": 0, "clicks": 0}
    try:
        next_send = time.monotonic()
        while True:
            now = time.monotonic()
            if now < next_send:
                time.sleep(next_send - now)

            bid_request = generate_bid_request()
            producer.send(
                config.topic_bid_requests,
                key=bid_request["id"],
                value=bid_request,
            )
            counts["bid_requests"] += 1

            if random.random() < config.bid_response_rate:
                bid_response = generate_bid_response(bid_request)
                producer.send(
                    config.topic_bid_responses,
                    key=bid_response["id"],
                    value=bid_response,
                )
                counts["bid_responses"] += 1

                if random.random() < config.win_rate:
                    impression = generate_impression(bid_request, bid_response)
                    producer.send(
                        config.topic_impressions,
                        key=impression["impression_id"],
                        value=impression,
                    )
                    counts["impressions"] += 1

                    if random.random() < config.click_rate:
                        click = generate_click(bid_request, impression)
                        producer.send(
                            config.topic_clicks,
                            key=click["click_id"],
                            value=click,
                        )
                        counts["clicks"] += 1

            total = counts["bid_requests"]
            if total % 100 == 0:
                producer.flush()
                logger.info(
                    "Produced: %d requests, %d responses, %d impressions, %d clicks",
                    counts["bid_requests"],
                    counts["bid_responses"],
                    counts["impressions"],
                    counts["clicks"],
                )

            next_send += interval
    except KeyboardInterrupt:
        logger.info(
            "Shutting down. Totals: %d requests, %d responses, "
            "%d impressions, %d clicks",
            counts["bid_requests"],
            counts["bid_responses"],
            counts["impressions"],
            counts["clicks"],
        )
    finally:
        producer.flush()
        producer.close()
        logger.info("Producer closed.")


if __name__ == "__main__":
    main()
