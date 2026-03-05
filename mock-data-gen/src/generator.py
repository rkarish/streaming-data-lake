"""
Kafka producer for the OpenRTB 2.6 event funnel.

This module implements the main event generation loop that:
1. Generates bid requests at a configurable rate (events per second)
2. Probabilistically generates correlated downstream events (responses, impressions, clicks)
3. Publishes all events to their respective Kafka topics as Avro records

The funnel conversion rates are configurable via environment variables,
allowing simulation of different market conditions.
"""

import logging
import os
import random
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

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

SCHEMA_DIR = Path(os.environ.get("AVRO_SCHEMA_DIR", "/app/schemas/avro"))


def _load_avro_serializer(
    schema_file: str, registry_client: SchemaRegistryClient
) -> AvroSerializer:
    schema_path = SCHEMA_DIR / schema_file
    try:
        schema_str = schema_path.read_text()
    except (FileNotFoundError, OSError) as exc:
        raise RuntimeError(
            f"Failed to load Avro schema from '{schema_path}'. "
            f"Resolved SCHEMA_DIR='{SCHEMA_DIR}'. "
            "Check the AVRO_SCHEMA_DIR environment variable and schema bind-mount configuration."
        ) from exc
    return AvroSerializer(registry_client, schema_str)


def _delivery_report(err, msg):
    if err is not None:
        logger.error("Delivery failed for %s: %s", msg.topic(), err)


def _produce(producers, topic, key, value):
    """Produce a message, handling BufferError with a poll-and-retry."""
    p = producers[topic]
    try:
        p.produce(topic=topic, key=key, value=value, on_delivery=_delivery_report)
    except BufferError:
        p.poll(1.0)
        p.produce(topic=topic, key=key, value=value, on_delivery=_delivery_report)
    p.poll(0)


def create_producers() -> dict[str, SerializingProducer]:
    """
    Create a SerializingProducer per Kafka topic with Avro value serialization.

    Each topic has its own producer because each uses a different Avro schema
    for value serialization. Keys are serialized as UTF-8 strings (UUIDs).

    Retry strategy:
    - Initial delay: 1 second
    - Maximum delay: 30 seconds (caps exponential growth)
    - Maximum attempts: 10

    Returns:
        dict mapping topic name to SerializingProducer
    """
    registry_client = SchemaRegistryClient({"url": config.schema_registry_url})

    topic_schemas = {
        config.topic_bid_requests: "bid_request.avsc",
        config.topic_bid_responses: "bid_response.avsc",
        config.topic_impressions: "impression.avsc",
        config.topic_clicks: "click.avsc",
    }

    key_serializer = StringSerializer("utf_8")
    producers = {}
    max_delay = 30.0
    max_retries = 10

    for topic, schema_file in topic_schemas.items():
        avro_serializer = _load_avro_serializer(schema_file, registry_client)
        delay = 1.0
        for attempt in range(1, max_retries + 1):
            try:
                producer = SerializingProducer({
                    "bootstrap.servers": config.kafka_bootstrap_servers,
                    "key.serializer": key_serializer,
                    "value.serializer": avro_serializer,
                })
                producers[topic] = producer
                logger.info("Created Avro producer for topic '%s'", topic)
                break
            except Exception as exc:
                if attempt == max_retries:
                    logger.error(
                        "Failed to create producer for '%s' after %d attempts",
                        topic, max_retries,
                    )
                    raise
                logger.warning(
                    "Producer creation attempt %d/%d for '%s' failed: %s. "
                    "Retrying in %.1fs...",
                    attempt, max_retries, topic, exc, delay,
                )
                time.sleep(delay)
                delay = min(delay * 2, max_delay)

    logger.info(
        "Connected to Kafka at %s (Schema Registry: %s)",
        config.kafka_bootstrap_servers,
        config.schema_registry_url,
    )
    return producers


def _flush_all(producers: dict[str, SerializingProducer]) -> None:
    for p in producers.values():
        p.flush()


def run_backfill(producers: dict[str, SerializingProducer], hours: int) -> dict:
    """
    Generate historical events spanning the past N hours at max speed.

    Produces events with timestamps evenly spread from (now - hours) to now,
    sent without rate limiting so Flink watermarks advance quickly through
    multiple window boundaries. After backfill completes, the caller switches
    to real-time mode.

    Args:
        producers: Dict mapping topic names to SerializingProducer instances
        hours: Number of hours of historical data to generate

    Returns:
        dict: Event counts produced during backfill
    """
    now = datetime.now(timezone.utc)
    start = now - timedelta(hours=hours)

    total_events = config.events_per_second * 3600 * hours
    interval_seconds = (hours * 3600) / total_events

    logger.info(
        "Backfill mode: generating %d events spanning %d hours (%s to %s)...",
        total_events, hours, start.isoformat(), now.isoformat(),
    )

    counts: dict = {"bid_requests": 0, "bid_responses": 0, "impressions": 0, "clicks": 0}

    for i in range(total_events):
        event_ts = start + timedelta(seconds=i * interval_seconds)

        bid_request = generate_bid_request(base_ts=event_ts)
        _produce(producers, config.topic_bid_requests, bid_request["id"], bid_request)
        counts["bid_requests"] += 1

        if random.random() < config.bid_response_rate:
            bid_response = generate_bid_response(bid_request)
            _produce(producers, config.topic_bid_responses, bid_response["id"], bid_response)
            counts["bid_responses"] += 1

            if random.random() < config.win_rate:
                impression = generate_impression(bid_request, bid_response)
                _produce(producers, config.topic_impressions, impression["impression_id"], impression)
                counts["impressions"] += 1

                if random.random() < config.click_rate:
                    click = generate_click(bid_request, impression)
                    _produce(producers, config.topic_clicks, click["click_id"], click)
                    counts["clicks"] += 1

        if counts["bid_requests"] % 1000 == 0:
            _flush_all(producers)
            logger.info(
                "Backfill progress: %d/%d requests (%.0f%%)",
                counts["bid_requests"], total_events,
                100 * counts["bid_requests"] / total_events,
            )

    _flush_all(producers)
    logger.info(
        "Backfill complete: %d requests, %d responses, %d impressions, %d clicks",
        counts["bid_requests"], counts["bid_responses"],
        counts["impressions"], counts["clicks"],
    )
    return counts


def main() -> None:
    """
    Main producer loop with rate-limited funnel event generation.

    Funnel flow (each stage is probabilistic):
        bid_request (100%)
            -> bid_response (60% of requests, configurable)
                -> impression (15% of responses, configurable)
                    -> click (5% of impressions, configurable)
    """
    producers = create_producers()

    if config.backfill_hours > 0:
        run_backfill(producers, config.backfill_hours)
        logger.info("Switching to real-time generation...")

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
            _produce(producers, config.topic_bid_requests, bid_request["id"], bid_request)
            counts["bid_requests"] += 1

            if random.random() < config.bid_response_rate:
                bid_response = generate_bid_response(bid_request)
                _produce(producers, config.topic_bid_responses, bid_response["id"], bid_response)
                counts["bid_responses"] += 1

                if random.random() < config.win_rate:
                    impression = generate_impression(bid_request, bid_response)
                    _produce(producers, config.topic_impressions, impression["impression_id"], impression)
                    counts["impressions"] += 1

                    if random.random() < config.click_rate:
                        click = generate_click(bid_request, impression)
                        _produce(producers, config.topic_clicks, click["click_id"], click)
                        counts["clicks"] += 1

            total = counts["bid_requests"]
            if total % 100 == 0:
                _flush_all(producers)
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
        _flush_all(producers)
        logger.info("Producers flushed and closed.")


if __name__ == "__main__":
    main()
