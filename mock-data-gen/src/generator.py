"""
Kafka producer for the OpenRTB 2.6 event funnel.

This module implements the main event generation loop that:
1. Generates bid requests at a configurable rate (events per second)
2. Probabilistically generates correlated downstream events (responses, impressions, clicks)
3. Publishes all events to their respective Kafka topics

The funnel conversion rates are configurable via environment variables,
allowing simulation of different market conditions.
"""

import json
import logging
import random
import time
from datetime import datetime, timedelta, timezone

from kafka import KafkaProducer

from src.config import config
from src.schemas import (
    generate_bid_request,
    generate_bid_response,
    generate_click,
    generate_impression,
)

# Configure logging with timestamps for observability
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)


def create_producer() -> KafkaProducer:
    """
    Create a KafkaProducer with exponential backoff retry on connection failure.

    This is important for containerized deployments where Kafka may not be
    immediately available when the generator starts. The exponential backoff
    prevents overwhelming Kafka during startup while ensuring eventual connection.

    Retry strategy:
    - Initial delay: 1 second
    - Maximum delay: 30 seconds (caps exponential growth)
    - Maximum attempts: 10 (fails after ~5 minutes of retrying)

    Returns:
        KafkaProducer: A connected Kafka producer instance

    Raises:
        Exception: If connection fails after all retry attempts
    """
    delay = 1.0  # Initial retry delay in seconds
    max_delay = 30.0  # Cap on exponential backoff
    max_retries = 10  # Give up after this many attempts

    for attempt in range(1, max_retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=config.kafka_bootstrap_servers,
                # Serialize message values as JSON-encoded UTF-8 strings
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                # Serialize message keys as UTF-8 strings (used for partitioning)
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
            # Exponential backoff: double the delay each time, up to max_delay
            delay = min(delay * 2, max_delay)

    # Unreachable, but satisfies type checkers
    raise RuntimeError("Failed to connect to Kafka")


def run_backfill(producer: KafkaProducer, hours: int) -> dict:
    """
    Generate historical events spanning the past N hours at max speed.

    Produces events with timestamps evenly spread from (now - hours) to now,
    sent without rate limiting so Flink watermarks advance quickly through
    multiple window boundaries. After backfill completes, the caller switches
    to real-time mode.

    Args:
        producer: Connected KafkaProducer instance
        hours: Number of hours of historical data to generate

    Returns:
        dict: Event counts produced during backfill
    """
    now = datetime.now(timezone.utc)
    start = now - timedelta(hours=hours)

    # Generate events_per_second * 3600 * hours total events across the window
    total_events = config.events_per_second * 3600 * hours
    interval_seconds = (hours * 3600) / total_events

    logger.info(
        "Backfill mode: generating %d events spanning %d hours (%s to %s)...",
        total_events, hours, start.isoformat(), now.isoformat(),
    )

    counts: dict = {"bid_requests": 0, "bid_responses": 0, "impressions": 0, "clicks": 0}

    for i in range(total_events):
        # Spread timestamps evenly across the backfill window
        event_ts = start + timedelta(seconds=i * interval_seconds)

        bid_request = generate_bid_request(base_ts=event_ts)
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

        # Flush and log every 1000 events
        if counts["bid_requests"] % 1000 == 0:
            producer.flush()
            logger.info(
                "Backfill progress: %d/%d requests (%.0f%%)",
                counts["bid_requests"], total_events,
                100 * counts["bid_requests"] / total_events,
            )

    producer.flush()
    logger.info(
        "Backfill complete: %d requests, %d responses, %d impressions, %d clicks",
        counts["bid_requests"], counts["bid_responses"],
        counts["impressions"], counts["clicks"],
    )
    return counts


def main() -> None:
    """
    Main producer loop with rate-limited funnel event generation.

    This function implements a precise rate-limited event generator using
    monotonic time to maintain consistent throughput regardless of processing
    time variations. The funnel logic produces correlated events:

    Funnel flow (each stage is probabilistic):
        bid_request (100%)
            └─> bid_response (60% of requests, configurable)
                    └─> impression (15% of responses, configurable)
                            └─> click (5% of impressions, configurable)

    Events are keyed by their unique ID for Kafka partitioning, ensuring
    all events for a given request/response/impression land on the same partition
    (if partition count matches or keys hash consistently).
    """
    producer = create_producer()

    # Run backfill if configured, then continue with real-time generation
    if config.backfill_hours > 0:
        run_backfill(producer, config.backfill_hours)
        logger.info("Switching to real-time generation...")

    # Calculate the interval between events to achieve target throughput
    eps = config.events_per_second
    interval = 1.0 / eps  # Time between events in seconds

    logger.info(
        "Starting funnel generator: %d bid-requests/sec "
        "(response_rate=%.2f, win_rate=%.2f, click_rate=%.2f)",
        eps,
        config.bid_response_rate,
        config.win_rate,
        config.click_rate,
    )

    # Track event counts for logging and final summary
    counts = {"bid_requests": 0, "bid_responses": 0, "impressions": 0, "clicks": 0}

    try:
        # Use monotonic time for precise rate limiting (not affected by system clock changes)
        next_send = time.monotonic()

        while True:
            # Sleep until it's time to send the next event
            now = time.monotonic()
            if now < next_send:
                time.sleep(next_send - now)

            # Stage 1: Always generate a bid request
            bid_request = generate_bid_request()
            producer.send(
                config.topic_bid_requests,
                key=bid_request["id"],  # Partition key for ordering
                value=bid_request,
            )
            counts["bid_requests"] += 1

            # Stage 2: Probabilistically generate a bid response (~60% fill rate)
            if random.random() < config.bid_response_rate:
                bid_response = generate_bid_response(bid_request)
                producer.send(
                    config.topic_bid_responses,
                    key=bid_response["id"],
                    value=bid_response,
                )
                counts["bid_responses"] += 1

                # Stage 3: Probabilistically generate an impression (~15% win rate)
                if random.random() < config.win_rate:
                    impression = generate_impression(bid_request, bid_response)
                    producer.send(
                        config.topic_impressions,
                        key=impression["impression_id"],
                        value=impression,
                    )
                    counts["impressions"] += 1

                    # Stage 4: Probabilistically generate a click (~2% CTR)
                    if random.random() < config.click_rate:
                        click = generate_click(bid_request, impression)
                        producer.send(
                            config.topic_clicks,
                            key=click["click_id"],
                            value=click,
                        )
                        counts["clicks"] += 1

            # Flush and log progress every 100 bid requests
            total = counts["bid_requests"]
            if total % 100 == 0:
                producer.flush()  # Ensure messages are sent before logging
                logger.info(
                    "Produced: %d requests, %d responses, %d impressions, %d clicks",
                    counts["bid_requests"],
                    counts["bid_responses"],
                    counts["impressions"],
                    counts["clicks"],
                )

            # Schedule next event (accumulates to maintain precise rate)
            next_send += interval

    except KeyboardInterrupt:
        # Graceful shutdown on Ctrl+C
        logger.info(
            "Shutting down. Totals: %d requests, %d responses, "
            "%d impressions, %d clicks",
            counts["bid_requests"],
            counts["bid_responses"],
            counts["impressions"],
            counts["clicks"],
        )
    finally:
        # Ensure all buffered messages are sent before exiting
        producer.flush()
        producer.close()
        logger.info("Producer closed.")


if __name__ == "__main__":
    main()
