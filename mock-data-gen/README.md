# Mock Data Generator

A Python service that generates realistic OpenRTB 2.6 ad-tech event data and publishes it to Apache Kafka. Produces a correlated event funnel: bid requests → bid responses → impressions → clicks.

## Overview

This generator simulates the full programmatic advertising auction lifecycle:

```
bid-request  →  bid-response  →  impression  →  click
   (100%)          (~60%)          (~15%)        (~2%)
```

Each downstream event is correlated to its parent via shared IDs (`request_id`, `response_id`, `impression_id`), enabling funnel analytics through joins.

## Requirements

- Python 3.12+
- Apache Kafka (running)

## Installation

### From source

```bash
cd mock-data-gen
pip install .
```

### Development install

```bash
pip install -e .
```

## Usage

### Run directly

```bash
python -m src.generator
```

### Run via entry point (after install)

```bash
mock-data-gen
```

### Run with Docker

```bash
docker build -t mock-data-gen .
docker run --rm -e KAFKA_BOOTSTRAP_SERVERS=kafka:9092 mock-data-gen
```

### Run with Docker Compose (from project root)

```bash
docker compose --profile generator up mock-data-gen
```

## Configuration

All configuration is via environment variables:

| Variable | Default | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` | Kafka broker address |
| `EVENTS_PER_SECOND` | `10` | Target bid-request throughput |
| `TOPIC_BID_REQUESTS` | `bid-requests` | Kafka topic for bid requests |
| `TOPIC_BID_RESPONSES` | `bid-responses` | Kafka topic for bid responses |
| `TOPIC_IMPRESSIONS` | `impressions` | Kafka topic for impressions |
| `TOPIC_CLICKS` | `clicks` | Kafka topic for clicks |
| `BID_RESPONSE_RATE` | `0.60` | Probability a bid request gets a response (~60% fill rate) |
| `WIN_RATE` | `0.15` | Probability a bid response wins the auction (~15%) |
| `CLICK_RATE` | `0.02` | Probability an impression generates a click (~2% CTR) |

### Example

```bash
KAFKA_BOOTSTRAP_SERVERS=localhost:29092 \
EVENTS_PER_SECOND=50 \
BID_RESPONSE_RATE=0.70 \
WIN_RATE=0.20 \
CLICK_RATE=0.03 \
python -m src.generator
```

## Kafka Topics

The generator produces JSON messages to 4 topics:

| Topic | Description | Key |
|---|---|---|
| `bid-requests` | OpenRTB 2.6 BidRequest objects | `request_id` |
| `bid-responses` | OpenRTB 2.6 BidResponse objects | `response_id` |
| `impressions` | Win notice / impression events | `impression_id` |
| `clicks` | Click tracking events | `click_id` |

## Data Model

### Bid Request (OpenRTB 2.6)

```json
{
  "id": "request-uuid",
  "imp": [{
    "id": "1",
    "banner": { "w": 300, "h": 250, "pos": 1 },
    "bidfloor": 0.50,
    "bidfloorcur": "USD",
    "secure": 1
  }],
  "site": {
    "id": "site-001",
    "domain": "example.com",
    "cat": ["IAB1"],
    "page": "https://example.com/article/123",
    "publisher": { "id": "pub-001", "name": "Example Publisher" }
  },
  "device": {
    "ua": "Mozilla/5.0...",
    "ip": "203.0.113.1",
    "geo": { "lat": 37.7749, "lon": -122.4194, "country": "USA", "region": "CA" },
    "devicetype": 2,
    "os": "iOS",
    "osv": "17.0"
  },
  "user": { "id": "user-uuid", "buyeruid": "buyer-uuid" },
  "at": 1,
  "tmax": 120,
  "cur": ["USD"],
  "source": { "fd": 1, "tid": "transaction-uuid" },
  "regs": { "coppa": 0, "ext": { "gdpr": 0 } },
  "event_timestamp": "2026-02-04T12:00:00.000Z",
  "received_at": "2026-02-04T12:00:00.025Z"
}
```

### Bid Response (OpenRTB 2.6)

```json
{
  "id": "response-uuid",
  "seatbid": [{
    "seat": "seat-alpha",
    "bid": [{
      "id": "bid-uuid",
      "impid": "1",
      "price": 2.50,
      "adid": "ad-uuid",
      "crid": "cr-1001",
      "adomain": ["ads.example.com"],
      "w": 300,
      "h": 250,
      "dealid": "deal-5"
    }]
  }],
  "bidid": "bidid-uuid",
  "cur": "USD",
  "ext": { "request_id": "request-uuid" },
  "event_timestamp": "2026-02-04T12:00:00.050Z"
}
```

### Impression

```json
{
  "impression_id": "impression-uuid",
  "request_id": "request-uuid",
  "response_id": "response-uuid",
  "imp_id": "1",
  "bidder_id": "seat-alpha",
  "win_price": 2.35,
  "win_currency": "USD",
  "creative_id": "cr-1001",
  "ad_domain": "ads.example.com",
  "event_timestamp": "2026-02-04T12:00:00.250Z"
}
```

### Click

```json
{
  "click_id": "click-uuid",
  "request_id": "request-uuid",
  "impression_id": "impression-uuid",
  "imp_id": "1",
  "bidder_id": "seat-alpha",
  "creative_id": "cr-1001",
  "click_url": "https://ads.example.com/click/uuid",
  "event_timestamp": "2026-02-04T12:00:05.000Z"
}
```

## Realistic Distributions

The generator uses weighted random distributions to produce realistic data:

| Field | Distribution |
|---|---|
| Banner sizes | 300x250 (40%), 728x90 (20%), 160x600 (15%), 320x50 (15%), 970x250 (10%) |
| Device types | Mobile/tablet (60%), Desktop (30%), Phone (10%) |
| Operating systems | iOS (35%), Android (35%), Windows (20%), macOS (10%) |
| Countries | USA (50%), GBR (15%), DEU (10%), CAN (10%), AUS (10%), FRA (5%) |
| Auction type | First-price (70%), Second-price (30%) |
| COPPA | Non-COPPA (95%), COPPA (5%) |
| GDPR | Non-GDPR (70%), GDPR (30%) |
| Deal ID | No deal (90%), PMP deal (10%) |

## Project Structure

```
mock-data-gen/
  pyproject.toml      # Python package configuration
  Dockerfile          # Container image definition
  README.md           # This file
  src/
    __init__.py
    config.py         # Environment variable configuration
    schemas.py        # OpenRTB 2.6 event generators
    generator.py      # Kafka producer main loop
```

## Dependencies

- `kafka-python-ng>=2.0.2` -- Kafka producer client
- `faker>=22.0.0` -- Realistic fake data generation

## Local Development

For local debugging against a Docker Compose Kafka instance, use the host-exposed port:

```bash
KAFKA_BOOTSTRAP_SERVERS=localhost:29092 python -m src.generator
```

Or use the VS Code launch configuration from the project root (see `.vscode/launch.json`).
