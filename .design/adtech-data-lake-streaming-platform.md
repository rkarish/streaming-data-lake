# AdTech Data Lake Streaming Platform - Design Document

## 1. Overview

A data lake streaming platform for adtech that produces a full OpenRTB 2.6 event funnel (bid requests, bid responses, impressions, clicks) to Apache Kafka, streams them through Apache Flink, and stores them in Apache Iceberg tables backed by MinIO (S3-compatible) object storage. Includes Trino as the SQL query engine, CloudBeaver as a web-based SQL IDE, and Apache Superset for dashboards and visualization. The system runs locally via Docker Compose and is designed for straightforward expansion to AWS (S3 + EMR/EKS) or GCP (GCS + Dataproc/GKE).

## 2. Goals

- Ingest high-volume OpenRTB 2.6 bid request and bid response events from Kafka
- Write events into partitioned Apache Iceberg tables with schema evolution support
- Provide a query layer for analytics over the lakehouse
- Run entirely locally with Docker Compose
- Maintain a clear path to deploy on AWS or GCP with minimal changes

## 3. Data Model (OpenRTB 2.6 Mock)

Mock data generators will produce realistic OpenRTB 2.6 JSON payloads and publish them to Kafka topics.

### 3.1 Kafka Topics

| Topic | Description |
|---|---|
| `bid-requests` | OpenRTB 2.6 BidRequest objects |
| `bid-responses` | OpenRTB 2.6 BidResponse objects |
| `impressions` | Win notice / impression tracking events |
| `clicks` | Click tracking events |

### 3.2 Core OpenRTB 2.6 BidRequest Fields (Mocked)

Based on the [IAB OpenRTB 2.6 spec](https://github.com/InteractiveAdvertisingBureau/openrtb2.x/blob/main/2.6.md):

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
  "user": {
    "id": "user-uuid",
    "buyeruid": "buyer-uuid"
  },
  "at": 1,
  "tmax": 120,
  "cur": ["USD"],
  "source": {
    "fd": 1,
    "tid": "transaction-uuid"
  },
  "regs": {
    "coppa": 0,
    "ext": { "gdpr": 0 }
  }
}
```

### 3.3 Iceberg Table Schema (bid_requests)

```
bid_requests (
  request_id       STRING,
  imp_id           STRING,
  imp_banner_w     INT,
  imp_banner_h     INT,
  imp_bidfloor     DOUBLE,
  site_id          STRING,
  site_domain      STRING,
  site_cat         ARRAY<STRING>,
  publisher_id     STRING,
  device_type      INT,
  device_os        STRING,
  device_geo_country STRING,
  device_geo_region  STRING,
  user_id          STRING,
  auction_type     INT,
  tmax             INT,
  currency         STRING,
  is_coppa         BOOLEAN,
  is_gdpr          BOOLEAN,
  event_timestamp  TIMESTAMP,
  received_at      TIMESTAMP
)
PARTITIONED BY (days(event_timestamp), device_geo_country)
```

### 3.4 Iceberg Table Schema (bid_responses)

```
bid_responses (
  response_id        STRING,
  request_id         STRING,      -- FK to bid_requests.request_id
  imp_id             STRING,
  bidder_id          STRING,      -- seat ID
  bid_price          DOUBLE,
  bid_currency       STRING,
  ad_domain          STRING,      -- advertiser domain
  creative_id        STRING,
  deal_id            STRING,      -- PMP deal ID (nullable)
  event_timestamp    TIMESTAMP,
  received_at        TIMESTAMP
)
PARTITIONED BY (days(event_timestamp))
```

### 3.5 Iceberg Table Schema (impressions)

```
impressions (
  impression_id      STRING,
  request_id         STRING,      -- FK to bid_requests.request_id
  response_id        STRING,      -- FK to bid_responses.response_id
  imp_id             STRING,
  bidder_id          STRING,
  win_price          DOUBLE,      -- clearing price
  win_currency       STRING,
  creative_id        STRING,
  ad_domain          STRING,
  event_timestamp    TIMESTAMP,
  received_at        TIMESTAMP
)
PARTITIONED BY (days(event_timestamp))
```

### 3.6 Iceberg Table Schema (clicks)

```
clicks (
  click_id           STRING,
  request_id         STRING,      -- FK to bid_requests.request_id
  impression_id      STRING,      -- FK to impressions.impression_id
  imp_id             STRING,
  bidder_id          STRING,
  creative_id        STRING,
  click_url          STRING,
  event_timestamp    TIMESTAMP,
  received_at        TIMESTAMP
)
PARTITIONED BY (days(event_timestamp))
```

## 4. Streaming Engine Options

Three approaches were evaluated for streaming Kafka data into Iceberg. **Apache Flink has been selected as the implementation choice.** Options B and C are retained below for reference.

---

### Option A: Apache Flink (Selected)

**Architecture:** Flink JobManager + TaskManager reading from Kafka, writing via the Flink Iceberg Sink.

**Why Flink is the strongest option:**

- **Native streaming engine.** Flink processes events continuously with true record-at-a-time semantics and low latency. No micro-batch overhead.
- **Dynamic Iceberg Sink.** The [Flink Dynamic Iceberg Sink](https://flink.apache.org/2025/11/11/from-stream-to-lakehouse-kafka-ingestion-with-the-flink-dynamic-iceberg-sink/) (available since late 2025) supports dynamic table creation, automatic schema evolution, and partition scheme changes without job restarts.
- **Flink SQL.** Pipelines can be defined entirely in SQL, reducing boilerplate. Example:
  ```sql
  INSERT INTO iceberg_catalog.db.bid_requests
  SELECT * FROM kafka_source;
  ```
- **Exactly-once guarantees** via Flink checkpointing + Iceberg's transactional commits.
- **Mature Kafka connector** with consumer group management, offset tracking, and watermark support.
- **State management** for sessionization, deduplication, and windowed aggregations if needed downstream.

**Considerations:**
- Two JVM-based services required (JobManager + TaskManager).
- Requires JVM-based job development (Java/Scala) for custom logic beyond SQL.
- Checkpoint storage needs to be configured (local FS or S3).

**Docker services:** `flink-jobmanager`, `flink-taskmanager`

**Cloud expansion:**
- AWS: Amazon Managed Flink (formerly Kinesis Data Analytics), or Flink on EKS
- GCP: Flink on GKE, or Dataproc with Flink

---

### Option B: Apache Spark Structured Streaming

**Architecture:** Spark driver + executors running a Structured Streaming job that reads from Kafka and writes to Iceberg via the DataSourceV2 API.

**Strengths:**
- Most widely adopted engine with the largest ecosystem and community.
- First-class Iceberg support via `spark-iceberg` runtime JAR.
- PySpark support enables Python-based pipeline development.
- Excellent for mixed batch + streaming workloads on the same codebase.
- [Official Docker quickstart](https://iceberg.apache.org/spark-quickstart/) available (`tabulario/spark-iceberg` image).
- Well-documented path to AWS EMR / GCP Dataproc.

**Streaming write example (PySpark):**
```python
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "bid-requests") \
    .load()

parsed = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

parsed.writeStream \
    .format("iceberg") \
    .outputMode("append") \
    .option("fanout-enabled", "true") \
    .option("checkpointLocation", "/tmp/checkpoints/bid-requests") \
    .toTable("iceberg_catalog.db.bid_requests")
```

**Considerations:**
- Micro-batch model adds inherent latency (typically 1-30 seconds per trigger interval).
- Higher memory footprint per executor compared to Flink.
- Small file problem is more pronounced; requires separate compaction jobs or Iceberg's `rewriteDataFiles` maintenance.

**Docker services:** `spark-iceberg` (includes Spark + Iceberg runtime), or `spark-master` + `spark-worker`

**Cloud expansion:**
- AWS: EMR Serverless, EMR on EKS, Glue
- GCP: Dataproc, Dataproc Serverless

---

### Option C: Kafka Connect with Iceberg Sink Connector

**Architecture:** Kafka Connect worker cluster running the Iceberg Sink Connector.

**Strengths:**
- Simplest operational model -- no custom code required.
- Declarative JSON configuration for connectors.
- The [Iceberg Sink Connector](https://github.com/tabular-io/iceberg-kafka-connect) supports schema evolution, partitioned writes, and exactly-once delivery.
- Lightweight resource footprint compared to Spark or Flink.
- Kafka Connect is a mature, well-understood component in the Kafka ecosystem.

**Configuration example:**
```json
{
  "name": "iceberg-sink-bid-requests",
  "config": {
    "connector.class": "io.tabular.iceberg.connect.IcebergSinkConnector",
    "tasks.max": "2",
    "topics": "bid-requests",
    "iceberg.tables": "db.bid_requests",
    "iceberg.catalog.type": "rest",
    "iceberg.catalog.uri": "http://iceberg-rest:8181",
    "iceberg.catalog.s3.endpoint": "http://minio:9000",
    "iceberg.catalog.warehouse": "s3://warehouse",
    "iceberg.control.commit.interval-ms": "10000"
  }
}
```

**Considerations:**
- No in-stream transformation capability. Data must arrive in a shape close to the target schema, or you need a separate SMT (Single Message Transform) chain.
- Limited to simple ETL patterns; cannot do windowed aggregations, joins, or stateful processing.
- Schema registry integration is needed for structured data handling.
- Less community momentum for the Iceberg connector compared to Flink/Spark integrations.

**Docker services:** `kafka-connect` (Confluent or Apache image with Iceberg plugin)

**Cloud expansion:**
- AWS: MSK Connect
- GCP: Confluent Cloud, or self-managed on GKE

---

### Comparison Matrix

| Criteria | Flink | Spark Structured Streaming | Kafka Connect |
|---|---|---|---|
| Latency | Low (true streaming) | Medium (micro-batch) | Medium (commit interval) |
| Iceberg integration | Dynamic Sink, schema evolution | DataSourceV2, append mode | Iceberg Sink Connector |
| Transformation capability | Full (SQL + Java/Scala) | Full (SQL + Python/Scala) | Limited (SMTs only) |
| Operational complexity | Medium-High | Medium | Low |
| Resource footprint | Medium | High | Low |
| Exactly-once | Yes (checkpointing) | Yes (checkpointing) | Yes (connector commits) |
| Schema evolution | Automatic (Dynamic Sink) | Manual (schema merge) | Via Schema Registry |
| Local dev experience | Good (Docker) | Good (Docker, notebooks) | Good (Docker) |
| Cloud managed options | Amazon Managed Flink, GKE | EMR, Dataproc | MSK Connect |
| Best for | Low-latency event streaming | Mixed batch+streaming | Simple pipe, no transforms |

## 5. Architecture (Local Docker Compose)

```
                    +-----------------+
                    | Mock Data Gen   |
                    | (OpenRTB 2.6)   |
                    +--------+--------+
                             |
                             v
                    +-----------------+
                    |   Apache Kafka  |
                    |   (KRaft mode)  |
                    +--------+--------+
                             |
                             v
              +-----------------------------+
              |    Streaming Engine          |
              |      (Apache Flink)         |
              +-------------+---------------+
                            |
                +-----------+-----------+
                |                       |
                v                       v
        +---------------+     +------------------+
        | Iceberg REST  |     | MinIO            |
        | Catalog       |     | (S3-compatible)  |
        +---------------+     +------------------+
                                        |
                                        v
                              +------------------+
                              | Iceberg Tables   |
                              | (Parquet files)  |
                              +------------------+
                                        |
                                        v
                              +------------------+
                              | Query Engine     |
                              | (Trino / Spark)  |
                              +------------------+
```

### 5.1 Docker Compose Services

| Service | Image / Base | Port | Purpose |
|---|---|---|---|
| `kafka` | `apache/kafka` (KRaft) | 9092 | Message broker, no ZooKeeper |
| `schema-registry` | `confluentinc/cp-schema-registry` | 8081 | Avro/JSON schema management |
| `mock-data-gen` | Custom (Python) | -- | Generates OpenRTB events to Kafka |
| `iceberg-rest` | `tabulario/iceberg-rest` | 8181 | Iceberg REST catalog |
| `minio` | `minio/minio` | 9000/9001 | S3-compatible object storage |
| `flink-jobmanager` | `flink` | 8081 | Flink JobManager |
| `flink-taskmanager` | `flink` | -- | Flink TaskManager |
| `trino` | `trinodb/trino` | 8080 | SQL query engine over Iceberg |

### 5.2 Storage Layout (MinIO / S3)

```
s3://warehouse/
  db/
    bid_requests/
      metadata/
        v1.metadata.json
        snap-*.avro
      data/
        event_timestamp_day=2026-02-03/device_geo_country=USA/
          00000-0-*.parquet
    bid_responses/
      ...
    impressions/
      ...
    clicks/
      ...
```

## 6. Mock Data Generator

A Python service using `faker` and custom logic to produce realistic OpenRTB 2.6 payloads.

### 6.1 Capabilities
- Configurable events-per-second rate
- Produces to all four Kafka topics
- Correlated bid request -> bid response -> impression -> click funnels
- Realistic distributions for geo, device types, IAB categories, bid floors
- JSON serialization with optional Avro (via Schema Registry)

### 6.2 Configuration (environment variables)

| Variable | Default | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` | Kafka broker address |
| `EVENTS_PER_SECOND` | `100` | Target throughput |
| `BID_RESPONSE_RATE` | `0.60` | % of requests that get a response |
| `WIN_RATE` | `0.15` | % of responses that win |
| `CLICK_RATE` | `0.02` | % of impressions that get clicked |

## 7. Table Maintenance

Iceberg tables written by streaming jobs accumulate small files. A maintenance strategy is required:

- **Compaction:** Periodic `rewriteDataFiles` action to merge small Parquet files (target 256-512 MB).
- **Snapshot expiry:** Remove old snapshots beyond a retention window (e.g., 7 days).
- **Orphan file cleanup:** Remove data files not referenced by any snapshot.
- **Sort order optimization:** Apply sort orders (e.g., by `site_domain`, `device_geo_country`) during compaction for better query performance.

For local dev, a scheduled Spark job or Flink maintenance job handles this. In production, this maps to a scheduled EMR/Dataproc job or Airflow DAG.

## 8. Query Layer

### 8.1 Trino (Primary)
Trino provides fast, distributed SQL queries over Iceberg tables. The Trino Iceberg connector supports:
- Predicate pushdown into Iceberg metadata (partition pruning, file skipping)
- Time travel queries (`SELECT * FROM table FOR TIMESTAMP AS OF ...`)
- Schema evolution transparency

### 8.2 Spark SQL (Alternative)
The same Spark deployment used for streaming can serve interactive queries via `spark-sql` or notebooks.

## 9. Cloud Expansion Path

### 9.1 AWS
| Local Component | AWS Equivalent |
|---|---|
| MinIO | Amazon S3 |
| Kafka (KRaft) | Amazon MSK |
| Iceberg REST Catalog | AWS Glue Catalog |
| Flink | Amazon Managed Flink / Flink on EKS |
| Spark | EMR Serverless / EMR on EKS |
| Kafka Connect | MSK Connect |
| Trino | Amazon Athena / Trino on EKS |

### 9.2 GCP
| Local Component | GCP Equivalent |
|---|---|
| MinIO | Google Cloud Storage |
| Kafka (KRaft) | Confluent Cloud on GCP / self-managed on GKE |
| Iceberg REST Catalog | BigLake Metastore / Nessie on GKE |
| Flink | Flink on GKE |
| Spark | Dataproc / Dataproc Serverless |
| Kafka Connect | Self-managed on GKE |
| Trino | Trino on GKE / BigQuery (with BigLake) |

### 9.3 What Changes for Cloud Deployment
- **Storage path:** `s3://bucket/...` or `gs://bucket/...` instead of MinIO endpoint
- **Catalog config:** Swap REST catalog URI for Glue/BigLake catalog type
- **Credentials:** IAM roles (AWS) or service accounts (GCP) instead of MinIO access keys
- **Networking:** VPC, security groups, private endpoints
- **Scaling:** Adjust parallelism, executor/taskmanager counts based on throughput

The streaming job code and Iceberg table definitions remain unchanged.

## 10. Project Structure

```
streaming-data-lake/
  .design/
    adtech-streaming-platform.md    # This document
  docker-compose.yml                # All local services
  mock-data-gen/
    pyproject.toml
    src/
      generator.py                  # OpenRTB event generator
      schemas.py                    # OpenRTB 2.6 data models
      config.py                     # Configuration
  streaming/
    flink/
      sql/
        create_tables.sql           # Flink SQL DDL
        insert_jobs.sql             # Flink SQL streaming jobs
    spark/
      jobs/
        streaming_ingest.py         # PySpark Structured Streaming job
        table_maintenance.py        # Compaction / snapshot management
    connect/
      connectors/
        iceberg-sink.json           # Kafka Connect connector config
  catalog/
    iceberg-rest-config.yaml        # REST catalog configuration
  trino/
    catalog/
      iceberg.properties            # Trino Iceberg catalog config
  scripts/
    setup.sh                        # Initialize topics, schemas, tables
    query-examples.sh               # Sample Trino queries
  CLAUDE.md
  README.md
```

## 11. Implementation Phases

### Phase 1: Foundation
- Docker Compose with Kafka (KRaft), MinIO, Iceberg REST catalog
- Mock data generator producing to `bid-requests` topic
- Basic Iceberg table creation

### Phase 2: Streaming Pipeline
- Implement Apache Flink streaming pipeline
- End-to-end: Kafka -> Flink -> Iceberg tables
- All four topics flowing

### Phase 3: Query & Analytics
- Trino integration with Iceberg catalog
- Sample analytical queries (fill rate, CPM by geo, win rate trends)
- Table maintenance jobs

### Phase 4: Data Exploration & Visualization
- CloudBeaver (web-based SQL IDE) connected to Trino for interactive querying of Iceberg tables
- Apache Superset connected to Trino for dashboards and data visualization
- Docker Compose services for both tools with pre-configured Trino datasource connections

#### CloudBeaver
- Image: `dbeaver/cloudbeaver:latest`
- Connects to Trino via the JDBC driver (Trino JDBC is bundled in CloudBeaver)
- Provides a browser-based SQL editor at `http://localhost:8978`
- Allows ad-hoc exploration of the `iceberg.db` schema without CLI access

#### Apache Superset
- Image: `apache/superset:latest`
- Connects to Trino via the `trino` Python driver (SQLAlchemy URI: `trino://trino@trino:8080/iceberg/db`)
- Provides dashboards and charts at `http://localhost:8088`
- Pre-configured with a simple visualization (e.g., bid requests by country, bid floor distribution)
- Bootstrap script to create the Trino datasource and sample charts on first run

#### New Docker Compose Services

| Service | Image | Port | Purpose |
|---|---|---|---|
| `cloudbeaver` | `dbeaver/cloudbeaver` | 8978 | Web SQL IDE for Trino |
| `superset` | `apache/superset` | 8088 | Dashboards & visualization |

### Phase 5: Full OpenRTB Funnel (bid-responses, impressions, clicks)

Extend the platform from a single `bid-requests` topic to the full AdTech event funnel. All downstream events are correlated to bid requests via `request_id`, enabling funnel analytics (fill rate, win rate, CTR) through Trino joins.

#### 5.1 Event Funnel & Correlation

```
bid-request  -->  bid-response  -->  impression  -->  click
  (100%)           (~60%)             (~15%)          (~2%)
```

Each event carries `request_id` (and `imp_id`) from the originating bid request. The generator produces correlated events: for each bid request, it probabilistically generates a bid response, then an impression (win notice), then a click, reusing IDs from the parent event.

#### 5.2 New Kafka Topics

| Topic | Description |
|---|---|
| `bid-responses` | OpenRTB 2.6 BidResponse objects (correlated to bid-requests via `request_id`) |
| `impressions` | Win notice / impression tracking events (correlated via `request_id`, `response_id`) |
| `clicks` | Click tracking events (correlated via `request_id`, `imp_id`) |

#### 5.3 Mock Data Generator Changes

**`config.py`** -- new environment variables:

| Variable | Default | Description |
|---|---|---|
| `TOPIC_BID_RESPONSES` | `bid-responses` | Kafka topic for bid responses |
| `TOPIC_IMPRESSIONS` | `impressions` | Kafka topic for impressions |
| `TOPIC_CLICKS` | `clicks` | Kafka topic for clicks |
| `BID_RESPONSE_RATE` | `0.60` | Probability a bid request gets a response |
| `WIN_RATE` | `0.15` | Probability a bid response wins (impression) |
| `CLICK_RATE` | `0.02` | Probability an impression gets clicked |

**`schemas.py`** -- new generator functions:

- `generate_bid_response(bid_request)` -- takes the parent bid request, reuses `request_id`, `imp_id`, generates bidder/price/creative fields
- `generate_impression(bid_request, bid_response)` -- takes parent request and response, reuses IDs, sets win price (typically <= bid price)
- `generate_click(bid_request, impression)` -- takes parent request and impression, reuses IDs, generates click URL

All timestamps are slightly after their parent event's timestamp to maintain realistic ordering.

**`generator.py`** -- funnel logic in the main loop:

```
for each tick:
    bid_request = generate_bid_request()
    produce(bid-requests, bid_request)

    if random() < BID_RESPONSE_RATE:
        bid_response = generate_bid_response(bid_request)
        produce(bid-responses, bid_response)

        if random() < WIN_RATE:
            impression = generate_impression(bid_request, bid_response)
            produce(impressions, impression)

            if random() < CLICK_RATE:
                click = generate_click(bid_request, impression)
                produce(clicks, click)
```

#### 5.4 Flink SQL Changes

**`create_tables.sql`** -- add 3 new Kafka source table DDLs:

- `kafka_bid_responses` -- maps the bid response JSON structure
- `kafka_impressions` -- maps the impression JSON structure
- `kafka_clicks` -- maps the click JSON structure

Each source table uses the same Kafka connector config pattern as `kafka_bid_requests` with the appropriate topic name and consumer group.

**`insert_jobs.sql`** -- use `STATEMENT SET` to run all 4 inserts in a single Flink job:

```sql
EXECUTE STATEMENT SET
BEGIN
  INSERT INTO iceberg_catalog.db.bid_requests SELECT ... FROM kafka_bid_requests;
  INSERT INTO iceberg_catalog.db.bid_responses SELECT ... FROM kafka_bid_responses;
  INSERT INTO iceberg_catalog.db.impressions SELECT ... FROM kafka_impressions;
  INSERT INTO iceberg_catalog.db.clicks SELECT ... FROM kafka_clicks;
END;
```

This is more efficient than submitting 4 separate jobs since they share the Flink runtime and checkpointing.

#### 5.5 Setup Script Changes (`setup.sh`)

- **Kafka topics**: Create `bid-responses`, `impressions`, `clicks` topics (3 partitions each)
- **Iceberg tables**: Create `db.bid_responses`, `db.impressions`, `db.clicks` tables via REST API with the schemas defined above
- **Trino verification**: Verify all 4 tables are visible via `SHOW TABLES`

#### 5.6 Query Examples (`query-examples.sh`)

Add funnel analytics queries that join across the tables:

- **Fill rate by country**: `bid_responses / bid_requests` grouped by `device_geo_country`
- **Win rate by bidder**: `impressions / bid_responses` grouped by `bidder_id`
- **CTR by creative**: `clicks / impressions` grouped by `creative_id`
- **Revenue by publisher**: `SUM(win_price)` from impressions joined to bid_requests grouped by `publisher_id`
- **Full funnel**: request -> response -> impression -> click counts in a single query
- **Average bid-to-win spread**: `AVG(bid_price - win_price)` from responses joined to impressions

#### 5.7 Superset Changes (`setup-dashboards.py`)

Add 3 new datasets for the new tables:
- `bid_responses` dataset
- `impressions` dataset
- `clicks` dataset

#### 5.8 Modified Files Summary

| File | Changes |
|---|---|
| `mock-data-gen/src/config.py` | Add 3 topic names + 3 funnel rate env vars |
| `mock-data-gen/src/schemas.py` | Add `generate_bid_response()`, `generate_impression()`, `generate_click()` |
| `mock-data-gen/src/generator.py` | Funnel logic: produce to all 4 topics with correlated events |
| `streaming/flink/sql/create_tables.sql` | Add 3 Kafka source table DDLs |
| `streaming/flink/sql/insert_jobs.sql` | Convert to `STATEMENT SET` with 4 INSERT statements |
| `scripts/setup.sh` | Create 3 new Kafka topics + 3 new Iceberg tables |
| `scripts/query-examples.sh` | Add funnel join queries |
| `superset/setup-dashboards.py` | Add 3 new datasets |
| `docker-compose.yml` | Add 3 new topic env vars to `mock-data-gen` |
| `README.md` | Document new topics, tables, funnel queries |

### Phase 6: Cloud Readiness
- Parameterize storage and catalog configs for AWS/GCP
- Document deployment steps for each cloud
- CI/CD pipeline for streaming job deployment

## 12. References

- [OpenRTB 2.6 Specification (IAB GitHub)](https://github.com/InteractiveAdvertisingBureau/openrtb2.x/blob/main/2.6.md)
- [OpenRTB 2.6 PDF (IAB Tech Lab)](https://iabtechlab.com/wp-content/uploads/2022/04/OpenRTB-2-6_FINAL.pdf)
- [Flink Dynamic Iceberg Sink (Apache Flink Blog)](https://flink.apache.org/2025/11/11/from-stream-to-lakehouse-kafka-ingestion-with-the-flink-dynamic-iceberg-sink/)
- [Iceberg Flink Getting Started](https://iceberg.apache.org/docs/latest/flink/)
- [Iceberg Spark Quickstart](https://iceberg.apache.org/spark-quickstart/)
- [Iceberg Spark Structured Streaming Docs](https://iceberg.apache.org/docs/1.4.0/spark-structured-streaming/)
- [Kafka Connect vs Flink vs Spark (Onehouse)](https://www.onehouse.ai/blog/kafka-connect-vs-flink-vs-spark-choosing-the-right-ingestion-framework)
- [Streaming Data Lakehouse with Flink, Kafka, Iceberg & Polaris](https://medium.com/@gilles.philippart/build-a-streaming-data-lakehouse-with-apache-flink-kafka-iceberg-and-polaris-473c47e04525)
- [Data Streaming Meets Lakehouse (Kai Waehner)](https://www.kai-waehner.de/blog/2025/11/19/data-streaming-meets-lakehouse-apache-iceberg-for-unified-real-time-and-batch-analytics/)
- [AWS Iceberg Streaming Examples](https://github.com/aws-samples/iceberg-streaming-examples)
