# AdTech Data Lake Streaming Platform - Design Document

## 1. Overview

A data lake streaming platform for adtech that consumes OpenRTB-formatted bid request/response events from Apache Kafka and writes them into Apache Iceberg tables backed by object storage. The system runs locally via Docker Compose and is designed for straightforward expansion to AWS (S3 + EMR/EKS) or GCP (GCS + Dataproc/GKE).

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

Similar flattened schemas will be created for `bid_responses`, `impressions`, and `clicks`.

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

### Phase 4: Cloud Readiness
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
