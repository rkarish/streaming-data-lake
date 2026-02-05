# AdTech Data Lake Streaming Platform

A data lake streaming platform for adtech that produces a full OpenRTB 2.6 event funnel (bid requests, bid responses, impressions, clicks) to Apache Kafka, streams them through Apache Flink, and stores them in Apache Iceberg tables backed by MinIO (S3-compatible) object storage. Includes Trino as the SQL query engine, CloudBeaver as a web-based SQL IDE, and Apache Superset for dashboards and visualization.

See [`.design/adtech-data-lake-streaming-platform.md`](.design/adtech-data-lake-streaming-platform.md) for the full design document.

## Architecture

```
Mock Data Gen  --->  Kafka (KRaft)
                         |
                         v
    +----------------------------------------------+
    |                   Flink                      |   
    |                                              |
    |  Core Funnel &    Streaming     Funnel       |
    |  Enrichment Job   Aggregation   Metrics Job  |
    +----------------------------------------------+
                         |
                         |
              +----------+----------+
              |                     |
        Iceberg REST            MinIO (S3)
         Catalog                    |
              |              Iceberg Tables
              |             (Parquet files)
              |                     |
              +----------+----------+
                         |
                   Trino (Query)
                         |
              +----------+----------+
              |                     |
        CloudBeaver           Superset
       (Web SQL IDE)         (Dashboards)
```

**Services:**

| Service | Image | Ports |
|---|---|---|
| `kafka` | `apache/kafka:3.8.1` (KRaft) | 29092 (host), 9092 (internal) |
| `minio` | `minio/minio:latest` | 9000 (S3), 9001 (console) |
| `iceberg-rest` | `tabulario/iceberg-rest:0.10.0` | 8181 |
| `mock-data-gen` | Custom (Python 3.12) | -- |
| `flink-jobmanager` | Custom (Flink 1.20 + Iceberg) | 8081 (Web UI) |
| `flink-taskmanager` | Custom (Flink 1.20 + Iceberg) | -- |
| `trino` | `trinodb/trino:467` | 8080 (Web UI) |
| `cloudbeaver` | `dbeaver/cloudbeaver:latest` | 8978 (Web UI) |
| `superset` | Custom (apache/superset + trino driver) | 8088 (Web UI) |
| `superset-postgres` | `postgres:16-alpine` | internal only |

## Prerequisites

- Docker and Docker Compose
- Python 3.12+ (for local development only)
- `curl` (for setup script)
- [Claude Code](https://claude.com/claude-code) with the `voltagent-data-ai` subagent (for AI-assisted development)

### Claude Code Subagent Setup

This project uses the `voltagent-data-ai:data-engineer` subagent for implementation and engineering tasks. Install it via the [VoltAgent plugin marketplace](https://github.com/VoltAgent/awesome-claude-code-subagents):

```bash
claude plugin marketplace add VoltAgent/awesome-claude-code-subagents
claude plugin install voltagent-data-ai
```

## Quick Start (Docker)

### 1. Build and start all services

```bash
docker compose up --build -d
```

This starts the infrastructure services (Kafka, MinIO, Iceberg, Flink, Trino, CloudBeaver, Superset) but **not** the mock data generator. To also start the generator (continuous event stream):

```bash
docker compose --profile generator up --build -d
```

To backfill historical data (useful for testing hourly window aggregations), set `BACKFILL_HOURS` to generate N hours of historical events at max speed before switching to real-time:

```bash
BACKFILL_HOURS=3 docker compose --profile generator up --build -d
```

Wait for all services to become healthy:

```bash
docker compose ps
```

### 2. Run the setup script

Creates the Kafka topics, MinIO bucket, Iceberg namespace + tables, submits the Flink streaming job, and verifies Trino connectivity:

```bash
bash scripts/setup.sh
```

### 3. Verify

Check that events are flowing through Kafka (4 topics: `bid-requests`, `bid-responses`, `impressions`, `clicks`):

```bash
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic bid-requests \
  --from-beginning \
  --max-messages 3
```

Verify the Flink job is running:

```bash
curl -s http://localhost:8081/jobs | python3 -m json.tool
```

Check that Parquet files appear in MinIO (after the first checkpoint, ~60s):

```bash
docker compose exec minio mc ls --recursive local/warehouse/db/bid_requests/
```

Open the Flink Web UI at [http://localhost:8081](http://localhost:8081) to monitor records received/sent.

Verify the Iceberg tables exist:

```bash
curl -s http://localhost:8181/v1/namespaces/db/tables | python3 -m json.tool
```

Query all 4 tables via Trino:

```bash
docker exec trino trino --catalog iceberg --schema db \
  --execute "SELECT 'bid_requests' AS t, COUNT(*) AS cnt FROM bid_requests
             UNION ALL SELECT 'bid_responses', COUNT(*) FROM bid_responses
             UNION ALL SELECT 'impressions', COUNT(*) FROM impressions
             UNION ALL SELECT 'clicks', COUNT(*) FROM clicks"
```

View generator logs:

```bash
docker compose logs mock-data-gen --tail 20
```

### 4. Read data from Kafka

Consume a few messages from any topic (replace `bid-requests` with `bid-responses`, `impressions`, or `clicks`):

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic bid-requests \
  --from-beginning \
  --max-messages 5
```

Pipe through `python3` for pretty-printed JSON:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic bid-requests \
  --from-beginning \
  --max-messages 1 | python3 -m json.tool
```

Check topic offsets (total message count per partition):

```bash
docker compose exec kafka /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server localhost:9092 \
  --topic bid-requests
```

Tail new messages in real time (Ctrl+C to stop):

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic bid-requests
```

### 5. Query the Iceberg table (Flink SQL)

Open a Flink SQL Client session:

```bash
docker compose exec flink-jobmanager /opt/flink/bin/sql-client.sh embedded
```

Register the Iceberg catalog (required each session):

```sql
CREATE CATALOG iceberg_catalog WITH (
    'type' = 'iceberg',
    'catalog-type' = 'rest',
    'uri' = 'http://iceberg-rest:8181',
    'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
    's3.endpoint' = 'http://minio:9000',
    's3.path-style-access' = 'true',
    'warehouse' = 's3://warehouse/'
);
```

Switch to batch mode (without this, queries run in streaming mode and never finish):

```sql
SET 'execution.runtime-mode' = 'batch';
```

Run queries:

```sql
-- Preview rows
SELECT * FROM iceberg_catalog.db.bid_requests LIMIT 10;

-- Count total records
SELECT COUNT(*) FROM iceberg_catalog.db.bid_requests;

-- Aggregation example
SELECT device_geo_country, COUNT(*) AS cnt
FROM iceberg_catalog.db.bid_requests
GROUP BY device_geo_country
ORDER BY cnt DESC;
```

Type `QUIT;` to exit the SQL client.

### 6. Query with Trino

Trino provides a standard SQL interface to the Iceberg tables. Run an ad-hoc query:

```bash
docker exec trino trino --catalog iceberg --schema db \
  --execute "SELECT device_geo_country, COUNT(*) AS cnt FROM bid_requests GROUP BY device_geo_country ORDER BY cnt DESC LIMIT 5"
```

Run the full set of sample analytical queries:

```bash
bash scripts/query-examples.sh
```

The Trino Web UI is available at [http://localhost:8080](http://localhost:8080) (no credentials required).

### 7. Table Maintenance

Run Iceberg table maintenance (compaction, snapshot expiry, orphan cleanup) via Trino:

```bash
bash scripts/maintenance.sh
```

### 8. CloudBeaver (Web SQL IDE)

Open [http://localhost:8978](http://localhost:8978). On the first launch you will need to complete the setup wizard:

1. Set an admin password (must meet complexity requirements, e.g. `Password123!`)
2. Finish the wizard

Once configured, click the CloudBeaver logo in the top-left corner to reach the main database UI. The pre-configured "Trino Iceberg" connection appears in the sidebar. Expand it to browse the `iceberg > db` schema with all 4 tables (`bid_requests`, `bid_responses`, `impressions`, `clicks`) and run SQL queries.

The workspace is persisted in a Docker volume (`cloudbeaver-workspace`), so subsequent restarts will skip the wizard. After a restart, you will need to click the settings icon in the top-right corner, click "Log In", and sign in with the admin credentials you set during the wizard (e.g. `cbadmin` / `Password123!`).

### 9. Superset (Charts & SQL Lab)

Open [http://localhost:8088](http://localhost:8088) and log in with `admin` / `password`. The setup script creates:

- A Trino database connection
- Datasets for all 8 tables (core funnel + enriched + aggregations + funnel metrics)
- Charts (visible under Charts):
  - "Bid Requests by Country" -- pie chart of request volume by geo
  - "Bid Responses by Bidder Seat" -- pie chart of responses per bidder
  - "Impressions by Bidder" -- pie chart of win counts per bidder
  - "Clicks by Creative" -- pie chart of click counts per creative
  - "Requests by Device Category" -- pie chart from enriched data (Desktop, Mobile Web, Mobile App, CTV)
  - "Test vs Production Traffic" -- pie chart of test/production split from enriched data
  - "Hourly Revenue by Country" -- pie chart of total revenue by country from hourly aggregations
  - "Rolling Win Count by Bidder" -- bar chart of win count and revenue from 5-minute rolling windows
  - "Funnel Conversion by Publisher" -- bar chart of bid requests/responses/impressions/clicks per publisher
- An "AdTech Data Lake Test" dashboard with auto-refresh (15s)

You can also use SQL Lab to run ad-hoc queries against Trino.

To re-run `setup-dashboards.py` after making changes (the script is bind-mounted, so local edits are reflected immediately):

```bash
docker exec superset python /app/setup-dashboards.py
```

### 10. Stop

If the mock data generator is running, you must include the `generator` profile when stopping -- otherwise the generator container keeps running and holds the Docker network open:

```bash
docker compose --profile generator down
```

If you haven't started the generator, a plain `docker compose down` is sufficient.

## Local Debug Environment

You can debug the mock data generator locally in VS Code while keeping the infrastructure services in Docker.

### 1. Build and start infrastructure services

```bash
docker compose build flink-jobmanager flink-taskmanager
docker compose up kafka minio iceberg-rest trino flink-jobmanager flink-taskmanager -d
```

Wait for all services to become healthy:

```bash
docker compose ps
```

### 2. Run the setup script

Creates the Kafka topics, MinIO bucket, Iceberg namespace + tables, submits the Flink streaming job, and verifies Trino connectivity:

```bash
bash scripts/setup.sh
```

### 3. Set up the local debug environment

This creates a `.venv` virtual environment and installs the generator's dependencies:

```bash
bash scripts/setup-generator-local-debug.sh
```

### 4. Run/debug with VS Code

Use the **"Mock Data Generator"** launch configuration (in `.vscode/launch.json`) to run or debug the generator. It points at `localhost:29092` and includes all funnel rate environment variables. Set breakpoints in `mock-data-gen/src/` and press F5.

To test hourly window aggregations without waiting an hour, use the **"Mock Data Generator (2h Backfill)"** launch configuration instead. It generates 2 hours of historical events at max speed before switching to real-time, so Flink's hourly tumble windows emit results within seconds.

### Debugging `setup-dashboards.py` (Superset)

The `setup-dashboards.py` script runs inside the Superset container but can be debugged from VS Code using remote attach. The script, along with `superset_config.py` and `bootstrap.sh`, is bind-mounted into the container via `docker-compose.yml`, so local edits are reflected immediately without rebuilding.

1. Make sure the Superset container is running:

```bash
docker compose up superset -d
```

2. Use the **"Superset Setup Dashboards (Remote)"** launch configuration and press F5. This automatically:
   - Runs the script inside the container with `debugpy` enabled (via the `superset-debug-script` preLaunchTask)
   - Waits for the debug server to start listening on port 5678
   - Attaches the VS Code debugger with path mappings from `superset/` to `/app`

3. Set breakpoints in `superset/setup-dashboards.py` and step through the code as usual.

### 5. Verify the Flink pipeline

Check that the Flink job is running:

```bash
curl -s http://localhost:8081/jobs | python3 -m json.tool
```

Open the Flink Web UI at [http://localhost:8081](http://localhost:8081) to monitor records received/sent.

After ~60 seconds (first Flink checkpoint), verify Parquet files are landing in MinIO:

```bash
docker compose exec minio mc ls --recursive local/warehouse/db/bid_requests/
```

## Configuration

### Mock Data Generator

| Variable | Default | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` (Docker) / `localhost:29092` (local) | Kafka broker address |
| `EVENTS_PER_SECOND` | `10` | Target bid-request throughput |
| `TOPIC_BID_REQUESTS` | `bid-requests` | Kafka topic for bid requests |
| `TOPIC_BID_RESPONSES` | `bid-responses` | Kafka topic for bid responses |
| `TOPIC_IMPRESSIONS` | `impressions` | Kafka topic for impressions |
| `TOPIC_CLICKS` | `clicks` | Kafka topic for clicks |
| `BID_RESPONSE_RATE` | `0.60` | Probability a bid request gets a response (~60% fill rate) |
| `WIN_RATE` | `0.15` | Probability a bid response wins the auction (~15%) |
| `CLICK_RATE` | `0.05` | Probability an impression generates a click (~5% CTR) |
| `BACKFILL_HOURS` | `0` | Generate N hours of historical data at max speed before real-time mode (0 = disabled) |

### Backfill Mode

The Flink streaming aggregations (hourly impressions by geo, funnel metrics by publisher) use 1-hour window buckets. In real-time mode, you'd need to wait a full hour before any aggregated results appear in the Iceberg tables. Backfill mode solves this by generating historical events with timestamps spread across the past N hours, sent at max speed without rate limiting. This causes Flink's watermarks to advance through multiple window boundaries within seconds, producing aggregated results almost immediately.

**How it works:**

1. The generator calculates `EVENTS_PER_SECOND * 3600 * BACKFILL_HOURS` total events
2. Each event gets a timestamp evenly spaced across the backfill window (e.g., 2 hours ago to now)
3. All events are sent to Kafka at max speed (no sleep between events)
4. Flink consumes the historical data, the interval joins match correlated events, and the hourly aggregations produce results
5. After backfill completes, the generator switches to real-time mode at the configured `EVENTS_PER_SECOND` rate

**Docker:**

```bash
# 2-hour backfill, then real-time at 10 eps
BACKFILL_HOURS=2 docker compose --profile generator up --build -d

# Watch backfill progress
docker compose logs -f mock-data-gen
```

**VS Code:** Select the **"Mock Data Generator (1h Backfill)"** or **"Mock Data Generator (2h Backfill)"** launch configuration and press F5.

**Verify results after backfill** (wait ~60s for the first Flink checkpoint to commit):

```bash
# Should show actual country codes (USA, GBR, DEU, etc.)
docker exec trino trino --execute \
  "SELECT * FROM iceberg.db.hourly_impressions_by_geo ORDER BY impression_count DESC LIMIT 10"

# Should show funnel metrics per publisher
docker exec trino trino --execute \
  "SELECT * FROM iceberg.db.hourly_funnel_by_publisher ORDER BY bid_requests DESC LIMIT 10"
```

### Flink Web UI

Access the Flink dashboard at [http://localhost:8081](http://localhost:8081) to monitor running jobs, checkpoints, and task metrics.

### Trino

Access the Trino Web UI at [http://localhost:8080](http://localhost:8080) (no credentials required). Run queries via CLI:

```bash
docker exec trino trino --catalog iceberg --schema db
```

### MinIO CLI (`mc`)

Install the MinIO Client to browse buckets and Iceberg Parquet files from your host machine:

```bash
# Install (macOS)
brew install minio/stable/mc

# Set up an alias for the local MinIO instance
mc alias set local http://localhost:9000 admin password

# List buckets
mc ls local

# List the warehouse bucket (Iceberg data root)
mc ls local/warehouse/

# Recursively list all Parquet and metadata files
mc ls --recursive local/warehouse/

# List files for a specific table
mc ls --recursive local/warehouse/db/bid_requests/
```

### MinIO Console

Access the MinIO web console at [http://localhost:9001](http://localhost:9001) with credentials `admin` / `password`. From the console you can browse the `warehouse` bucket, navigate into `db/<table_name>/` directories, and inspect or download individual Parquet and Iceberg metadata files.

### Iceberg REST Catalog

The catalog API is available at [http://localhost:8181](http://localhost:8181). List tables:

```bash
curl -s http://localhost:8181/v1/namespaces/db/tables | python3 -m json.tool
```

### CloudBeaver

Access the CloudBeaver web SQL IDE at [http://localhost:8978](http://localhost:8978) with credentials `admin` / `password`. A Trino connection is pre-configured and available immediately.

### Superset

Access Superset at [http://localhost:8088](http://localhost:8088) with credentials `admin` / `password`. The Trino connection, dataset, and a sample chart are created automatically by the setup script. Superset uses a dedicated PostgreSQL instance (`superset-postgres`) for metadata and `SimpleCache` (in-memory) instead of Redis.

## Project Structure

```
streaming-data-lake/
  .design/
    adtech-streaming-platform.md   # Design document
  docker-compose.yml               # All local services
  mock-data-gen/
    pyproject.toml                 # Python dependencies
    Dockerfile                     # Container image
    src/
      config.py                    # Configuration from env vars
      schemas.py                   # OpenRTB 2.6 event generators (bid request, response, impression, click)
      generator.py                 # Kafka producer loop
  streaming/
    flink/
      Dockerfile                   # Custom Flink 1.20 image with Iceberg JARs
      submit-sql-job.sh            # Waits for deps, submits SQL job
      sql/
        create_tables.sql          # Flink SQL DDL (catalogs + source/sink tables)
        insert_jobs.sql            # Flink SQL DML (streaming inserts)
        aggregation_jobs.sql       # Windowed aggregations (hourly geo, rolling bidder metrics)
        funnel_jobs.sql            # 4-way interval join funnel metrics by publisher
  trino/
    catalog/
      iceberg.properties           # Iceberg connector config for Trino
  cloudbeaver/
    initial-data-sources.conf      # Pre-configured Trino JDBC connection
    initial-data.conf              # Admin credentials (skip setup wizard)
  superset/
    Dockerfile                     # Custom Superset image with Trino driver
    superset_config.py             # Superset configuration (SimpleCache, no Redis)
    bootstrap.sh                   # DB migration, admin creation, server start
    setup-dashboards.py            # REST API script to create dashboards
  scripts/
    setup.sh                       # Initialize 4 topics, bucket, 4 tables, Flink job, verify Trino, setup dashboards
    setup-generator-local-debug.sh # Set up local .venv for debugging the generator
    query-examples.sh              # Sample analytical queries via Trino
    maintenance.sh                 # Iceberg table maintenance for all 4 tables (compaction, expiry, cleanup)
```
