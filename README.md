# AdTech Data Lake Streaming Platform

A data lake streaming platform for adtech that produces a full OpenRTB 2.6 event funnel (bid requests, bid responses, impressions, clicks) as Avro-serialized messages to Apache Kafka (with Confluent Schema Registry for schema governance and evolution), streams them through Apache Flink, and stores them in Apache Iceberg tables backed by MinIO (S3-compatible) object storage. Includes Trino as the SQL query engine, CloudBeaver as a web-based SQL IDE, and Apache Superset for dashboards and visualization.

See [`.design/adtech-data-lake-streaming-platform.md`](.design/adtech-data-lake-streaming-platform.md) for the full design document.

## Architecture

```
                    Schema Registry
                     (Avro schemas)
                         ^
                         |
Mock Data Gen  --->  Kafka (KRaft)         ┌─ Docker Compose ───┐
   (Avro)                |                 │  (infrastructure)  │
                         |                 └────────┬───────────┘
                         v                          |
    ┌─ Kubernetes (Docker Desktop) ──────┐          |
    |                                    | host.docker.internal
    |  ┌────────────┐  ┌─────────────┐   |          |
    |  | Ingestion  |  | Aggregation |   |<---------+
    |  | Cluster    |  | Cluster     |   |
    |  └────────────┘  └─────────────┘   |
    |  ┌────────────┐  ┌─────────────┐   |
    |  | Funnel     |  | Flink       |   |
    |  | Cluster    |  | Operator    |   |
    |  └────────────┘  └─────────────┘   |
    |  Argo CD (GitOps)                  |
    └────────────────────────────────────┘
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
| `kafka` | `apache/kafka:3.8.1` (KRaft) | 29092 (host), 39092 (K8s), 9092 (internal) |
| `schema-registry` | `confluentinc/cp-schema-registry:7.8.0` | 8085 |
| `minio` | `minio/minio:latest` | 9000 (S3), 9001 (console) |
| `iceberg-rest` | `tabulario/iceberg-rest:1.6.0` | 8181 |
| `mock-data-gen` | Custom (Python 3.12, Avro) | -- |
| `gitea` | `gitea/gitea:1.23` (k8s profile) | 3000 |
| `trino` | `trinodb/trino:467` | 8080 (Web UI) |
| `cloudbeaver` | `dbeaver/cloudbeaver:latest` | 8978 (Web UI) |
| `superset` | Custom (apache/superset + trino driver) | 8088 (Web UI) |
| `superset-postgres` | `postgres:16-alpine` | internal only |

**Kubernetes Services (Docker Desktop):**

| Service | Mode | Description |
|---|---|---|
| Flink Operator | Both | Manages FlinkDeployment CRDs |
| `flink-ingestion` | Application | Flink cluster for Kafka-to-Iceberg ingestion |
| `flink-aggregation` | Application | Flink cluster for windowed aggregations |
| `flink-funnel` | Application | Flink cluster for funnel metrics |
| `flink-session` | Session | Single Flink cluster for all jobs |
| `ingestion-job` / `aggregation-job` / `funnel-job` | Session | K8s Jobs that submit SQL via `sql-client.sh` |
| `kafka`, `schema-registry`, `iceberg-rest`, `minio` | Session | K8s Services routing to Docker Compose on host |
| Argo CD | Both | GitOps controller syncing from Gitea |
| cert-manager | Both | TLS certificate management for operator |

## Prerequisites

- Docker and Docker Compose
- Docker Desktop with Kubernetes enabled (for Flink deployment)
- `kubectl` and `helm` CLI tools
- Python 3.12+ (for local development only)
  - On Ubuntu/Debian, also install `python3-venv`: `sudo apt install python3-venv`
- `curl` (for setup script)
- [Claude Code](https://claude.com/claude-code) with the `voltagent-data-ai` subagent (for AI-assisted development)

## Quick Start (Docker)

### 1. Build and start all services

```bash
docker compose up --build -d
```

This starts the infrastructure services (Kafka, Schema Registry, MinIO, Iceberg, Trino, CloudBeaver, Superset) but **not** the mock data generator or Flink (which runs on Kubernetes). To also start the generator (continuous event stream):

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

Waits for Schema Registry, configures BACKWARD compatibility, creates Kafka topics, MinIO bucket, Iceberg namespace + tables, and verifies Trino connectivity:

```bash
bash scripts/setup.sh
```

### 3. Deploy Flink on Kubernetes

Flink streaming jobs run on Docker Desktop's Kubernetes cluster via the Flink Kubernetes Operator. Two deployment modes are available:

**Application Mode** (default) -- 3 independent Flink clusters with full job isolation:

```bash
bash k8s/scripts/setup-k8s.sh
```

**Session Mode** -- single Flink cluster, lower resource usage:

```bash
bash k8s/scripts/setup-k8s.sh --mode session
```

Session mode deploys K8s Services (`kafka`, `schema-registry`, `iceberg-rest`, `minio`) that route to Docker Compose services, and submits SQL via Flink's built-in `sql-client.sh` through K8s Jobs.

Verify Flink is running:

```bash
kubectl get flinkdeployments -n flink
kubectl get pods -n flink
```

Access the Flink Web UI via port-forward (started automatically by the setup script):

```bash
# Application mode
kubectl port-forward svc/flink-ingestion-rest -n flink 8081:8081
# Session mode
kubectl port-forward svc/flink-session-rest -n flink 8081:8081
# Open http://localhost:8081
```

### 4. Verify

Check that events are flowing through Kafka. Messages are Avro-encoded (binary), so `kafka-console-consumer` will show raw bytes. To verify message count, check topic offsets instead:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server localhost:9092 \
  --topic bid-requests
```

Check that Parquet files appear in MinIO (after the first checkpoint, ~60s):

```bash
docker compose exec minio mc ls --recursive local/warehouse/db/bid_requests/
```

Verify the Iceberg tables exist:

```bash
curl -s http://localhost:8181/v1/namespaces/db/tables | python3 -m json.tool
```

Query core and quality/metrics tables via Trino:

```bash
docker exec trino trino --catalog iceberg --schema db \
  --execute "SELECT table_name
             FROM information_schema.tables
             WHERE table_schema='db'
             ORDER BY table_name"
```

View generator logs:

```bash
docker compose logs mock-data-gen --tail 20
```

### 4. Read data from Kafka

Messages are Avro-encoded with Confluent Schema Registry wire format (magic byte + 4-byte schema ID + Avro binary). The standard `kafka-console-consumer` will show raw binary. To inspect messages, use topic offsets or query the data via Trino after it lands in Iceberg.

Check topic offsets (total message count per partition):

```bash
docker compose exec kafka /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server localhost:9092 \
  --topic bid-requests
```

Verify schemas are registered in Schema Registry:

```bash
# List all registered subjects
curl -s http://localhost:8085/subjects | python3 -m json.tool

# View the latest schema for a topic
curl -s http://localhost:8085/subjects/bid-requests-value/versions/latest | python3 -m json.tool
```

### 5. Query with Trino

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

### 6. Table Maintenance

Run Iceberg table maintenance (compaction, snapshot expiry, orphan cleanup) via Trino:

```bash
bash scripts/maintenance.sh
```

### Quality and Realtime Metrics Tables

The pipeline now materializes additional operational tables in `iceberg.db`:

- `dq_rejected_events` -- rejected request/impression rows with reason codes (`TEST_PUBLISHER`, `PRIVATE_IP`, `NON_POSITIVE_BIDFLOOR`)
- `dq_event_quality_hourly` -- hourly quality KPIs with per-event duplicate breakdown (requests, responses, wins, clicks) and overall duplicate rate
- `bid_landscape_hourly` -- hourly bid density and bid price metrics per publisher
- `realtime_serving_metrics_1m` -- one-minute impressions/clicks/revenue/CTR by bidder
- `funnel_leakage_hourly` -- stage leakage rates across request/response/impression/click

Quick validation query:

```bash
docker exec trino trino --catalog iceberg --schema db \
  --execute "SELECT window_start, duplicate_bid_request_rate, duplicate_bid_response_rate,
                    duplicate_win_rate, duplicate_click_rate, duplicate_rate_all
             FROM dq_event_quality_hourly
             ORDER BY window_start DESC
             LIMIT 10"
```

### 7. CloudBeaver (Web SQL IDE)

Open [http://localhost:8978](http://localhost:8978). On the first launch you will need to complete the setup wizard:

1. Set an admin password (must meet complexity requirements, e.g. `Password123!`)
2. Finish the wizard

Once configured, click the CloudBeaver logo in the top-left corner to reach the main database UI. The pre-configured "Trino Iceberg" connection appears in the sidebar. Expand it to browse the `iceberg > db` schema (core, enriched, quality, and aggregate tables) and run SQL queries.

The workspace is persisted in a Docker volume (`cloudbeaver-workspace`), so subsequent restarts will skip the wizard. After a restart, you will need to click the settings icon in the top-right corner, click "Log In", and sign in with the admin credentials you set during the wizard (e.g. `cbadmin` / `Password123!`).

### 8. Superset (Charts & SQL Lab)

Open [http://localhost:8088](http://localhost:8088) and log in with `admin` / `password`. The setup script creates:

- A Trino database connection
- Datasets for core funnel and aggregate tables
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

### 9. Stop

If the mock data generator is running, you must include the `generator` profile when stopping -- otherwise the generator container keeps running and holds the Docker network open:

```bash
docker compose --profile generator down
```

If you haven't started the generator, a plain `docker compose down` is sufficient.

To tear down the Kubernetes resources (Flink clusters, operator, cert-manager, Argo CD):

```bash
bash k8s/scripts/teardown-k8s.sh
```

## Local Debug Environment

You can debug the mock data generator locally in VS Code while keeping the infrastructure services in Docker.

### 1. Build and start infrastructure services

```bash
docker compose up kafka schema-registry minio iceberg-rest trino -d
```

Wait for all services to become healthy:

```bash
docker compose ps
```

### 2. Run the setup script

Creates Kafka topics, MinIO bucket, Iceberg namespace + tables, and verifies Trino connectivity:

```bash
bash scripts/setup.sh
```

Then deploy Flink on Kubernetes:

```bash
bash k8s/scripts/setup-k8s.sh
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

Check that the Flink jobs are running on Kubernetes:

```bash
kubectl get flinkdeployments -n flink
kubectl get pods -n flink
```

Access the Flink Web UI via port-forward:

```bash
kubectl port-forward svc/flink-ingestion-rest -n flink 8081:8081
# Open http://localhost:8081
```

After ~60 seconds (first Flink checkpoint), verify Parquet files are landing in MinIO:

```bash
docker compose exec minio mc ls --recursive local/warehouse/db/bid_requests/
```

## VS Code Tasks

Run any task via **Terminal > Run Task...** (or `Cmd+Shift+P` > "Tasks: Run Task"). All tasks are defined in `.vscode/tasks.json`.

### Lifecycle

| Task | Description |
|---|---|
| `reload-system (application-mode)` | Stop all services, restart, and run full setup with Flink Application Mode (3 clusters) |
| `reload-system (session-mode)` | Stop all services, restart, and run full setup with Flink Session Mode (1 cluster) |
| `start-services` | Start all infrastructure services |
| `stop-services` | Stop all services |
| `run-setup` | Run the setup script (topics, bucket, tables, Flink jobs, Trino/Superset verification) |

### Data Generator

| Task | Description |
|---|---|
| `start-data-generator` | Start the mock data generator container |
| `stop-data-generator` | Stop the mock data generator container |
| `backfill-data-1h` | Start the generator with 1 hour of historical backfill |

### Flink & Maintenance

| Task | Description |
|---|---|
| `deploy-flink-k8s (application-mode)` | Deploy Flink as 3 independent clusters on Kubernetes |
| `deploy-flink-k8s (session-mode)` | Deploy Flink as 1 session cluster with 3 jobs on Kubernetes |
| `teardown-flink-k8s` | Tear down Flink Kubernetes resources |
| `run-table-maintenance` | Run Iceberg table maintenance (compaction, snapshot expiry, orphan cleanup) |
| `run-query-examples` | Run sample analytical queries via Trino |

### Logs

| Task | Description |
|---|---|
| `view-flink-logs` | Tail Flink pod logs (kubectl logs) |
| `view-kafka-logs` | Tail Kafka broker logs |
| `view-generator-logs` | Tail mock data generator logs |

### Kafka Topic Consumers

| Task | Description |
|---|---|
| `tail-topic-bid-requests` | Tail messages from the `bid-requests` topic |
| `tail-topic-bid-responses` | Tail messages from the `bid-responses` topic |
| `tail-topic-impressions` | Tail messages from the `impressions` topic |
| `tail-topic-clicks` | Tail messages from the `clicks` topic |

### Diagnostics

| Task | Description |
|---|---|
| `list-kafka-topics` | Describe all Kafka topics (partitions, replicas, offsets) |
| `check-service-health` | Show container status and port mappings |

## Configuration

### Mock Data Generator

| Variable | Default | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` (Docker) / `localhost:29092` (local) | Kafka broker address |
| `SCHEMA_REGISTRY_URL` | `http://schema-registry:8081` (Docker) / `http://localhost:8085` (local) | Confluent Schema Registry URL for Avro serialization |
| `EVENTS_PER_SECOND` | `10` | Target bid-request throughput |
| `TOPIC_BID_REQUESTS` | `bid-requests` | Kafka topic for bid requests |
| `TOPIC_BID_RESPONSES` | `bid-responses` | Kafka topic for bid responses |
| `TOPIC_IMPRESSIONS` | `impressions` | Kafka topic for impressions |
| `TOPIC_CLICKS` | `clicks` | Kafka topic for clicks |
| `BID_RESPONSE_RATE` | `0.60` | Probability a bid request gets a response (~60% fill rate) |
| `WIN_RATE` | `0.15` | Probability a bid response wins the auction (~15%) |
| `CLICK_RATE` | `0.05` | Probability an impression generates a click (~5% CTR) |
| `DUPLICATE_BID_REQUEST_RATE` | `0.00` | Probability of emitting one duplicate copy of each bid request (same key/payload) |
| `DUPLICATE_BID_RESPONSE_RATE` | `0.00` | Probability of emitting one duplicate copy of each bid response (same key/payload) |
| `DUPLICATE_IMPRESSION_RATE` | `0.00` | Probability of emitting one duplicate copy of each impression (same key/payload) |
| `DUPLICATE_CLICK_RATE` | `0.00` | Probability of emitting one duplicate copy of each click (same key/payload) |
| `BACKFILL_HOURS` | `0` | Generate N hours of historical data at max speed before real-time mode (0 = disabled) |

### Duplicate Injection Mode

Use duplicate rates to simulate producer retries/replays and validate deduplication logic:

```bash
# Emit duplicates for requests/responses while keeping impression/click duplicates off
DUPLICATE_BID_REQUEST_RATE=0.10 \
DUPLICATE_BID_RESPONSE_RATE=0.10 \
DUPLICATE_IMPRESSION_RATE=0.00 \
DUPLICATE_CLICK_RATE=0.00 \
docker compose --profile generator up --build -d
```

Generator logs include duplicate counters so you can confirm injected duplicate volume.

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

Flink runs on Kubernetes. Access the dashboard via port-forward:

```bash
kubectl port-forward svc/flink-ingestion-rest -n flink 8081:8081
```

Then open [http://localhost:8081](http://localhost:8081) to monitor running jobs, checkpoints, and task metrics. Replace `flink-ingestion` with `flink-aggregation` or `flink-funnel` to view other clusters.

Note: The setup script starts port-forwards automatically in the background for both Flink (`:8081`) and Argo CD (`:8443`). If they get killed, re-run manually with the commands above.

### Argo CD

Access the Argo CD UI at [https://localhost:8443](https://localhost:8443). Credentials:
- **Username**: `admin`
- **Password**: printed at the end of `setup-k8s.sh` output (auto-generated by Argo CD on install)

### Gitea

Local Git server for GitOps. Starts with all other services:

```bash
docker compose up gitea -d
```

Access at [http://localhost:3000](http://localhost:3000).

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

### Schema Registry

The Confluent Schema Registry provides Avro schema governance with BACKWARD compatibility enforcement. The API is available at [http://localhost:8085](http://localhost:8085).

```bash
# List all registered subjects
curl -s http://localhost:8085/subjects | python3 -m json.tool

# View the latest schema for a topic
curl -s http://localhost:8085/subjects/bid-requests-value/versions/latest | python3 -m json.tool

# Check global compatibility level
curl -s http://localhost:8085/config | python3 -m json.tool
```

Avro schema files (`.avsc`) are in `schemas/avro/` and are bind-mounted into the mock data generator container.

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
  docker-compose.yml               # Infrastructure services (Kafka, MinIO, Trino, etc.)
  schemas/
    avro/
      bid_request.avsc             # Avro schema for bid requests
      bid_response.avsc            # Avro schema for bid responses
      impression.avsc              # Avro schema for impressions
      click.avsc                   # Avro schema for clicks
  mock-data-gen/
    pyproject.toml                 # Python dependencies
    Dockerfile                     # Container image
    src/
      config.py                    # Configuration from env vars
      schemas.py                   # OpenRTB 2.6 event generators (bid request, response, impression, click)
      generator.py                 # Kafka producer loop
  streaming/
    flink/
      Dockerfile                   # Custom Flink 1.20 image with Iceberg JARs + SQL Runner
      k8s-entrypoint.sh            # Endpoint parameterization for Application Mode
      sql-runner/                  # Java app for Flink Application Mode (session mode uses sql-client.sh)
        pom.xml
        src/main/java/.../SqlRunner.java
      sql/
        create_tables.sql          # Flink SQL DDL (catalogs + source/sink tables)
        insert_jobs.sql            # Flink SQL DML (streaming inserts)
        aggregation_jobs.sql       # Windowed aggregations (hourly geo, rolling bidder metrics)
        funnel_jobs.sql            # 4-way interval join funnel metrics by publisher
  k8s/
    flink/
      base/                        # Kustomize base (namespace, RBAC, external K8s Services + Endpoints)
      overlays/
        application-mode/          # 3 independent FlinkDeployment clusters
        session-mode/              # Single session cluster + 3 K8s Jobs (sql-client.sh)
      operator/
        values.yaml                # Flink Operator Helm values
    argocd/
      application.yaml             # Argo CD Application CRD
    scripts/
      setup-k8s.sh                 # Full K8s setup (cert-manager, operator, Flink, Argo CD)
      teardown-k8s.sh              # Clean teardown of all K8s resources
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
    setup.sh                       # Schema Registry, topics, bucket, tables, verify Trino, dashboards
    setup-generator-local-debug.sh # Set up local .venv for debugging the generator
    query-examples.sh              # Sample analytical queries via Trino
    maintenance.sh                 # Iceberg table maintenance for all core/enriched/quality/aggregate tables
```
