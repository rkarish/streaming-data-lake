# Project Instructions

## Design Documents

All design documents are located in the `.design/` directory. Review these before making architectural decisions or implementing new features.

- [AdTech Data Lake Streaming Platform](.design/adtech-data-lake-streaming-platform.md) - Core platform design covering the OpenRTB 2.6 data model, streaming engine options (Flink, Spark, Kafka Connect), Iceberg table schemas, Docker Compose architecture, and cloud expansion paths for AWS/GCP.

## Subagent Usage

Always use the `voltagent-data-ai:data-engineer` subagent for all implementation and engineering tasks in this project. This agent specializes in data pipelines, ETL/ELT processes, and data infrastructure, which aligns with the Kafka-to-Iceberg streaming platform being built here.

## Coding standards

1. Don't use blank spaces in JSON data inside of shell scripts, new lines and indentation is fine.
2. Don't use excessive whitespace in .sql files, new lines and indentation is fine.