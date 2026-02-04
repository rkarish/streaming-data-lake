# Project Instructions

## Design Documents

All design documents are located in the `.design/` directory. Review these before making architectural decisions or implementing new features.

- [AdTech Data Lake Streaming Platform](.design/adtech-data-lake-streaming-platform.md)

## Subagent Usage

Always use the `voltagent-data-ai:data-engineer` subagent for all implementation and engineering tasks in this project. This agent specializes in data pipelines, ETL/ELT processes, and data infrastructure, which aligns with the Kafka-to-Iceberg streaming platform being built here.

## Coding standards

1. Do not use excessive whitespace in JSON data inside of shell scripts or in .sql files, new lines and indentation is fine.