# Project Instructions

## Design Documents

All design documents are located in the `.design/` directory. Review these before making architectural decisions or implementing new features.

- [AdTech Data Lake Streaming Platform](.design/adtech-data-lake-streaming-platform.md)

## Subagent Usage

Always use the `voltagent-data-ai:data-engineer` subagent for all implementation and engineering tasks in this project. This agent specializes in data pipelines, ETL/ELT processes, and data infrastructure, which aligns with the Kafka-to-Iceberg streaming platform being built here.

## MCP Tools

Use the `mcp__ide__getDiagnostics` tool to check for Pylance/linting errors after editing Python files. Pass the file URI (e.g., `file:///path/to/file.py`) to get diagnostics for a specific file, or omit it to get diagnostics for all open files.

## Coding Standards

1. Do not use excessive whitespace in JSON data inside of shell scripts or in .sql files, new lines and indentation is fine.
2. Do not reference development phases (e.g. "Phase 5", "Phase 7") in code comments or documentation outside of the `.design/` directory. Phase labels are internal planning artifacts and should not leak into the codebase.