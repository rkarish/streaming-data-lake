# Project Instructions

## Project Context

Although named "playground", this project is a **reference architecture** for an adtech data lake. It should be built to production standards: proper data modeling, integer surrogate keys, SCD Type 2 dimensions, clean Python packaging, idempotent scripts, and best practices throughout. Treat every decision as if this were a production system.

## Design Documents

All design documents are located in the `.design/` directory. Review these before making architectural decisions or implementing new features.

- [AdTech Data Playground](.design/adtech-data-playground.md)

## Coding Standards

1. Do not use excessive whitespace in JSON data inside of shell scripts or in .sql files, new lines and indentation is fine.
2. Do not reference development phases (e.g. "Phase 5", "Phase 7") in code comments or documentation outside of the `.design/` directory. Phase labels are internal planning artifacts and should not leak into the codebase.
