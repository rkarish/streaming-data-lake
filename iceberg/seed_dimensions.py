#!/usr/bin/env python3
"""Seed Iceberg dimension tables with static mock data via PyIceberg."""

import os
import sys
from datetime import datetime, timezone

import pyarrow as pa
from pyiceberg.catalog import load_catalog
from mock_data_gen.dimension_mapping import build_dsp_hierarchy, build_reference_dimensions

# Mapping from dimension key -> Iceberg table name
TABLE_MAP = {
    "agencies": "dim_agency",
    "advertisers": "dim_advertiser",
    "campaigns": "dim_campaign",
    "line_items": "dim_line_item",
    "strategies": "dim_strategy",
    "creatives": "dim_creative",
    "bidders": "dim_bidder",
    "publishers": "dim_publisher",
    "deals": "dim_deal",
    "geos": "dim_geo",
    "device_types": "dim_device_type",
    "device_os_list": "dim_device_os",
    "browsers": "dim_browser",
}


def rows_to_arrow(rows: list[dict], table) -> pa.Table:
    """Convert a list of dicts to a PyArrow table matching the Iceberg schema."""
    iceberg_schema = table.schema()
    arrow_schema = iceberg_schema.as_arrow()

    columns = {}
    for field in arrow_schema:
        values = []
        for row in rows:
            v = row.get(field.name)
            # Convert SCD timestamp strings to datetime objects
            if field.name in ("valid_from", "valid_to"):
                if v is not None:
                    v = datetime.fromisoformat(v).replace(tzinfo=timezone.utc)
            values.append(v)
        columns[field.name] = values

    return pa.table(columns, schema=arrow_schema)


def main():
    catalog_config = {
        "type": "rest",
        "uri": os.environ.get("ICEBERG_REST_URL", "http://localhost:8181"),
        "s3.endpoint": os.environ.get("S3_ENDPOINT", "http://localhost:9000"),
        "s3.access-key-id": os.environ.get("AWS_ACCESS_KEY_ID", "admin"),
        "s3.secret-access-key": os.environ.get("AWS_SECRET_ACCESS_KEY", "password"),
        "s3.path-style-access": "true",
        "warehouse": os.environ.get("WAREHOUSE", "s3://warehouse/"),
    }

    catalog = load_catalog("rest", **catalog_config)

    # Build all dimension data
    dsp = build_dsp_hierarchy()
    ref = build_reference_dimensions()
    all_data = {**dsp, **ref}

    errors = 0
    total = 0
    for key, table_name in TABLE_MAP.items():
        rows = all_data[key]
        total += 1
        try:
            table = catalog.load_table(("db", table_name))
            # Skip if table already has data (idempotent)
            scan = table.scan(limit=1)
            existing = scan.to_arrow()
            if len(existing) > 0:
                print(f"    SKIP  db.{table_name} (already seeded, {len(rows)} rows)")
                continue

            arrow_table = rows_to_arrow(rows, table)
            table.append(arrow_table)
            print(f"    OK    db.{table_name} ({len(rows)} rows seeded)")
        except Exception as e:
            print(f"    FAIL  db.{table_name}: {e}")
            errors += 1

    print(f"\n    {total - errors}/{total} dimension tables seeded.")
    sys.exit(1 if errors else 0)


if __name__ == "__main__":
    main()
