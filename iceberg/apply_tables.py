#!/usr/bin/env python3
"""Create Iceberg tables from YAML definitions using PyIceberg REST catalog."""

import os
import re
import sys
from pathlib import Path

import yaml
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import (
    DayTransform,
    HourTransform,
    IdentityTransform,
    MonthTransform,
    YearTransform,
)
from pyiceberg.types import (
    BooleanType,
    DoubleType,
    FloatType,
    IntegerType,
    ListType,
    LongType,
    NestedField,
    StringType,
    TimestampType,
    TimestamptzType,
)

SIMPLE_TYPES = {
    "string": StringType(),
    "int": IntegerType(),
    "long": LongType(),
    "float": FloatType(),
    "double": DoubleType(),
    "boolean": BooleanType(),
    "timestamp": TimestampType(),
    "timestamptz": TimestamptzType(),
}

TRANSFORMS = {
    "identity": IdentityTransform,
    "day": DayTransform,
    "hour": HourTransform,
    "month": MonthTransform,
    "year": YearTransform,
}

LIST_PATTERN = re.compile(r"^list<(\w+)>$")


def parse_type(type_str, element_id):
    """Parse a YAML type string into a PyIceberg type.

    Returns (iceberg_type, next_element_id). The element_id counter is only
    advanced for complex types that need internal IDs (e.g. ListType).
    """
    if type_str in SIMPLE_TYPES:
        return SIMPLE_TYPES[type_str], element_id

    m = LIST_PATTERN.match(type_str)
    if m:
        inner = SIMPLE_TYPES.get(m.group(1))
        if inner is None:
            raise ValueError(f"Unsupported list element type: {m.group(1)}")
        return ListType(element_id=element_id, element_type=inner, element_required=False), element_id + 1

    raise ValueError(f"Unsupported type: {type_str}")


def build_schema(definition):
    """Build a PyIceberg Schema from a YAML table definition."""
    fields = []
    field_id = 1
    id_field_names = set(definition.get("identifier_fields", []))
    # Reserve IDs for top-level fields first, then use higher IDs for nested types
    next_element_id = len(definition["schema"]) + 1

    for col in definition["schema"]:
        col_type, next_element_id = parse_type(col["type"], next_element_id)
        fields.append(NestedField(
            field_id=field_id,
            name=col["name"],
            field_type=col_type,
            required=col["name"] in id_field_names,
        ))
        field_id += 1

    # Resolve identifier field names to IDs
    id_field_ids = []
    if definition.get("identifier_fields"):
        name_to_id = {f.name: f.field_id for f in fields}
        for name in definition["identifier_fields"]:
            if name not in name_to_id:
                raise ValueError(f"Identifier field '{name}' not found in schema")
            id_field_ids.append(name_to_id[name])

    return Schema(*fields, identifier_field_ids=id_field_ids)


def build_partition_spec(schema, definition):
    """Build a PyIceberg PartitionSpec from a YAML table definition."""
    if not definition.get("partition_spec"):
        return PartitionSpec()

    name_to_id = {f.name: f.field_id for f in schema.fields}
    part_fields = []
    part_field_id = 1000

    for spec in definition["partition_spec"]:
        transform_name = spec["transform"]
        source_name = spec["source"]

        if transform_name not in TRANSFORMS:
            raise ValueError(f"Unsupported transform: {transform_name}")
        if source_name not in name_to_id:
            raise ValueError(f"Partition source '{source_name}' not found in schema")

        if transform_name == "identity":
            field_name = source_name
        else:
            field_name = f"{source_name}_{transform_name}"

        part_fields.append(PartitionField(
            source_id=name_to_id[source_name],
            field_id=part_field_id,
            transform=TRANSFORMS[transform_name](),
            name=field_name,
        ))
        part_field_id += 1

    return PartitionSpec(*part_fields)


def _field_type_name(field_type):
    """Return a comparable string for an Iceberg field type."""
    if isinstance(field_type, ListType):
        return f"list<{_field_type_name(field_type.element_type)}>"
    return type(field_type).__name__


def check_drift(existing_table, defn):
    """Compare an existing Iceberg table against a YAML definition.

    Returns a list of drift warning strings, or an empty list if no drift.
    """
    warnings = []

    # -- Column drift (added/removed, type changes, requiredness changes) --
    existing_fields = {f.name: f for f in existing_table.schema().fields}
    yaml_cols = {c["name"] for c in defn["schema"]}
    existing_cols = set(existing_fields.keys())

    added = yaml_cols - existing_cols
    removed = existing_cols - yaml_cols
    if added:
        warnings.append(f"columns added in YAML: {sorted(added)}")
    if removed:
        warnings.append(f"columns removed from YAML: {sorted(removed)}")

    id_field_names = set(defn.get("identifier_fields", []))
    # Reserve element IDs consistent with build_schema
    next_element_id = len(defn["schema"]) + 1
    for col in defn["schema"]:
        if col["name"] not in existing_fields:
            continue
        ef = existing_fields[col["name"]]
        yaml_type, next_element_id = parse_type(col["type"], next_element_id)
        if _field_type_name(ef.field_type) != _field_type_name(yaml_type):
            warnings.append(
                f"column '{col['name']}' type: "
                f"catalog={_field_type_name(ef.field_type)}, "
                f"yaml={_field_type_name(yaml_type)}"
            )
        yaml_required = col["name"] in id_field_names
        if ef.required != yaml_required:
            warnings.append(
                f"column '{col['name']}' required: "
                f"catalog={ef.required}, yaml={yaml_required}"
            )

    # -- Identifier field drift --
    existing_id_ids = set(existing_table.schema().identifier_field_ids)
    existing_id_names = {
        f.name for f in existing_table.schema().fields
        if f.field_id in existing_id_ids
    }
    yaml_id_names = set(defn.get("identifier_fields", []))
    if existing_id_names != yaml_id_names:
        warnings.append(
            f"identifier_fields: catalog={sorted(existing_id_names)}, "
            f"yaml={sorted(yaml_id_names)}"
        )

    # -- Partition spec drift --
    existing_parts = [
        (pf.name, type(pf.transform).__name__)
        for pf in existing_table.spec().fields
    ]
    yaml_parts = []
    if defn.get("partition_spec"):
        for spec in defn["partition_spec"]:
            t = spec["transform"]
            s = spec["source"]
            name = s if t == "identity" else f"{s}_{t}"
            yaml_parts.append((name, TRANSFORMS[t].__name__))
    if existing_parts != yaml_parts:
        fmt = lambda ps: [(n, t) for n, t in ps]
        warnings.append(
            f"partition_spec: catalog={fmt(existing_parts)}, "
            f"yaml={fmt(yaml_parts)}"
        )

    return warnings


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

    tables_dir = Path(__file__).parent / "tables"
    yml_files = sorted(tables_dir.glob("*.yml"))
    if not yml_files:
        print("No YAML table definitions found in", tables_dir)
        sys.exit(1)

    definitions = []
    for f in yml_files:
        with open(f) as fh:
            definitions.append(yaml.safe_load(fh))

    # Collect unique namespaces and ensure they exist
    namespaces = {d["namespace"] for d in definitions}
    for ns in sorted(namespaces):
        try:
            catalog.create_namespace(ns)
            print(f"    Created namespace '{ns}'")
        except NamespaceAlreadyExistsError:
            pass

    errors = 0
    for defn in definitions:
        ns = defn["namespace"]
        table_name = defn["table"]
        identifier = (ns, table_name)

        try:
            existing = catalog.load_table(identifier)
            drift = check_drift(existing, defn)
            if drift:
                for msg in drift:
                    print(f"    WARN  {ns}.{table_name}: {msg}")
            else:
                col_count = len(existing.schema().fields)
                print(f"    OK    {ns}.{table_name} (exists, {col_count} columns)")
            continue
        except NoSuchTableError:
            pass

        try:
            schema = build_schema(defn)
            partition_spec = build_partition_spec(schema, defn)
            properties = dict(defn.get("properties", {}))
            properties["format-version"] = str(defn.get("format_version", 2))

            catalog.create_table(
                identifier=identifier,
                schema=schema,
                partition_spec=partition_spec,
                properties=properties,
            )

            id_fields = defn.get("identifier_fields", [])
            label = "upsert" if id_fields else "append"
            print(f"    OK    {ns}.{table_name} (created, {label}, {len(defn['schema'])} columns)")
        except Exception as e:
            print(f"    FAIL  {ns}.{table_name}: {e}")
            errors += 1

    total = len(definitions)
    print(f"\n    {total - errors}/{total} tables ready.")
    sys.exit(1 if errors else 0)


if __name__ == "__main__":
    main()
