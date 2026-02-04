#!/usr/bin/env python3
"""Set up Superset resources for the AdTech Data Lake Streaming Platform.

Creates a Trino database connection, bid_requests dataset, and a pie chart
showing bid requests by country.
All operations are idempotent -- existing resources are skipped.
"""

import sys
import json
import requests

SUPERSET_URL = "http://localhost:8088"


def login(session):
    """Log in and return an access token."""
    resp = session.post(
        f"{SUPERSET_URL}/api/v1/security/login",
        json={
            "username": "admin",
            "password": "password",
            "provider": "db",
            "refresh": True
        }
    )
    if resp.status_code != 200:
        print(f"ERROR: Login failed: {resp.status_code} {resp.text}")
        sys.exit(1)
    token = resp.json()["access_token"]
    session.headers.update({"Authorization": f"Bearer {token}"})
    print("Logged in as admin")
    return token


def get_csrf_token(session):
    """Fetch a CSRF token and set it on the session."""
    resp = session.get(f"{SUPERSET_URL}/api/v1/security/csrf_token/")
    if resp.status_code != 200:
        print(f"ERROR: Failed to get CSRF token: {resp.status_code} {resp.text}")
        sys.exit(1)
    csrf_token = resp.json()["result"]
    session.headers.update({"X-CSRFToken": csrf_token})
    print("Obtained CSRF token")
    return csrf_token


def find_database(session, name):
    """Return the database ID if a database with the given name exists."""
    resp = session.get(
        f"{SUPERSET_URL}/api/v1/database/",
        params={"q": json.dumps({"filters": [{"col": "database_name", "opr": "eq", "value": name}]})}
    )
    if resp.status_code != 200:
        print(f"ERROR: Failed to list databases: {resp.status_code} {resp.text}")
        sys.exit(1)
    results = resp.json().get("result", [])
    if results:
        return results[0]["id"]
    return None


def create_database(session):
    """Create the Trino database connection, returning its ID."""
    db_name = "Trino AdTech"
    existing_id = find_database(session, db_name)
    if existing_id:
        print(f"Database '{db_name}' already exists (id={existing_id}), skipping")
        return existing_id
    resp = session.post(
        f"{SUPERSET_URL}/api/v1/database/",
        json={
            "database_name": db_name,
            "engine": "trino",
            "sqlalchemy_uri": "trino://trino@trino:8080/iceberg/db",
            "expose_in_sqllab": True,
            "allow_ctas": False,
            "allow_cvas": False,
            "allow_dml": False,
            "allow_run_async": False
        }
    )
    if resp.status_code not in (200, 201):
        print(f"ERROR: Failed to create database: {resp.status_code} {resp.text}")
        sys.exit(1)
    db_id = resp.json()["id"]
    print(f"Created database '{db_name}' (id={db_id})")
    return db_id


def find_dataset(session, table_name, database_id):
    """Return the dataset ID if a dataset with the given table name exists."""
    resp = session.get(
        f"{SUPERSET_URL}/api/v1/dataset/",
        params={"q": json.dumps({"filters": [
            {"col": "table_name", "opr": "eq", "value": table_name},
            {"col": "database", "opr": "rel_o_m", "value": database_id}
        ]})}
    )
    if resp.status_code != 200:
        print(f"ERROR: Failed to list datasets: {resp.status_code} {resp.text}")
        sys.exit(1)
    results = resp.json().get("result", [])
    if results:
        return results[0]["id"]
    return None


def create_dataset(session, database_id, table_name):
    """Create a dataset for the given table, returning its ID."""
    existing_id = find_dataset(session, table_name, database_id)
    if existing_id:
        print(f"Dataset '{table_name}' already exists (id={existing_id}), skipping")
        return existing_id
    resp = session.post(
        f"{SUPERSET_URL}/api/v1/dataset/",
        json={
            "database": database_id,
            "table_name": table_name,
            "schema": "db"
        }
    )
    if resp.status_code not in (200, 201):
        print(f"ERROR: Failed to create dataset '{table_name}': {resp.status_code} {resp.text}")
        sys.exit(1)
    ds_id = resp.json()["id"]
    print(f"Created dataset '{table_name}' (id={ds_id})")
    return ds_id


def find_chart(session, name):
    """Return the chart ID if a chart with the given name exists."""
    resp = session.get(
        f"{SUPERSET_URL}/api/v1/chart/",
        params={"q": json.dumps({"filters": [{"col": "slice_name", "opr": "eq", "value": name}]})}
    )
    if resp.status_code != 200:
        print(f"ERROR: Failed to list charts: {resp.status_code} {resp.text}")
        sys.exit(1)
    results = resp.json().get("result", [])
    if results:
        return results[0]["id"]
    return None


def create_chart(session, chart_name, viz_type, dataset_id, params):
    """Create a chart with the given config, returning its ID."""
    existing_id = find_chart(session, chart_name)
    if existing_id:
        print(f"Chart '{chart_name}' already exists (id={existing_id}), skipping")
        return existing_id
    resp = session.post(
        f"{SUPERSET_URL}/api/v1/chart/",
        json={
            "slice_name": chart_name,
            "viz_type": viz_type,
            "datasource_id": dataset_id,
            "datasource_type": "table",
            "params": json.dumps(params)
        }
    )
    if resp.status_code not in (200, 201):
        print(f"ERROR: Failed to create chart '{chart_name}': {resp.status_code} {resp.text}")
        sys.exit(1)
    chart_id = resp.json()["id"]
    print(f"Created chart '{chart_name}' (id={chart_id})")
    return chart_id


def main():
    print("Setting up Superset for AdTech Data Lake...")
    session = requests.Session()
    login(session)
    get_csrf_token(session)
    database_id = create_database(session)
    ds_bid_requests = create_dataset(session, database_id, "bid_requests")
    ds_bid_responses = create_dataset(session, database_id, "bid_responses")
    ds_impressions = create_dataset(session, database_id, "impressions")
    ds_clicks = create_dataset(session, database_id, "clicks")

    count_metric = lambda col: {
        "expressionType": "SIMPLE",
        "column": {"column_name": col},
        "aggregate": "COUNT",
        "label": "COUNT(*)"
    }

    create_chart(session, "Bid Requests by Country", "pie", ds_bid_requests, {
        "viz_type": "pie",
        "groupby": ["device_geo_country"],
        "metric": count_metric("request_id"),
        "adhoc_filters": [],
        "row_limit": 100,
        "sort_by_metric": True,
        "color_scheme": "supersetColors",
        "show_labels": True,
        "show_legend": True
    })

    create_chart(session, "Bid Responses by Bidder Seat", "pie", ds_bid_responses, {
        "viz_type": "pie",
        "groupby": ["seat"],
        "metric": count_metric("response_id"),
        "adhoc_filters": [],
        "row_limit": 100,
        "sort_by_metric": True,
        "color_scheme": "supersetColors",
        "show_labels": True,
        "show_legend": True
    })

    create_chart(session, "Impressions by Bidder", "pie", ds_impressions, {
        "viz_type": "pie",
        "groupby": ["bidder_id"],
        "metric": count_metric("impression_id"),
        "adhoc_filters": [],
        "row_limit": 100,
        "sort_by_metric": True,
        "color_scheme": "supersetColors",
        "show_labels": True,
        "show_legend": True
    })

    create_chart(session, "Clicks by Creative", "pie", ds_clicks, {
        "viz_type": "pie",
        "groupby": ["creative_id"],
        "metric": count_metric("click_id"),
        "adhoc_filters": [],
        "row_limit": 50,
        "sort_by_metric": True,
        "color_scheme": "supersetColors",
        "show_labels": True,
        "show_legend": True
    })

    print("Setup complete")


if __name__ == "__main__":
    main()
