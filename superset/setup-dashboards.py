#!/usr/bin/env python3
"""Set up Superset resources for the AdTech Data Lake Streaming Platform.

Creates:
- Trino database connection
- Datasets for all Iceberg tables (core funnel + enriched/aggregation + funnel metrics)
- Charts for visualizing the data:
  - Core: Bid requests by country, responses by bidder seat, impressions by bidder, clicks by creative
  - Requests by device category, test vs production traffic, hourly revenue by bidder

All operations are idempotent -- existing resources are skipped.
"""

import os
import sys
import json
import requests

if os.environ.get("ENABLE_DEBUGGER") == "true":
    import debugpy
    debugpy.listen(("0.0.0.0", 5678))
    print("Waiting for debugger to attach on port 5678...")
    debugpy.wait_for_client()
    print("Debugger attached!")

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


def create_dashboard_orm(title, slug, chart_id, refresh_frequency=15):
    """Create a single-chart dashboard with auto-refresh via Superset ORM.

    The REST API does not establish the dashboard-slices relationship needed
    for charts to render, so we use the ORM directly.
    """
    from superset.app import create_app
    app = create_app()
    with app.app_context():
        from superset import db
        from superset.models.dashboard import Dashboard
        from superset.models.slice import Slice
        existing = db.session.query(Dashboard).filter_by(slug=slug).first()  # type: ignore[attr-defined]
        if existing:
            print(f"Dashboard '{title}' already exists (id={existing.id}), skipping")
            return existing.id
        chart = db.session.query(Slice).filter_by(id=chart_id).first()  # type: ignore[attr-defined]
        if not chart:
            print(f"ERROR: Chart id={chart_id} not found")
            sys.exit(1)
        chart_key = f"CHART-{chart_id}"
        position = {
            "DASHBOARD_VERSION_KEY": "v2",
            "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
            "GRID_ID": {"type": "GRID", "id": "GRID_ID", "children": ["ROW-0"], "parents": ["ROOT_ID"]},
            "HEADER_ID": {"type": "HEADER", "id": "HEADER_ID", "meta": {"text": title}},
            "ROW-0": {
                "type": "ROW",
                "id": "ROW-0",
                "children": [chart_key],
                "parents": ["ROOT_ID", "GRID_ID"],
                "meta": {"background": "BACKGROUND_TRANSPARENT"}
            },
            chart_key: {
                "type": "CHART",
                "id": chart_key,
                "children": [],
                "parents": ["ROOT_ID", "GRID_ID", "ROW-0"],
                "meta": {"width": 12, "height": 100, "chartId": chart_id, "sliceName": chart.slice_name}
            }
        }
        dash = Dashboard(**{  # type: ignore[call-arg]
            "dashboard_title": title,
            "slug": slug,
            "published": True,
            "position_json": json.dumps(position),
            "json_metadata": json.dumps({
                "refresh_frequency": refresh_frequency,
                "timed_refresh_immune_slices": [],
                "color_scheme": "supersetColors"
            })
        })
        dash.slices = [chart]
        db.session.add(dash)  # type: ignore[attr-defined]
        db.session.commit()  # type: ignore[attr-defined]
        print(f"Created dashboard '{title}' (id={dash.id}, auto-refresh={refresh_frequency}s)")
        return dash.id


def main():
    print("Setting up Superset for AdTech Data Lake...")
    session = requests.Session()
    login(session)
    get_csrf_token(session)
    database_id = create_database(session)

    # Core funnel tables
    ds_bid_requests = create_dataset(session, database_id, "bid_requests")
    ds_bid_responses = create_dataset(session, database_id, "bid_responses")
    ds_impressions = create_dataset(session, database_id, "impressions")
    ds_clicks = create_dataset(session, database_id, "clicks")

    # Enriched and aggregation tables
    ds_enriched = create_dataset(session, database_id, "bid_requests_enriched")
    ds_hourly_geo = create_dataset(session, database_id, "hourly_impressions_by_geo")
    ds_rolling_bidder = create_dataset(session, database_id, "rolling_metrics_by_bidder")

    # Funnel metrics table
    ds_funnel = create_dataset(session, database_id, "hourly_funnel_by_publisher")

    count_metric = lambda col: {
        "expressionType": "SIMPLE",
        "column": {"column_name": col},
        "aggregate": "COUNT",
        "label": "COUNT(*)"
    }

    sum_metric = lambda col, label: {
        "expressionType": "SIMPLE",
        "column": {"column_name": col},
        "aggregate": "SUM",
        "label": label
    }

    # Core funnel charts
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

    # Device classification chart
    create_chart(session, "Requests by Device Category", "pie", ds_enriched, {
        "viz_type": "pie",
        "groupby": ["device_category"],
        "metric": count_metric("request_id"),
        "adhoc_filters": [],
        "row_limit": 10,
        "sort_by_metric": True,
        "color_scheme": "supersetColors",
        "show_labels": True,
        "show_legend": True
    })

    # Test vs production traffic chart
    create_chart(session, "Test vs Production Traffic", "pie", ds_enriched, {
        "viz_type": "pie",
        "groupby": ["is_test_traffic"],
        "metric": count_metric("request_id"),
        "adhoc_filters": [],
        "row_limit": 10,
        "sort_by_metric": True,
        "color_scheme": "supersetColors",
        "show_labels": True,
        "show_legend": True
    })

    # Hourly aggregation chart (uses actual country data from impressions-bid_requests interval join)
    create_chart(session, "Hourly Revenue by Country", "pie", ds_hourly_geo, {
        "viz_type": "pie",
        "groupby": ["device_geo_country"],
        "metric": sum_metric("total_revenue", "Total Revenue"),
        "adhoc_filters": [],
        "row_limit": 20,
        "sort_by_metric": True,
        "color_scheme": "supersetColors",
        "show_labels": True,
        "show_legend": True
    })

    # Rolling metrics chart
    create_chart(session, "Rolling Win Count by Bidder", "echarts_timeseries_bar", ds_rolling_bidder, {
        "viz_type": "echarts_timeseries_bar",
        "x_axis": "bidder_id",
        "metrics": [sum_metric("win_count", "Win Count"), sum_metric("revenue", "Revenue")],
        "groupby": [],
        "adhoc_filters": [],
        "row_limit": 10,
        "order_desc": True,
        "x_axis_sort": "Win Count",
        "x_axis_sort_asc": False,
        "color_scheme": "supersetColors",
        "show_legend": True
    })

    # Funnel conversion chart
    funnel_chart_id = create_chart(session, "Funnel Conversion by Publisher", "echarts_timeseries_bar", ds_funnel, {
        "viz_type": "echarts_timeseries_bar",
        "x_axis": "publisher_id",
        "metrics": [
            sum_metric("clicks", "Clicks"),
            sum_metric("impressions", "Impressions"),
            sum_metric("bid_responses", "Bid Responses"),
            sum_metric("bid_requests", "Bid Requests")
        ],
        "groupby": [],
        "adhoc_filters": [],
        "row_limit": 20,
        "order_desc": True,
        "x_axis_sort": "Bid Responses",
        "x_axis_sort_asc": False,
        "color_scheme": "supersetColors",
        "show_legend": True
    })

    print("Charts setup complete")

    # Dashboard with auto-refresh (15 seconds) -- uses ORM because the REST
    # API does not establish the dashboard-slices relationship.
    create_dashboard_orm("AdTech Data Lake", "adtech-data-lake",
                         funnel_chart_id, refresh_frequency=15)

    print("Setup complete")


if __name__ == "__main__":
    main()
