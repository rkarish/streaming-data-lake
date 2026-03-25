import logging

from airflow.providers.trino.hooks.trino import TrinoHook

from materialization_config import MaterializationEntry

logger = logging.getLogger(__name__)

TRINO_CONN_ID = "trino_default"


def _get_hook() -> TrinoHook:
    return TrinoHook(trino_conn_id=TRINO_CONN_ID)


def _fetch_scalar(hook: TrinoHook, sql: str):
    row = hook.get_first(sql)
    return row[0] if row else None


def table_exists(hook: TrinoHook, table_name: str) -> bool:
    count = _fetch_scalar(
        hook,
        f"SELECT count(*) FROM information_schema.tables "
        f"WHERE table_schema = 'db' AND table_name = '{table_name}'",
    )
    return int(count or 0) > 0


def get_watermark(hook: TrinoHook, mat_table: str) -> str | None:
    result = _fetch_scalar(
        hook,
        f"SELECT CAST(last_materialized_at AS VARCHAR) "
        f"FROM iceberg.db.materialization_watermarks "
        f"WHERE table_name = '{mat_table}'",
    )
    return str(result) if result else None


def set_watermark(hook: TrinoHook, entry: MaterializationEntry) -> None:
    hook.run(
        f"DELETE FROM iceberg.db.materialization_watermarks "
        f"WHERE table_name = '{entry.mat_table}'"
    )
    hook.run(
        f"INSERT INTO iceberg.db.materialization_watermarks "
        f"SELECT '{entry.mat_table}', MAX({entry.fact_ts_col}) "
        f"FROM iceberg.db.{entry.fact_table}"
    )


def check_or_full_load(entry: MaterializationEntry) -> str | None:
    """Check if mat table exists. If not, do full CTAS. Returns watermark or None."""
    hook = _get_hook()

    if not table_exists(hook, entry.mat_table):
        logger.info("Creating %s via full materialization", entry.mat_table)
        hook.run(
            f"CREATE TABLE iceberg.db.{entry.mat_table} AS "
            f"SELECT * FROM iceberg.db.{entry.view_name}"
        )
        row_count = _fetch_scalar(
            hook, f"SELECT count(*) FROM iceberg.db.{entry.mat_table}"
        )
        logger.info("Full load complete: %s rows", row_count)
        set_watermark(hook, entry)
        return None

    watermark = get_watermark(hook, entry.mat_table)

    if not watermark:
        logger.info("No watermark for %s, running full reload", entry.mat_table)
        hook.run(f"DELETE FROM iceberg.db.{entry.mat_table}")
        hook.run(
            f"INSERT INTO iceberg.db.{entry.mat_table} "
            f"SELECT * FROM iceberg.db.{entry.view_name}"
        )
        row_count = _fetch_scalar(
            hook, f"SELECT count(*) FROM iceberg.db.{entry.mat_table}"
        )
        logger.info("Full load complete: %s rows", row_count)
        set_watermark(hook, entry)
        return None

    logger.info("Watermark for %s: %s", entry.mat_table, watermark)
    return watermark


def pass_0_lookback_repair(
    entry: MaterializationEntry, watermark: str, lookback_hours: int
) -> bool:
    """Late-arrival repair within lookback window. Returns True if repair occurred."""
    if lookback_hours <= 0:
        return False

    if entry.fact_ts_col == "window_start":
        logger.info(
            "Skipping lookback for %s (Flink aggregate, windows are immutable)",
            entry.mat_table,
        )
        return False

    hook = _get_hook()

    lookback_start = str(
        _fetch_scalar(
            hook,
            f"SELECT CAST(TIMESTAMP '{watermark}' "
            f"- INTERVAL '{lookback_hours}' HOUR AS VARCHAR)",
        )
    )

    fact_count = int(
        _fetch_scalar(
            hook,
            f"SELECT count(*) FROM iceberg.db.{entry.fact_table} "
            f"WHERE {entry.fact_ts_col} >= TIMESTAMP '{lookback_start}' "
            f"AND {entry.fact_ts_col} <= TIMESTAMP '{watermark}'",
        )
        or 0
    )
    mat_count = int(
        _fetch_scalar(
            hook,
            f"SELECT count(*) FROM iceberg.db.{entry.mat_table} "
            f"WHERE {entry.view_ts_col} >= TIMESTAMP '{lookback_start}' "
            f"AND {entry.view_ts_col} <= TIMESTAMP '{watermark}'",
        )
        or 0
    )

    if fact_count == mat_count:
        return False

    logger.info(
        "Late arrivals detected in lookback window: %d fact rows vs %d mat rows",
        fact_count,
        mat_count,
    )

    hook.run(
        f"DELETE FROM iceberg.db.{entry.mat_table} "
        f"WHERE {entry.view_ts_col} >= TIMESTAMP '{lookback_start}' "
        f"AND {entry.view_ts_col} <= TIMESTAMP '{watermark}'"
    )
    hook.run(
        f"INSERT INTO iceberg.db.{entry.mat_table} "
        f"SELECT * FROM iceberg.db.{entry.view_name} "
        f"WHERE {entry.view_ts_col} >= TIMESTAMP '{lookback_start}' "
        f"AND {entry.view_ts_col} <= TIMESTAMP '{watermark}'"
    )

    repaired = _fetch_scalar(
        hook,
        f"SELECT count(*) FROM iceberg.db.{entry.mat_table} "
        f"WHERE {entry.view_ts_col} >= TIMESTAMP '{lookback_start}' "
        f"AND {entry.view_ts_col} <= TIMESTAMP '{watermark}'",
    )
    logger.info(
        "Repaired lookback window [%s .. %s]: %s rows",
        lookback_start,
        watermark,
        repaired,
    )
    return True


def pass_1_dimension_repair(
    entry: MaterializationEntry,
    watermark: str,
    pass0_repaired: bool,
    lookback_hours: int,
) -> int:
    """Repair rows affected by dimension changes. Returns count of dims repaired."""
    if not entry.dim_checks:
        return 0

    hook = _get_hook()
    dim_changed_count = 0

    lookback_start = None
    if pass0_repaired and lookback_hours > 0:
        lookback_start = str(
            _fetch_scalar(
                hook,
                f"SELECT CAST(TIMESTAMP '{watermark}' "
                f"- INTERVAL '{lookback_hours}' HOUR AS VARCHAR)",
            )
        )

    for dc in entry.dim_checks:
        changed = int(
            _fetch_scalar(
                hook,
                f"SELECT count(*) FROM iceberg.db.{dc.dim_table} "
                f"WHERE valid_from > TIMESTAMP '{watermark}'",
            )
            or 0
        )

        if changed == 0:
            continue

        logger.info(
            "Dimension change: %s (%d new versions)", dc.dim_table, changed
        )

        dim_repair_upper = ""
        if lookback_start:
            dim_repair_upper = (
                f"AND {entry.view_ts_col} < TIMESTAMP '{lookback_start}'"
            )

        hook.run(
            f"DELETE FROM iceberg.db.{entry.mat_table} "
            f"WHERE {dc.fk_col} IN ("
            f"  SELECT {dc.dim_pk} FROM iceberg.db.{dc.dim_table} "
            f"  WHERE valid_from > TIMESTAMP '{watermark}'"
            f") {dim_repair_upper}"
        )

        hook.run(
            f"INSERT INTO iceberg.db.{entry.mat_table} "
            f"SELECT * FROM iceberg.db.{entry.view_name} "
            f"WHERE {dc.fk_col} IN ("
            f"  SELECT {dc.dim_pk} FROM iceberg.db.{dc.dim_table} "
            f"  WHERE valid_from > TIMESTAMP '{watermark}'"
            f") AND {entry.view_ts_col} <= TIMESTAMP '{watermark}' "
            f"{dim_repair_upper}"
        )

        dim_changed_count += 1

    if dim_changed_count > 0:
        logger.info("Repaired %d dimension change(s)", dim_changed_count)

    return dim_changed_count


def pass_2_new_data_append(entry: MaterializationEntry, watermark: str) -> int:
    """Append new fact data since watermark. Returns count of new rows."""
    hook = _get_hook()

    new_rows = int(
        _fetch_scalar(
            hook,
            f"SELECT count(*) FROM iceberg.db.{entry.fact_table} "
            f"WHERE {entry.fact_ts_col} > TIMESTAMP '{watermark}'",
        )
        or 0
    )

    if new_rows == 0:
        logger.info("No new fact data for %s", entry.mat_table)
        return 0

    hook.run(
        f"INSERT INTO iceberg.db.{entry.mat_table} "
        f"SELECT * FROM iceberg.db.{entry.view_name} "
        f"WHERE {entry.view_ts_col} > TIMESTAMP '{watermark}'"
    )
    logger.info("Appended %d new fact rows to %s", new_rows, entry.mat_table)
    return new_rows


def pass_3_funnel_repair(watermark: str) -> int:
    """Repair incomplete funnel rows where downstream events arrived late."""
    hook = _get_hook()

    repair_responses = int(
        _fetch_scalar(
            hook,
            f"SELECT count(*) FROM iceberg.db.mat_full_funnel m "
            f"WHERE m.request_timestamp <= TIMESTAMP '{watermark}' "
            f"AND m.has_response = false "
            f"AND EXISTS (SELECT 1 FROM iceberg.db.bid_responses r "
            f"WHERE r.request_id = m.request_id)",
        )
        or 0
    )

    repair_impressions = int(
        _fetch_scalar(
            hook,
            f"SELECT count(*) FROM iceberg.db.mat_full_funnel m "
            f"WHERE m.request_timestamp <= TIMESTAMP '{watermark}' "
            f"AND m.has_impression = false AND m.has_response = true "
            f"AND EXISTS ("
            f"  SELECT 1 FROM iceberg.db.impressions i "
            f"  JOIN iceberg.db.bid_responses r ON r.response_id = i.response_id "
            f"  WHERE r.request_id = m.request_id)",
        )
        or 0
    )

    repair_clicks = int(
        _fetch_scalar(
            hook,
            f"SELECT count(*) FROM iceberg.db.mat_full_funnel m "
            f"WHERE m.request_timestamp <= TIMESTAMP '{watermark}' "
            f"AND m.has_click = false AND m.has_impression = true "
            f"AND EXISTS ("
            f"  SELECT 1 FROM iceberg.db.clicks c "
            f"  JOIN iceberg.db.impressions i ON i.impression_id = c.impression_id "
            f"  JOIN iceberg.db.bid_responses r ON r.response_id = i.response_id "
            f"  WHERE r.request_id = m.request_id)",
        )
        or 0
    )

    repair_count = repair_responses + repair_impressions + repair_clicks
    if repair_count == 0:
        return 0

    logger.info(
        "Stale funnel rows: %d responses, %d impressions, %d clicks",
        repair_responses,
        repair_impressions,
        repair_clicks,
    )

    hook.run(
        f"DELETE FROM iceberg.db.mat_full_funnel "
        f"WHERE request_timestamp <= TIMESTAMP '{watermark}' "
        f"AND ("
        f"  (has_response = false AND EXISTS ("
        f"    SELECT 1 FROM iceberg.db.bid_responses r "
        f"    WHERE r.request_id = mat_full_funnel.request_id))"
        f"  OR (has_impression = false AND has_response = true AND EXISTS ("
        f"    SELECT 1 FROM iceberg.db.impressions i "
        f"    JOIN iceberg.db.bid_responses r ON r.response_id = i.response_id "
        f"    WHERE r.request_id = mat_full_funnel.request_id))"
        f"  OR (has_click = false AND has_impression = true AND EXISTS ("
        f"    SELECT 1 FROM iceberg.db.clicks c "
        f"    JOIN iceberg.db.impressions i ON i.impression_id = c.impression_id "
        f"    JOIN iceberg.db.bid_responses r ON r.response_id = i.response_id "
        f"    WHERE r.request_id = mat_full_funnel.request_id))"
        f")"
    )

    hook.run(
        f"INSERT INTO iceberg.db.mat_full_funnel "
        f"SELECT v.* FROM iceberg.db.v_event_enriched_full_funnel v "
        f"WHERE v.request_id IN ("
        f"  SELECT br.request_id FROM iceberg.db.bid_requests br "
        f"  WHERE br.event_timestamp <= TIMESTAMP '{watermark}' "
        f"  AND ("
        f"    EXISTS (SELECT 1 FROM iceberg.db.bid_responses r "
        f"      WHERE r.request_id = br.request_id "
        f"      AND r.event_timestamp > TIMESTAMP '{watermark}')"
        f"    OR EXISTS (SELECT 1 FROM iceberg.db.impressions i "
        f"      JOIN iceberg.db.bid_responses r ON r.response_id = i.response_id "
        f"      WHERE r.request_id = br.request_id "
        f"      AND i.event_timestamp > TIMESTAMP '{watermark}')"
        f"    OR EXISTS (SELECT 1 FROM iceberg.db.clicks c "
        f"      JOIN iceberg.db.impressions i ON i.impression_id = c.impression_id "
        f"      JOIN iceberg.db.bid_responses r ON r.response_id = i.response_id "
        f"      WHERE r.request_id = br.request_id "
        f"      AND c.event_timestamp > TIMESTAMP '{watermark}')"
        f"  )"
        f")"
    )

    logger.info(
        "Repaired %d incomplete funnel rows (%d responses, %d impressions, %d clicks)",
        repair_count,
        repair_responses,
        repair_impressions,
        repair_clicks,
    )
    return repair_count
