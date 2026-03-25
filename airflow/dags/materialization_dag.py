"""Incremental materialization of dimension-enriched views.

Migrates the 4-pass algorithm from scripts/materialize.sh into Airflow.
Each materialization entry runs as a parallel TaskGroup with sequential passes.
"""

from datetime import datetime

from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.task_group import TaskGroup

from materialization_config import MATERIALIZATIONS, MaterializationEntry
from materialization_runner import (
    check_or_full_load,
    pass_0_lookback_repair,
    pass_1_dimension_repair,
    pass_2_new_data_append,
    pass_3_funnel_repair,
    set_watermark,
)

TRINO_CONN_ID = "trino_default"


@dag(
    dag_id="materialization",
    schedule="*/15 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["etl", "materialization"],
    params={"lookback_hours": Param(0, type="integer", minimum=0)},
)
def materialization_dag():
    ensure_watermark = SQLExecuteQueryOperator(
        task_id="ensure_watermark_table",
        conn_id=TRINO_CONN_ID,
        sql=(
            "CREATE TABLE IF NOT EXISTS iceberg.db.materialization_watermarks ("
            "  table_name VARCHAR,"
            "  last_materialized_at TIMESTAMP(6) WITH TIME ZONE"
            ")"
        ),
        do_xcom_push=False,
    )

    for entry in MATERIALIZATIONS:
        _build_task_group(entry, ensure_watermark)


def _build_task_group(entry: MaterializationEntry, upstream):
    with TaskGroup(group_id=entry.mat_table) as tg:

        @task(task_id="check_or_full_load")
        def _check_or_full_load(**context):
            return check_or_full_load(entry)

        @task(task_id="pass_0_lookback_repair")
        def _pass_0(watermark, **context):
            if watermark is None:
                return False
            lookback = context["params"]["lookback_hours"]
            return pass_0_lookback_repair(entry, watermark, lookback)

        @task(task_id="pass_1_dimension_repair")
        def _pass_1(watermark, pass0_repaired, **context):
            if watermark is None:
                return 0
            lookback = context["params"]["lookback_hours"]
            return pass_1_dimension_repair(
                entry, watermark, pass0_repaired, lookback
            )

        @task(task_id="pass_2_new_data_append")
        def _pass_2(watermark, **context):
            if watermark is None:
                return 0
            return pass_2_new_data_append(entry, watermark)

        @task(task_id="update_watermark", trigger_rule="all_done")
        def _update_watermark(**context):
            from materialization_runner import _get_hook

            hook = _get_hook()
            set_watermark(hook, entry)

        watermark = _check_or_full_load()
        p0 = _pass_0(watermark)
        _pass_1(watermark, p0)
        p2 = _pass_2(watermark)

        if entry.has_funnel_repair:

            @task(task_id="pass_3_funnel_repair")
            def _pass_3(watermark, **context):
                if watermark is None:
                    return 0
                return pass_3_funnel_repair(watermark)

            _pass_3(watermark) >> _update_watermark()
        else:
            p2 >> _update_watermark()

    upstream >> tg


materialization_dag()
