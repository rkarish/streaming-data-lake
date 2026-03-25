"""Daily Iceberg table maintenance: compaction, snapshot expiry, orphan cleanup.

Migrates scripts/maintenance.sh into Airflow. Each table gets a TaskGroup
with a gate check and three sequential SQLExecuteQueryOperator tasks.
"""

from datetime import datetime

from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.models.param import Param
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.trino.hooks.trino import TrinoHook
from airflow.utils.task_group import TaskGroup

from maintenance_config import MAINTENANCE_TABLES

TRINO_CONN_ID = "trino_default"


@dag(
    dag_id="maintenance",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["etl", "maintenance"],
    params={
        "compact_target": Param("128MB", type="string"),
        "expire_threshold": Param("7d", type="string"),
        "orphan_threshold": Param("7d", type="string"),
    },
)
def maintenance_dag():
    for table_name in MAINTENANCE_TABLES:
        _build_table_group(table_name)


def _build_table_group(table_name: str):
    with TaskGroup(group_id=table_name):

        @task(task_id="check_exists")
        def check_exists(**context):
            hook = TrinoHook(trino_conn_id=TRINO_CONN_ID)
            try:
                hook.run(f"DESCRIBE {table_name}")
            except Exception:
                raise AirflowSkipException(
                    f"Table {table_name} does not exist, skipping"
                )

        compact = SQLExecuteQueryOperator(
            task_id="compact",
            conn_id=TRINO_CONN_ID,
            sql=(
                f"ALTER TABLE {table_name} EXECUTE "
                f"optimize(file_size_threshold => "
                f"'{{{{ params.compact_target }}}}')"
            ),
            do_xcom_push=False,
        )

        expire = SQLExecuteQueryOperator(
            task_id="expire_snapshots",
            conn_id=TRINO_CONN_ID,
            sql=(
                f"ALTER TABLE {table_name} EXECUTE "
                f"expire_snapshots(retention_threshold => "
                f"'{{{{ params.expire_threshold }}}}')"
            ),
            do_xcom_push=False,
        )

        orphan = SQLExecuteQueryOperator(
            task_id="remove_orphan_files",
            conn_id=TRINO_CONN_ID,
            sql=(
                f"ALTER TABLE {table_name} EXECUTE "
                f"remove_orphan_files(retention_threshold => "
                f"'{{{{ params.orphan_threshold }}}}')"
            ),
            do_xcom_push=False,
        )

        check_exists() >> compact >> expire >> orphan


maintenance_dag()
