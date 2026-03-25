import logging
import os
from pathlib import Path

from airflow.providers.trino.hooks.trino import TrinoHook

logger = logging.getLogger(__name__)

TRINO_CONN_ID = "trino_default"
SQL_DIR = os.environ.get("TRINO_SQL_DIR", "/opt/airflow/trino_sql")


def deploy_views(sql_dir: str | None = None) -> tuple[int, int]:
    """Read and execute all *.sql view files. Returns (success_count, error_count)."""
    sql_path = Path(sql_dir or SQL_DIR)

    if not sql_path.is_dir():
        raise FileNotFoundError(f"SQL directory not found: {sql_path}")

    sql_files = sorted(sql_path.glob("*.sql"))
    if not sql_files:
        logger.warning("No .sql files found in %s", sql_path)
        return 0, 0

    hook = TrinoHook(trino_conn_id=TRINO_CONN_ID)
    success_count = 0
    error_count = 0

    for sql_file in sql_files:
        view_name = sql_file.stem
        sql = sql_file.read_text().strip()

        try:
            hook.run(sql)
            logger.info("OK    %s", view_name)
            success_count += 1
        except Exception:
            logger.exception("FAIL  %s", view_name)
            error_count += 1

    logger.info("%d views created, %d errors", success_count, error_count)

    if error_count > 0:
        raise RuntimeError(
            f"View deployment failed: {error_count} error(s) out of "
            f"{success_count + error_count} views"
        )

    return success_count, error_count
