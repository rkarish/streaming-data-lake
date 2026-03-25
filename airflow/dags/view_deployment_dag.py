"""Deploy Trino views from SQL files.

Manually triggered DAG that reads trino/sql/*.sql and executes each
CREATE OR REPLACE VIEW statement. Designed to be triggered via the
Airflow REST API as part of a deployment workflow.
"""

from datetime import datetime

from airflow.decorators import dag, task


@dag(
    dag_id="view_deployment",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["etl", "deployment"],
)
def view_deployment_dag():

    @task
    def deploy_all_views(**context):
        from view_runner import deploy_views

        success, errors = deploy_views()
        return {"views_created": success, "errors": errors}

    deploy_all_views()


view_deployment_dag()
