"""DAG: Bronze (Spark) -> Silver -> Gold -> reconcile. ЕТАП 3.

Стан «що вже оброблено» лежить у _ingested_at таблиць, не в інтервалі розкладу.
Тому data_interval тут не використовую: подія може приїхати пізніше за своє occurred_at.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT = "/opt/airflow/project"
DBT_BIN = "/home/airflow/dbt-venv/bin/dbt"
DBT = f"cd {PROJECT} && DBT_TARGET_PATH=/tmp/dbt-target DBT_LOG_PATH=/tmp/dbt-logs {DBT_BIN}"
DBT_DIRS = "--project-dir dbt_rides --profiles-dir dbt_rides"

with DAG(
    dag_id="rides_medallion",
    description="Landing -> Bronze -> Silver -> Gold для подій поїздок",
    schedule="*/5 * * * *",
    start_date=datetime(2024, 1, 15),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
        "execution_timeout": timedelta(minutes=20),
    },
    tags=["rides"],
) as dag:
    bronze_spark = BashOperator(
        task_id="bronze_spark",
        bash_command=f"cd {PROJECT} && python bronze_job.py",
    )
    bronze_contract = BashOperator(
        task_id="bronze_contract",
        bash_command=f"{DBT} test --select source:bronze {DBT_DIRS}",
    )
    silver = BashOperator(
        task_id="silver",
        bash_command=f"{DBT} build --selector silver --indirect-selection cautious {DBT_DIRS}",
    )
    gold = BashOperator(
        task_id="gold",
        bash_command=f"{DBT} build --selector gold --indirect-selection cautious {DBT_DIRS}",
    )
    reconcile = BashOperator(
        task_id="reconcile",
        bash_command=f"{DBT} test --selector reconcile {DBT_DIRS}",
    )

    bronze_spark >> bronze_contract >> silver >> gold >> reconcile
