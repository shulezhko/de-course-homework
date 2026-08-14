"""DAG github_archive_daily — щодня качаємо годину GH Archive і пишемо в DuckDB."""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from gh_sensor import GHArchiveSensor
from include.gh_etl import download, validate, load_to_duckdb, summarize

DB_PATH = "/opt/airflow/data/github_analytics.duckdb"
LANDING_DIR = "/opt/airflow/data/landing"

with DAG(
    dag_id="github_archive_daily",
    schedule="0 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["github", "etl", "duckdb"],
    max_active_runs=1,
    default_args={
        "owner": "airflow",
        "retries": 2,
        "retry_delay": timedelta(seconds=30),
    },
) as dag:

    # 1) Sensor — чекаємо, поки файл буде на сервері
    check_availability = GHArchiveSensor(
        task_id="check_availability",
        hour=14,
        timeout=600,
        poke_interval=60,
        mode="reschedule",
    )

    # 2) Завантажуємо архів
    def _download_archive(ds, **_):
        path = download(ds, LANDING_DIR)
        print(f"downloaded -> {path}")
        return path   # XCom

    download_archive = PythonOperator(
        task_id="download_archive",
        python_callable=_download_archive,
    )

    # 3) Валідація (перевіряє розмір і структуру)
    def _validate_file(ti, **_):
        path = ti.xcom_pull(task_ids="download_archive")
        validate(path)
        return path

    validate_file = PythonOperator(
        task_id="validate_file",
        python_callable=_validate_file,
    )

    # 4) Вантажимо в DuckDB (DELETE+INSERT за ds — ідемпотентно)
    def _load_to_duckdb(ds, ti, **_):
        path = ti.xcom_pull(task_ids="download_archive")
        rows = load_to_duckdb(path, ds, DB_PATH)
        print(f"{ds}: loaded {rows} rows")
        return rows

    load_to_duckdb_task = PythonOperator(
        task_id="load_to_duckdb",
        python_callable=_load_to_duckdb,
    )

    # 5) Друкуємо підсумок
    def _notify_completion(ds, ti, **_):
        stats = summarize(ds, DB_PATH)
        print(f"done {ds}: {stats['rows']} rows, {stats['event_types']} types")

    notify_completion = PythonOperator(
        task_id="notify_completion",
        python_callable=_notify_completion,
    )

    # граф залежностей — лінійний ланцюжок
    check_availability >> download_archive >> validate_file >> load_to_duckdb_task >> notify_completion
