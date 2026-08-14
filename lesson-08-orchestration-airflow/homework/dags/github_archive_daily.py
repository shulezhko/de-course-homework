"""
DAG: github_archive_daily

Щоденний pipeline, що завантажує одну годину (14:00 UTC) подій GitHub Archive
у локальний DuckDB. Оркестрація п'яти задач у лінію:

    check_availability → download_archive → validate_file → load_to_duckdb → notify_completion

Ідемпотентність: усі задачі працюють з logical_date (ds) від Airflow,
повторний прогін того самого дня не дублює дані.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Custom sensor — перевіряє доступність файлу через HTTP HEAD
from gh_sensor import GHArchiveSensor

# ETL-функції (дано, не чіпаємо)
from include.gh_etl import download, validate, load_to_duckdb, summarize

# ---------------------------------------------------------------------------
# Константи (шляхи всередині контейнера)
# ---------------------------------------------------------------------------
DB_PATH = "/opt/airflow/data/github_analytics.duckdb"
LANDING_DIR = "/opt/airflow/data/landing"

# ---------------------------------------------------------------------------
# Default args: retry 2 рази з затримкою 30 секунд
# ---------------------------------------------------------------------------
DEFAULT_ARGS = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
}

# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------
with DAG(
    dag_id="github_archive_daily",
    description="Завантаження GitHub Archive (14:00 UTC) у DuckDB щодня",
    schedule="0 6 * * *",           # щодня о 06:00 UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,                  # не доганяємо автоматично пропущені дні
    default_args=DEFAULT_ARGS,
    tags=["github", "etl", "duckdb"],
    max_active_runs=1,              # DuckDB — single-writer, один run за раз
) as dag:

    # ----- Задача 1: Sensor — чекаємо появи файлу на GH Archive -----
    check_availability = GHArchiveSensor(
        task_id="check_availability",
        hour=14,                    # перевіряємо годину 14:00
        timeout=600,                # максимум 10 хвилин очікування
        poke_interval=60,           # перевірка раз на хвилину
        mode="reschedule",          # звільняє worker-слот між перевірками
    )

    # ----- Задача 2: Завантажити архів -----
    def _download_archive(ds, **_):
        """Качає файл і повертає шлях (автоматично потрапляє в XCom)."""
        path = download(ds, LANDING_DIR)
        print(f"Downloaded: {path}")
        return path

    download_archive = PythonOperator(
        task_id="download_archive",
        python_callable=_download_archive,
    )

    # ----- Задача 3: Валідація файлу -----
    def _validate_file(ti, **_):
        """Дістає шлях з XCom і валідує файл."""
        path = ti.xcom_pull(task_ids="download_archive")
        validate(path)
        print(f"Validation passed: {path}")
        return path  # прокидаємо далі через XCom

    validate_file = PythonOperator(
        task_id="validate_file",
        python_callable=_validate_file,
    )

    # ----- Задача 4: Завантаження у DuckDB -----
    def _load_to_duckdb(ds, ti, **_):
        """Дістає шлях з XCom і вантажить дані у DuckDB (ідемпотентно за ds)."""
        path = ti.xcom_pull(task_ids="download_archive")
        rows = load_to_duckdb(path, ds, DB_PATH)
        print(f"Loaded {rows} events for {ds}")
        return rows

    load_to_duckdb_task = PythonOperator(
        task_id="load_to_duckdb",
        python_callable=_load_to_duckdb,
    )

    # ----- Задача 5: Підсумок / нотифікація -----
    def _notify_completion(ds, ti, **_):
        """Виводить підсумок за день — кількість рядків і типів подій."""
        stats = summarize(ds, DB_PATH)
        rows_loaded = ti.xcom_pull(task_ids="load_to_duckdb")
        print(
            f"✅ Pipeline complete for {ds}: "
            f"{stats['rows']} rows, {stats['event_types']} event types "
            f"(loaded this run: {rows_loaded})"
        )

    notify_completion = PythonOperator(
        task_id="notify_completion",
        python_callable=_notify_completion,
    )

    # ----- Залежності: лінійний граф -----
    (
        check_availability
        >> download_archive
        >> validate_file
        >> load_to_duckdb_task
        >> notify_completion
    )
