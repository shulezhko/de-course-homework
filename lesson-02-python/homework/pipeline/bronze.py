
"""
Bronze stage — read the raw NDJSON and flatten it to one wide table.

TODO (Завдання 1): реалізуйте build_bronze().
Контракт колонок та типів: див. CONTRACTS.md → "bronze".

Підказки:
  * читайте NDJSON ліниво: pl.scan_ndjson(config.LANDING_FILE, schema=config.LANDING_SCHEMA)
  * розгортайте вкладені структури через .struct.field("...")
  * created_at -> datetime: .str.to_datetime("%Y-%m-%dT%H:%M:%SZ", time_zone="UTC")
  * commit_count: довжина списку payload.commits; для не-PushEvent коміти
    відсутні -> заповніть 0 (.list.len().fill_null(0))
  * запишіть результат у config.BRONZE_FILE (Parquet) і поверніть DataFrame
"""

from __future__ import annotations

from pathlib import Path  # <--- Перевірте, чи є цей імпорт на початку файлу
import polars as pl

from . import config


def build_bronze() -> pl.DataFrame:
    df = pl.scan_ndjson(config.LANDING_FILE, schema=config.LANDING_SCHEMA)

    bronze_df = df.select(
        pl.col("id").alias("event_id"),
        pl.col("type").alias("event_type"),
        pl.col("actor").struct.field("id").alias("actor_id"),
        pl.col("actor").struct.field("login").alias("actor_login"),
        pl.col("repo").struct.field("id").alias("repo_id"),
        pl.col("repo").struct.field("name").alias("repo_name"),
        pl.col("created_at").str.to_datetime("%Y-%m-%dT%H:%M:%SZ", time_zone="UTC"),
        pl.col("public"),
        pl.col("payload").struct.field("action").alias("action"),
        pl.col("payload")
        .struct.field("commits")
        .list.len()
        .fill_null(0)
        .alias("commit_count"),
    )

    res = bronze_df.collect()

    # Створюємо папку для файлу через Path(config.BRONZE_FILE).parent
    out_file = Path(config.BRONZE_FILE)
    out_file.parent.mkdir(parents=True, exist_ok=True)
    
    # Записуємо Parquet
    res.write_parquet(out_file)

    return res  