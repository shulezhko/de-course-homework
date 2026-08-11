"""Bronze stage — read the raw NDJSON and flatten it to one wide table."""

from __future__ import annotations

from pathlib import Path

import polars as pl

from . import config


def build_bronze() -> pl.DataFrame:
    # читаємо сирі дані з landing ліниво, тільки потрібні колонки
    df = pl.scan_ndjson(config.LANDING_FILE, schema=config.LANDING_SCHEMA)

    # розгортаємо вкладені поля у пласку табличку
    bronze = df.select(
        pl.col("id").alias("event_id"),
        pl.col("type").alias("event_type"),
        pl.col("actor").struct.field("id").alias("actor_id"),
        pl.col("actor").struct.field("login").alias("actor_login"),
        pl.col("repo").struct.field("id").alias("repo_id"),
        pl.col("repo").struct.field("name").alias("repo_name"),
        pl.col("created_at")
        .str.to_datetime("%Y-%m-%dT%H:%M:%SZ", time_zone="UTC"),
        pl.col("public"),
        pl.col("payload").struct.field("action").alias("action"),
        # commit_count — довжина масиву commits, якщо null -> 0
        pl.col("payload")
        .struct.field("commits")
        .list.len()
        .fill_null(0)
        .cast(pl.Int64)
        .alias("commit_count"),
    ).collect()

    # зберігаємо parquet
    out = Path(config.BRONZE_FILE)
    out.parent.mkdir(parents=True, exist_ok=True)
    bronze.write_parquet(out)

    return bronze
