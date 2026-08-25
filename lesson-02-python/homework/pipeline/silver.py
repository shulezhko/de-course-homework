"""Silver stage — clean, filter and de-duplicate the bronze events."""

from __future__ import annotations

import polars as pl

from . import config


def build_silver(bronze: pl.DataFrame) -> pl.DataFrame:
    df = bronze.lazy()

    # залишаємо лише цільові типи подій
    silver = (
        df.filter(pl.col("event_type").is_in(config.TARGET_EVENT_TYPES))
        # прибираємо рядки з пустим/null repo_name
        .filter(pl.col("repo_name").is_not_null())
        .filter(pl.col("repo_name") != "")
        # прибираємо null у ключових полях
        .filter(pl.col("event_id").is_not_null())
        .filter(pl.col("created_at").is_not_null())
        # гарантуємо унікальність
        .unique(subset=["event_id"])
    ).collect()

    silver.write_parquet(config.SILVER_FILE, mkdir=True)
    return silver


def write_silver_partitioned(silver: pl.DataFrame) -> None:
    # hive-партиціонування за event_type
    silver.write_parquet(
        config.SILVER_PARTITIONED_DIR,
        partition_by=["event_type"],
        mkdir=True,
    )
