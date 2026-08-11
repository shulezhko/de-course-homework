"""Silver stage — clean, filter and de-duplicate the bronze events."""

from __future__ import annotations

from pathlib import Path

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

    out = Path(config.SILVER_FILE)
    out.parent.mkdir(parents=True, exist_ok=True)
    silver.write_parquet(out)

    return silver


def write_silver_partitioned(silver: pl.DataFrame) -> None:
    out_dir = Path(config.SILVER_PARTITIONED_DIR)
    out_dir.mkdir(parents=True, exist_ok=True)

    # hive-партиціонування за event_type
    silver.write_parquet(
        out_dir,
        partition_by=["event_type"],
    )
