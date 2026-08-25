"""Gold stage — three analytics tables built from silver."""

from __future__ import annotations

import polars as pl

from . import config

GOLD_DIR = str(config.DATA_DIR / "gold")


def build_repo_activity(silver: pl.DataFrame) -> pl.DataFrame:
    # агрегація по repo_name, сортуємо спадно за event_count
    result = (
        silver.group_by("repo_name")
        .agg(
            pl.col("event_id").count().cast(pl.Int64).alias("event_count"),
            pl.col("event_type").n_unique().cast(pl.Int64).alias("distinct_event_types"),
        )
        .sort("event_count", descending=True)
    )

    result.write_parquet(f"{GOLD_DIR}/repo_activity.parquet", mkdir=True)
    return result


def build_activity_per_minute(silver: pl.DataFrame) -> pl.DataFrame:
    # кількість подій по хвилинах
    result = (
        silver.with_columns(pl.col("created_at").dt.truncate("1m").alias("minute"))
        .group_by("minute")
        .agg(pl.col("event_id").count().cast(pl.Int64).alias("event_count"))
        .sort("minute")
    )

    result.write_parquet(f"{GOLD_DIR}/activity_per_minute.parquet", mkdir=True)
    return result


def build_push_commits_by_repo(silver: pl.DataFrame) -> pl.DataFrame:
    # лише PushEvent — пуші і коміти по репо
    result = (
        silver.filter(pl.col("event_type") == "PushEvent")
        .group_by("repo_name")
        .agg(
            pl.col("event_id").count().cast(pl.Int64).alias("push_events"),
            pl.col("commit_count").sum().cast(pl.Int64).alias("total_commits"),
        )
    )

    result.write_parquet(f"{GOLD_DIR}/push_commits_by_repo.parquet", mkdir=True)
    return result
