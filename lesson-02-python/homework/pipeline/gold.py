"""Gold stage — analytics tables built from silver."""

from __future__ import annotations

from pathlib import Path
import polars as pl

from . import config


def _get_gold_dir() -> Path:
    # Отримуємо директорію data/gold
    gold_dir = Path(config.SILVER_FILE).parent.parent / "gold"
    gold_dir.mkdir(parents=True, exist_ok=True)
    return gold_dir


def build_repo_activity(
    silver_df: pl.DataFrame | pl.LazyFrame | None = None,
) -> pl.DataFrame:
    # Завдання 4: Агрегація по repo_name
    if silver_df is None:
        df = pl.scan_parquet(config.SILVER_FILE)
    elif isinstance(silver_df, pl.DataFrame):
        df = silver_df.lazy()
    else:
        df = silver_df

    res = (
        df.group_by("repo_name")
        .agg(
            pl.col("event_id").count().cast(pl.Int64).alias("event_count"),
            pl.col("event_type").n_unique().cast(pl.Int64).alias("distinct_event_types"),
        )
        .sort("event_count", descending=True)
        .collect()
    )

    out_file = _get_gold_dir() / "repo_activity.parquet"
    res.write_parquet(out_file)
    return res


def build_activity_per_minute(
    silver_df: pl.DataFrame | pl.LazyFrame | None = None,
) -> pl.DataFrame:
    # Завдання 5: Активність по хвилинах
    if silver_df is None:
        df = pl.scan_parquet(config.SILVER_FILE)
    elif isinstance(silver_df, pl.DataFrame):
        df = silver_df.lazy()
    else:
        df = silver_df

    res = (
        df.with_columns(pl.col("created_at").dt.truncate("1m").alias("minute"))
        .group_by("minute")
        .agg(pl.col("event_id").count().cast(pl.Int64).alias("event_count"))
        .sort("minute")
        .collect()
    )

    out_file = _get_gold_dir() / "activity_per_minute.parquet"
    res.write_parquet(out_file)
    return res


def build_push_commits_by_repo(
    silver_df: pl.DataFrame | pl.LazyFrame | None = None,
) -> pl.DataFrame:
    # Завдання 6: Пуші та коміти по репозиторіях
    if silver_df is None:
        df = pl.scan_parquet(config.SILVER_FILE)
    elif isinstance(silver_df, pl.DataFrame):
        df = silver_df.lazy()
    else:
        df = silver_df

    res = (
        df.filter(pl.col("event_type") == "PushEvent")
        .group_by("repo_name")
        .agg(
            pl.col("event_id").count().cast(pl.Int64).alias("push_events"),
            pl.col("commit_count").sum().cast(pl.Int64).alias("total_commits"),
        )
        .collect()
    )

    out_file = _get_gold_dir() / "push_commits_by_repo.parquet"
    res.write_parquet(out_file)
    return res