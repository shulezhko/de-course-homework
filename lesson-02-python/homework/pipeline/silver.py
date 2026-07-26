"""Silver stage — clean, filter and de-duplicate bronze events."""

from __future__ import annotations

from pathlib import Path
import polars as pl

from . import config


def build_silver(bronze_df: pl.DataFrame | pl.LazyFrame | None = None) -> pl.DataFrame:
   
    if bronze_df is None:
        df = pl.scan_parquet(config.BRONZE_FILE)
    elif isinstance(bronze_df, pl.DataFrame):
        df = bronze_df.lazy()
    else:
        df = bronze_df

    
    silver_df = (
        df.filter(pl.col("event_type").is_in(config.TARGET_EVENT_TYPES))
        .filter(pl.col("repo_name").is_not_null())
        .filter(pl.col("repo_name") != "")
        .filter(pl.col("event_id").is_not_null())
        .filter(pl.col("created_at").is_not_null())
        .unique(subset=["event_id"])
    )

    res = silver_df.collect()

    out_file = Path(config.SILVER_FILE)
    out_file.parent.mkdir(parents=True, exist_ok=True)
    res.write_parquet(out_file)

    return res


def write_silver_partitioned(silver_df: pl.DataFrame) -> None:
    out_dir = Path(config.SILVER_PARTITIONED_DIR)
    out_dir.mkdir(parents=True, exist_ok=True)

    silver_df.write_parquet(
        out_dir,
        partition_by=["event_type"],
    )   