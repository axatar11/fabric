"""Shared Bronze column resolution and trim/cast helpers."""

from __future__ import annotations

import re

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F


def _bronze_column_key(name: str) -> str:
    """Match headers regardless of spaces/underscores (``Average weekly hours`` ~ ``AverageWeeklyHours``)."""
    return re.sub(r"[\s_]+", "", name.lower())


def _bronze_column(df: DataFrame, *names: str) -> Column:
    """Resolve a bronze column (exact lower-case, then space/underscore-insensitive match)."""
    lower_map = {c.lower(): c for c in df.columns}
    norm_map = {_bronze_column_key(c): c for c in df.columns}
    for name in names:
        key = name.lower()
        if key in lower_map:
            return F.col(lower_map[key])
        nkey = _bronze_column_key(name)
        if nkey in norm_map:
            return F.col(norm_map[nkey])
    raise ValueError(
        f"Bronze table missing any of: {', '.join(names)} (available: {', '.join(df.columns)})"
    )


def _bronze_column_or_null(df: DataFrame, *names: str, dtype: str = "string") -> Column:
    """Like ``_bronze_column`` but returns typed null when the column is absent."""
    lower_map = {c.lower(): c for c in df.columns}
    norm_map = {_bronze_column_key(c): c for c in df.columns}
    for name in names:
        key = name.lower()
        if key in lower_map:
            return F.col(lower_map[key])
        nkey = _bronze_column_key(name)
        if nkey in norm_map:
            return F.col(norm_map[nkey])
    return F.lit(None).cast(dtype)

def _trim_bronze_string(df: DataFrame, *names: str) -> Column:
    """Trim bronze string column (PascalCase or snake_case source names)."""
    return F.trim(_bronze_column(df, *names).cast("string"))


def _trim_bronze_decimal(
    df: DataFrame, *names: str, precision: int = 18, scale: int = 8
) -> Column:
    """Trim source text then cast to decimal (e.g. hour averages)."""
    return F.trim(_bronze_column(df, *names).cast("string")).cast(
        f"decimal({precision},{scale})"
    )


def _bronze_timestamp(df: DataFrame, *names: str) -> Column:
    return _bronze_column(df, *names).cast("timestamp")


def _dedupe_by_columns(df: DataFrame, *key_exprs: Column) -> DataFrame:
    """Keep one row per composite partition key (ROW_NUMBER = 1)."""
    from pyspark.sql.window import Window

    if len(key_exprs) == 1:
        partition = key_exprs[0]
    else:
        partition = F.concat_ws("|", *[F.coalesce(c.cast("string"), F.lit("")) for c in key_exprs])
    w = Window.partitionBy(partition).orderBy(partition)
    return df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")

