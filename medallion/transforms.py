from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from medallion.config import NO_COUNTRY


def trim_lower(col_name: str) -> Column:
    return F.lower(F.trim(F.col(col_name)))


def trim_col(col_name: str) -> Column:
    return F.trim(F.col(col_name))


def is_valid_email(col: Column) -> Column:
    normalized = F.lower(F.trim(col))
    return (
        col.isNotNull()
        & (F.trim(col) != "")
        & normalized.rlike(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
    )


def date_from_yyyymm(month_no_col: Column) -> Column:
    year = (month_no_col / F.lit(100)).cast("int")
    month = (month_no_col % F.lit(100)).cast("int")
    return F.make_date(year, month, F.lit(1))


def load_country_lookup(spark, country_table: str) -> DataFrame:
    return spark.table(country_table).select(
        F.col("Country").alias("_country_key"),
        F.col("DisplayName").alias("_country_display"),
    )


def with_normalized_country(
    df: DataFrame,
    country_source_col: str,
    country_lookup: DataFrame,
    output_col: str = "OktaUserCountry",
) -> DataFrame:
    return (
        df.join(
            country_lookup,
            F.col(country_source_col) == F.col("_country_key"),
            "left",
        )
        .withColumn(
            output_col,
            F.coalesce(F.col("_country_display"), F.lit(NO_COUNTRY)),
        )
        .drop("_country_key", "_country_display")
    )


def cursor_usage_record_key() -> Column:
    parts = [
        F.coalesce(F.col("CursorUsageUser"), F.lit("")),
        F.coalesce(F.col("CursorUsageDate").cast("string"), F.lit("")),
        F.coalesce(F.col("CursorUsageKind"), F.lit("")),
        F.coalesce(F.col("CursorUsageModel"), F.lit("")),
        F.coalesce(F.col("CursorUsageTotalTokens").cast("string"), F.lit("")),
        F.coalesce(F.col("CursorUsageCost").cast("string"), F.lit("")),
    ]
    return F.sha2(F.concat_ws("|", *parts), 256)
