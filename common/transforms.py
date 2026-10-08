"""Reusable Spark column transforms for HC, Okta, and Cursor pipelines."""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from common.config import NO_COUNTRY


def trim_lower(col_name: str) -> Column:
    """``lower(trim(col))`` expression."""
    return F.lower(F.trim(F.col(col_name)))


def is_valid_email(col: Column) -> Column:
    """Boolean column: non-empty value matching a simple email pattern."""
    normalized = F.lower(F.trim(col))
    return (
        col.isNotNull()
        & (F.trim(col) != "")
        & normalized.rlike(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
    )


def date_from_yyyymm(month_no_col: Column) -> Column:
    """Convert YYYYMM integer column to first-of-month ``date``."""
    year = (month_no_col / F.lit(100)).cast("int")
    month = (month_no_col % F.lit(100)).cast("int")
    return F.make_date(year, month, F.lit(1))


def load_country_lookup(spark, country_table: str) -> DataFrame:
    """Read country reference table; return ``Country`` / ``DisplayName`` key columns."""
    from common.io import read_table

    return read_table(spark, country_table).select(
        F.col("Country").alias("_country_key"),
        F.col("DisplayName").alias("_country_display"),
    )


def with_normalized_country(
    df: DataFrame,
    country_source_col: str,
    country_lookup: DataFrame,
    output_col: str = "OktaUserCountry",
) -> DataFrame:
    """Map raw country to display name; use ``NO_COUNTRY`` when unmatched."""
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


def join_okta_for_cursor_usage(base: DataFrame, okta_slim: DataFrame) -> DataFrame:
    """Equi-joins only (no OR) — avoids BroadcastNestedLoopJoin on local Windows Spark."""
    on_login = base.join(
        okta_slim,
        base.CursorUsageUser == okta_slim.OktaUserEmail,
        "inner",
    )
    on_full = base.join(
        okta_slim,
        base.CursorUsageUser == okta_slim.OktaUserFullEmail,
        "inner",
    )
    return on_login.unionByName(on_full).dropDuplicates()


def join_hc_for_cursor_usage(
    joined: DataFrame, hc_slim: DataFrame, division_inc_default: str
) -> DataFrame:
    """Left attach HC on email key, then coalesce attrs from full-email key match."""
    hc_attrs = [c for c in hc_slim.columns if c != "HCKeyEmail"]
    with_email = joined.join(
        hc_slim,
        joined.CursorUsageKeyEmail == hc_slim.HCKeyEmail,
        "left",
    )
    hc_alt = hc_slim.select(
        F.col("HCKeyEmail").alias("_hc_alt_key"),
        *[F.col(c).alias(f"_hc_alt_{c}") for c in hc_attrs],
    )
    with_both = with_email.join(
        hc_alt,
        with_email.CursorUsageKeyFullEmail == hc_alt._hc_alt_key,
        "left",
    )
    for name in hc_attrs:
        alt = f"_hc_alt_{name}"
        if alt in with_both.columns:
            with_both = with_both.withColumn(name, F.coalesce(F.col(name), F.col(alt)))
    drop_cols = ["_hc_alt_key", *[f"_hc_alt_{c}" for c in hc_attrs]]
    with_both = with_both.drop(*[c for c in drop_cols if c in with_both.columns])
    return with_both.withColumn(
        "HCDivisionINC",
        F.coalesce(F.col("HCDivisionINC"), F.lit(division_inc_default)),
    )


def cursor_active_daily_aggregate(silver: DataFrame) -> DataFrame:
    """GROUP BY CursorUsageDate, OktaUserId — Request = COUNT(*), dims = first per group."""
    return silver.groupBy("CursorUsageDate", "OktaUserId").agg(
        F.first("CursorUsageMonthNo", ignorenulls=True).alias("CursorUsageMonthNo"),
        F.count(F.lit(1)).cast("int").alias("Request"),
        F.first("OktaUserFullName", ignorenulls=True).alias("OktaUserFullName"),
        F.first("OktaUserEmployeeNumber", ignorenulls=True).alias("OktaUserEmployeeNumber"),
        F.first("OktaUserEmail", ignorenulls=True).alias("OktaUserEmail"),
        F.first("OktaUserFullEmail", ignorenulls=True).alias("OktaUserFullEmail"),
        F.first("OktaUserRegion", ignorenulls=True).alias("OktaUserRegion"),
        F.first("OktaUserCountry", ignorenulls=True).alias("OktaUserCountry"),
        F.first("HCBusinessUnit", ignorenulls=True).alias("HCBusinessUnit"),
        F.first("HCJobTitle", ignorenulls=True).alias("HCJobTitle"),
        F.first("HCOktaUserCountry", ignorenulls=True).alias("HCOktaUserCountry"),
        F.first("HCOffice", ignorenulls=True).alias("HCOffice"),
        F.first("HCTalentSegment", ignorenulls=True).alias("HCTalentSegment"),
        F.first("HCDivision", ignorenulls=True).alias("HCDivision"),
        F.first("HCBusinessUnitCode", ignorenulls=True).alias("HCBusinessUnitCode"),
    )


def cursor_active_daily_merge_key() -> Column:
    """Merge grain: one row per (CursorUsageDate, OktaUserId)."""
    return cursor_onboard_merge_key()


def cursor_onboard_daily_dedupe(silver: DataFrame) -> DataFrame:
    """Keep latest ingestion row per ``(CursorUsageDate, OktaUserId)``."""
    from pyspark.sql.window import Window

    w = Window.partitionBy("CursorUsageDate", "OktaUserId").orderBy(
        F.col("CursorUsageIngestionDate").desc_nulls_last()
    )
    return (
        silver.withColumn("_onboard_rn", F.row_number().over(w))
        .filter(F.col("_onboard_rn") == 1)
        .drop("_onboard_rn")
    )


def cursor_onboard_merge_key() -> Column:
    """SHA2 surrogate key: ``CursorUsageDate | OktaUserId``."""
    return F.sha2(
        F.concat_ws(
            "|",
            F.coalesce(F.col("CursorUsageDate").cast("string"), F.lit("")),
            F.coalesce(F.col("OktaUserId"), F.lit("")),
        ),
        256,
    )


def cursor_usage_record_key() -> Column:
    """SHA2 row key for silver ``cursor_usage`` (event-level merge)."""
    parts = [
        F.coalesce(F.col("CursorUsageUser"), F.lit("")),
        F.coalesce(F.col("CursorUsageDate").cast("string"), F.lit("")),
        F.coalesce(F.col("CursorUsageKind"), F.lit("")),
        F.coalesce(F.col("CursorUsageModel"), F.lit("")),
        F.coalesce(F.col("CursorUsageTotalTokens").cast("string"), F.lit("")),
        F.coalesce(F.col("CursorUsageCost").cast("string"), F.lit("")),
    ]
    return F.sha2(F.concat_ws("|", *parts), 256)
