"""HC, Okta, and Cursor usage transforms."""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from Common.config import NO_COUNTRY


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
    from Common.io import read_table

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
    joined, _rejects = split_cursor_okta_join(base, okta_slim)
    return joined


def split_cursor_okta_join(
    base: DataFrame, okta_slim: DataFrame
) -> tuple[DataFrame, DataFrame]:
    """Return (Okta-matched rows, base rows with no Okta match)."""
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
    joined = on_login.unionByName(on_full).dropDuplicates()
    matched = joined.select(*base.columns).dropDuplicates()
    rejects = base.join(matched, base.columns, "left_anti")
    return joined, rejects


def exclude_month_nos(df: DataFrame, month_col: str, closed_month_nos: list[int]) -> DataFrame:
    """Drop rows whose ``month_col`` is in ``closed_month_nos`` (closed reporting months)."""
    if not closed_month_nos:
        return df
    return df.filter(~F.col(month_col).isin(closed_month_nos))


def purview_copilot_min_creation_date(
    close_month_no: int | None,
    *,
    default_min: str | None = None,
) -> str:
    """Daily: config/env min date; month-close: first day of ``close_month_no`` (YYYYMM)."""
    if close_month_no is not None:
        year, month = divmod(int(close_month_no), 100)
        return f"{year}-{month:02d}-01"
    from Common import config
    import os

    return default_min or os.environ.get(
        "MEDALLION_PURVIEW_MIN_CREATION_DATE",
        config.PURVIEW_COPILOT_MIN_CREATION_DATE,
    )


def filter_to_month_no(
    df: DataFrame, month_col: str, month_no: int | None
) -> DataFrame:
    """Keep only rows for one YYYYMM (month-close scope)."""
    if month_no is None:
        return df
    return df.filter(F.col(month_col).cast("int") == F.lit(int(month_no)))


def filter_through_close_month_ts(
    df: DataFrame, ts_col: str, month_no: int | None
) -> DataFrame:
    """Month-close: keep rows with ``ts_col`` before the first day after ``month_no`` (YYYYMM)."""
    if month_no is None:
        return df
    y, m = divmod(int(month_no), 100)
    ny, nm = (y + 1, 1) if m == 12 else (y, m + 1)
    bound = F.to_timestamp(F.make_date(F.lit(ny), F.lit(nm), F.lit(1)))
    return df.filter(F.col(ts_col).cast("timestamp") < bound)


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
    key_cols = (
        "CursorUsageUser",
        "CursorUsageDate",
        "CursorUsageKind",
        "CursorUsageModel",
        "CursorUsageMaxMode",
        "CursorUsageInputWithCacheWrite",
        "CursorUsageInputWithoutCacheWrite",
        "CursorUsageCacheRead",
        "CursorUsageOutputTokens",
        "CursorUsageTotalTokens",
        "CursorUsageCost",
        "CursorUsageFileName",
        "CursorUsageIngestionDate",
        "CursorUsageServiceAccountID",
        "CursorUsageCloudAgentID",
        "CursorUsageAutomationID",
        "OktaUserId",
    )
    parts = [F.coalesce(F.col(c).cast("string"), F.lit("")) for c in key_cols]
    return F.sha2(F.concat_ws("|", *parts), 256)


def cursor_usage_record_key() -> Column:
    """SHA2 row key for silver ``cursor_usage`` (event-level merge)."""
    key_cols = (
        "CursorUsageUser",
        "CursorUsageDate",
        "CursorUsageKind",
        "CursorUsageModel",
        "CursorUsageMaxMode",
        "CursorUsageInputWithCacheWrite",
        "CursorUsageInputWithoutCacheWrite",
        "CursorUsageCacheRead",
        "CursorUsageOutputTokens",
        "CursorUsageTotalTokens",
        "CursorUsageCost",
        "CursorUsageFileName",
        "CursorUsageIngestionDate",
        "CursorUsageServiceAccountID",
        "CursorUsageCloudAgentID",
        "CursorUsageAutomationID",
        "OktaUserId",
    )
    parts = [
        F.coalesce(F.col(c).cast("string"), F.lit("")) for c in key_cols
    ]
    return F.sha2(F.concat_ws("|", *parts), 256)

