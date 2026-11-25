"""OpenAI Codex usage Bronze → Silver (OKTA email match priorities 0–5)."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from Common.transform_bronze import _bronze_column

_OKTA_OUT = (
    "okta_user_id",
    "okta_user_country",
    "okta_user_email",
    "okta_user_employee_number",
    "okta_user_full_email",
    "okta_user_full_name",
    "okta_user_region",
)

_PRIORITIES: tuple[tuple[int, str, bool], ...] = (
    (1, "codex_usage_email_normalized", False),
    (2, "codex_usage_email_without_10", False),
    (3, "codex_usage_email_without_st", False),
    (4, "codex_usage_email_without_domain", True),
    (5, "codex_usage_email_without_st_domain", True),
)


def _okta_col(df: DataFrame, snake: str, pascal: str) -> F.Column:
    if snake in df.columns:
        return F.col(snake)
    if pascal in df.columns:
        return F.col(pascal)
    raise ValueError(
        f"Okta silver missing {snake} / {pascal} (available: {', '.join(df.columns)})"
    )


def okta_slim_for_codex(okta: DataFrame) -> DataFrame:
    """Normalize ``okta_user_for_ai`` (PascalCase) or ``core_okta_user`` (snake) for Codex joins."""
    email = F.lower(F.trim(_okta_col(okta, "okta_user_email", "OktaUserEmail").cast("string")))
    full_email = F.lower(
        F.trim(_okta_col(okta, "okta_user_full_email", "OktaUserFullEmail").cast("string"))
    )
    email_local = F.when(email.contains("@"), F.split(email, "@").getItem(0))
    full_local = F.when(full_email.contains("@"), F.split(full_email, "@").getItem(0))
    return okta.select(
        _okta_col(okta, "okta_user_id", "OktaUserId").alias("okta_user_id"),
        email.alias("okta_user_email"),
        full_email.alias("okta_user_full_email"),
        F.when(email.contains("@"), email_local).alias("okta_user_email_without_domain"),
        F.when(full_email.contains("@"), full_local).alias(
            "okta_user_full_email_without_domain"
        ),
        _okta_col(okta, "okta_user_employee_number", "OktaUserEmployeeNumber").alias(
            "okta_user_employee_number"
        ),
        _okta_col(okta, "okta_user_full_name", "OktaUserFullName").alias("okta_user_full_name"),
        _okta_col(okta, "okta_user_region", "OktaUserRegion").alias("okta_user_region"),
        _okta_col(okta, "okta_user_country", "OktaUserCountry").alias("okta_user_country"),
    ).filter(F.col("okta_user_id").isNotNull())


def _codex_email_variants(source_email: F.Column) -> tuple[F.Column, ...]:
    """Priority 0 normalization plus match keys for priorities 2–5."""
    norm = F.lower(F.trim(source_email))
    local = F.split(norm, "@").getItem(0)
    without_10 = F.when(
        local.rlike(r"^10[0-9]+$"),
        F.regexp_replace(norm, r"^10", ""),
    )
    without_st = F.when(
        local.like("%.st"),
        F.regexp_replace(norm, r"\.st@", "@"),
    )
    without_domain = F.when(norm.contains("@"), local)
    without_st_domain = F.when(
        without_domain.like("%.st"),
        F.regexp_replace(without_domain, r"\.st$", ""),
    )
    return norm, without_10, without_st, without_domain, without_st_domain


def parse_codex_bronze_base(bronze: DataFrame) -> DataFrame:
    """Bronze ``openai_usage`` grain with Codex email normalization columns."""
    source = F.lower(F.trim(_bronze_column(bronze, "source_email").cast("string")))
    norm, w10, wst, wdom, wst_dom = _codex_email_variants(source)
    record_id = _bronze_column(bronze, "id", "record_id", "usage_id")
    return bronze.select(
        record_id.alias("codex_usage_record_id"),
        norm.alias("codex_usage_email_normalized"),
        w10.alias("codex_usage_email_without_10"),
        wst.alias("codex_usage_email_without_st"),
        wdom.alias("codex_usage_email_without_domain"),
        wst_dom.alias("codex_usage_email_without_st_domain"),
    ).dropDuplicates(["codex_usage_record_id"])


def _priority_matches(
    base: DataFrame,
    okta: DataFrame,
    src_col: str,
    priority: int,
    *,
    local_part: bool,
) -> DataFrame:
    keys = (
        base.select(
            F.col("codex_usage_record_id").alias("_record_key"),
            F.col(src_col).alias("_match_key"),
        )
        .filter(F.col("_match_key").isNotNull())
        .dropDuplicates(["_record_key", "_match_key"])
    )
    if local_part:
        on_login = keys.join(
            okta,
            F.col("_match_key") == F.col("okta_user_email_without_domain"),
            "inner",
        )
        on_full = keys.join(
            okta,
            F.col("_match_key") == F.col("okta_user_full_email_without_domain"),
            "inner",
        )
    else:
        on_login = keys.join(okta, F.col("_match_key") == F.col("okta_user_email"), "inner")
        on_full = keys.join(
            okta, F.col("_match_key") == F.col("okta_user_full_email"), "inner"
        )
    okta_pick = [c for c in _OKTA_OUT if c in on_login.columns]
    matched = on_login.select(
        F.col("_record_key"), *[F.col(c) for c in okta_pick]
    ).unionByName(on_full.select(F.col("_record_key"), *[F.col(c) for c in okta_pick]))
    tie = Window.partitionBy("_record_key").orderBy(F.col("okta_user_id"))
    return (
        matched.withColumn("_prio", F.lit(priority))
        .withColumn("_tie", F.row_number().over(tie))
        .filter(F.col("_tie") == 1)
        .drop("_tie")
    )


def join_codex_okta_with_priorities(base: DataFrame, okta: DataFrame) -> DataFrame:
    """Apply matching rules 1–5; one OKTA user per source record (lowest priority wins)."""
    slim = okta_slim_for_codex(okta)
    all_matches: DataFrame | None = None
    for prio, col, local in _PRIORITIES:
        chunk = _priority_matches(base, slim, col, prio, local_part=local)
        all_matches = chunk if all_matches is None else all_matches.unionByName(chunk)
    if all_matches is None or all_matches.limit(1).count() == 0:
        null_okta = [F.lit(None).cast("string").alias(c) for c in _OKTA_OUT]
        return base.select(
            F.col("codex_usage_record_id"),
            F.col("codex_usage_email_normalized"),
            F.col("codex_usage_email_without_10"),
            F.col("codex_usage_email_without_st"),
            F.col("codex_usage_email_without_domain"),
            F.col("codex_usage_email_without_st_domain"),
            *null_okta,
        )
    best = Window.partitionBy("_record_key").orderBy(F.col("_prio"))
    winners = (
        all_matches.withColumn("_rn", F.row_number().over(best))
        .filter(F.col("_rn") == 1)
        .drop("_rn", "_prio")
    )
    joined = base.join(
        winners,
        base.codex_usage_record_id == winners._record_key,
        "left",
    ).drop("_record_key")
    return joined.select(
        F.col("codex_usage_record_id"),
        F.col("codex_usage_email_normalized"),
        F.col("codex_usage_email_without_10"),
        F.col("codex_usage_email_without_st"),
        F.col("codex_usage_email_without_domain"),
        F.col("codex_usage_email_without_st_domain"),
        *[F.col(c) for c in _OKTA_OUT],
    )


def codex_usage_matching_log_output(rejects: DataFrame) -> DataFrame:
    """Unmatched rows for ``codex_usage_matching_log``."""
    return (
        rejects.withColumn("codex_matching_log_reason", F.lit("okta_join_miss"))
        .withColumn("codex_matching_log_loaded_at", F.current_timestamp())
    )


def build_codex_usage_silver(
    bronze: DataFrame, okta: DataFrame
) -> tuple[DataFrame, DataFrame]:
    """Return (``codex_usage`` silver, ``codex_usage_matching_log`` rows)."""
    base = parse_codex_bronze_base(bronze)
    joined = join_codex_okta_with_priorities(base, okta)
    silver = joined.filter(F.col("okta_user_id").isNotNull())
    rejects = joined.filter(F.col("okta_user_id").isNull())
    log_df = codex_usage_matching_log_output(rejects)
    return silver, log_df