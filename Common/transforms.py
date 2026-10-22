"""Reusable Spark column transforms for HC, Okta, and cursor_usage pipelines."""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, BooleanType, StringType, StructField, StructType

from Common.config import HC_DIVISION_INC_DEFAULT, NO_COUNTRY


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


def _bronze_column(df: DataFrame, *names: str) -> Column:
    """Resolve a bronze column when Fabric uses PascalCase or snake_case."""
    lower_map = {c.lower(): c for c in df.columns}
    for name in names:
        key = name.lower()
        if key in lower_map:
            return F.col(lower_map[key])
    raise ValueError(f"Bronze table missing any of: {', '.join(names)}")


def _bronze_column_or_null(df: DataFrame, *names: str, dtype: str = "string") -> Column:
    """Like ``_bronze_column`` but returns typed null when the column is absent."""
    lower_map = {c.lower(): c for c in df.columns}
    for name in names:
        key = name.lower()
        if key in lower_map:
            return F.col(lower_map[key])
    return F.lit(None).cast(dtype)


def purview_copilot_interaction_type(
    app_identity: Column, agent_name: Column
) -> Column:
    """Classify Copilot interaction from AppIdentity / agent name."""
    return (
        F.when(app_identity.like("Copilot.Fabric.%"), F.lit("Power BI / Fabric"))
        .when(app_identity.like("Copilot.Studio.%"), F.lit("Copilot Studio Agent"))
        .when(agent_name.isin("Analyst", "Researcher"), F.lit("Specialized Agent"))
        .when(app_identity.like("MicrosoftAgent.%"), F.lit("Built-in Agent"))
        .when(
            app_identity.isin(
                "Copilot.M365Copilot.WebChat",
                "Copilot.M365Copilot.Bizchat",
            ),
            F.lit("Web Chat"),
        )
        .when(app_identity == "Copilot.M365Copilot.Apps", F.lit("Microsoft 365 App"))
        .otherwise(F.lit("Other"))
    )


def join_okta_on_user_email(
    base: DataFrame, okta_slim: DataFrame, user_email_col: str
) -> DataFrame:
    """Inner join Okta on short or full email (equi-joins only)."""
    user = F.col(user_email_col)
    on_login = base.join(okta_slim, user == okta_slim.OktaUserEmail, "inner")
    on_full = base.join(okta_slim, user == okta_slim.OktaUserFullEmail, "inner")
    return on_login.unionByName(on_full).dropDuplicates()


def join_hc_on_email_keys(
    joined: DataFrame,
    hc_slim: DataFrame,
    key_email_col: str,
    key_full_email_col: str,
    division_inc_default: str = HC_DIVISION_INC_DEFAULT,
) -> DataFrame:
    """Left attach HC attributes using monthly email keys."""
    hc_attrs = [c for c in hc_slim.columns if c != "HCKeyEmail"]
    with_email = joined.join(
        hc_slim,
        F.col(key_email_col) == hc_slim.HCKeyEmail,
        "left",
    )
    hc_alt = hc_slim.select(
        F.col("HCKeyEmail").alias("_hc_alt_key"),
        *[F.col(c).alias(f"_hc_alt_{c}") for c in hc_attrs],
    )
    with_both = with_email.join(
        hc_alt,
        F.col(key_full_email_col) == hc_alt._hc_alt_key,
        "left",
    )
    for name in hc_attrs:
        alt = f"_hc_alt_{name}"
        if alt in with_both.columns:
            with_both = with_both.withColumn(
                name, F.coalesce(F.col(name), F.col(alt))
            )
    drop_cols = ["_hc_alt_key", *[f"_hc_alt_{c}" for c in hc_attrs]]
    with_both = with_both.drop(*[c for c in drop_cols if c in with_both.columns])
    return with_both.withColumn(
        "HCDivisionINC",
        F.coalesce(F.col("HCDivisionINC"), F.lit(division_inc_default)),
    )


def _calendar_month_end_exclusive(yyyy_mm: str) -> str:
    year_s, month_s = yyyy_mm.split("-", 1)
    year, month = int(year_s), int(month_s)
    if month == 12:
        return f"{year + 1}-01-01"
    return f"{year}-{month + 1:02d}-01"


def purview_copilot_bronze_scan_predicate(
    *,
    creation_month: str | None = None,
    filenames: list[str] | None = None,
    min_creation_date: str | None = None,
    creation_col: str = "CreationDate",
    file_col: str = "FileName",
) -> str | None:
    """Optional DataFusion SQL for deltalake ``scan(predicate=...)`` (smaller OneLake read)."""
    clauses: list[str] = []
    if creation_month:
        start = f"{creation_month}-01"
        end = _calendar_month_end_exclusive(creation_month)
        clauses.append(f"{creation_col} >= '{start}' AND {creation_col} < '{end}'")
    elif min_creation_date:
        clauses.append(f"{creation_col} >= '{min_creation_date}'")
    if filenames:
        quoted = ", ".join("'" + name.replace("'", "''") + "'" for name in filenames)
        clauses.append(f"{file_col} IN ({quoted})")
    if not clauses:
        return None
    return " AND ".join(f"({part})" for part in clauses)


def build_purview_copilot_usage_silver(
    bronze: DataFrame,
    okta: DataFrame,
    hc: DataFrame,
    *,
    min_creation_date: str,
) -> DataFrame:
    """Bronze purview_copilot_usage → silver grain (one row per record_id) with Okta + HC."""
    audit = _bronze_column(bronze, "AuditData", "audit_data")
    record_id = _bronze_column(bronze, "RecordId", "record_id")
    creation = F.to_timestamp(F.trim(_bronze_column(bronze, "CreationDate", "creation_date")))
    creation_date = creation.cast("date")
    file_name = _bronze_column_or_null(bronze, "FileName", "file_name", "string")
    ingestion = _bronze_column_or_null(
        bronze, "IngestionDate", "ingestion_date", "timestamp"
    ).cast("timestamp")
    user_id = _bronze_column(bronze, "UserId", "user_id")

    msg_type = ArrayType(
        StructType(
            [
                StructField("isPrompt", BooleanType(), True),
                StructField("JailbreakDetected", BooleanType(), True),
            ]
        )
    )
    plugin_type = ArrayType(
        StructType([StructField("Id", StringType(), True)])
    )
    resource_type = ArrayType(StringType())

    audit_json = audit.cast("string")
    messages_json = F.get_json_object(audit_json, "$.CopilotEventData.Messages")
    plugins_json = F.get_json_object(audit_json, "$.CopilotEventData.AISystemPlugin")
    resources_json = F.get_json_object(audit_json, "$.CopilotEventData.AccessedResources")

    app_identity = F.get_json_object(audit_json, "$.AppIdentity")
    application_name = F.get_json_object(audit_json, "$.ApplicationName")
    agent_name = F.get_json_object(audit_json, "$.AgentName")

    base = (
        bronze.filter(creation_date >= F.lit(min_creation_date))
        .filter(is_valid_email(F.trim(user_id)))
        .select(
            record_id.alias("purview_copilot_usage_record_id"),
            creation_date.alias("purview_copilot_usage_date"),
            _bronze_column(bronze, "RecordType", "record_type").cast("int").alias(
                "purview_copilot_usage_record_type"
            ),
            _bronze_column(bronze, "Operation", "operation").alias(
                "purview_copilot_usage_operation"
            ),
            F.lower(F.trim(user_id)).alias("purview_copilot_usage_email"),
            _bronze_column(
                bronze, "AssociatedAdminUnits", "associated_admin_units"
            ).alias("purview_copilot_usage_associated_admin_units"),
            _bronze_column(
                bronze, "AssociatedAdminUnitsNames", "associated_admin_units_names"
            ).alias("purview_copilot_usage_associated_admin_units_names"),
            app_identity.alias("purview_copilot_usage_app_identity"),
            application_name.alias("purview_copilot_usage_application_name"),
            F.get_json_object(audit_json, "$.CopilotEventData.AppHost").alias(
                "purview_copilot_usage_app_host"
            ),
            agent_name.alias("purview_copilot_usage_agent_name"),
            F.get_json_object(audit_json, "$.CopilotEventData.TargetAgentName").alias(
                "purview_copilot_usage_target_agent_name"
            ),
            F.get_json_object(audit_json, "$.CopilotEventData.LicenseType").alias(
                "purview_copilot_usage_license_type"
            ),
            F.get_json_object(audit_json, "$.CopilotEventData.ConversationId").alias(
                "purview_copilot_usage_conversation_id"
            ),
            F.get_json_object(audit_json, "$.CopilotEventData.ThreadId").alias(
                "purview_copilot_usage_thread_id"
            ),
            F.get_json_object(
                audit_json, "$.CopilotEventData.ModelTransparencyDetails[0].ModelName"
            ).alias("purview_copilot_usage_model_name"),
            F.get_json_object(
                audit_json,
                "$.CopilotEventData.ModelTransparencyDetails[0].ModelProviderName",
            ).alias("purview_copilot_usage_model_provider_name"),
            F.get_json_object(audit_json, "$.CopilotLogVersion").alias(
                "purview_copilot_usage_copilot_log_version"
            ),
            file_name.alias("purview_copilot_usage_file_name"),
            ingestion.alias("purview_copilot_usage_ingestion_date"),
            messages_json.alias("_messages_json"),
            plugins_json.alias("_plugins_json"),
            resources_json.alias("_resources_json"),
        )
        .withColumn(
            "_messages",
            F.from_json(F.col("_messages_json"), msg_type),
        )
        .withColumn(
            "_plugins",
            F.from_json(F.col("_plugins_json"), plugin_type),
        )
        .withColumn(
            "_resources",
            F.from_json(F.col("_resources_json"), resource_type),
        )
        .withColumn(
            "purview_copilot_usage_message_count",
            F.when(F.col("_messages").isNull(), 0).otherwise(F.size(F.col("_messages"))),
        )
        .withColumn(
            "purview_copilot_usage_prompt_count",
            F.when(F.col("_messages").isNull(), 0).otherwise(
                F.expr("size(filter(_messages, x -> x.isPrompt))")
            ),
        )
        .withColumn(
            "purview_copilot_usage_jailbreak_detected",
            F.coalesce(
                F.expr("exists(_messages, x -> x.JailbreakDetected)"),
                F.lit(False),
            ),
        )
        .withColumn(
            "purview_copilot_usage_plugin_count",
            F.when(F.col("_plugins").isNull(), 0).otherwise(F.size(F.col("_plugins"))),
        )
        .withColumn(
            "purview_copilot_usage_uses_bing_web_search",
            F.coalesce(
                F.expr("exists(_plugins, x -> x.Id = 'BingWebSearch')"),
                F.lit(False),
            ),
        )
        .withColumn(
            "purview_copilot_usage_accessed_resource_count",
            F.when(F.col("_resources").isNull(), 0).otherwise(F.size(F.col("_resources"))),
        )
        .drop("_messages_json", "_plugins_json", "_resources_json", "_messages", "_plugins", "_resources")
    )

    base = base.dropDuplicates(["purview_copilot_usage_record_id"])

    base = base.withColumn(
        "purview_copilot_usage_has_prompt",
        F.col("purview_copilot_usage_prompt_count") > 0,
    ).withColumn(
        "purview_copilot_usage_interaction_type",
        purview_copilot_interaction_type(
            F.col("purview_copilot_usage_app_identity"),
            F.col("purview_copilot_usage_agent_name"),
        ),
    ).withColumn(
        "purview_copilot_usage_has_model_info",
        F.col("purview_copilot_usage_model_name").isNotNull()
        | F.col("purview_copilot_usage_model_provider_name").isNotNull(),
    ).withColumn(
        "purview_copilot_usage_has_resource_access",
        F.col("purview_copilot_usage_accessed_resource_count") > 0,
    ).withColumn(
        "purview_copilot_usage_month_no",
        (
            F.year("purview_copilot_usage_date") * F.lit(100)
            + F.month("purview_copilot_usage_date")
        ).cast("int"),
    )

    okta_slim = okta.select(
        "OktaUserId",
        "OktaUserEmail",
        "OktaUserFullEmail",
        "OktaUserEmployeeNumber",
        "OktaUserFullName",
        "OktaUserRegion",
        "OktaUserCountry",
    )
    joined = join_okta_on_user_email(base, okta_slim, "purview_copilot_usage_email")
    joined = joined.withColumn(
        "purview_copilot_usage_key_email",
        F.when(
            F.col("OktaUserEmail").isNotNull(),
            F.concat(
                F.col("purview_copilot_usage_month_no").cast("string"),
                F.lit("/"),
                F.col("OktaUserEmail"),
            ),
        ),
    ).withColumn(
        "purview_copilot_usage_key_full_email",
        F.when(
            F.col("OktaUserFullEmail").isNotNull(),
            F.concat(
                F.col("purview_copilot_usage_month_no").cast("string"),
                F.lit("/"),
                F.col("OktaUserFullEmail"),
            ),
        ),
    )

    hc_slim = hc.select(
        F.col("HCKeyEmail"),
        F.col("HCBusinessUnit"),
        F.col("HCBusinessUnitCode"),
        F.col("HCDivision"),
        F.col("HCJobTitle"),
        F.col("HCOffice"),
        F.col("HCTalentSegment"),
        F.col("Month_No").alias("HCMonthNo"),
        F.col("OktaUserCountry").alias("HCOktaUserCountry"),
        F.col("HCDivisionINC"),
    )
    joined = join_hc_on_email_keys(
        joined,
        hc_slim,
        "purview_copilot_usage_key_email",
        "purview_copilot_usage_key_full_email",
    )

    return joined.select(
        "purview_copilot_usage_record_id",
        "purview_copilot_usage_date",
        "purview_copilot_usage_record_type",
        "purview_copilot_usage_operation",
        "purview_copilot_usage_email",
        "purview_copilot_usage_associated_admin_units",
        "purview_copilot_usage_associated_admin_units_names",
        "purview_copilot_usage_app_identity",
        "purview_copilot_usage_application_name",
        "purview_copilot_usage_app_host",
        "purview_copilot_usage_agent_name",
        "purview_copilot_usage_target_agent_name",
        "purview_copilot_usage_license_type",
        "purview_copilot_usage_conversation_id",
        "purview_copilot_usage_thread_id",
        "purview_copilot_usage_model_name",
        "purview_copilot_usage_model_provider_name",
        "purview_copilot_usage_message_count",
        "purview_copilot_usage_prompt_count",
        "purview_copilot_usage_has_prompt",
        "purview_copilot_usage_jailbreak_detected",
        "purview_copilot_usage_plugin_count",
        "purview_copilot_usage_uses_bing_web_search",
        "purview_copilot_usage_accessed_resource_count",
        "purview_copilot_usage_interaction_type",
        "purview_copilot_usage_has_model_info",
        "purview_copilot_usage_has_resource_access",
        "purview_copilot_usage_copilot_log_version",
        "purview_copilot_usage_month_no",
        "purview_copilot_usage_file_name",
        "purview_copilot_usage_ingestion_date",
        F.col("OktaUserCountry").alias("okta_user_country"),
        F.col("OktaUserEmail").alias("okta_user_email"),
        F.col("OktaUserEmployeeNumber").alias("okta_user_employee_number"),
        F.col("OktaUserFullEmail").alias("okta_user_full_email"),
        F.col("OktaUserFullName").alias("okta_user_full_name"),
        F.col("OktaUserId").alias("okta_user_id"),
        F.col("OktaUserRegion").alias("okta_user_region"),
        "purview_copilot_usage_key_email",
        "purview_copilot_usage_key_full_email",
        F.col("HCBusinessUnit").alias("hc_business_unit"),
        F.col("HCBusinessUnitCode").alias("hc_business_unit_code"),
        F.col("HCDivision").alias("hc_division"),
        F.col("HCJobTitle").alias("hc_job_title"),
        F.col("HCOffice").alias("hc_office"),
        F.col("HCTalentSegment").alias("hc_talent_segment"),
        F.col("HCMonthNo").alias("hc_month_no"),
        F.col("HCOktaUserCountry").alias("hc_okta_user_country"),
        F.col("HCDivisionINC").alias("hc_division_inc"),
    )


def purview_copilot_use_case_code(interaction_type: Column) -> Column:
    """Map silver interaction type to fixed Active use-case codes."""
    return (
        F.when(interaction_type == "Web Chat", F.lit("UC-9007-COP-001"))
        .when(interaction_type == "Microsoft 365 App", F.lit("UC-9007-COP-002"))
        .when(interaction_type == "Built-in Agent", F.lit("UC-9007-COP-003"))
        .when(interaction_type == "Copilot Studio Agent", F.lit("UC-9007-COP-004"))
        .when(interaction_type == "Specialized Agent", F.lit("UC-9007-COP-005"))
        .when(interaction_type == "Power BI / Fabric", F.lit("UC-9007-COP-006"))
        .otherwise(F.lit("UC-9007-COP-007"))
    )


def purview_active_copilot_aggregate(silver: DataFrame) -> DataFrame:
    """Group by user, date, interaction type; Request = sum(prompts); keep groups with Request >= 1."""
    grouped = (
        silver.groupBy(
            "purview_copilot_usage_date",
            "okta_user_id",
            "purview_copilot_usage_interaction_type",
        )
        .agg(
            F.first("purview_copilot_usage_month_no", ignorenulls=True)
            .cast("int")
            .alias("Month_No"),
            F.sum("purview_copilot_usage_prompt_count").cast("int").alias("Request"),
            F.first("okta_user_full_name", ignorenulls=True).alias("okta_user_full_name"),
            F.first("okta_user_employee_number", ignorenulls=True).alias(
                "okta_user_employee_number"
            ),
            F.first("okta_user_email", ignorenulls=True).alias("okta_user_email"),
            F.first("okta_user_full_email", ignorenulls=True).alias("okta_user_full_email"),
            F.first("okta_user_region", ignorenulls=True).alias("okta_user_region"),
            F.first("okta_user_country", ignorenulls=True).alias("okta_user_country"),
            F.first("hc_business_unit", ignorenulls=True).alias("hc_business_unit"),
            F.first("hc_job_title", ignorenulls=True).alias("hc_job_title"),
            F.first("hc_okta_user_country", ignorenulls=True).alias("hc_okta_user_country"),
            F.first("hc_office", ignorenulls=True).alias("hc_office"),
            F.first("hc_talent_segment", ignorenulls=True).alias("hc_talent_segment"),
            F.first("hc_division", ignorenulls=True).alias("hc_division"),
            F.first("hc_business_unit_code", ignorenulls=True).alias("hc_business_unit_code"),
        )
        .filter(F.col("Request") >= 1)
    )
    return grouped


def purview_active_copilot_merge_key() -> Column:
    """Merge grain: one row per (activity date, Okta user, interaction type)."""
    return F.sha2(
        F.concat_ws(
            "|",
            F.coalesce(F.col("purview_copilot_usage_date").cast("string"), F.lit("")),
            F.coalesce(F.col("okta_user_id"), F.lit("")),
            F.coalesce(F.col("purview_copilot_usage_interaction_type"), F.lit("")),
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
    parts = [
        F.coalesce(F.col(c).cast("string"), F.lit("")) for c in key_cols
    ]
    return F.sha2(F.concat_ws("|", *parts), 256)
