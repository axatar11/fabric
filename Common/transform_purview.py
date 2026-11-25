"""Purview / M365 Copilot transforms."""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, BooleanType, StringType, StructField, StructType

from Common.config import HC_DIVISION_INC_DEFAULT
from Common.transform_bronze import _bronze_column, _bronze_column_or_null
from Common.transform_cursor import (
    exclude_month_nos,
    filter_through_close_month_ts,
    filter_to_month_no,
    is_valid_email,
)


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


def purview_copilot_email_fallback(email_col: Column) -> Column:
    """Alternate email when local part starts with ``10`` (SQL ``SUBSTRING(email, 3, LEN(email))``)."""
    # PySpark substring() requires a constant length; 512 covers any email remainder from pos 3.
    return F.when(
        email_col.rlike(r"^10[^@]*@"),
        F.substring(email_col, 3, 512),
    )


def split_purview_okta_join(
    base: DataFrame,
    okta_slim: DataFrame,
    user_email_col: str,
    fallback_email_col: str,
) -> tuple[DataFrame, DataFrame]:
    """Return (Okta-matched rows, base rows with no Okta match after fallback)."""
    joined = join_okta_purview_with_fallback(
        base, okta_slim, user_email_col, fallback_email_col
    )
    matched = joined.filter(F.col("OktaUserId").isNotNull())
    rejects = joined.filter(F.col("OktaUserId").isNull()).select(*base.columns)
    return matched, rejects


def purview_copilot_okta_rejects_output(rejects: DataFrame) -> DataFrame:
    """Annotate Purview bronze-grain rejects for the silver reject table."""
    return (
        rejects.withColumn("okta_reject_reason", F.lit("okta_join_miss"))
        .withColumn("okta_reject_loaded_at", F.current_timestamp())
    )


def join_okta_purview_with_fallback(
    base: DataFrame,
    okta_slim: DataFrame,
    user_email_col: str,
    fallback_email_col: str,
) -> DataFrame:
    """Left join Okta: primary email (short then full), else fallback email — equi-joins only."""
    user = F.col(user_email_col)
    fallback = F.col(fallback_email_col)
    okta_cols = [
        "OktaUserId",
        "OktaUserEmail",
        "OktaUserFullEmail",
        "OktaUserEmployeeNumber",
        "OktaUserFullName",
        "OktaUserRegion",
        "OktaUserCountry",
    ]

    def _coalesce_prefixed(df: DataFrame, prefixes: list[str]) -> DataFrame:
        out = df
        for name in okta_cols:
            out = out.withColumn(name, F.coalesce(*[F.col(f"{p}{name}") for p in prefixes]))
        drop = [f"{p}{c}" for p in prefixes for c in okta_cols]
        return out.drop(*[c for c in drop if c in out.columns])

    p1 = okta_slim.select(*[F.col(c).alias(f"_p1_{c}") for c in okta_cols])
    p2 = okta_slim.select(*[F.col(c).alias(f"_p2_{c}") for c in okta_cols])
    primary = base.join(p1, user == F.col("_p1_OktaUserEmail"), "left")
    primary = primary.join(p2, user == F.col("_p2_OktaUserFullEmail"), "left")
    primary = _coalesce_prefixed(primary, ["_p1_", "_p2_"])

    f1 = okta_slim.select(*[F.col(c).alias(f"_f1_{c}") for c in okta_cols])
    f2 = okta_slim.select(*[F.col(c).alias(f"_f2_{c}") for c in okta_cols])
    with_fb = primary.join(f1, fallback == F.col("_f1_OktaUserEmail"), "left")
    with_fb = with_fb.join(f2, fallback == F.col("_f2_OktaUserFullEmail"), "left")
    out = with_fb
    for name in okta_cols:
        # Primary match fills `name`; fallback-only must not keep null emails when OktaUserId
        # was already set from a prior column in this loop (when/when bug on fallback path).
        out = out.withColumn(
            name,
            F.coalesce(F.col(name), F.col(f"_f1_{name}"), F.col(f"_f2_{name}")),
        )
    drop_fb = [f"_f1_{c}" for c in okta_cols] + [f"_f2_{c}" for c in okta_cols]
    return out.drop(*[c for c in drop_fb if c in out.columns])


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


def copilot_bronze_scan_predicate(
    close_month_no: int | None = None,
    *,
    filenames: list[str] | None = None,
) -> str | None:
    """OneLake scan filter for Purview bronze (month-close or daily min CreationDate)."""
    from Common import config
    import os

    if close_month_no is not None:
        year, month = divmod(int(close_month_no), 100)
        return purview_copilot_bronze_scan_predicate(
            creation_month=f"{year}-{month:02d}",
            filenames=filenames,
        )
    return purview_copilot_bronze_scan_predicate(
        min_creation_date=os.environ.get(
            "MEDALLION_PURVIEW_MIN_CREATION_DATE",
            config.PURVIEW_COPILOT_MIN_CREATION_DATE,
        ),
        filenames=filenames,
    )


def purview_copilot_bronze_scan_predicate(
    *,
    creation_month: str | None = None,
    filenames: list[str] | None = None,
    min_creation_date: str | None = None,
    creation_col: str | None = None,
    file_col: str = "FileName",
) -> str | None:
    """Optional DataFusion SQL for deltalake ``scan(predicate=...)`` on **CreationDate** (+ FileName)."""
    from Common import config

    if creation_col is None:
        creation_col = config.PURVIEW_BRONZE_FILTER_DATE_COLUMN
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


def parse_purview_copilot_bronze_base(
    bronze: DataFrame,
    *,
    min_creation_date: str,
    close_month_no: int | None = None,
    frozen_month_nos: list[int] | None = None,
) -> DataFrame:
    """Bronze → parsed row shape (AuditData JSON expanded; no Okta/HC joins)."""
    audit = _bronze_column(bronze, "AuditData", "audit_data")
    record_id = _bronze_column(bronze, "RecordId", "record_id")
    creation = F.to_timestamp(F.trim(_bronze_column(bronze, "CreationDate", "creation_date")))
    creation_date = creation.cast("date")
    file_name = _bronze_column_or_null(bronze, "FileName", "file_name", "string")
    ingestion = _bronze_column_or_null(
        bronze, "ExecutionDate", "execution_date", "IngestionDate", "ingestion_date", "timestamp"
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
        "purview_copilot_usage_email_fallback",
        purview_copilot_email_fallback(F.col("purview_copilot_usage_email")),
    )

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

    base = filter_to_month_no(base, "purview_copilot_usage_month_no", close_month_no)
    base = filter_through_close_month_ts(
        base, "purview_copilot_usage_date", close_month_no
    )
    if frozen_month_nos:
        base = exclude_month_nos(
            base, "purview_copilot_usage_month_no", frozen_month_nos
        )
    return base


def join_purview_copilot_to_silver(
    base: DataFrame,
    okta: DataFrame,
    hc: DataFrame,
    *,
    close_month_no: int | None = None,
) -> tuple[DataFrame, DataFrame]:
    """Parsed Purview base + Okta/HC silver → (silver, Okta rejects)."""
    okta_slim = okta.select(
        F.col("OktaUserId"),
        F.lower(F.trim(F.col("OktaUserEmail"))).alias("OktaUserEmail"),
        F.lower(F.trim(F.col("OktaUserFullEmail"))).alias("OktaUserFullEmail"),
        F.col("OktaUserEmployeeNumber"),
        F.col("OktaUserFullName"),
        F.col("OktaUserRegion"),
        F.col("OktaUserCountry"),
    )
    joined, okta_rejects = split_purview_okta_join(
        base,
        okta_slim,
        "purview_copilot_usage_email",
        "purview_copilot_usage_email_fallback",
    )
    joined = joined.dropDuplicates(["purview_copilot_usage_record_id"])
    okta_rejects = purview_copilot_okta_rejects_output(
        okta_rejects.dropDuplicates(["purview_copilot_usage_record_id"])
    )
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
    hc_slim = filter_to_month_no(hc_slim, "HCMonthNo", close_month_no)
    joined = join_hc_on_email_keys(
        joined,
        hc_slim,
        "purview_copilot_usage_key_email",
        "purview_copilot_usage_key_full_email",
    )

    silver = joined.select(
        "purview_copilot_usage_record_id",
        "purview_copilot_usage_date",
        "purview_copilot_usage_record_type",
        "purview_copilot_usage_operation",
        "purview_copilot_usage_email",
        "purview_copilot_usage_email_fallback",
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
    return silver, okta_rejects


def build_purview_copilot_usage_silver(
    bronze: DataFrame,
    okta: DataFrame,
    hc: DataFrame,
    *,
    min_creation_date: str,
    close_month_no: int | None = None,
    frozen_month_nos: list[int] | None = None,
) -> tuple[DataFrame, DataFrame]:
    """Bronze purview_copilot_usage → (silver with Okta match, Okta rejects)."""
    base = parse_purview_copilot_bronze_base(
        bronze,
        min_creation_date=min_creation_date,
        close_month_no=close_month_no,
        frozen_month_nos=frozen_month_nos,
    )
    return join_purview_copilot_to_silver(
        base, okta, hc, close_month_no=close_month_no
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


def purview_copilot_automation_poc(interaction_type: Column) -> Column:
    """``fact_copilot_active.automation_poc`` by interaction type (business POC table).

    Silver label is ``Power BI / Fabric`` (slash), from ``purview_copilot_interaction_type`` — not hyphenated.
    """
    power_bi = interaction_type == "Power BI / Fabric"
    others = interaction_type.isin("Other", "Others")
    agents = interaction_type.isin(
        "Built-in Agent", "Copilot Studio Agent", "Specialized Agent"
    )
    return (
        F.when(interaction_type == "Web Chat", F.lit("0.70"))
        .when(interaction_type == "Microsoft 365 App", F.lit("0.617"))
        .when(agents, F.lit("1.00"))
        .when(power_bi, F.lit("0.70"))
        .when(others, F.lit("0.617"))
        .otherwise(F.lit(None))
    ).cast("decimal(18,8)")


def purview_copilot_total_hours_saved_poc(interaction_type: Column) -> Column:
    """``fact_copilot_active.totalHoursSaved_poc`` by interaction type (``Power BI / Fabric`` with slash)."""
    power_bi = interaction_type == "Power BI / Fabric"
    others = interaction_type.isin("Other", "Others")
    agents = interaction_type.isin(
        "Built-in Agent", "Copilot Studio Agent", "Specialized Agent"
    )
    return (
        F.when(interaction_type == "Web Chat", F.lit("0.066667"))
        .when(interaction_type == "Microsoft 365 App", F.lit("0.79"))
        .when(agents, F.lit("1.14"))
        .when(power_bi, F.lit("2.43"))
        .when(others, F.lit("1.69"))
        .otherwise(F.lit(None))
    ).cast("decimal(18,8)")


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


def purview_copilot_onboard_daily_dedupe(silver: DataFrame) -> DataFrame:
    """One row per ``(purview_copilot_usage_date, okta_user_id)`` — latest ingestion, then record id."""
    from pyspark.sql.window import Window

    w = Window.partitionBy("purview_copilot_usage_date", "okta_user_id").orderBy(
        F.col("purview_copilot_usage_ingestion_date").desc_nulls_last(),
        F.col("purview_copilot_usage_record_id").desc_nulls_last(),
    )
    return (
        silver.withColumn("_onboard_rn", F.row_number().over(w))
        .filter(F.col("_onboard_rn") == 1)
        .drop("_onboard_rn")
    )


def purview_copilot_onboard_merge_key() -> Column:
    """SHA2 surrogate key: activity date ``|`` Okta user id."""
    return F.sha2(
        F.concat_ws(
            "|",
            F.coalesce(F.col("purview_copilot_usage_date").cast("string"), F.lit("")),
            F.coalesce(F.col("okta_user_id"), F.lit("")),
        ),
        256,
    )



