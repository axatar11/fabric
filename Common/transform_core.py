"""Core reporting entities (Bronze → Silver)."""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from Common.config import NO_COUNTRY
from Common.transform_bronze import (
    _bronze_column,
    _bronze_timestamp,
    _dedupe_by_columns,
    _trim_bronze_string,
)


def _nonempty_trim(col: Column) -> Column:
    return col.isNotNull() & (F.trim(col.cast("string")) != "")


def _trim_lower_bronze(df: DataFrame, *names: str) -> Column:
    return F.lower(F.trim(_bronze_column(df, *names).cast("string")))


def _bronze_boolean(df: DataFrame, *names: str) -> Column:
    return _bronze_column(df, *names).cast("boolean")


def build_core_asset_silver(bronze: DataFrame) -> DataFrame:
    asset_id = _trim_bronze_string(bronze, "id")
    out = bronze.select(
        asset_id.alias("asset_id"),
        _trim_bronze_string(bronze, "display_name").alias("asset_display_name"),
        _trim_bronze_string(bronze, "asset_code").alias("asset_asset_code"),
        _bronze_timestamp(bronze, "execution_date").alias("asset_execution_date"),
    )
    return out.filter(_nonempty_trim(F.col("asset_id")))


def build_core_country_silver(
    bronze: DataFrame, normalization_countries_core: DataFrame
) -> DataFrame:
    country_id = _trim_bronze_string(bronze, "id")
    norm = normalization_countries_core.select(
        F.trim(F.col("normalization_countries_core_id").cast("string")).alias("_core_id"),
        F.trim(F.col("normalization_countries_core_normalized_country").cast("string")).alias(
            "_normalized"
        ),
    )
    joined = bronze.join(norm, country_id == F.col("_core_id"), "left")
    cid = _trim_bronze_string(joined, "id")
    out = joined.select(
        cid.alias("country_id"),
        F.col("_normalized").alias("country_display_name"),
        _bronze_timestamp(joined, "execution_date").alias("country_execution_date"),
    )
    out = out.filter(_nonempty_trim(F.col("country_id")))
    return _dedupe_by_columns(out, F.col("country_id"))


def build_core_billing_silver(bronze: DataFrame) -> DataFrame:
    billing_id = _trim_bronze_string(bronze, "id")
    out = bronze.select(
        billing_id.alias("core_billing_id"),
        _trim_bronze_string(bronze, "billing_type").alias("core_billing_billing_type"),
        _trim_bronze_string(bronze, "billing_identifier").alias(
            "core_billing_billing_identifier"
        ),
        _bronze_timestamp(bronze, "created_at").alias("core_billing_created_at"),
        _bronze_timestamp(bronze, "updated_at").alias("core_billing_updated_at"),
        _bronze_timestamp(bronze, "deleted_at").alias("core_billing_deleted_at"),
        _trim_bronze_string(bronze, "client_name").alias("core_billing_client_name"),
        _trim_bronze_string(bronze, "country_od", "country_id").alias("core_billing_country_id"),
        _trim_bronze_string(bronze, "tenant").alias("core_billing_tenant"),
        _trim_bronze_string(bronze, "region").alias("core_billing_region"),
        _bronze_timestamp(bronze, "execution_date").alias("core_billing_execution_date"),
    )
    return out.filter(_nonempty_trim(F.col("core_billing_id")))


def build_core_project_silver(bronze: DataFrame) -> DataFrame:
    project_id = _trim_bronze_string(bronze, "id")
    billing_id = _trim_bronze_string(bronze, "billing_id")
    out = bronze.select(
        project_id.alias("core_project_id"),
        _trim_bronze_string(bronze, "display_name").alias("core_project_display_name"),
        _bronze_boolean(bronze, "active").alias("core_project_active"),
        _bronze_timestamp(bronze, "create_at", "created_at").alias("core_project_create_at"),
        _bronze_timestamp(bronze, "updated_at").alias("core_project_updated_at"),
        _bronze_timestamp(bronze, "deleted_at").alias("core_project_deleted_at"),
        billing_id.alias("core_project_billing_id"),
        _bronze_boolean(bronze, "corporate").alias("core_project_corporate"),
        _trim_bronze_string(bronze, "tenant").alias("core_project_tenant"),
        _bronze_boolean(bronze, "is_public").alias("core_project_is_public"),
        _bronze_timestamp(bronze, "execution_date").alias("core_project_execution_date"),
    )
    return out.filter(
        _nonempty_trim(F.col("core_project_id")) & _nonempty_trim(F.col("core_project_billing_id"))
    )


def build_core_okta_user_silver(
    bronze: DataFrame, normalization_countries_okta: DataFrame
) -> DataFrame:
    norm = normalization_countries_okta.select(
        F.trim(F.col("normalization_countries_okta_country").cast("string")).alias("_src"),
        F.trim(F.col("normalization_countries_okta_normalization").cast("string")).alias(
            "_norm"
        ),
    )
    raw_country = _trim_bronze_string(bronze, "country")
    joined = bronze.join(norm, raw_country == F.col("_src"), "left")
    out = joined.select(
        _trim_bronze_string(joined, "id").alias("okta_user_id"),
        _trim_bronze_string(joined, "display_name").alias("okta_user_full_name"),
        _trim_bronze_string(joined, "first_name").alias("okta_user_first_name"),
        _trim_bronze_string(joined, "last_name").alias("okta_user_last_name"),
        _trim_lower_bronze(joined, "login").alias("okta_user_email"),
        _trim_lower_bronze(joined, "email").alias("okta_user_full_email"),
        _trim_bronze_string(joined, "employee_number").alias("okta_user_employee_number"),
        _trim_bronze_string(joined, "region").alias("okta_user_region"),
        _trim_bronze_string(joined, "okta_id").alias("okta_user_okta_id"),
        _bronze_timestamp(joined, "updated_at").alias("okta_user_updated_at"),
        _bronze_timestamp(joined, "deleted_at").alias("okta_user_deleted_at"),
        F.coalesce(F.col("_norm"), F.lit(NO_COUNTRY)).alias("okta_user_country"),
        _trim_bronze_string(joined, "okta_tenant").alias("okta_user_tenant"),
        _bronze_timestamp(joined, "execution_date").alias("okta_user_execution_date"),
    )
    return out.filter(
        _nonempty_trim(F.col("okta_user_id"))
        & (F.lower(F.trim(F.col("okta_user_region").cast("string"))) != F.lit("japan-onentt"))
        & (F.lower(F.trim(F.col("okta_user_tenant").cast("string"))) != F.lit("connect-onentt"))
    )


def build_dim_project_silver(
    core_project: DataFrame,
    core_billing: DataFrame,
    core_country: DataFrame,
) -> DataFrame:
    b = core_billing.select(
        F.col("core_billing_id"),
        F.col("core_billing_billing_type"),
        F.col("core_billing_billing_identifier"),
        F.col("core_billing_client_name"),
        F.col("core_billing_tenant"),
        F.col("core_billing_region"),
        F.col("core_billing_country_id"),
    )
    c = core_country.select(
        F.col("country_id"),
        F.col("country_display_name"),
    )
    p = core_project
    joined = (
        p.join(b, p.core_project_billing_id == b.core_billing_id, "left")
        .join(c, b.core_billing_country_id == c.country_id, "left")
    )
    return joined.select(
        p.core_project_id.alias("dim_project_project_id"),
        p.core_project_display_name.alias("dim_project_project_display_name"),
        p.core_project_active.alias("dim_project_active"),
        p.core_project_create_at.alias("dim_project_created_at"),
        p.core_project_updated_at.alias("dim_project_updated_at"),
        p.core_project_deleted_at.alias("dim_project_deleted_at"),
        p.core_project_billing_id.alias("dim_project_billing_id"),
        p.core_project_corporate.alias("dim_project_corporate"),
        p.core_project_tenant.alias("dim_project_tenant"),
        p.core_project_is_public.alias("dim_project_is_public"),
        b.core_billing_billing_type.alias("dim_project_billing_type"),
        b.core_billing_billing_identifier.alias("dim_project_billing_identifier"),
        b.core_billing_client_name.alias("dim_project_client_name"),
        b.core_billing_tenant.alias("dim_project_billing_tenant"),
        b.core_billing_region.alias("dim_project_billing_region"),
        c.country_id.alias("dim_project_country_id"),
        c.country_display_name.alias("dim_project_country"),
    )


def build_core_use_case_implementation_silver(bronze: DataFrame) -> DataFrame:
    use_case = _trim_bronze_string(bronze, "use_case")
    out = bronze.select(
        use_case.alias("core_use_case_implementation_use_case"),
        _trim_bronze_string(bronze, "display_name").alias(
            "core_use_case_implementation_display_name"
        ),
        _trim_bronze_string(bronze, "description").alias(
            "core_use_case_implementation_description"
        ),
        _trim_bronze_string(bronze, "hours_saved").alias(
            "core_use_case_implementation_hours_saved"
        ),
        _trim_bronze_string(bronze, "automation").alias(
            "core_use_case_implementation_automation"
        ),
        _bronze_boolean(bronze, "is_ai").alias("core_use_case_implementation_is_ai"),
        _bronze_boolean(bronze, "enabled").alias("core_use_case_implementation_enabled"),
        _bronze_timestamp(bronze, "deleted_at").alias("core_use_case_implementation_deleted_at"),
        _trim_bronze_string(bronze, "month_year").alias("core_use_case_implementation_month_year"),
        _bronze_timestamp(bronze, "execution_date").alias(
            "core_use_case_implementation_execution_date"
        ),
    )
    return out.filter(_nonempty_trim(F.col("core_use_case_implementation_use_case")))


def build_core_use_case_adjustment_silver(bronze: DataFrame) -> DataFrame:
    adj_id = _trim_bronze_string(bronze, "id")
    out = bronze.select(
        adj_id.alias("core_use_case_adjustment_id"),
        _bronze_timestamp(bronze, "start_date").alias("core_use_case_adjustment_start_date"),
        _bronze_timestamp(bronze, "end_date").alias("core_use_case_adjustment_end_date"),
        _trim_bronze_string(bronze, "use_case").alias("core_use_case_adjustment_use_case"),
        _trim_bronze_string(bronze, "hours_saved").alias("core_use_case_adjustment_hours_saved"),
        _trim_bronze_string(bronze, "automation").alias("core_use_case_adjustment_automation"),
        _trim_bronze_string(bronze, "modified_by").alias("core_use_case_adjustment_modified_by"),
        _trim_bronze_string(bronze, "month_year").alias("core_use_case_adjustment_month_year"),
        _bronze_timestamp(bronze, "execution_date").alias("core_use_case_adjustment_execution_date"),
    )
    return out.filter(_nonempty_trim(F.col("core_use_case_adjustment_id")))