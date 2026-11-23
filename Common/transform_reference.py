"""Reporting reference tables (Bronze → Silver)."""

from __future__ import annotations

from pyspark.sql import DataFrame

from Common.transform_bronze import (
    _bronze_column,
    _bronze_timestamp,
    _dedupe_by_columns,
    _trim_bronze_decimal,
    _trim_bronze_string,
)


def build_area_normalized_silver(bronze: DataFrame) -> DataFrame:
    return bronze.select(
        _trim_bronze_string(bronze, "AreaUsers", "areausers").alias(
            "area_normalized_area_users"
        ),
        _trim_bronze_string(bronze, "AreaCore", "areacore").alias("area_normalized_area_core"),
        _trim_bronze_string(bronze, "Division_INC", "division_inc").alias(
            "area_normalized_division_inc"
        ),
    )


def build_tcv_emails_silver(bronze: DataFrame) -> DataFrame:
    return bronze.select(
        _trim_bronze_string(bronze, "opco", "op_co").alias("tcv_emails_op_co"),
        _trim_bronze_string(bronze, "opportunity_owner").alias("tcv_emails_opportunity_owner"),
        _trim_bronze_string(bronze, "owner_email").alias("tcv_emails_owner_email"),
    )


def build_tcv_countries_region_silver(bronze: DataFrame) -> DataFrame:
    return bronze.select(
        _trim_bronze_string(bronze, "country").alias("tcv_countries_region_country"),
        _trim_bronze_string(bronze, "display_name").alias("tcv_countries_region_display_name"),
        _trim_bronze_string(bronze, "regioninc", "regionINC").alias("tcv_countries_region_regioninc"),
    )


def build_tcv_stage_normalized_silver(bronze: DataFrame) -> DataFrame:
    return bronze.select(
        _trim_bronze_string(bronze, "Stage", "stage", "normalized_stage").alias(
            "tcv_stage_normalized_stage"
        ),
        _trim_bronze_string(
            bronze, "Stage Normalized", "stage_Normalized", "stage_normalized"
        ).alias("tcv_stage_normalized_stage_Normalized"),
    )


def build_axet_business_unit_silver(bronze: DataFrame) -> DataFrame:
    oneerp = F.trim(_bronze_column(bronze, "oneerp_code").cast("string"))
    return bronze.select(
        _trim_bronze_string(bronze, "Business Unit", "business_unit", "business unit").alias(
            "axet_business_unit_business_unit"
        ),
        oneerp.alias("axet_business_unit_oneerp_code"),
        _bronze_timestamp(bronze, "execution_date").alias("axet_business_unit_execution_date"),
    )


def build_country_average_hours_silver(bronze: DataFrame) -> DataFrame:
    """Source headers per scope doc: ``Country Normalized``, ``Average month hours``, etc."""
    country_norm = _trim_bronze_string(bronze, "Country Normalized")
    deduped = _dedupe_by_columns(bronze, country_norm)
    country_out = _trim_bronze_string(deduped, "Country Normalized")
    return deduped.select(
        country_out.alias("country_average_hours_country_normalized"),
        _trim_bronze_decimal(deduped, "Average month hours").alias(
            "country_average_hours_average_month_hours"
        ),
        _trim_bronze_decimal(deduped, "Average weekly hours").alias(
            "country_average_hours_average_weekly_hours"
        ),
        _bronze_timestamp(deduped, "execution_date").alias("country_average_hours_execution_date"),
    )


def build_normalization_countries_okta_silver(bronze: DataFrame) -> DataFrame:
    country = _trim_bronze_string(bronze, "Country", "country")
    deduped = _dedupe_by_columns(bronze, country)
    country_out = _trim_bronze_string(deduped, "Country", "country")
    return deduped.select(
        country_out.alias("normalization_countries_okta_country"),
        _trim_bronze_string(deduped, "Normalization", "normalization").alias(
            "normalization_countries_okta_normalization"
        ),
        _bronze_timestamp(deduped, "execution_date").alias(
            "normalization_countries_okta_execution_date"
        ),
    )


def build_normalization_countries_core_silver(bronze: DataFrame) -> DataFrame:
    return bronze.select(
        _trim_bronze_string(bronze, "id").alias("normalization_countries_core_id"),
        _trim_bronze_string(bronze, "display_name").alias(
            "normalization_countries_core_display_name"
        ),
        _trim_bronze_string(bronze, "Normalized country", "normalized_country").alias(
            "normalization_countries_core_normalized_country"
        ),
        _bronze_timestamp(bronze, "execution_date").alias(
            "normalization_countries_core_execution_date"
        ),
    )


def build_prl_office_locations_silver(bronze: DataFrame) -> DataFrame:
    return bronze.select(
        _trim_bronze_string(bronze, "Nombre", "nombre", "name").alias("prl_office_locations_name"),
        _trim_bronze_string(bronze, "oneerp_code").alias("prl_office_locations_oneerp_code"),
        _trim_bronze_string(bronze, "Country", "country").alias("prl_office_locations_country"),
        _trim_bronze_string(bronze, "Division", "division").alias("prl_office_locations_division"),
        _bronze_timestamp(bronze, "execution_date").alias("prl_office_locations_execution_date"),
    )


def iter_reference_silver_pipelines():
    """Bronze FQN, Silver FQN, transform, short label — used by ``NB_ReferenceTables_BronzeToSilver``."""
    from Common import config

    return (
        (
            config.TABLE_AREA_NORMALIZED_BRONZE,
            config.TABLE_AREA_NORMALIZED_SILVER,
            build_area_normalized_silver,
            "area_normalized",
        ),
        (
            config.TABLE_TCV_EMAILS_BRONZE,
            config.TABLE_TCV_EMAILS_SILVER,
            build_tcv_emails_silver,
            "tcv_emails",
        ),
        (
            config.TABLE_TCV_COUNTRIES_REGION_BRONZE,
            config.TABLE_TCV_COUNTRIES_REGION_SILVER,
            build_tcv_countries_region_silver,
            "tcv_countries_region",
        ),
        (
            config.TABLE_TCV_STAGE_NORMALIZED_BRONZE,
            config.TABLE_TCV_STAGE_NORMALIZED_SILVER,
            build_tcv_stage_normalized_silver,
            "tcv_stage_normalized",
        ),
        (
            config.TABLE_AXET_BUSINESS_UNIT_BRONZE,
            config.TABLE_AXET_BUSINESS_UNIT_SILVER,
            build_axet_business_unit_silver,
            "axet_business_unit",
        ),
        (
            config.TABLE_COUNTRY_AVERAGE_HOURS_BRONZE,
            config.TABLE_COUNTRY_AVERAGE_HOURS_SILVER,
            build_country_average_hours_silver,
            "country_average_hours",
        ),
        (
            config.TABLE_NORMALIZATION_COUNTRIES_OKTA_BRONZE,
            config.TABLE_NORMALIZATION_COUNTRIES_OKTA_SILVER,
            build_normalization_countries_okta_silver,
            "normalization_countries_okta",
        ),
        (
            config.TABLE_NORMALIZATION_COUNTRIES_CORE_BRONZE,
            config.TABLE_NORMALIZATION_COUNTRIES_CORE_SILVER,
            build_normalization_countries_core_silver,
            "normalization_countries_core",
        ),
        (
            config.TABLE_PRL_OFFICE_LOCATIONS_BRONZE,
            config.TABLE_PRL_OFFICE_LOCATIONS_SILVER,
            build_prl_office_locations_silver,
            "prl_office_locations",
        ),
    )
