"""Notebook entry: `%run ./common/bootstrap` — Spark session + pipeline helpers."""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any

_REPO = Path(__file__).resolve().parent.parent
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from common import config, io, transforms


def _local_settings() -> dict[str, Any]:
    out: dict[str, Any] = {}
    try:
        from common import local_settings as ls  # type: ignore

        out["create_spark"] = getattr(ls, "create_spark", None)
        out["spark_extra_config"] = getattr(ls, "SPARK_EXTRA_CONFIG", {})
        out["table_path_overrides"] = getattr(ls, "ONELAKE_TABLE_OVERRIDES", {})
    except ImportError:
        pass
    return out


def _runtime() -> str:
    profile = os.environ.get("MEDALLION_SPARK_PROFILE", "auto").lower()
    if profile in ("fabric", "databricks", "local"):
        return profile
    if os.environ.get("DATABRICKS_RUNTIME_VERSION"):
        return "databricks"
    try:
        import notebookutils  # noqa: F401

        return "fabric"
    except ImportError:
        pass
    return "local"


def _maven_packages() -> str | None:
    """Only when not using SPARK_HOME jars — mismatched Delta/Scala causes GenTraversableOnce errors."""
    if os.environ.get("SPARK_HOME") and os.environ.get("MEDALLION_FORCE_MAVEN_PACKAGES") != "1":
        return None
    override = os.environ.get("MEDALLION_SPARK_PACKAGES")
    if override:
        return override
    import pyspark

    ver = pyspark.__version__.split(".")
    major = int(ver[0])
    if major >= 4:
        delta = os.environ.get("MEDALLION_DELTA_PACKAGES", "io.delta:delta-spark_2.13:4.0.0")
    else:
        delta = os.environ.get("MEDALLION_DELTA_PACKAGES", "io.delta:delta-spark_2.12:3.2.0")
    azure = os.environ.get(
        "MEDALLION_AZURE_PACKAGES",
        "org.apache.hadoop:hadoop-azure:3.3.6,com.azure:azure-storage-blob:12.25.1",
    )
    return f"{delta},{azure}"


def _configure_onelake(builder):
    host = config.ONELAKE_HOST
    tenant = os.environ.get("FABRIC_TENANT_ID", "")
    client_id = os.environ.get("FABRIC_CLIENT_ID", "")
    client_secret = os.environ.get("FABRIC_CLIENT_SECRET", "")
    p = "fs.azure.account"
    return (
        builder.config(f"{p}.auth.type.{host}", "OAuth")
        .config(
            f"{p}.oauth.provider.type.{host}",
            "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
        )
        .config(f"{p}.oauth2.client.id.{host}", client_id)
        .config(f"{p}.oauth2.client.secret.{host}", client_secret)
        .config(
            f"{p}.oauth2.client.endpoint.{host}",
            f"https://login.microsoftonline.com/{tenant}/oauth2/token",
        )
    )


def _build_spark(app_name: str):
    from pyspark.sql import SparkSession

    builder = SparkSession.builder.appName(app_name)
    packages = _maven_packages()
    if packages:
        builder = (
            builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .config("spark.jars.packages", packages)
        )
    builder = _configure_onelake(builder)
    for k, v in _local_settings().get("spark_extra_config", {}).items():
        builder = builder.config(k, v)
    return builder.getOrCreate()


def _get_spark(notebook_globals: dict[str, Any], app_name: str):
    existing = notebook_globals.get("spark")
    if existing is not None:
        try:
            existing.sparkContext
            return existing, {"runtime": _runtime(), "mode": "attached"}
        except Exception:
            pass
    settings = _local_settings()
    create = settings.get("create_spark")
    if callable(create):
        return create(app_name), {"runtime": "local", "mode": "custom"}
    rt = _runtime()
    if rt in ("fabric", "databricks"):
        from pyspark.sql import SparkSession

        return SparkSession.builder.getOrCreate(), {"runtime": rt, "mode": "getOrCreate"}
    return _build_spark(app_name), {"runtime": "local", "mode": "created"}


def _register_tables(spark) -> None:
    if os.environ.get("MEDALLION_REGISTER_TABLES") != "1":
        return
    overrides = _local_settings().get("table_path_overrides") or {}

    def root(lakehouse_id: str) -> str:
        return (
            f"abfss://{config.WORKSPACEID}@{config.ONELAKE_HOST}/"
            f"{lakehouse_id}.Lakehouse/Tables"
        )

    paths = {
        config.TABLE_CURSOR_BRONZE: f"{root(config.LAKEHOUSE_AI_MEDALLION_ID)}/Bronze/cursor_usage",
        config.TABLE_HC_BRONZE: f"{root(config.LAKEHOUSE_AI_MEDALLION_BRONZE_ID)}/dbo/HC_Bronze_Historical",
        config.TABLE_OKTA_BRONZE: f"{root(config.LAKEHOUSE_AI_MEDALLION_BRONZE_ID)}/dbo/CoreOktaUser",
        config.TABLE_COUNTRY: f"{root(config.LAKEHOUSE_AI_MEDALLION_SILVER_ID)}/dbo/CoreCountry_Excel_Hours_Historic",
        config.TABLE_HC_SILVER: f"{root(config.LAKEHOUSE_AI_MEDALLION_ID)}/Silver/HC_Silver_Historical",
        config.TABLE_OKTA_SILVER: f"{root(config.LAKEHOUSE_AI_MEDALLION_ID)}/Silver/Okta_User_for_AI",
        config.TABLE_CURSOR_SILVER: f"{root(config.LAKEHOUSE_AI_MEDALLION_ID)}/Silver/cursor_usage",
        config.TABLE_FACT_CURSOR_ACTIVE: f"{root(config.LAKEHOUSE_AI_MEDALLION_ID)}/Gold/Fact_Cursor_Active",
    }
    paths.update(overrides)
    for fqn, abfss in paths.items():
        try:
            if spark.catalog.tableExists(fqn):
                continue
            spark.sql(f"CREATE TABLE IF NOT EXISTS {fqn} USING DELTA LOCATION '{abfss}'")
        except Exception as exc:
            print(f"Skip register {fqn}: {exc}")


def show_sample(df, n: int = 10) -> None:
    try:
        display(df.limit(n))  # noqa: F821
    except NameError:
        df.limit(n).show(truncate=False)


def init_notebook(notebook_globals: dict[str, Any], app_name: str = "Medallion") -> None:
    from pyspark.sql import functions as F

    spark, info = _get_spark(notebook_globals, app_name)
    _register_tables(spark)
    notebook_globals.update(
        {
            "spark": spark,
            "F": F,
            "write_full_table": io.write_full_table,
            "merge_incremental": io.merge_incremental,
            "trim_lower": transforms.trim_lower,
            "is_valid_email": transforms.is_valid_email,
            "date_from_yyyymm": transforms.date_from_yyyymm,
            "load_country_lookup": transforms.load_country_lookup,
            "with_normalized_country": transforms.with_normalized_country,
            "cursor_usage_record_key": transforms.cursor_usage_record_key,
            "show_sample": show_sample,
        }
    )
    for name in config.CONFIG_NAMES:
        notebook_globals[name] = getattr(config, name)
    print(
        f"Bootstrap OK: runtime={info.get('runtime')} mode={info.get('mode')} "
        f"spark={spark.version}"
    )


init_notebook(globals())
