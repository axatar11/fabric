from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any

from medallion import config


def find_repo_root() -> Path:
    """Locate repo root (folder that contains the medallion package)."""
    here = Path(__file__).resolve().parent.parent
    if (here / "medallion" / "config.py").is_file():
        return here
    cwd = Path.cwd()
    for candidate in (cwd, *cwd.parents):
        if (candidate / "medallion" / "config.py").is_file():
            return candidate
    return here


def detect_runtime() -> str:
    """Return one of: fabric, databricks, local."""
    profile = os.environ.get("MEDALLION_SPARK_PROFILE", "auto").lower()
    if profile in ("fabric", "databricks", "local"):
        return profile

    if os.environ.get("DATABRICKS_RUNTIME_VERSION"):
        return "databricks"
    try:
        import dbutils  # type: ignore  # noqa: F401

        return "databricks"
    except ImportError:
        pass

    try:
        import notebookutils  # type: ignore  # noqa: F401

        return "fabric"
    except ImportError:
        pass

    if os.environ.get("FABRIC_WORKSPACE_ID") or os.environ.get("TRIDENT_ENVIRONMENT"):
        return "fabric"

    return "local"


def _load_optional_local_settings() -> dict[str, Any]:
    settings: dict[str, Any] = {}
    root = find_repo_root()
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))
    try:
        import medallion.local_settings as local_settings  # type: ignore

        settings["spark_extra_config"] = getattr(
            local_settings, "SPARK_EXTRA_CONFIG", {}
        )
        settings["table_path_overrides"] = getattr(
            local_settings, "ONELAKE_TABLE_OVERRIDES", {}
        )
    except ImportError:
        pass
    return settings


def onelake_tables_root(lakehouse_id: str) -> str:
    return (
        f"abfss://{config.WORKSPACEID}@{config.ONELAKE_HOST}/"
        f"{lakehouse_id}.Lakehouse/Tables"
    )


def default_table_path_mappings() -> dict[str, str]:
    root_medallion = onelake_tables_root(config.LAKEHOUSE_AI_MEDALLION_ID)
    root_bronze = onelake_tables_root(config.LAKEHOUSE_AI_MEDALLION_BRONZE_ID)
    root_silver = onelake_tables_root(config.LAKEHOUSE_AI_MEDALLION_SILVER_ID)
    return {
        config.TABLE_CURSOR_BRONZE: f"{root_medallion}/Bronze/cursor_usage",
        config.TABLE_HC_BRONZE: f"{root_bronze}/dbo/HC_Bronze_Historical",
        config.TABLE_OKTA_BRONZE: f"{root_bronze}/dbo/CoreOktaUser",
        config.TABLE_COUNTRY: f"{root_silver}/dbo/CoreCountry_Excel_Hours_Historic",
        config.TABLE_HC_SILVER: f"{root_medallion}/Silver/HC_Silver_Historical",
        config.TABLE_OKTA_SILVER: f"{root_medallion}/Silver/Okta_User_for_AI",
        config.TABLE_CURSOR_SILVER: f"{root_medallion}/Silver/cursor_usage",
        config.TABLE_FACT_CURSOR_ACTIVE: f"{root_medallion}/Gold/Fact_Cursor_Active",
    }


def register_delta_table_if_missing(spark, table_fqn: str, abfss_path: str) -> None:
    if spark.catalog.tableExists(table_fqn):
        return
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {table_fqn} USING DELTA LOCATION '{abfss_path}'"
    )


def register_pipeline_tables(spark, overrides: dict[str, str] | None = None) -> None:
    mappings = default_table_path_mappings()
    if overrides:
        mappings.update(overrides)
    for fqn, path in mappings.items():
        register_delta_table_if_missing(spark, fqn, path)


def _configure_onelake_oauth(builder):
    host = config.ONELAKE_HOST
    tenant = os.environ.get("FABRIC_TENANT_ID", "")
    client_id = os.environ.get("FABRIC_CLIENT_ID", "")
    client_secret = os.environ.get("FABRIC_CLIENT_SECRET", "")
    prefix = "fs.azure.account"
    return (
        builder.config(f"{prefix}.auth.type.{host}", "OAuth")
        .config(
            f"{prefix}.oauth.provider.type.{host}",
            "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
        )
        .config(f"{prefix}.oauth2.client.id.{host}", client_id)
        .config(f"{prefix}.oauth2.client.secret.{host}", client_secret)
        .config(
            f"{prefix}.oauth2.client.endpoint.{host}",
            f"https://login.microsoftonline.com/{tenant}/oauth2/token",
        )
    )


def build_local_spark(app_name: str = "MedallionLocal"):
    from pyspark.sql import SparkSession

    delta_packages = os.environ.get(
        "MEDALLION_DELTA_PACKAGES", "io.delta:delta-spark_2.12:3.2.0"
    )
    azure_packages = os.environ.get(
        "MEDALLION_AZURE_PACKAGES",
        "org.apache.hadoop:hadoop-azure:3.3.6,com.azure:azure-storage-blob:12.25.1",
    )
    packages = os.environ.get(
        "MEDALLION_SPARK_PACKAGES", f"{delta_packages},{azure_packages}"
    )

    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.jars.packages", packages)
    )
    builder = _configure_onelake_oauth(builder)

    local_settings = _load_optional_local_settings()
    for key, value in local_settings.get("spark_extra_config", {}).items():
        builder = builder.config(key, value)

    return builder.getOrCreate()


def _existing_spark_from_notebook(notebook_globals: dict[str, Any] | None):
    if not notebook_globals:
        return None
    session = notebook_globals.get("spark")
    if session is None:
        return None
    try:
        session.sparkContext
        return session
    except Exception:
        return None


def get_or_create_spark(
    app_name: str = "MedallionLocal",
    notebook_globals: dict[str, Any] | None = None,
) -> tuple[Any, dict[str, Any]]:
    runtime = detect_runtime()
    existing = _existing_spark_from_notebook(notebook_globals)
    if existing is not None:
        register = runtime == "local" or os.environ.get("MEDALLION_REGISTER_TABLES", "").lower() in (
            "1",
            "true",
            "yes",
        )
        return existing, {
            "runtime": runtime,
            "mode": "attached",
            "register_tables": register,
        }

    if runtime in ("fabric", "databricks"):
        from pyspark.sql import SparkSession

        session = SparkSession.builder.getOrCreate()
        return session, {
            "runtime": runtime,
            "mode": "getOrCreate",
            "register_tables": False,
        }

    spark = build_local_spark(app_name=app_name)
    return spark, {"runtime": "local", "mode": "created", "register_tables": True}


def maybe_register_tables(spark, info: dict[str, Any]) -> None:
    if os.environ.get("MEDALLION_REGISTER_TABLES", "").lower() in ("0", "false", "no"):
        return
    if not info.get("register_tables") and detect_runtime() != "local":
        return
    if detect_runtime() in ("fabric", "databricks") and not os.environ.get(
        "MEDALLION_REGISTER_TABLES"
    ):
        return

    local_settings = _load_optional_local_settings()
    register_pipeline_tables(
        spark, overrides=local_settings.get("table_path_overrides")
    )
