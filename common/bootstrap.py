"""Notebook entry: `%run ./common/bootstrap`"""

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


def _spark_home_version() -> str | None:
    home = os.environ.get("SPARK_HOME")
    if not home:
        return None
    release = Path(home) / "RELEASE"
    if not release.is_file():
        return None
    text = release.read_text(encoding="utf-8", errors="ignore")
    for token in text.replace("\n", " ").split():
        if token[0:1].isdigit():
            return token.split("-")[0]
    return None


def _prepare_pyspark_env() -> None:
    """Avoid pip PySpark + SPARK_HOME version mix (causes GenTraversableOnce / catalog errors)."""
    import pyspark

    pip_ver = pyspark.__version__.split(".")
    home_ver = _spark_home_version()
    if home_ver and home_ver.split(".")[0] != pip_ver[0]:
        if os.environ.get("MEDALLION_USE_SPARK_HOME") == "1":
            print(
                f"WARNING: SPARK_HOME={home_ver} but pip pyspark={pyspark.__version__} "
                "(set MEDALLION_USE_SPARK_HOME only if versions match)"
            )
        else:
            print(
                f"Unsetting SPARK_HOME ({home_ver}); pip pyspark is {pyspark.__version__}. "
                "Set MEDALLION_USE_SPARK_HOME=1 to force SPARK_HOME."
            )
            os.environ.pop("SPARK_HOME", None)
            os.environ.pop("PYSPARK_PYTHON", None)


def _hadoop_version() -> str:
    """Match Spark's bundled Hadoop (PySpark 3.5.x ships 3.3.4)."""
    override = os.environ.get("MEDALLION_HADOOP_VERSION")
    if override:
        return override
    try:
        import pyspark

        if pyspark.__version__.startswith("3.5."):
            return "3.3.4"
    except ImportError:
        pass
    return "3.3.4"


def _onelake_jar_packages() -> str:
    """Delta + Hadoop Azure (required for abfss:// OneLake paths)."""
    hv = _hadoop_version()
    default_azure = (
        f"org.apache.hadoop:hadoop-azure:{hv},com.azure:azure-storage-blob:12.25.1"
    )
    azure = os.environ.get("MEDALLION_AZURE_PACKAGES", default_azure)
    delta = os.environ.get("MEDALLION_DELTA_PACKAGES", "io.delta:delta-spark_2.12:3.2.0")
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


def _apply_onelake_spark_conf(spark) -> None:
    host = config.ONELAKE_HOST
    tenant = os.environ.get("FABRIC_TENANT_ID", "")
    client_id = os.environ.get("FABRIC_CLIENT_ID", "")
    client_secret = os.environ.get("FABRIC_CLIENT_SECRET", "")
    p = "fs.azure.account"
    spark.conf.set(f"{p}.auth.type.{host}", "OAuth")
    spark.conf.set(
        f"{p}.oauth.provider.type.{host}",
        "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    )
    spark.conf.set(f"{p}.oauth2.client.id.{host}", client_id)
    spark.conf.set(f"{p}.oauth2.client.secret.{host}", client_secret)
    spark.conf.set(
        f"{p}.oauth2.client.endpoint.{host}",
        f"https://login.microsoftonline.com/{tenant}/oauth2/token",
    )


def create_onelake_spark(app_name: str = "Medallion"):
    """Local Spark with Delta + Hadoop Azure (abfss) + OneLake OAuth env vars."""
    spark, _, _ = _build_spark(app_name)
    return spark


def _build_spark(app_name: str):
    _prepare_pyspark_env()
    from pyspark.sql import SparkSession

    if os.environ.get("MEDALLION_FRESH_SPARK") == "1":
        try:
            SparkSession.getActiveSession().stop()  # type: ignore[union-attr]
        except Exception:
            pass

    packages = os.environ.get("MEDALLION_SPARK_PACKAGES") or _onelake_jar_packages()
    hadoop_ver = _hadoop_version()
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.jars.packages", packages)
    )
    builder = _configure_onelake(builder)
    for k, v in _local_settings().get("spark_extra_config", {}).items():
        builder = builder.config(k, v)
    spark = builder.getOrCreate()
    _apply_onelake_spark_conf(spark)
    spark.range(1).count()
    return spark, packages, hadoop_ver


def _get_spark(notebook_globals: dict[str, Any], app_name: str):
    existing = notebook_globals.get("spark")
    if existing is not None and os.environ.get("MEDALLION_FRESH_SPARK") != "1":
        try:
            existing.sparkContext
            return existing, {"runtime": _runtime(), "mode": "attached"}
        except Exception:
            pass
    settings = _local_settings()
    create = settings.get("create_spark")
    if callable(create):
        spark = create(app_name)
        _apply_onelake_spark_conf(spark)
        return spark, {"runtime": "local", "mode": "custom"}
    rt = _runtime()
    if rt in ("fabric", "databricks"):
        from pyspark.sql import SparkSession

        return SparkSession.builder.getOrCreate(), {"runtime": rt, "mode": "getOrCreate"}
    spark, packages, hadoop_ver = _build_spark(app_name)
    return spark, {
        "runtime": "local",
        "mode": "created",
        "jar_packages": packages,
        "hadoop_version": hadoop_ver,
    }


def show_sample(df, n: int = 10) -> None:
    try:
        display(df.limit(n))  # noqa: F821
    except NameError:
        df.limit(n).show(truncate=False)


def init_notebook(notebook_globals: dict[str, Any], app_name: str = "Medallion") -> None:
    from pyspark.sql import functions as F

    spark, info = _get_spark(notebook_globals, app_name)
    read_table = lambda name: io.read_table(spark, name)  # noqa: E731

    notebook_globals.update(
        {
            "spark": spark,
            "F": F,
            "read_table": read_table,
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

    import pyspark

    hv = info.get("hadoop_version") or _hadoop_version()
    print(
        f"Bootstrap OK: runtime={info.get('runtime')} mode={info.get('mode')} "
        f"spark={spark.version} pyspark={pyspark.__version__} "
        f"hadoop_azure={hv} path_reads={io._use_path_reads()}"
    )
    jars = info.get("jar_packages")
    if jars and io._use_path_reads():
        print(f"spark.jars.packages={jars}")
    if io._use_path_reads() and info.get("mode") in ("attached", "custom"):
        print(
            "OneLake abfss reads need hadoop-azure on this Spark session. "
            "Use create_onelake_spark in local_settings, or restart kernel / "
            "MEDALLION_FRESH_SPARK=1 if you see SecureAzureBlobFileSystem errors."
        )


init_notebook(globals())
