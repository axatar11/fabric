"""Notebook entry: `%run ../common/bootstrap` from silver/ or gold/ notebooks."""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any

_REPO = Path(__file__).resolve().parent.parent
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from common import config, fabric_storage, io, transforms


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


def _spark_jar_packages() -> str:
    """Delta + hadoop-azure for local OneLake abfss reads."""
    override = os.environ.get("MEDALLION_SPARK_PACKAGES")
    if override:
        return override
    delta = os.environ.get("MEDALLION_DELTA_PACKAGES", "io.delta:delta-spark_2.12:3.2.0")
    if not io._use_path_reads():
        return delta
    hv = os.environ.get("MEDALLION_HADOOP_VERSION", "3.3.4")
    azure = os.environ.get(
        "MEDALLION_AZURE_PACKAGES",
        f"org.apache.hadoop:hadoop-azure:{hv},com.azure:azure-storage-blob:12.25.1",
    )
    return f"{delta},{azure}"


def create_onelake_spark(app_name: str = "Medallion"):
    """Local SparkSession (Delta SQL). OneLake reads use Azure CLI via read_table()."""
    spark, _ = _build_spark(app_name)
    return spark


def _build_spark(app_name: str):
    _prepare_pyspark_env()
    from pyspark.sql import SparkSession

    if os.environ.get("MEDALLION_FRESH_SPARK") == "1":
        try:
            SparkSession.getActiveSession().stop()  # type: ignore[union-attr]
        except Exception:
            pass

    packages = _spark_jar_packages()
    if io._use_path_reads():
        fabric_storage.refresh_abfs_token_env()
    builder = SparkSession.builder.appName(app_name)
    if io._use_path_reads():
        builder = builder.config("spark.jars", fabric_storage.onelake_cli_token_jar())
    builder = (
        builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.jars.packages", packages)
    )
    for k, v in _local_settings().get("spark_extra_config", {}).items():
        builder = builder.config(k, v)
    if io._use_path_reads():
        builder = (
            builder.config("spark.sql.autoBroadcastJoinThreshold", "-1")
            .config("spark.sql.adaptive.autoBroadcastJoinThreshold", "-1")
        )
    spark = builder.getOrCreate()
    if io._use_path_reads():
        fabric_storage.apply_azure_cli_abfs_conf(spark)
    spark.range(1).count()
    return spark, packages


def _get_spark(notebook_globals: dict[str, Any], app_name: str):
    existing = notebook_globals.get("spark")
    if existing is not None and os.environ.get("MEDALLION_FRESH_SPARK") != "1":
        try:
            existing.sparkContext
            if io._use_path_reads():
                fabric_storage.apply_azure_cli_abfs_conf(existing)
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
    spark, packages = _build_spark(app_name)
    return spark, {
        "runtime": "local",
        "mode": "created",
        "jar_packages": packages,
    }


def show_sample(df, n: int = 10) -> None:
    try:
        display(df.limit(n))  # noqa: F821
    except NameError:
        df.limit(n).show(truncate=False)


def _ensure_onelake_read_deps() -> None:
    if not io._use_path_reads():
        return
    missing: list[str] = []
    try:
        import azure.identity  # noqa: F401
    except ImportError:
        missing.append("azure-identity")
    try:
        import deltalake  # noqa: F401
    except ImportError:
        missing.append("deltalake[pyarrow]")
    try:
        fabric_storage.onelake_cli_token_jar()
    except FileNotFoundError:
        missing.append("common/jars/onelake-cli-token-provider.jar")
    if missing:
        req = Path(__file__).resolve().parent.parent / "requirements-local-spark.txt"
        raise RuntimeError(
            "Local OneLake reads use az login (default: deltalake + Fabric endpoint).\n"
            f"Missing: {', '.join(missing)}\n"
            f"Python: {sys.executable}\n"
            f"  python -m pip install -r {req}\n"
            "Then restart the Jupyter kernel and re-run bootstrap."
        )


def _bind_read_table_broken_lineage(spark):
    def read_table_broken_lineage(table_fqn: str, label: str):
        return io.read_table_broken_lineage(spark, table_fqn, label)

    return read_table_broken_lineage


def _bind_break_lineage_local(spark):
    """Accept break_lineage_local(df, label) or break_lineage_local(spark, df, label)."""

    def break_lineage_local(*args):
        if len(args) == 2:
            df, label = args
            return io.break_lineage_local(spark, df, label)
        if len(args) == 3 and args[0] is spark:
            _, df, label = args
            return io.break_lineage_local(spark, df, label)
        raise TypeError(
            "break_lineage_local(df, label) or break_lineage_local(spark, df, label)"
        )

    return break_lineage_local


def _notebook_reload_io_helpers(
    notebook_globals: dict[str, Any], notebook_globals_override: dict[str, Any] | None = None
) -> None:
    """Reload common.io after git pull — keeps spark and in-memory DataFrames."""
    import importlib

    importlib.reload(io)
    importlib.reload(transforms)
    ng = notebook_globals_override if notebook_globals_override is not None else notebook_globals
    spark = ng.get("spark")
    if spark is None:
        raise RuntimeError("No spark session in notebook; run init_notebook first.")
    ng["read_table"] = lambda name: io.read_table(spark, name)  # noqa: E731
    ng["read_table_broken_lineage"] = _bind_read_table_broken_lineage(spark)
    ng["load_delta_path"] = lambda path: io.load_delta_path(spark, path)
    ng["write_full_table"] = io.write_full_table
    ng["merge_incremental"] = io.merge_incremental
    ng["save_table_cache"] = io.save_table_cache
    ng["load_table_cache"] = lambda name: io.load_table_cache(spark, name)
    ng["apply_local_merge_spark_conf"] = io.apply_local_merge_spark_conf
    ng["break_lineage_local"] = _bind_break_lineage_local(spark)
    ng["publish_merge_staging"] = io.publish_merge_staging
    ng["reload_io_helpers"] = _bind_reload_io_helpers(ng)
    print("Reloaded common.io (spark session and existing DataFrames unchanged).")


def reload_io_helpers(notebook_globals: dict[str, Any]) -> None:
    _notebook_reload_io_helpers(notebook_globals)


def _bind_reload_io_helpers(notebook_globals: dict[str, Any]):
    """Notebook-safe callback — never shadow the module-level reload_io_helpers name."""

    def _reload(notebook_globals_override: dict[str, Any] | None = None) -> None:
        _notebook_reload_io_helpers(
            notebook_globals, notebook_globals_override=notebook_globals_override
        )

    return _reload


def init_notebook(notebook_globals: dict[str, Any], app_name: str = "Medallion") -> None:
    import importlib

    from pyspark.sql import functions as F

    importlib.reload(io)
    importlib.reload(transforms)
    _ensure_onelake_read_deps()
    spark, info = _get_spark(notebook_globals, app_name)
    read_table = lambda name: io.read_table(spark, name)  # noqa: E731

    notebook_globals.update(
        {
            "spark": spark,
            "F": F,
            "read_table": read_table,
            "read_table_broken_lineage": _bind_read_table_broken_lineage(spark),
            "load_delta_path": lambda path: io.load_delta_path(spark, path),
            "bronze_cursor_path": config.ONELAKE_CURSOR_BRONZE_PATH,
            "write_full_table": io.write_full_table,
            "merge_incremental": io.merge_incremental,
            "apply_local_merge_spark_conf": io.apply_local_merge_spark_conf,
            "break_lineage_local": _bind_break_lineage_local(spark),
            "publish_merge_staging": io.publish_merge_staging,
            "trim_lower": transforms.trim_lower,
            "is_valid_email": transforms.is_valid_email,
            "date_from_yyyymm": transforms.date_from_yyyymm,
            "load_country_lookup": transforms.load_country_lookup,
            "with_normalized_country": transforms.with_normalized_country,
            "cursor_usage_record_key": transforms.cursor_usage_record_key,
            "cursor_active_daily_aggregate": transforms.cursor_active_daily_aggregate,
            "cursor_active_daily_merge_key": transforms.cursor_active_daily_merge_key,
            "cursor_onboard_daily_dedupe": transforms.cursor_onboard_daily_dedupe,
            "cursor_onboard_merge_key": transforms.cursor_onboard_merge_key,
            "show_sample": show_sample,
            "reload_io_helpers": _bind_reload_io_helpers(notebook_globals),
            "save_table_cache": io.save_table_cache,
            "load_table_cache": lambda name: io.load_table_cache(spark, name),
        }
    )
    for name in config.CONFIG_NAMES:
        if hasattr(config, name):
            notebook_globals[name] = getattr(config, name)
    if "TABLE_FACT_CURSOR_ONBOARD" not in notebook_globals:
        notebook_globals["TABLE_FACT_CURSOR_ONBOARD"] = (
            f"{config.GOLD_DATABASE}.{config.GOLD_SCHEMA}.fact_cursor_onboard"
        )
    if "FACT_CURSOR_ONBOARD_MERGE_KEY" not in notebook_globals:
        notebook_globals["FACT_CURSOR_ONBOARD_MERGE_KEY"] = "OnboardSurrogateKey"

    import pyspark

    if io._use_deltalake_read():
        read_mode = "deltalake-to_pandas"
    elif os.environ.get("MEDALLION_PYSPARK_ABFSS_READ", "").lower() in ("1", "true", "yes"):
        read_mode = "pyspark-delta-load"
    else:
        read_mode = "deltalake-scan"
    print(
        f"Bootstrap OK: runtime={info.get('runtime')} mode={info.get('mode')} "
        f"spark={spark.version} pyspark={pyspark.__version__} "
        f"path_reads={io._use_path_reads()} onelake_read={read_mode}"
    )
    jars = info.get("jar_packages")
    if jars:
        print(f"spark.jars.packages={jars}")
    if io._use_path_reads():
        if read_mode == "deltalake-scan":
            print(
                "OneLake reads: deltalake scan() + az login -> Spark "
                "(column mapping; avoids local PySpark abfss hang)."
            )
        elif read_mode == "deltalake-to_pandas":
            print("OneLake reads: deltalake to_pandas (may show NaN on column-mapped tables).")
        else:
            print(
                "OneLake reads: spark.read.format('delta').load(abfss://...) "
                "(can hang locally; prefer default deltalake-scan)."
            )
        cursor_path = io.delta_path_for_table(config.TABLE_CURSOR_BRONZE)
        if cursor_path:
            print(f"TABLE_CURSOR_BRONZE -> {cursor_path}")
        print("Tip: set MEDALLION_DEBUG_READ=1 before bootstrap to log read progress.")
        print(
            "Tip: large tables — read_table() one at a time; transient Azure errors auto-retry."
        )
        if io._local_table_cache_enabled():
            print(
                "Local table cache ON (default): parquet under ~/.fabric/medallion_cache/. "
                "Set MEDALLION_CACHE_REFRESH=1 to force OneLake. reload_io_helpers(globals()) "
                "after git pull — no kernel restart."
            )
        if io._use_deltalake_onelake_write():
            print(
                "OneLake writes: deltalake + az login (merge/overwrite). "
                "Set MEDALLION_ONELAKE_WRITE=pyspark to use JVM abfss (often hangs)."
            )


init_notebook(globals())
