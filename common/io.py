from __future__ import annotations

import os
from typing import TYPE_CHECKING

from common.config import ONELAKE_HOST, WORKSPACEID

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


def onelake_table_path(lakehouse_id: str, *subpath: str) -> str:
    """Same abfss layout as NB_Cursor_Bronze (guid/Tables/..., no .Lakehouse suffix)."""
    suffix = ".Lakehouse" if os.environ.get("ONELAKE_LAKEHOUSE_SUFFIX", "").lower() in (
        "1",
        "true",
        "yes",
    ) else ""
    base = f"abfss://{WORKSPACEID}@{ONELAKE_HOST}/{lakehouse_id}{suffix}/Tables"
    return "/".join([base, *subpath])


def delta_path_for_table(table_fqn: str) -> str | None:
    from common import config

    med = config.LAKEHOUSE_AI_MEDALLION_ID
    bronze = config.LAKEHOUSE_AI_MEDALLION_BRONZE_ID
    silver = config.LAKEHOUSE_AI_MEDALLION_SILVER_ID
    mapping = {
        config.TABLE_CURSOR_BRONZE: config.ONELAKE_CURSOR_BRONZE_PATH,
        config.TABLE_HC_BRONZE: onelake_table_path(bronze, "dbo/HC_Bronze_Historical"),
        config.TABLE_OKTA_BRONZE: onelake_table_path(bronze, "dbo/CoreOktaUser"),
        config.TABLE_COUNTRY: onelake_table_path(
            silver, "dbo/CoreCountry_Excel_Hours_Historic"
        ),
        config.TABLE_HC_SILVER: onelake_table_path(med, "Silver/hc_silver_historical"),
        config.TABLE_OKTA_SILVER: onelake_table_path(med, "Silver/okta_user_for_ai"),
        config.TABLE_CURSOR_SILVER: onelake_table_path(med, "Silver/cursor_usage"),
        config.TABLE_FACT_CURSOR_ACTIVE: onelake_table_path(
            med, "Gold/Fact_Cursor_Active"
        ),
    }
    try:
        from common import local_settings as ls  # type: ignore

        mapping.update(getattr(ls, "ONELAKE_TABLE_OVERRIDES", {}) or {})
    except ImportError:
        pass
    return mapping.get(table_fqn)


def _use_path_reads() -> bool:
    flag = os.environ.get("MEDALLION_READ_VIA_PATH")
    if flag is not None:
        return flag.lower() in ("1", "true", "yes")
    if os.environ.get("DATABRICKS_RUNTIME_VERSION"):
        return False
    try:
        import notebookutils  # noqa: F401

        return False
    except ImportError:
        pass
    if os.environ.get("MEDALLION_SPARK_PROFILE", "auto") == "fabric":
        return False
    return True


def _use_deltalake_read() -> bool:
    """True when env forces deltalake-only (see _resolve_read_backend for per-table auto)."""
    flag = os.environ.get("MEDALLION_DELTALAKE_READ")
    if flag is None:
        return False
    return flag.lower() in ("1", "true", "yes")


def _resolve_read_backend(path: str) -> str:
    """Default deltalake scan (Fabric column mapping, fast). PySpark abfss often hangs locally."""
    if os.environ.get("MEDALLION_PYSPARK_ABFSS_READ", "").lower() in ("1", "true", "yes"):
        return "pyspark"
    if _use_deltalake_read():
        _read_debug(
            "read_table: MEDALLION_DELTALAKE_READ=1 uses to_pandas (NaN on column-mapped tables)"
        )
        return "deltalake"
    return "scan"


def _read_debug(msg: str) -> None:
    if os.environ.get("MEDALLION_DEBUG_READ", "").lower() in ("1", "true", "yes"):
        print(msg, flush=True)


def _scan_retriable_error(exc: BaseException) -> bool:
    msg = str(exc).lower()
    return any(
        token in msg
        for token in (
            "microsoftazure",
            "http error",
            "body error",
            "connection",
            "timeout",
            "timed out",
            "401",
            "403",
            "503",
            "reset",
            "broken pipe",
        )
    )


def _batches_to_pandas(batches) -> "pd.DataFrame":
    import pandas as pd

    first = batches[0]
    names = list(first.column_names)
    columns = {name: [] for name in names}
    for batch in batches:
        for i, name in enumerate(batch.column_names):
            columns[name].extend(batch.column(i).to_pylist())
    return pd.DataFrame(columns)[names]


def _scan_batches_to_spark(spark: SparkSession, reader) -> tuple[DataFrame, int]:
    """Stream deltalake scan batches into Spark (chunked to limit RAM on large tables)."""
    row_count = 0
    next_progress = 50_000
    chunk_limit = max(10_000, int(os.environ.get("MEDALLION_SCAN_CHUNK_ROWS", "75000")))
    pending: list = []
    pending_rows = 0
    spark_df: DataFrame | None = None

    def flush_chunk() -> None:
        nonlocal spark_df, pending, pending_rows
        if not pending:
            return
        part = spark.createDataFrame(_batches_to_pandas(pending))
        spark_df = part if spark_df is None else spark_df.unionByName(part, allowMissingColumns=True)
        pending = []
        pending_rows = 0

    for batch in reader:
        pending.append(batch)
        pending_rows += batch.num_rows
        row_count += batch.num_rows
        if row_count >= next_progress:
            _read_debug(f"read_table: scan ... {row_count} rows streamed")
            next_progress += 50_000
        if pending_rows >= chunk_limit:
            flush_chunk()

    flush_chunk()
    if spark_df is None:
        return spark.createDataFrame([], schema=None), 0
    return spark_df, row_count


def _read_delta_via_deltalake_scan(spark: SparkSession, path: str) -> DataFrame:
    """Azure CLI + deltalake DataFusion scan (column mapping / deletion vectors) -> Spark."""
    import time

    from deltalake import DeltaTable

    from common.fabric_storage import fabric_storage_options, refresh_abfs_token_env

    max_retries = max(1, int(os.environ.get("MEDALLION_SCAN_RETRIES", "6")))
    last_error: BaseException | None = None
    for attempt in range(1, max_retries + 1):
        try:
            refresh_abfs_token_env()
            opts = fabric_storage_options()
            _read_debug(f"read_table: deltalake scan {path} (attempt {attempt}/{max_retries})")
            dt = DeltaTable(path, storage_options=opts)
            _read_debug("read_table: deltalake scan() streaming batches...")
            reader = dt.scan()
            df, row_count = _scan_batches_to_spark(spark, reader)
            _read_debug(f"read_table: scan -> Spark ({row_count} rows)")
            return df
        except Exception as exc:
            last_error = exc
            if attempt >= max_retries or not _scan_retriable_error(exc):
                raise
            wait = min(2**attempt, 15)
            _read_debug(
                f"read_table: scan failed ({exc!s}); refresh token and retry in {wait}s..."
            )
            time.sleep(wait)
    assert last_error is not None
    raise last_error


def _read_delta_via_deltalake(spark: SparkSession, path: str) -> DataFrame:
    """Azure CLI + deltalake (Fabric endpoint) -> Spark DataFrame; same auth as NB_Cursor_Bronze."""
    from deltalake import DeltaTable

    from common.fabric_storage import fabric_storage_options

    _read_debug(f"read_table: deltalake open {path}")
    dt = DeltaTable(path, storage_options=fabric_storage_options())
    _read_debug("read_table: deltalake loading data...")
    # PySpark createDataFrame(pyarrow Table) can yield all-NULL rows; pandas bridge matches NB_Cursor_Bronze.
    pdf = dt.to_pandas()
    _read_debug(f"read_table: {len(pdf)} rows -> Spark")
    return spark.createDataFrame(pdf)


def load_delta_path(spark: SparkSession, path: str) -> DataFrame:
    """Read OneLake Delta path locally (same backend selection as ``read_table``)."""
    backend = _resolve_read_backend(path)
    if backend == "pyspark":
        return _read_delta_via_pyspark(spark, path)
    if backend == "deltalake":
        return _read_delta_via_deltalake(spark, path)
    return _read_delta_via_deltalake_scan(spark, path)


def _read_delta_via_pyspark(spark: SparkSession, path: str) -> DataFrame:
    from common.fabric_storage import apply_azure_cli_abfs_conf

    _read_debug(f"read_table: pyspark delta.load {path}")
    apply_azure_cli_abfs_conf(spark)
    return spark.read.format("delta").load(path)


def read_table(spark: SparkSession, table_fqn: str) -> DataFrame:
    """Local: PySpark Delta on abfss (az login). Fabric runtime: spark.table."""
    if _use_path_reads():
        import gc

        gc.collect()
        path = delta_path_for_table(table_fqn)
        if path:
            backend = _resolve_read_backend(path)
            _read_debug(f"read_table: {table_fqn} backend={backend}")
            if backend == "pyspark":
                return _read_delta_via_pyspark(spark, path)
            if backend == "deltalake":
                return _read_delta_via_deltalake(spark, path)
            return _read_delta_via_deltalake_scan(spark, path)
    return spark.table(table_fqn)


def write_full_table(df: DataFrame, table_name: str) -> None:
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table_name)
    )


def merge_incremental(spark, df: DataFrame, table_name: str, merge_key: str) -> None:
    if not spark.catalog.tableExists(table_name):
        write_full_table(df, table_name)
        return
    from delta.tables import DeltaTable

    target = DeltaTable.forName(spark, table_name)
    (
        target.alias("target")
        .merge(df.alias("source"), f"target.{merge_key} = source.{merge_key}")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
