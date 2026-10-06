from __future__ import annotations

import os
from pathlib import Path
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


def _deltalake_schema_to_spark(dt) -> "StructType":
    from pyspark.sql.types import StringType, StructField, StructType
    from pyspark.sql.pandas.types import from_arrow_type

    arrow_schema = dt.schema().to_arrow()
    fields = []
    for field in arrow_schema:
        try:
            spark_type = from_arrow_type(field.type)
        except Exception:
            spark_type = StringType()
        fields.append(StructField(field.name, spark_type, nullable=True))
    return StructType(fields)


def _pandas_for_spark_schema(pdf: "pd.DataFrame", spark_schema: "StructType") -> "pd.DataFrame":
    import pandas as pd

    names = [f.name for f in spark_schema.fields]
    for name in names:
        if name not in pdf.columns:
            pdf[name] = pd.NA
    pdf = pdf[names]
    for field in spark_schema.fields:
        col = field.name
        if pdf[col].isna().all():
            pdf[col] = pdf[col].astype("object")
    return pdf


def _scan_batches_to_spark(
    spark: SparkSession, reader, spark_schema: "StructType"
) -> tuple[DataFrame, int]:
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
        pdf = _pandas_for_spark_schema(_batches_to_pandas(pending), spark_schema)
        part = spark.createDataFrame(pdf, schema=spark_schema)
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
            _read_debug(
                f"read_table: scan chunk flush to Spark "
                f"(batch rows>={chunk_limit}, total so far {row_count})"
            )
            flush_chunk()

    flush_chunk()
    if spark_df is None:
        return spark.createDataFrame([], schema=spark_schema), 0
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
            spark_schema = _deltalake_schema_to_spark(dt)
            _read_debug("read_table: deltalake scan() streaming batches...")
            reader = dt.scan()
            df, row_count = _scan_batches_to_spark(spark, reader, spark_schema)
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
    spark_schema = _deltalake_schema_to_spark(dt)
    pdf = _pandas_for_spark_schema(dt.to_pandas(), spark_schema)
    _read_debug(f"read_table: {len(pdf)} rows -> Spark")
    return spark.createDataFrame(pdf, schema=spark_schema)


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


def _local_table_cache_enabled() -> bool:
    flag = os.environ.get("MEDALLION_LOCAL_TABLE_CACHE", "1")
    return flag.lower() not in ("0", "false", "no")


def _local_cache_refresh() -> bool:
    return os.environ.get("MEDALLION_CACHE_REFRESH", "").lower() in ("1", "true", "yes")


def local_cache_path(table_fqn: str) -> Path:
    override = os.environ.get("MEDALLION_LOCAL_CACHE_DIR")
    root = Path(override) if override else Path.home() / ".fabric" / "medallion_cache"
    safe = table_fqn.replace(".", "__")
    return root / safe


def _read_from_local_cache(spark: SparkSession, table_fqn: str) -> DataFrame | None:
    if not _local_table_cache_enabled() or _local_cache_refresh():
        return None
    path = local_cache_path(table_fqn)
    if not path.is_dir() or not any(path.glob("*.parquet")):
        return None
    _read_debug(f"read_table: local cache hit {path}")
    return spark.read.parquet(str(path))


def _write_local_cache(df: DataFrame, table_fqn: str) -> None:
    if not _local_table_cache_enabled():
        return
    path = local_cache_path(table_fqn)
    path.parent.mkdir(parents=True, exist_ok=True)
    _read_debug(f"read_table: saving local cache {path}")
    df.write.mode("overwrite").parquet(str(path))


def save_table_cache(df: DataFrame, table_fqn: str) -> Path:
    """Persist an in-memory DataFrame locally (survives kernel restart)."""
    path = local_cache_path(table_fqn)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write.mode("overwrite").parquet(str(path))
    return path


def load_table_cache(spark: SparkSession, table_fqn: str) -> DataFrame:
    """Load a table from local parquet cache written by read_table or save_table_cache."""
    path = local_cache_path(table_fqn)
    if not path.is_dir() or not any(path.glob("*.parquet")):
        raise FileNotFoundError(f"No local cache for {table_fqn} at {path}")
    return spark.read.parquet(str(path))


def read_table(spark: SparkSession, table_fqn: str) -> DataFrame:
    """Local: OneLake read (with optional local parquet cache). Fabric: spark.table."""
    if _use_path_reads():
        import gc

        gc.collect()
        cached = _read_from_local_cache(spark, table_fqn)
        if cached is not None:
            return cached
        path = delta_path_for_table(table_fqn)
        if path:
            backend = _resolve_read_backend(path)
            _read_debug(f"read_table: {table_fqn} backend={backend}")
            if backend == "pyspark":
                df = _read_delta_via_pyspark(spark, path)
            elif backend == "deltalake":
                df = _read_delta_via_deltalake(spark, path)
            else:
                df = _read_delta_via_deltalake_scan(spark, path)
            _write_local_cache(df, table_fqn)
            return df
    return spark.table(table_fqn)


def _onelake_delta_path(table_fqn: str) -> str | None:
    if not _use_path_reads():
        return None
    return delta_path_for_table(table_fqn)


def _use_deltalake_onelake_write() -> bool:
    if not _use_path_reads():
        return False
    flag = os.environ.get("MEDALLION_ONELAKE_WRITE", "deltalake").lower()
    return flag not in ("pyspark", "spark", "abfss")


def _merge_predicate(merge_key: str) -> str:
    key = merge_key if merge_key.startswith("`") else f"`{merge_key}`"
    return f"target.{key} = source.{key}"


def _spark_df_to_deltalake(df: DataFrame, path: str, *, mode: str = "overwrite") -> None:
    from deltalake import write_deltalake

    from common.fabric_storage import fabric_storage_options, refresh_abfs_token_env

    refresh_abfs_token_env()
    opts = fabric_storage_options()
    _read_debug(f"deltalake write mode={mode} {path} (converting Spark -> pandas)...")
    pdf = df.toPandas()
    _read_debug(f"deltalake write uploading {len(pdf)} rows...")
    write_deltalake(
        path,
        pdf,
        mode=mode,
        schema_mode="overwrite",
        storage_options=opts,
    )
    _read_debug("deltalake write done")


def _deltalake_table_exists(path: str) -> bool:
    from deltalake import DeltaTable

    from common.fabric_storage import fabric_storage_options, refresh_abfs_token_env

    refresh_abfs_token_env()
    try:
        DeltaTable(path, storage_options=fabric_storage_options())
        return True
    except Exception:
        return False


def write_full_table(df: DataFrame, table_name: str) -> None:
    path = _onelake_delta_path(table_name)
    if path and _use_deltalake_onelake_write():
        _spark_df_to_deltalake(df, path, mode="overwrite")
        return
    writer = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    if path:
        from common.fabric_storage import apply_azure_cli_abfs_conf

        apply_azure_cli_abfs_conf(df.sparkSession)
        _read_debug(f"write_full_table: pyspark save {path}")
        writer.save(path)
        return
    writer.saveAsTable(table_name)


def merge_incremental(spark, df: DataFrame, table_name: str, merge_key: str) -> None:
    path = _onelake_delta_path(table_name)
    if path and _use_deltalake_onelake_write():
        from deltalake import DeltaTable, write_deltalake

        from common.fabric_storage import fabric_storage_options, refresh_abfs_token_env

        refresh_abfs_token_env()
        opts = fabric_storage_options()
        _read_debug(f"merge_incremental: deltalake path {path}")
        _read_debug("merge_incremental: Spark -> pandas for source batch...")
        source = df.toPandas()
        if not _deltalake_table_exists(path):
            _read_debug("merge_incremental: new table (deltalake overwrite)")
            write_deltalake(
                path,
                source,
                mode="overwrite",
                schema_mode="overwrite",
                storage_options=opts,
            )
            return
        dt = DeltaTable(path, storage_options=opts)
        predicate = _merge_predicate(merge_key)
        _read_debug(f"merge_incremental: deltalake merge on {predicate}")
        (
            dt.merge(
                source,
                predicate,
                source_alias="source",
                target_alias="target",
            )
            .when_matched_update_all()
            .when_not_matched_insert_all()
            .execute()
        )
        _read_debug("merge_incremental: deltalake merge done")
        return

    from delta.tables import DeltaTable as SparkDeltaTable

    if path:
        from common.fabric_storage import apply_azure_cli_abfs_conf

        apply_azure_cli_abfs_conf(spark)
        _read_debug(f"merge_incremental: pyspark abfss path {path}")
        if not SparkDeltaTable.isDeltaTable(spark, path):
            write_full_table(df, table_name)
            return
        target = SparkDeltaTable.forPath(spark, path)
    else:
        if not spark.catalog.tableExists(table_name):
            write_full_table(df, table_name)
            return
        target = SparkDeltaTable.forName(spark, table_name)
    (
        target.alias("target")
        .merge(df.alias("source"), f"target.{merge_key} = source.{merge_key}")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
