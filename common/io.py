from __future__ import annotations

import os
import shutil
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


def _read_progress(msg: str) -> None:
    print(msg, flush=True)


def _skip_auto_cache_on_read() -> bool:
    return os.environ.get("MEDALLION_SKIP_AUTO_CACHE", "").lower() in ("1", "true", "yes")


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
    from pyspark.sql.types import (
        BooleanType,
        ByteType,
        DateType,
        DoubleType,
        FloatType,
        IntegerType,
        LongType,
        ShortType,
        StringType,
        TimestampType,
    )

    names = [f.name for f in spark_schema.fields]
    for name in names:
        if name not in pdf.columns:
            pdf[name] = pd.NA
    pdf = pdf[names]
    for field in spark_schema.fields:
        col = field.name
        dt = field.dataType
        if isinstance(dt, StringType):
            # Avoid Arrow null/int inference on string columns (e.g. HCOffice codes).
            pdf[col] = pdf[col].astype("string")
        elif isinstance(dt, (IntegerType, LongType, ShortType, ByteType)):
            pdf[col] = pd.to_numeric(pdf[col], errors="coerce").astype("Int64")
        elif isinstance(dt, (DoubleType, FloatType)):
            pdf[col] = pd.to_numeric(pdf[col], errors="coerce")
        elif isinstance(dt, BooleanType):
            pdf[col] = pdf[col].astype("boolean")
        elif isinstance(dt, DateType):
            pdf[col] = pd.to_datetime(pdf[col], errors="coerce").dt.date
        elif isinstance(dt, TimestampType):
            pdf[col] = pd.to_datetime(pdf[col], errors="coerce")
        elif pdf[col].isna().all():
            pdf[col] = pdf[col].astype("object")
    return pdf


_LOCAL_CACHE_SCHEMA_FILE = "_spark_schema.json"


def _local_cache_schema_path(cache_dir: Path) -> Path:
    return cache_dir / _LOCAL_CACHE_SCHEMA_FILE


def _write_local_cache_schema(cache_dir: Path, spark_schema: "StructType") -> None:
    _local_cache_schema_path(cache_dir).write_text(spark_schema.json(), encoding="utf-8")


def _read_local_cache_schema(cache_dir: Path) -> "StructType | None":
    import json

    from pyspark.sql.types import StructType

    path = _local_cache_schema_path(cache_dir)
    if not path.is_file():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        return StructType.fromJson(payload)
    except Exception as exc:
        _read_debug(f"read_table: ignore cache schema file ({exc!s})")
        return None


def _spark_schema_for_table_fqn(table_fqn: str) -> "StructType | None":
    delta_path = delta_path_for_table(table_fqn)
    if not delta_path:
        return None
    try:
        from deltalake import DeltaTable

        from common.fabric_storage import fabric_storage_options, refresh_abfs_token_env

        refresh_abfs_token_env()
        dt = DeltaTable(delta_path, storage_options=fabric_storage_options())
        return _deltalake_schema_to_spark(dt)
    except Exception:
        return None


def _write_parquet_with_spark_schema(
    pdf: "pd.DataFrame", spark_schema: "StructType", file_path: Path
) -> None:
    """Write one parquet file with stable Arrow types across scan chunks."""
    import pyarrow as pa
    import pyarrow.parquet as pq
    from pyspark.sql.pandas.types import to_arrow_type

    file_path.parent.mkdir(parents=True, exist_ok=True)
    pdf = _pandas_for_spark_schema(pdf, spark_schema)
    arrow_fields = [
        pa.field(f.name, to_arrow_type(f.dataType), nullable=True) for f in spark_schema.fields
    ]
    arrow_schema = pa.schema(arrow_fields)
    table = pa.Table.from_pandas(
        pdf, schema=arrow_schema, preserve_index=False, safe=False
    )
    pq.write_table(table, file_path)


def _write_pandas_to_parquet_dir(
    pdf: "pd.DataFrame", path: Path, *, spark_schema: "StructType | None" = None
) -> None:
    """PyArrow parquet (avoids Spark parquet Python worker crashes on Windows)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        shutil.rmtree(path, ignore_errors=True)
    path.mkdir(parents=True, exist_ok=True)
    out = path / "part-00000.parquet"
    if spark_schema is not None:
        _write_parquet_with_spark_schema(pdf, spark_schema, out)
        _write_local_cache_schema(path, spark_schema)
    else:
        import pyarrow as pa
        import pyarrow.parquet as pq

        table = pa.Table.from_pandas(pdf, preserve_index=False)
        pq.write_table(table, out)


def _scan_batches_to_spark(
    spark: SparkSession,
    reader,
    spark_schema: "StructType",
    *,
    cache_dir: Path | None = None,
) -> tuple[DataFrame, int]:
    """Stream deltalake scan batches into Spark (chunked to limit RAM on large tables)."""
    row_count = 0
    next_progress = 50_000
    chunk_limit = max(10_000, int(os.environ.get("MEDALLION_SCAN_CHUNK_ROWS", "75000")))
    pending: list = []
    pending_rows = 0
    spark_df: DataFrame | None = None
    cache_part = 0
    if cache_dir is not None:
        cache_dir.parent.mkdir(parents=True, exist_ok=True)
        if cache_dir.exists():
            shutil.rmtree(cache_dir, ignore_errors=True)
        cache_dir.mkdir(parents=True, exist_ok=True)
        _write_local_cache_schema(cache_dir, spark_schema)

    def flush_chunk() -> None:
        nonlocal spark_df, pending, pending_rows, cache_part
        if not pending:
            return
        pdf = _pandas_for_spark_schema(_batches_to_pandas(pending), spark_schema)
        if cache_dir is not None:
            part_path = cache_dir / f"part-{cache_part:05d}.parquet"
            _write_parquet_with_spark_schema(pdf, spark_schema, part_path)
            cache_part += 1
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


def _read_delta_via_deltalake_scan(
    spark: SparkSession, path: str, *, cache_dir: Path | None = None
) -> DataFrame:
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
            df, row_count = _scan_batches_to_spark(
                spark, reader, spark_schema, cache_dir=cache_dir
            )
            _read_debug(f"read_table: scan -> Spark ({row_count} rows)")
            if cache_dir is not None and cache_file_exists(cache_dir):
                _read_progress(f"read_table: cached (scan) -> {cache_dir}")
            return df
        except Exception as exc:
            last_error = exc
            if cache_dir is not None:
                shutil.rmtree(cache_dir, ignore_errors=True)
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


def cache_file_exists(cache_dir: Path) -> bool:
    return cache_dir.is_dir() and any(cache_dir.glob("*.parquet"))


def local_cache_path(table_fqn: str) -> Path:
    override = os.environ.get("MEDALLION_LOCAL_CACHE_DIR")
    root = Path(override) if override else Path.home() / ".fabric" / "medallion_cache"
    safe = table_fqn.replace(".", "__")
    return root / safe


def _read_parquet_cache_dir(
    spark: SparkSession, cache_dir: Path, table_fqn: str
) -> DataFrame:
    schema = _read_local_cache_schema(cache_dir) or _spark_schema_for_table_fqn(table_fqn)
    if schema is not None:
        return spark.read.schema(schema).parquet(str(cache_dir))
    return spark.read.parquet(str(cache_dir))


def _read_from_local_cache(spark: SparkSession, table_fqn: str) -> DataFrame | None:
    if not _local_table_cache_enabled() or _local_cache_refresh():
        return None
    path = local_cache_path(table_fqn)
    if not path.is_dir() or not any(path.glob("*.parquet")):
        return None
    _read_progress(f"read_table: cache hit {table_fqn}")
    return _read_parquet_cache_dir(spark, path, table_fqn)


def _local_parquet_use_spark_writer() -> bool:
    flag = os.environ.get("MEDALLION_STAGING_SPARK_PARQUET", "1")
    return flag.lower() not in ("0", "false", "no")


def _write_spark_parquet_dir(df: DataFrame, path: Path) -> None:
    """JVM parquet to file:// (no Python worker / Arrow collect — safe for large join plans)."""
    if path.exists():
        shutil.rmtree(path, ignore_errors=True)
    path.mkdir(parents=True, exist_ok=True)
    target = path.resolve().as_uri()
    df.write.mode("overwrite").parquet(target)


def _write_df_to_local_parquet(df: DataFrame, path: Path) -> None:
    """Materialize locally for cache or merge staging."""
    parts = max(1, int(os.environ.get("MEDALLION_MERGE_COALESCE", "1")))
    df = _sanitize_df_for_export(df.coalesce(parts))
    if _use_path_reads() and _local_parquet_use_spark_writer():
        try:
            _write_spark_parquet_dir(df, path)
            return
        except Exception as exc:
            _read_debug(
                f"local Spark parquet write failed ({exc!s}); falling back to PyArrow collect"
            )
    spark = df.sparkSession
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")
    pdf = df.toPandas()
    _write_pandas_to_parquet_dir(pdf, path, spark_schema=df.schema)


def _write_local_cache(df: DataFrame, table_fqn: str) -> None:
    if not _local_table_cache_enabled():
        return
    path = local_cache_path(table_fqn)
    _read_debug(f"read_table: saving local cache {path}")
    try:
        _write_df_to_local_parquet(df, path)
        _read_progress(f"read_table: cached {table_fqn} -> {path}")
    except Exception as exc:
        # Do not fail the OneLake read if cache write fails (e.g. partial dir on disk).
        _read_debug(f"read_table: local cache write failed: {exc}")


def save_table_cache(df: DataFrame, table_fqn: str) -> Path:
    """Persist an in-memory DataFrame locally (survives kernel restart)."""
    path = local_cache_path(table_fqn)
    try:
        _write_df_to_local_parquet(df, path)
        _read_progress(f"save_table_cache: {table_fqn} -> {path}")
    except Exception as exc:
        raise RuntimeError(f"save_table_cache failed for {table_fqn}: {exc}") from exc
    return path


def load_table_cache(spark: SparkSession, table_fqn: str) -> DataFrame:
    """Load a table from local parquet cache written by read_table or save_table_cache."""
    path = local_cache_path(table_fqn)
    if not path.is_dir() or not any(path.glob("*.parquet")):
        raise FileNotFoundError(f"No local cache for {table_fqn} at {path}")
    return _read_parquet_cache_dir(spark, path, table_fqn)


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
            _read_progress(f"read_table: {table_fqn} backend={backend}")
            if backend == "pyspark":
                df = _read_delta_via_pyspark(spark, path)
            elif backend == "deltalake":
                df = _read_delta_via_deltalake(spark, path)
            else:
                cache_dir = None
                if (
                    _local_table_cache_enabled()
                    and not _skip_auto_cache_on_read()
                    and not _local_cache_refresh()
                ):
                    cache_dir = local_cache_path(table_fqn)
                df = _read_delta_via_deltalake_scan(spark, path, cache_dir=cache_dir)
                if cache_dir is None or not cache_file_exists(cache_dir):
                    if not _skip_auto_cache_on_read():
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


def _merge_chunk_rows() -> int:
    return max(5_000, int(os.environ.get("MEDALLION_MERGE_CHUNK_ROWS", "50000")))


def _merge_coalesce_parts() -> int:
    return max(1, int(os.environ.get("MEDALLION_MERGE_COALESCE", "1")))


def apply_local_merge_spark_conf(spark: SparkSession) -> None:
    """Call before building write_df locally — avoids broadcast joins that crash Python workers."""
    if not _use_path_reads():
        return
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    spark.conf.set("spark.sql.adaptive.autoBroadcastJoinThreshold", "-1")
    spark.conf.set("spark.sql.adaptive.enabled", "false")
    spark.conf.set("spark.sql.join.preferSortMergeJoin", "true")
    parts = os.environ.get("MEDALLION_MERGE_SHUFFLE_PARTITIONS", "4")
    spark.conf.set("spark.sql.shuffle.partitions", parts)


def merge_staging_path(table_fqn: str) -> Path:
    safe = table_fqn.replace(".", "__")
    return Path.home() / ".fabric" / "merge_staging" / safe


def break_lineage_local(spark: SparkSession, df: DataFrame, label: str) -> DataFrame:
    """Write/read local parquet so later joins are not stuck on an old broadcast plan."""
    apply_local_merge_spark_conf(spark)
    path = Path.home() / ".fabric" / "merge_staging" / "_break" / label
    df = _sanitize_df_for_export(df)
    _read_progress(f"break_lineage_local: {label} -> {path}")
    _write_df_to_local_parquet(df.coalesce(_merge_coalesce_parts()), path)
    return spark.read.parquet(str(path))


def publish_merge_staging(df: DataFrame, table_fqn: str) -> Path:
    """Call at end of transform — merge_incremental reads this (no Spark collect on join plan)."""
    apply_local_merge_spark_conf(df.sparkSession)
    path = merge_staging_path(table_fqn)
    df = _sanitize_df_for_export(df)
    _read_progress(f"publish_merge_staging: {table_fqn} -> {path}")
    _write_df_to_local_parquet(df.coalesce(_merge_coalesce_parts()), path)
    return path


def _parquet_staging_batches(staging: Path):
    import pyarrow.parquet as pq

    chunk_rows = _merge_chunk_rows()
    if not staging.is_dir() or not any(staging.glob("*.parquet")):
        return
    for parquet_file in sorted(staging.glob("*.parquet")):
        for batch in pq.ParquetFile(parquet_file).iter_batches(batch_size=chunk_rows):
            yield batch.to_pandas()


def _sanitize_df_for_export(df: DataFrame) -> DataFrame:
    from pyspark.sql import functions as F
    from pyspark.sql.types import DecimalType

    for field in df.schema.fields:
        if isinstance(field.dataType, DecimalType):
            df = df.withColumn(field.name, F.col(field.name).cast("double"))
    return df


def _spark_to_pandas_batches(df: DataFrame):
    """Fallback only — prefer publish_merge_staging + _parquet_staging_batches."""
    staging = merge_staging_path("_adhoc_fallback")
    publish_merge_staging(df, "_adhoc_fallback")
    yield from _parquet_staging_batches(staging)


def _spark_df_to_deltalake(df: DataFrame, path: str, *, mode: str = "overwrite") -> None:
    from deltalake import write_deltalake

    from common.fabric_storage import fabric_storage_options, refresh_abfs_token_env

    refresh_abfs_token_env()
    opts = fabric_storage_options()
    _read_debug(f"deltalake write mode={mode} {path} (Spark -> pandas chunks)...")
    for i, pdf in enumerate(_spark_to_pandas_batches(df)):
        write_mode = mode if i == 0 else "append"
        _read_debug(f"deltalake write chunk {i + 1} ({len(pdf)} rows) mode={write_mode}")
        write_deltalake(
            path,
            pdf,
            mode=write_mode,
            schema_mode="overwrite" if i == 0 else "merge",
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
        _read_progress(f"merge_incremental: deltalake path {path}")
        predicate = _merge_predicate(merge_key)
        staging = merge_staging_path(table_name)
        if not staging.is_dir() or not any(staging.glob("*.parquet")):
            raise RuntimeError(
                f"No merge staging parquet at {staging}. "
                "Re-run the transform cell (apply_local_merge_spark_conf first); "
                "end with publish_merge_staging(write_df, TABLE_CURSOR_SILVER)."
            )
        table_exists = _deltalake_table_exists(path)
        dt = None
        chunk_no = 0
        for source in _parquet_staging_batches(staging):
            chunk_no += 1
            _read_progress(f"merge_incremental: chunk {chunk_no} ({len(source)} rows)...")
            if not table_exists:
                write_deltalake(
                    path,
                    source,
                    mode="overwrite",
                    schema_mode="overwrite",
                    storage_options=opts,
                )
                table_exists = True
                dt = DeltaTable(path, storage_options=opts)
                continue
            assert dt is not None
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
        if chunk_no == 0:
            _read_progress("merge_incremental: no rows to merge")
        else:
            _read_progress("merge_incremental: deltalake merge done")
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
