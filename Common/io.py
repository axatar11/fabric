"""OneLake read/write, local parquet cache, and merge staging for pipeline notebooks."""

from __future__ import annotations

import os
import shutil
from pathlib import Path
from typing import TYPE_CHECKING

from Common.config import ONELAKE_HOST, WORKSPACEID

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


def onelake_table_path(lakehouse_id: str, *subpath: str) -> str:
    """Build ``abfss://{workspace}@onelake…/{lakehouse}/Tables/…`` path."""
    suffix = ".Lakehouse" if os.environ.get("ONELAKE_LAKEHOUSE_SUFFIX", "").lower() in (
        "1",
        "true",
        "yes",
    ) else ""
    base = f"abfss://{WORKSPACEID}@{ONELAKE_HOST}/{lakehouse_id}{suffix}/Tables"
    return "/".join([base, *subpath])


def _default_gold_table_fqn(table_name: str) -> str:
    from Common import config

    db = getattr(config, "GOLD_DATABASE", "AI_Medallion")
    schema = getattr(config, "GOLD_SCHEMA", "Gold")
    return f"{db}.{schema}.{table_name}"


def delta_path_for_table(table_fqn: str) -> str | None:
    """Resolve a catalog FQN (``TABLE_*``) to a OneLake Delta abfss path."""
    from Common import config

    med = config.LAKEHOUSE_AI_MEDALLION_ID
    bronze = config.LAKEHOUSE_AI_MEDALLION_BRONZE_ID
    silver = config.LAKEHOUSE_AI_MEDALLION_SILVER_ID
    mapping = {
        config.TABLE_CURSOR_BRONZE: config.ONELAKE_CURSOR_BRONZE_PATH,
        config.TABLE_PURVIEW_COPILOT_BRONZE: config.ONELAKE_PURVIEW_COPILOT_BRONZE_PATH,
        config.TABLE_HC_BRONZE: onelake_table_path(bronze, "dbo/HC_Bronze_Historical"),
        config.TABLE_OKTA_BRONZE: onelake_table_path(bronze, "dbo/CoreOktaUser"),
        config.TABLE_COUNTRY: onelake_table_path(
            config.LAKEHOUSE_AI_MEDALLION_SILVER_DB_ID,
            "dbo/CoreCountry_Excel_Hours_Historic",
        ),
        config.TABLE_HC_SILVER: onelake_table_path(med, "Silver/hc_silver_historical"),
        config.TABLE_OKTA_SILVER: onelake_table_path(med, "Silver/okta_user_for_ai"),
        config.TABLE_CURSOR_SILVER: onelake_table_path(med, "Silver/cursor_usage"),
        config.TABLE_CURSOR_OKTA_REJECT: onelake_table_path(
            med, "Silver/cursor_usage_okta_reject"
        ),
        config.TABLE_PURVIEW_COPILOT_SILVER: onelake_table_path(
            med, "Silver/purview_copilot_usage"
        ),
        config.TABLE_PURVIEW_COPILOT_OKTA_REJECT: onelake_table_path(
            med, "Silver/purview_copilot_usage_okta_reject"
        ),
    }
    active_fqn = getattr(
        config, "TABLE_FACT_CURSOR_ACTIVE", _default_gold_table_fqn("fact_cursor_active")
    )
    mapping[active_fqn] = onelake_table_path(
        med,
        os.environ.get("ONELAKE_GOLD_FACT_CURSOR_ACTIVE", "Gold/fact_cursor_active"),
    )
    onboard_fqn = getattr(
        config, "TABLE_FACT_CURSOR_ONBOARD", _default_gold_table_fqn("fact_cursor_onboard")
    )
    mapping[onboard_fqn] = onelake_table_path(
        med,
        os.environ.get("ONELAKE_GOLD_FACT_CURSOR_ONBOARD", "Gold/fact_cursor_onboard"),
    )
    copilot_active_fqn = getattr(
        config, "TABLE_FACT_COPILOT_ACTIVE", _default_gold_table_fqn("fact_copilot_active")
    )
    mapping[copilot_active_fqn] = onelake_table_path(
        med,
        os.environ.get("ONELAKE_GOLD_FACT_COPILOT_ACTIVE", "Gold/fact_copilot_active"),
    )
    mapping[config.TABLE_INGEST_FREEZE_CONTROL] = onelake_table_path(
        med, "Gold/ingest_freeze_control"
    )
    try:
        from Common import local_settings as ls  # type: ignore

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


def _scan_error_message(exc: BaseException) -> str:
    return str(exc).lower()


def _scan_error_is_auth(exc: BaseException) -> bool:
    msg = _scan_error_message(exc)
    return any(t in msg for t in ("401", "403", "unauthorized", "forbidden"))


def _scan_error_is_transport(exc: BaseException) -> bool:
    """Stream/connect failures while reading parquet from OneLake blob (not fixable by token refresh)."""
    msg = _scan_error_message(exc)
    return any(
        t in msg
        for t in (
            "body error",
            "request or response body",
            "response body error",
            "error sending request",
            "client error (connect)",
            "operation timed out",
            "connection reset",
            "broken pipe",
        )
    )


def _scan_retriable_error(exc: BaseException) -> bool:
    if _scan_error_is_auth(exc):
        return True
    if _scan_error_is_transport(exc):
        return True
    msg = _scan_error_message(exc)
    return any(t in msg for t in ("microsoftazure", "503", "timeout", "timed out"))


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


def _dataframe_is_empty(df: DataFrame) -> bool:
    try:
        return df.isEmpty()
    except AttributeError:
        return df.rdd.isEmpty()


def _write_empty_local_parquet_dir(path: Path, spark_schema: "StructType") -> None:
    """PyArrow empty parquet (Spark coalesce(1) often fails on 0-row plans on Windows)."""
    import pandas as pd

    if path.exists():
        shutil.rmtree(path, ignore_errors=True)
    path.mkdir(parents=True, exist_ok=True)
    pdf = pd.DataFrame(columns=[f.name for f in spark_schema.fields])
    _write_pandas_to_parquet_dir(pdf, path, spark_schema=spark_schema)


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

        from Common.fabric_storage import fabric_storage_options, refresh_abfs_token_env

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
    wipe_cache: bool = True,
) -> tuple[DataFrame, int]:
    """Stream deltalake scan batches into Spark (chunked to limit RAM on large tables)."""
    import time

    row_count = 0
    rows_committed = 0
    skip_rows = 0
    next_progress = 50_000
    chunk_limit = max(10_000, int(os.environ.get("MEDALLION_SCAN_CHUNK_ROWS", "75000")))
    heartbeat_sec = max(
        5, int(os.environ.get("MEDALLION_SCAN_HEARTBEAT_SEC", "20"))
    )
    last_heartbeat = time.monotonic()
    pending: list = []
    pending_rows = 0
    spark_df: DataFrame | None = None
    cache_part = 0
    if cache_dir is not None:
        cache_dir.parent.mkdir(parents=True, exist_ok=True)
        if wipe_cache:
            _clear_scan_cache_dir(cache_dir)
            cache_dir.mkdir(parents=True, exist_ok=True)
        elif not cache_dir.is_dir():
            cache_dir.mkdir(parents=True, exist_ok=True)
        if wipe_cache or not _scan_rows_committed_path(cache_dir).is_file():
            skip_rows = 0
            cache_part = len(list(cache_dir.glob("part-*.parquet")))
        else:
            skip_rows = _read_scan_rows_committed(cache_dir)
            cache_part = len(list(cache_dir.glob("part-*.parquet")))
            if skip_rows > 0:
                _read_progress(
                    f"read_table: resume OneLake scan from row {skip_rows:,} "
                    f"({cache_part} parquet part(s) already on disk)"
                )
        _write_local_cache_schema(cache_dir, spark_schema)

    cache_only = cache_dir is not None

    def flush_chunk() -> None:
        nonlocal spark_df, pending, pending_rows, cache_part, rows_committed
        if not pending:
            return
        pdf = _pandas_for_spark_schema(_batches_to_pandas(pending), spark_schema)
        chunk_rows = len(pdf)
        if cache_dir is not None:
            part_path = cache_dir / f"part-{cache_part:05d}.parquet"
            _write_parquet_with_spark_schema(pdf, spark_schema, part_path)
            cache_part += 1
            rows_committed += chunk_rows
            _write_scan_rows_committed(cache_dir, rows_committed)
        if not cache_only:
            part = spark.createDataFrame(pdf, schema=spark_schema)
            spark_df = (
                part
                if spark_df is None
                else spark_df.unionByName(part, allowMissingColumns=True)
            )
        pending = []
        pending_rows = 0

    for batch in reader:
        if skip_rows > 0:
            n = batch.num_rows
            if n <= skip_rows:
                skip_rows -= n
                continue
            batch = _slice_record_batch(batch, skip_rows)
            skip_rows = 0
        pending.append(batch)
        pending_rows += batch.num_rows
        row_count += batch.num_rows
        now = time.monotonic()
        if row_count == batch.num_rows or now - last_heartbeat >= heartbeat_sec:
            _read_progress(
                f"read_table: scan streaming… {row_count:,} rows from OneLake so far"
            )
            last_heartbeat = now
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
    total_rows = rows_committed if cache_dir is not None else row_count
    if cache_only:
        return spark.createDataFrame([], schema=spark_schema), total_rows
    if spark_df is None:
        return spark.createDataFrame([], schema=spark_schema), 0
    return spark_df, total_rows


def _read_delta_via_deltalake_scan(
    spark: SparkSession,
    path: str,
    *,
    cache_dir: Path | None = None,
    table_fqn: str | None = None,
    scan_predicate: str | None = None,
    scan_columns: tuple[str, ...] | list[str] | None = None,
) -> DataFrame:
    """Azure CLI + deltalake DataFusion scan (column mapping / deletion vectors) -> Spark."""
    import time

    from deltalake import DeltaTable

    from Common.fabric_storage import fabric_storage_options, refresh_abfs_token_env

    max_retries = max(1, int(os.environ.get("MEDALLION_SCAN_RETRIES", "6")))
    max_transport = max(1, int(os.environ.get("MEDALLION_SCAN_TRANSPORT_RETRIES", "6")))
    transport_failures = 0
    last_error: BaseException | None = None
    for attempt in range(1, max_retries + 1):
        force_token = attempt > 1 and transport_failures == 0
        refresh_abfs_token_env(force=force_token)
        opts = fabric_storage_options(force=force_token)
        _read_progress(
            f"read_table: deltalake scan {path[:80]}… (pass {attempt}/{max_retries})"
        )
        limit = opts.get("OBJECT_STORE_CONCURRENCY_LIMIT")
        if limit:
            _read_debug(
                f"read_table: OneLake object-store concurrency limit={limit} "
                "(raise MEDALLION_OBJECT_STORE_CONCURRENCY if reads are too slow)"
            )
        try:
            while transport_failures < max_transport:
                try:
                    dt = DeltaTable(path, storage_options=opts)
                    spark_schema = _deltalake_schema_to_spark(dt)
                    scan_kwargs: dict = {}
                    if scan_predicate:
                        scan_kwargs["predicate"] = scan_predicate
                        _read_progress(f"read_table: scan predicate={scan_predicate!r}")
                    if scan_columns:
                        scan_kwargs["columns"] = list(scan_columns)
                        _read_progress(
                            f"read_table: scan column projection ({len(scan_columns)} cols)"
                        )
                    _read_progress("read_table: deltalake scan() streaming batches…")
                    reader = dt.scan(**scan_kwargs) if scan_kwargs else dt.scan()
                    wipe_cache = _scan_should_wipe_cache(
                        cache_dir, transport_failures=transport_failures
                    )
                    df, row_count = _scan_batches_to_spark(
                        spark,
                        reader,
                        spark_schema,
                        cache_dir=cache_dir,
                        wipe_cache=wipe_cache,
                    )
                    _read_progress(f"read_table: scan finished ({row_count:,} rows)")
                    if cache_dir is not None:
                        if row_count == 0 and not cache_file_exists(cache_dir):
                            _write_empty_local_parquet_dir(cache_dir, spark_schema)
                        if cache_file_exists(cache_dir):
                            _mark_cache_complete(cache_dir, row_count)
                            _read_progress(
                                f"read_table: opening local parquet cache ({row_count:,} rows) -> {cache_dir}"
                            )
                            return _read_parquet_cache_dir(
                                spark, cache_dir, table_fqn or ""
                            )
                    return df
                except Exception as exc:
                    last_error = exc
                    if not _scan_retriable_error(exc):
                        raise
                    if _scan_error_is_transport(exc):
                        transport_failures += 1
                        committed = (
                            _read_scan_rows_committed(cache_dir) if cache_dir else 0
                        )
                        if transport_failures >= max_transport:
                            partial = ""
                            if committed > 0 and cache_dir is not None:
                                partial = (
                                    f"\nPartial cache: {committed:,} rows at {cache_dir}. "
                                    "Re-run with REBUILD_MEDALLION_CACHE=False "
                                    "(scan resumes from committed rows; same as Cursor reads)."
                                )
                            raise RuntimeError(
                                "OneLake parquet download failed mid-stream (HTTP body/connect error). "
                                "Not fixed by az login alone.\n"
                                f"Error: {exc!s}\n"
                                f"Cache root: {Path.home() / '.fabric' / 'medallion_cache'}"
                                f"{partial}\n"
                                "Use MEDALLION_OBJECT_STORE_CONCURRENCY=1 on Windows; "
                                "re-run to resume partial cache (do not delete part-*.parquet)."
                            ) from exc
                        wait = min(5 * transport_failures, 20)
                        _read_progress(
                            f"read_table: OneLake stream interrupted ({transport_failures}/{max_transport}), "
                            f"resume partial cache in {wait}s…"
                        )
                        time.sleep(wait)
                        continue
                    raise
            continue
        except RuntimeError:
            raise
        except Exception as exc:
            last_error = exc
            if attempt >= max_retries:
                raise
            wait = min(2**attempt, 15)
            _read_debug(
                f"read_table: scan failed ({exc!s}); refresh token and retry in {wait}s..."
            )
            time.sleep(wait)
    assert last_error is not None
    raise last_error


def _read_delta_via_deltalake(spark: SparkSession, path: str) -> DataFrame:
    """Azure CLI + deltalake (Fabric endpoint) -> Spark DataFrame."""
    from deltalake import DeltaTable

    from Common.fabric_storage import fabric_storage_options

    _read_debug(f"read_table: deltalake open {path}")
    dt = DeltaTable(path, storage_options=fabric_storage_options())
    _read_debug("read_table: deltalake loading data...")
    # PySpark createDataFrame(pyarrow Table) can yield all-NULL rows; pandas bridge is safer.
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
    from Common.fabric_storage import apply_azure_cli_abfs_conf

    _read_debug(f"read_table: pyspark delta.load {path}")
    apply_azure_cli_abfs_conf(spark)
    return spark.read.format("delta").load(path)


def _local_table_cache_enabled() -> bool:
    flag = os.environ.get("MEDALLION_LOCAL_TABLE_CACHE", "1")
    return flag.lower() not in ("0", "false", "no")


def _local_cache_refresh() -> bool:
    return os.environ.get("MEDALLION_CACHE_REFRESH", "").lower() in ("1", "true", "yes")


def sync_rebuild_medallion_cache_env(rebuild: bool) -> None:
    """Notebook knob: True = scan OneLake and refresh local cache; False = use cache."""
    os.environ["MEDALLION_LOCAL_TABLE_CACHE"] = "1"
    os.environ.setdefault("MEDALLION_SCAN_TRANSPORT_RETRIES", "6")
    os.environ.setdefault("MEDALLION_OBJECT_STORE_CONCURRENCY", "1")
    if rebuild:
        os.environ["MEDALLION_CACHE_REFRESH"] = "1"
    else:
        os.environ.pop("MEDALLION_CACHE_REFRESH", None)


def cache_file_exists(cache_dir: Path) -> bool:
    return cache_dir.is_dir() and any(cache_dir.glob("*.parquet"))


def _cache_complete_marker(cache_dir: Path) -> Path:
    return cache_dir / "_cache_complete"


def _scan_rows_committed_path(cache_dir: Path) -> Path:
    return cache_dir / "_scan_rows_committed"


def cache_read_ready(cache_dir: Path) -> bool:
    """True when local parquet cache finished a full OneLake scan (not a partial download)."""
    return cache_file_exists(cache_dir) and _cache_complete_marker(cache_dir).is_file()


def _scan_resume_partial_enabled() -> bool:
    return os.environ.get("MEDALLION_SCAN_RESUME_PARTIAL", "1").lower() not in (
        "0",
        "false",
        "no",
    )


def _read_scan_rows_committed(cache_dir: Path) -> int:
    try:
        return int(_scan_rows_committed_path(cache_dir).read_text().strip())
    except (OSError, ValueError):
        return 0


def _write_scan_rows_committed(cache_dir: Path, rows: int) -> None:
    _scan_rows_committed_path(cache_dir).write_text(str(rows))


def _mark_cache_complete(cache_dir: Path, row_count: int) -> None:
    _cache_complete_marker(cache_dir).write_text(str(row_count))
    _scan_rows_committed_path(cache_dir).unlink(missing_ok=True)


def _clear_scan_cache_dir(cache_dir: Path) -> None:
    if cache_dir.exists():
        shutil.rmtree(cache_dir, ignore_errors=True)


def _scan_should_wipe_cache(cache_dir: Path | None, *, transport_failures: int) -> bool:
    if cache_dir is None:
        return False
    if _local_cache_refresh():
        return True
    if transport_failures > 0 and _scan_resume_partial_enabled():
        return False
    return _read_scan_rows_committed(cache_dir) == 0


def _slice_record_batch(batch, offset: int):
    if offset <= 0:
        return batch
    return batch.slice(offset)


def local_cache_path(table_fqn: str) -> Path:
    override = os.environ.get("MEDALLION_LOCAL_CACHE_DIR")
    root = Path(override) if override else Path.home() / ".fabric" / "medallion_cache"
    safe = table_fqn.replace(".", "__")
    return root / safe


def local_cache_path_for_read(
    table_fqn: str,
    *,
    scan_predicate: str | None = None,
) -> Path:
    """Separate cache dirs per deltalake scan predicate (month-close vs full/min-date)."""
    base = local_cache_path(table_fqn)
    if not scan_predicate or not str(scan_predicate).strip():
        return base
    import hashlib

    digest = hashlib.sha256(scan_predicate.strip().encode("utf-8")).hexdigest()[:16]
    return base.parent / f"{base.name}__scan_{digest}"


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
    if not cache_read_ready(path):
        return None
    _read_progress(f"read_table: cache hit {table_fqn}")
    return _read_parquet_cache_dir(spark, path, table_fqn)


def _local_parquet_use_spark_writer() -> bool:
    flag = os.environ.get("MEDALLION_STAGING_SPARK_PARQUET", "1")
    return flag.lower() not in ("0", "false", "no")


def _staging_prefer_chunked_collect() -> bool:
    """Driver-side staging (avoids JVM parquet OOM on large local Windows runs)."""
    return not _local_parquet_use_spark_writer()


def _staging_collect_partitions() -> int:
    """Partitions for driver-side staging collect (never 1 on large Purview/Cursor joins)."""
    raw = os.environ.get(
        "MEDALLION_STAGING_COLLECT_PARTITIONS",
        os.environ.get("MEDALLION_STAGING_WRITE_PARTITIONS", "8"),
    )
    return max(4, int(raw))


def _write_df_via_local_iterator_chunks(df: DataFrame, path: Path) -> None:
    """Driver-side chunked materialization (avoids one large collectToPython on Windows)."""
    import pandas as pd

    chunk_rows = max(5_000, int(os.environ.get("MEDALLION_STAGING_COLLECT_ROWS", "25000")))
    spark_schema = df.schema
    columns = [field.name for field in spark_schema.fields]
    if path.exists():
        shutil.rmtree(path, ignore_errors=True)
    path.mkdir(parents=True, exist_ok=True)
    buffer: list[dict] = []
    part_idx = 0

    def flush() -> None:
        nonlocal buffer, part_idx
        if not buffer:
            return
        pdf = pd.DataFrame(buffer, columns=columns)
        out_file = path / f"part-{part_idx:05d}.parquet"
        _write_parquet_with_spark_schema(pdf, spark_schema, out_file)
        if part_idx == 0:
            _write_local_cache_schema(path, spark_schema)
        buffer = []
        part_idx += 1

    prefetch = os.environ.get("MEDALLION_STAGING_PREFETCH_PARTITIONS", "1").lower() in (
        "1",
        "true",
        "yes",
    )
    def _collect_rows(row_iter) -> None:
        try:
            for row in row_iter:
                buffer.append(row.asDict())
                if len(buffer) >= chunk_rows:
                    flush()
        except Exception as exc:
            msg = str(exc).lower()
            if "connection refused" in msg or "connectionreset" in msg:
                raise RuntimeError(
                    "Spark JVM disconnected during merge staging collect (often OOM or crash). "
                    "Restart the kernel, re-run from bootstrap; keep "
                    "MEDALLION_STAGING_COLLECT_PARTITIONS=8+, "
                    "MEDALLION_STAGING_CHECKPOINT=0 (localCheckpoint OOMs on large Purview joins). "
                    "Do not call break_lineage_local before publish_merge_staging."
                ) from exc
            raise

    partition_wise = os.environ.get("MEDALLION_STAGING_PARTITION_WISE", "1").lower() in (
        "1",
        "true",
        "yes",
    )
    n_parts = df.rdd.getNumPartitions()
    if partition_wise and n_parts > 1:
        from pyspark.sql import functions as F

        for pid in range(n_parts):
            _read_progress(
                f"materialize staging: driver collect partition {pid + 1}/{n_parts}"
            )
            part_df = df.filter(F.spark_partition_id() == F.lit(pid))
            try:
                row_iter = part_df.toLocalIterator(prefetchPartitions=False)
            except TypeError:
                row_iter = part_df.toLocalIterator()
            _collect_rows(row_iter)
    else:
        try:
            row_iter = df.toLocalIterator(prefetchPartitions=prefetch)
        except TypeError:
            row_iter = df.toLocalIterator()
        _collect_rows(row_iter)
    flush()
    if not any(path.glob("*.parquet")):
        raise RuntimeError("localIterator produced no parquet output")


def _write_spark_parquet_dir(df: DataFrame, path: Path) -> None:
    """JVM parquet to file:// (no Python worker / Arrow collect — safe for large join plans)."""
    if path.exists():
        shutil.rmtree(path, ignore_errors=True)
    path.mkdir(parents=True, exist_ok=True)
    target = path.resolve().as_uri()
    df.write.mode("overwrite").parquet(target)


def _staging_jvm_partition_counts() -> list[int]:
    start = max(
        4,
        int(
            os.environ.get(
                "MEDALLION_COLLAPSE_PARTITIONS",
                os.environ.get(
                    "MEDALLION_STAGING_WRITE_PARTITIONS",
                    str(max(8, _staging_write_partitions())),
                ),
            )
        ),
    )
    counts: list[int] = []
    for n in (start, max(start * 2, 16), 32, 64):
        if n not in counts:
            counts.append(n)
    return counts


def _write_staging_jvm_parquet(df: DataFrame, path: Path) -> bool:
    """Try JVM parquet only (no driver toLocalIterator). Returns True on success."""
    df = _sanitize_df_for_export(df)
    last_exc: BaseException | None = None
    for parts in _staging_jvm_partition_counts():
        try:
            staged = df.repartition(parts)
            _read_progress(
                f"materialize staging: Spark JVM parquet ({parts} partitions)"
            )
            _write_spark_parquet_dir(staged, path)
            return True
        except Exception as exc:
            last_exc = exc
            _read_progress(f"staging JVM parquet failed ({parts} parts): {exc!s}")
    if last_exc is not None:
        _read_debug(f"staging JVM parquet exhausted: {last_exc!s}")
    return False


def collapse_lineage_to_local_parquet(
    spark: SparkSession, df: DataFrame, label: str
) -> DataFrame:
    """One JVM materialize of a heavy plan, then read parquet (short lineage for publish)."""
    apply_local_merge_spark_conf(spark)
    path = Path.home() / ".fabric" / "merge_staging" / "_collapse" / label
    if not _write_staging_jvm_parquet(df, path):
        raise RuntimeError(
            f"collapse_lineage_to_local_parquet failed for {label!r} (often Java heap space). "
            "Use COPILOT_CLOSE_MONTH_NO for one-month bronze, PYSPARK_DRIVER_MEMORY=12g+, and "
            "parse_purview_copilot_bronze_base → collapse before Okta/HC joins. "
            "Do not use driver toLocalIterator on the full Purview join plan."
        )
    _read_progress(f"collapse_lineage: read back local parquet ({label})")
    return spark.read.parquet(str(path))


def _write_df_to_local_parquet(
    df: DataFrame, path: Path, *, staging: bool = False
) -> None:
    """Materialize locally for cache or merge staging."""
    if not staging and _dataframe_is_empty(df):
        _write_empty_local_parquet_dir(path, df.schema)
        return
    if staging and _use_path_reads():
        if _write_staging_jvm_parquet(df, path):
            return
        mode = os.environ.get("MEDALLION_STAGING_MODE", "auto").lower()
        if mode in ("jvm_only", "spark_only"):
            raise RuntimeError(
                "Merge staging JVM parquet failed and MEDALLION_STAGING_MODE forbids "
                "driver collect. Fix: COPILOT_CLOSE_MONTH_NO, more driver memory, "
                "parse_purview_copilot_bronze_base then collapse before joins."
            )
    if staging and _use_path_reads() and _staging_prefer_chunked_collect():
        parts = _staging_collect_partitions()
        df = _sanitize_df_for_export(df)
        # localCheckpoint(eager=True) OOMs on large Purview silver (#146: no JVM materialize here).
        if os.environ.get("MEDALLION_STAGING_CHECKPOINT", "0").lower() in (
            "1",
            "true",
            "yes",
        ):
            df = _materialize_local_checkpoint(df.sparkSession, df)
        df = df.repartition(parts)
        _read_progress(
            f"materialize staging: chunked driver collect ({parts} partitions, "
            "MEDALLION_STAGING_SPARK_PARQUET=0)"
        )
        _write_df_via_local_iterator_chunks(df, path)
        return
    if staging:
        df = _df_for_staging_write(df)
    else:
        parts = max(1, int(os.environ.get("MEDALLION_MERGE_COALESCE", "1")))
        df = _sanitize_df_for_export(df.coalesce(parts))
    if _use_path_reads() and _local_parquet_use_spark_writer():
        part_candidates: list[int] = []
        if staging:
            base = _staging_write_partitions()
            part_candidates = [base, max(base, 8), 16, 32]
        else:
            part_candidates = [max(1, int(os.environ.get("MEDALLION_MERGE_COALESCE", "1"))), 8]
        seen_parts: set[int] = set()
        for parts in part_candidates:
            if parts in seen_parts:
                continue
            seen_parts.add(parts)
            try:
                staged = _sanitize_df_for_export(df)
                staged = staged.repartition(parts) if parts > 1 else staged.coalesce(1)
                _write_spark_parquet_dir(staged, path)
                return
            except Exception as exc:
                _read_debug(
                    f"local Spark parquet write failed ({parts} parts, {exc!s}); retrying"
                )
        if staging:
            try:
                spark = df.sparkSession
                df = _materialize_local_checkpoint(spark, df)
                _write_spark_parquet_dir(
                    df.repartition(_staging_write_partitions()), path
                )
                return
            except Exception as exc2:
                _read_debug(
                    f"localCheckpoint parquet write failed ({exc2!s}); "
                    "falling back to chunked driver collect"
                )
    spark = df.sparkSession
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")
    try:
        _write_df_via_local_iterator_chunks(df, path)
        return
    except Exception as exc:
        _read_debug(f"chunked localIterator write failed ({exc!s}); falling back to toPandas")
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
    """Write a DataFrame to ``medallion_cache`` (explicit cache; survives kernel restart)."""
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
                if _local_table_cache_enabled() and not _skip_auto_cache_on_read():
                    cache_dir = local_cache_path(table_fqn)
                    if cache_read_ready(cache_dir) and not _local_cache_refresh():
                        return _read_parquet_cache_dir(spark, cache_dir, table_fqn)
                df = _read_delta_via_deltalake_scan(
                    spark, path, cache_dir=cache_dir, table_fqn=table_fqn
                )
                if cache_dir is None or not cache_read_ready(cache_dir):
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


def _merge_key_column_name(merge_key: str) -> str:
    return merge_key.strip("`")


def _dedupe_pandas_merge_source(source, merge_key: str):
    """Delta MERGE requires at most one source row per merge key."""
    key = _merge_key_column_name(merge_key)
    if key not in getattr(source, "columns", ()):
        return source
    before = len(source)
    out = source.drop_duplicates(subset=[key], keep="last")
    dropped = before - len(out)
    if dropped:
        _read_progress(
            f"merge_incremental: dropped {dropped} duplicate {key} row(s) in chunk"
        )
    return out


def _merge_chunk_rows() -> int:
    return max(5_000, int(os.environ.get("MEDALLION_MERGE_CHUNK_ROWS", "50000")))


def _merge_coalesce_parts() -> int:
    return max(1, int(os.environ.get("MEDALLION_MERGE_COALESCE", "1")))


def _staging_write_partitions() -> int:
    """Partitions for local break/staging parquet (coalesce(1) often crashes Python workers on large Windows runs)."""
    override = os.environ.get("MEDALLION_STAGING_WRITE_PARTITIONS")
    if override is not None:
        return max(1, int(override))
    coalesce = os.environ.get("MEDALLION_MERGE_COALESCE")
    if coalesce is not None and int(coalesce) > 1:
        return max(1, int(coalesce))
    return max(4, int(os.environ.get("MEDALLION_MERGE_SHUFFLE_PARTITIONS", "4")))


def _df_for_staging_write(df: DataFrame) -> DataFrame:
    df = _sanitize_df_for_export(df)
    parts = max(4, _staging_write_partitions())
    return df.repartition(parts)


def apply_local_merge_spark_conf(spark: SparkSession) -> None:
    """Call before building write_df locally — avoids broadcast joins that crash Python workers."""
    if not _use_path_reads():
        return
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    spark.conf.set("spark.sql.adaptive.autoBroadcastJoinThreshold", "-1")
    spark.conf.set("spark.sql.adaptive.enabled", "false")
    spark.conf.set("spark.sql.join.preferSortMergeJoin", "true")
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")
    spark.conf.set("spark.sql.execution.arrow.pyspark.fallback.enabled", "false")
    parts = os.environ.get("MEDALLION_MERGE_SHUFFLE_PARTITIONS", "4")
    spark.conf.set("spark.sql.shuffle.partitions", parts)
    # Large AuditData bronze parquet: vectorized reader can OOM on Windows local Spark.
    spark.conf.set("spark.sql.parquet.enableVectorizedReader", "false")
    spark.conf.set(
        "spark.sql.files.maxPartitionBytes",
        os.environ.get("MEDALLION_MAX_PARTITION_BYTES", "67108864"),
    )


def _local_checkpoint_dir() -> Path:
    path = Path.home() / ".fabric" / "spark_checkpoint"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _materialize_local_checkpoint(spark: SparkSession, df: DataFrame) -> DataFrame:
    spark.sparkContext.setCheckpointDir(str(_local_checkpoint_dir()))
    _read_progress("materialize: localCheckpoint (eager)")
    return df.localCheckpoint(eager=True)


def merge_staging_path(table_fqn: str) -> Path:
    """Local folder for merge/overwrite staging parquet (`~/.fabric/merge_staging/…`)."""
    safe = table_fqn.replace(".", "__")
    return Path.home() / ".fabric" / "merge_staging" / safe


def read_table_broken_lineage(
    spark: SparkSession,
    table_fqn: str,
    label: str,
    *,
    scan_predicate: str | None = None,
    scan_columns: tuple[str, ...] | list[str] | None = None,
) -> DataFrame:
    """``read_table`` then materialize for joins — prefer local cache parquet over ``break_lineage`` collect."""
    cache_path = local_cache_path_for_read(table_fqn, scan_predicate=scan_predicate)
    if scan_predicate:
        _read_progress(
            f"read_table_broken_lineage: scoped OneLake scan -> {cache_path.name}"
        )
    lake_path = delta_path_for_table(table_fqn)
    scan_backend = bool(
        lake_path and _use_path_reads() and _resolve_read_backend(lake_path) == "scan"
    )

    if (
        _local_table_cache_enabled()
        and not _local_cache_refresh()
        and cache_read_ready(cache_path)
    ):
        _read_progress(f"read_table_broken_lineage: cache hit ({label})")
        return _read_parquet_cache_dir(spark, cache_path, table_fqn)

    if scan_backend and _local_table_cache_enabled():
        if _local_cache_refresh() or not cache_read_ready(cache_path):
            _read_progress(f"read_table_broken_lineage: scan -> local cache ({table_fqn})")
            _read_delta_via_deltalake_scan(
                spark,
                lake_path,
                cache_dir=cache_path,
                table_fqn=table_fqn,
                scan_predicate=scan_predicate,
                scan_columns=scan_columns,
            )
            if cache_read_ready(cache_path):
                _read_progress(
                    f"read_table_broken_lineage: opened cache after scan ({label})"
                )
                return _read_parquet_cache_dir(spark, cache_path, table_fqn)

    df = read_table(spark, table_fqn)
    if _local_table_cache_enabled() and cache_read_ready(cache_path):
        _read_progress(
            f"read_table_broken_lineage: re-open cache after read_table ({label})"
        )
        return _read_parquet_cache_dir(spark, cache_path, table_fqn)
    if scan_backend:
        _read_progress(
            f"read_table_broken_lineage: {label} — scan DataFrame in-process "
            "(MEDALLION_LOCAL_TABLE_CACHE=0; no break_lineage)"
        )
        return df
    return break_lineage_local(spark, df, label)


def break_lineage_local(spark: SparkSession, df: DataFrame, label: str) -> DataFrame:
    """Write/read local parquet so later joins are not stuck on an old broadcast plan."""
    apply_local_merge_spark_conf(spark)
    path = Path.home() / ".fabric" / "merge_staging" / "_break" / label
    _read_progress(f"break_lineage_local: {label} -> {path}")
    _write_df_to_local_parquet(df, path, staging=True)
    return spark.read.parquet(str(path))


def publish_merge_staging(df: DataFrame, table_fqn: str) -> Path:
    """Call at end of transform — merge_incremental reads this (no Spark collect on join plan)."""
    spark = df.sparkSession
    apply_local_merge_spark_conf(spark)
    path = merge_staging_path(table_fqn)
    _read_progress(f"publish_merge_staging: {table_fqn} -> {path}")
    if os.environ.get("MEDALLION_PURVIEW_COLLAPSE_BEFORE_PUBLISH", "1").lower() in (
        "1",
        "true",
        "yes",
    ):
        from Common import config

        if table_fqn in (
            config.TABLE_PURVIEW_COPILOT_SILVER,
            config.TABLE_PURVIEW_COPILOT_OKTA_REJECT,
        ):
            label = table_fqn.rsplit(".", 1)[-1]
            df = collapse_lineage_to_local_parquet(spark, df, label)
    _write_df_to_local_parquet(df, path, staging=True)
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


def _deltalake_write_staging_batches(
    staging: Path, path: str, *, mode: str = "overwrite"
) -> None:
    from deltalake import write_deltalake

    from Common.fabric_storage import fabric_storage_options, refresh_abfs_token_env

    refresh_abfs_token_env()
    opts = fabric_storage_options()
    _read_debug(f"deltalake write mode={mode} {path} (local staging -> OneLake)...")
    chunk_no = 0
    for pdf in _parquet_staging_batches(staging):
        chunk_no += 1
        write_mode = mode if chunk_no == 1 else "append"
        _read_debug(
            f"deltalake write chunk {chunk_no} ({len(pdf)} rows) mode={write_mode}"
        )
        write_deltalake(
            path,
            pdf,
            mode=write_mode,
            schema_mode="overwrite" if chunk_no == 1 else "merge",
            storage_options=opts,
        )
    if chunk_no == 0:
        _read_progress("deltalake write: staging empty — nothing written")
    else:
        _read_debug("deltalake write done")


def _deltalake_table_exists(path: str) -> bool:
    from deltalake import DeltaTable

    from Common.fabric_storage import fabric_storage_options, refresh_abfs_token_env

    refresh_abfs_token_env()
    try:
        DeltaTable(path, storage_options=fabric_storage_options())
        return True
    except Exception:
        return False


def parse_process_month_no(raw: str | int | None) -> int | None:
    """Parse YYYYMM int or None (used for ``MEDALLION_CURSOR_CLOSE_MONTH_NO``)."""
    if raw is None:
        return None
    if isinstance(raw, int):
        return raw
    text = str(raw).strip()
    return int(text) if text else None


def sync_cursor_close_month_env(month_no: int | None) -> None:
    """Set env from notebook ``CURSOR_CLOSE_MONTH_NO`` before ``%run bootstrap``."""
    if month_no is not None:
        os.environ["MEDALLION_CURSOR_CLOSE_MONTH_NO"] = str(int(month_no))
    else:
        os.environ.pop("MEDALLION_CURSOR_CLOSE_MONTH_NO", None)


def cursor_close_month_no() -> int | None:
    """Active month-close YYYYMM from env (``MEDALLION_CURSOR_CLOSE_MONTH_NO``)."""
    raw = os.environ.get("MEDALLION_CURSOR_CLOSE_MONTH_NO", "").strip()
    return parse_process_month_no(raw or None)


def sync_copilot_close_month_env(month_no: int | None) -> None:
    """Set env from notebook ``COPILOT_CLOSE_MONTH_NO`` before ``%run bootstrap``."""
    if month_no is not None:
        os.environ["MEDALLION_COPILOT_CLOSE_MONTH_NO"] = str(int(month_no))
    else:
        os.environ.pop("MEDALLION_COPILOT_CLOSE_MONTH_NO", None)


def copilot_close_month_no() -> int | None:
    """Active month-close YYYYMM from env (``MEDALLION_COPILOT_CLOSE_MONTH_NO``)."""
    raw = os.environ.get("MEDALLION_COPILOT_CLOSE_MONTH_NO", "").strip()
    return parse_process_month_no(raw or None)


def replace_months_in_frame(
    spark: SparkSession,
    table_name: str,
    month_df: DataFrame,
    month_col: str,
    month_nos: list[int],
) -> DataFrame:
    """Keep other months in target; replace listed ``month_nos`` with ``month_df`` (may be empty)."""
    from pyspark.sql import functions as F

    if not month_nos:
        return month_df
    month_set = [int(m) for m in month_nos]
    try:
        existing = read_table(spark, table_name)
        if existing.limit(1).count() > 0:
            kept = existing.filter(~F.col(month_col).cast("int").isin(month_set))
            if month_df.limit(1).count() > 0:
                return kept.unionByName(month_df, allowMissingColumns=True)
            return kept
    except Exception:
        pass
    return month_df


def replace_one_month_in_frame(
    spark: SparkSession,
    table_name: str,
    month_df: DataFrame,
    month_col: str,
    month_no: int,
) -> DataFrame:
    """Keep other months from target table; replace rows for ``month_no`` with ``month_df``."""
    return replace_months_in_frame(spark, table_name, month_df, month_col, [int(month_no)])


def write_cursor_okta_rejects(
    spark: SparkSession,
    rejects_df: DataFrame,
    processed_month_nos: list[int],
    *,
    table_name: str | None = None,
) -> None:
    """Replace reject rows for months in this batch; frozen months never in ``processed_month_nos``."""
    from Common import config

    if not processed_month_nos:
        return
    target = table_name or config.TABLE_CURSOR_OKTA_REJECT
    out = replace_months_in_frame(
        spark, target, rejects_df, "CursorUsageMonthNo", processed_month_nos
    )
    write_full_table(out, target)


def read_ingest_frozen_month_nos(spark: SparkSession, product: str) -> list[int]:
    """``Month_No`` from ``ingest_freeze_control`` for ``product`` or ``all``."""
    from Common import config
    from pyspark.sql import functions as F

    product_key = product.strip().lower()
    try:
        df = read_table(spark, config.TABLE_INGEST_FREEZE_CONTROL)
    except Exception:
        return []
    if df.limit(1).count() == 0:
        return []
    rows = (
        df.filter(F.lower(F.col("product")).isin(product_key, "all"))
        .select(F.col("Month_No").cast("int").alias("_m"))
        .where(F.col("_m").isNotNull())
        .distinct()
        .collect()
    )
    return sorted(int(r["_m"]) for r in rows)


def append_ingest_freeze_month(
    spark: SparkSession,
    product: str,
    month_no: int,
    *,
    frozen_by: str | None = None,
    notes: str = "Month-close auto-freeze",
) -> None:
    """Append ``ingest_freeze_control`` row (skip if that product/month already frozen)."""
    from datetime import datetime, timezone

    import pandas as pd
    from deltalake import write_deltalake

    from Common import config
    from Common.fabric_storage import fabric_storage_options, refresh_abfs_token_env

    product_key = product.strip().lower()
    if frozen_by is None:
        frozen_by = f"{product_key}_pipeline"
    if month_no in read_ingest_frozen_month_nos(spark, product_key):
        _read_progress(
            f"freeze: {product_key} Month_No {month_no} already in ingest_freeze_control — skip"
        )
        return
    ts = datetime.now(timezone.utc)
    pdf = pd.DataFrame(
        [
            {
                "product": product_key,
                "Month_No": int(month_no),
                "frozen_at": ts,
                "frozen_by": frozen_by,
                "notes": notes,
            }
        ]
    )
    path = _onelake_delta_path(config.TABLE_INGEST_FREEZE_CONTROL)
    if path and _use_deltalake_onelake_write():
        refresh_abfs_token_env()
        opts = fabric_storage_options()
        exists = _deltalake_table_exists(path)
        _read_progress(
            f"freeze: deltalake append {product_key} Month_No {month_no} "
            f"({'append' if exists else 'create'})"
        )
        write_deltalake(
            path,
            pdf,
            mode="append" if exists else "overwrite",
            storage_options=opts,
        )
    else:
        from pyspark.sql import Row

        df = spark.createDataFrame(
            [
                Row(
                    product=product_key,
                    Month_No=int(month_no),
                    frozen_at=ts,
                    frozen_by=frozen_by,
                    notes=notes,
                )
            ]
        )
        append_table(df, config.TABLE_INGEST_FREEZE_CONTROL)
    _read_progress(f"freeze: appended {product_key} Month_No {month_no} to ingest_freeze_control")


def append_cursor_freeze_month(
    spark: SparkSession,
    month_no: int,
    *,
    frozen_by: str = "cursor_pipeline",
    notes: str = "Month-close auto-freeze",
) -> None:
    """Append ``ingest_freeze_control`` row for Cursor (skip if already frozen)."""
    append_ingest_freeze_month(
        spark, "cursor", month_no, frozen_by=frozen_by, notes=notes
    )


def append_copilot_freeze_month(
    spark: SparkSession,
    month_no: int,
    *,
    frozen_by: str = "copilot_pipeline",
    notes: str = "Month-close auto-freeze",
) -> None:
    """Append ``ingest_freeze_control`` row for Copilot (skip if already frozen)."""
    append_ingest_freeze_month(
        spark, "copilot", month_no, frozen_by=frozen_by, notes=notes
    )


def _distinct_month_nos(df: DataFrame, month_col: str) -> list[int]:
    from pyspark.sql import functions as F

    rows = (
        df.select(F.col(month_col).cast("int").alias("_m"))
        .where(F.col("_m").isNotNull())
        .distinct()
        .collect()
    )
    return sorted(int(r["_m"]) for r in rows)


def read_cursor_frozen_month_nos(spark: SparkSession) -> list[int]:
    """``Month_No`` values from ``ingest_freeze_control`` for ``cursor`` or ``all``."""
    return read_ingest_frozen_month_nos(spark, "cursor")


def read_copilot_frozen_month_nos(spark: SparkSession) -> list[int]:
    """``Month_No`` values from ``ingest_freeze_control`` for ``copilot`` or ``all``."""
    return read_ingest_frozen_month_nos(spark, "copilot")


def _frozen_month_nos_to_skip(
    frozen: list[int],
    close_month_no: int | None = None,
) -> list[int]:
    if close_month_no is not None:
        close = int(close_month_no)
        frozen = [m for m in frozen if m != close]
    return frozen


def cursor_frozen_month_nos_to_skip(
    spark: SparkSession,
    close_month_no: int | None = None,
) -> list[int]:
    """Frozen months to exclude from daily merge/write.

    When ``close_month_no`` is set (month-close), that month may still be reloaded even if
    already listed in ``ingest_freeze_control``.
    """
    return _frozen_month_nos_to_skip(read_cursor_frozen_month_nos(spark), close_month_no)


def copilot_frozen_month_nos_to_skip(
    spark: SparkSession,
    close_month_no: int | None = None,
) -> list[int]:
    """Copilot frozen months to skip on daily silver/gold (month-close may reload close month)."""
    return _frozen_month_nos_to_skip(read_copilot_frozen_month_nos(spark), close_month_no)


def cursor_excluded_month_nos(
    spark: SparkSession,
    gold_table: str,
    batch_df: DataFrame,
    month_col: str = "Month_No",
    *,
    close_month_no: int | None = None,
) -> tuple[list[int], int | None]:
    """Months to skip on gold merge: freeze control (``gold_table`` unused)."""
    _ = gold_table
    excluded = cursor_frozen_month_nos_to_skip(spark, close_month_no)
    batch_months = _distinct_month_nos(batch_df, month_col)
    batch_max = max(batch_months) if batch_months else None
    return excluded, batch_max


def copilot_excluded_month_nos(
    spark: SparkSession,
    gold_table: str,
    batch_df: DataFrame,
    month_col: str = "Month_No",
    *,
    close_month_no: int | None = None,
) -> tuple[list[int], int | None]:
    """Copilot gold merge skip list from ``ingest_freeze_control``."""
    _ = gold_table
    excluded = copilot_frozen_month_nos_to_skip(spark, close_month_no)
    batch_months = _distinct_month_nos(batch_df, month_col)
    batch_max = max(batch_months) if batch_months else None
    return excluded, batch_max


def write_copilot_okta_rejects(
    spark: SparkSession,
    rejects_df: DataFrame,
    processed_month_nos: list[int],
    *,
    table_name: str | None = None,
) -> None:
    """Replace copilot reject rows for months in this batch."""
    from Common import config

    if not processed_month_nos:
        return
    target = table_name or config.TABLE_PURVIEW_COPILOT_OKTA_REJECT
    out = replace_months_in_frame(
        spark,
        target,
        rejects_df,
        "purview_copilot_usage_month_no",
        processed_month_nos,
    )
    write_full_table(out, target)


def append_table(df: DataFrame, table_name: str) -> None:
    """Append rows to a Delta table (creates the table if it does not exist)."""
    path = _onelake_delta_path(table_name)
    staging = merge_staging_path(table_name)
    if path and _use_deltalake_onelake_write():
        apply_local_merge_spark_conf(df.sparkSession)
        safe = table_name.replace(".", "__")
        df = break_lineage_local(df.sparkSession, df, f"append_break_{safe}")
        publish_merge_staging(df, table_name)
        if not staging.is_dir() or not any(staging.glob("*.parquet")):
            raise RuntimeError(
                f"No merge staging parquet at {staging}. "
                f"Re-run transform, then append_table(your_df, {table_name!r})."
            )
        exists = _deltalake_table_exists(path)
        _deltalake_write_staging_batches(
            staging, path, mode="append" if exists else "overwrite"
        )
        return
    writer = df.write.format("delta").mode("append")
    if path:
        from Common.fabric_storage import apply_azure_cli_abfs_conf

        apply_azure_cli_abfs_conf(df.sparkSession)
        _read_debug(f"append_table: pyspark save {path}")
        writer.save(path)
        return
    writer.saveAsTable(table_name)


def write_full_table(df: DataFrame | None, table_name: str) -> None:
    """Overwrite a Delta table (full load). Local: staging parquet → deltalake OneLake.

    When ``df`` is provided, this calls ``publish_merge_staging`` once. Do not publish
    before calling (month-close notebooks: ``replace_one_month_in_frame`` then here only).
    """
    path = _onelake_delta_path(table_name)
    staging = merge_staging_path(table_name)
    if path and _use_deltalake_onelake_write():
        if df is not None:
            apply_local_merge_spark_conf(df.sparkSession)
            publish_merge_staging(df, table_name)
        if not staging.is_dir() or not any(staging.glob("*.parquet")):
            raise RuntimeError(
                f"No merge staging parquet at {staging}. "
                "Re-run transform with apply_local_merge_spark_conf, then "
                f"write_full_table(your_df, {table_name!r})."
            )
        _deltalake_write_staging_batches(staging, path, mode="overwrite")
        return
    if df is None:
        raise RuntimeError(
            f"write_full_table({table_name!r}): DataFrame required for non-deltalake write"
        )
    writer = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    if path:
        from Common.fabric_storage import apply_azure_cli_abfs_conf

        apply_azure_cli_abfs_conf(df.sparkSession)
        _read_debug(f"write_full_table: pyspark save {path}")
        writer.save(path)
        return
    writer.saveAsTable(table_name)


def _write_silver_daily_preserve_frozen(
    spark: SparkSession,
    write_df: DataFrame,
    table_name: str,
    merge_key: str,
    month_col: str,
    frozen: list[int],
    *,
    product_label: str,
) -> None:
    from pyspark.sql import functions as F

    if not frozen:
        merge_incremental(spark, write_df, table_name, merge_key)
        return
    out = write_df
    try:
        existing = read_table(spark, table_name)
        if existing.limit(1).count() > 0:
            kept = existing.filter(F.col(month_col).cast("int").isin([int(m) for m in frozen]))
            if out.limit(1).count() > 0:
                out = kept.unionByName(out, allowMissingColumns=True)
            else:
                out = kept
    except Exception:
        pass
    _read_progress(f"{product_label} silver daily: preserved frozen Month_No {frozen}")
    write_full_table(out, table_name)


def write_cursor_silver_daily(
    spark: SparkSession,
    write_df: DataFrame,
    table_name: str,
    merge_key: str,
    *,
    close_month_no: int | None = None,
) -> None:
    """Daily cursor silver: merge when nothing frozen; else keep frozen months from target (like HC)."""
    frozen = cursor_frozen_month_nos_to_skip(spark, close_month_no)
    _write_silver_daily_preserve_frozen(
        spark,
        write_df,
        table_name,
        merge_key,
        "CursorUsageMonthNo",
        frozen,
        product_label="cursor",
    )


def write_copilot_silver_daily(
    spark: SparkSession,
    write_df: DataFrame,
    table_name: str,
    merge_key: str,
    *,
    close_month_no: int | None = None,
) -> None:
    """Daily copilot silver: merge open months; preserve frozen months already in target."""
    frozen = copilot_frozen_month_nos_to_skip(spark, close_month_no)
    _write_silver_daily_preserve_frozen(
        spark,
        write_df,
        table_name,
        merge_key,
        "purview_copilot_usage_month_no",
        frozen,
        product_label="copilot",
    )


def merge_incremental(spark, df: DataFrame, table_name: str, merge_key: str) -> None:
    """Upsert via Delta merge on ``merge_key``. Expects ``publish_merge_staging`` output locally."""
    path = _onelake_delta_path(table_name)
    if path and _use_deltalake_onelake_write():
        from deltalake import DeltaTable, write_deltalake

        from Common.fabric_storage import fabric_storage_options, refresh_abfs_token_env

        refresh_abfs_token_env()
        opts = fabric_storage_options()
        _read_progress(f"merge_incremental: deltalake path {path}")
        predicate = _merge_predicate(merge_key)
        staging = merge_staging_path(table_name)
        if not staging.is_dir() or not any(staging.glob("*.parquet")):
            if df is not None:
                _read_progress(
                    f"merge_incremental: no staging at {staging}; "
                    f"publish_merge_staging from DataFrame ({table_name})"
                )
                publish_merge_staging(df, table_name)
            else:
                raise RuntimeError(
                    f"No merge staging parquet at {staging}. "
                    "Re-run the transform cell (apply_local_merge_spark_conf first); "
                    f"end with publish_merge_staging(your_df, {table_name!r})."
                )
        table_exists = _deltalake_table_exists(path)
        dt = None
        if not table_exists:
            try:
                dt = DeltaTable(path, storage_options=opts)
                table_exists = True
                _read_progress("merge_incremental: existing Delta table found (merge, not overwrite)")
            except Exception:
                dt = None
        chunk_no = 0
        for source in _parquet_staging_batches(staging):
            chunk_no += 1
            source = _dedupe_pandas_merge_source(source, merge_key)
            if len(source) == 0:
                _read_progress(f"merge_incremental: chunk {chunk_no} empty after dedupe — skip")
                continue
            _read_progress(f"merge_incremental: chunk {chunk_no} ({len(source)} rows)...")
            if not table_exists:
                _read_progress("merge_incremental: new table — initial overwrite")
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
            if dt is None:
                dt = DeltaTable(path, storage_options=opts)
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
        from Common.fabric_storage import apply_azure_cli_abfs_conf

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
