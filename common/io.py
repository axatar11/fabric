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
        config.TABLE_HC_SILVER: onelake_table_path(med, "Silver/HC_Silver_Historical"),
        config.TABLE_OKTA_SILVER: onelake_table_path(med, "Silver/Okta_User_for_AI"),
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


def _read_delta_via_deltalake(spark: SparkSession, path: str) -> DataFrame:
    """Optional fallback (NB_Cursor_Bronze); set MEDALLION_DELTALAKE_READ=1."""
    from deltalake import DeltaTable

    from common.fabric_storage import fabric_storage_options

    dt = DeltaTable(path, storage_options=fabric_storage_options())
    table = dt.to_pyarrow_table()
    from pyspark.sql.types import StructField, StructType
    from pyspark.sql.pandas.types import from_arrow_type

    schema = StructType(
        [
            StructField(field.name, from_arrow_type(field.type), nullable=field.nullable)
            for field in table.schema
        ]
    )
    cols = [table.column(i).to_pylist() for i in range(table.num_columns)]
    if not cols:
        return spark.createDataFrame([], schema)
    rows = list(zip(*cols))
    return spark.createDataFrame(rows, schema=schema)


def _read_delta_via_pyspark(spark: SparkSession, path: str) -> DataFrame:
    from common.fabric_storage import apply_azure_cli_abfs_conf

    apply_azure_cli_abfs_conf(spark)
    return spark.read.format("delta").load(path)


def read_table(spark: SparkSession, table_fqn: str) -> DataFrame:
    """Local: PySpark Delta on OneLake abfss (az login). Fabric: spark.table."""
    if _use_path_reads():
        path = delta_path_for_table(table_fqn)
        if path:
            if os.environ.get("MEDALLION_DELTALAKE_READ", "").lower() in (
                "1",
                "true",
                "yes",
            ):
                return _read_delta_via_deltalake(spark, path)
            return _read_delta_via_pyspark(spark, path)
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
