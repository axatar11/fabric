from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def write_full_table(df: DataFrame, table_name: str) -> None:
    """Full load (overwrite) for Silver / dimension-style Gold tables."""
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table_name)
    )


def merge_incremental(
    spark,
    df: DataFrame,
    table_name: str,
    merge_key: str,
) -> None:
    """Incremental upsert on a single merge key column (Delta Lake)."""
    if not spark.catalog.tableExists(table_name):
        write_full_table(df, table_name)
        return

    from delta.tables import DeltaTable

    target = DeltaTable.forName(spark, table_name)
    (
        target.alias("target")
        .merge(
            df.alias("source"),
            f"target.{merge_key} = source.{merge_key}",
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
