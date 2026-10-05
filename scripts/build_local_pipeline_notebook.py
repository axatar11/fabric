#!/usr/bin/env python3
"""Generate NB_Medallion_Local_Pipeline.ipynb from existing Fabric notebooks.

This script only writes/updates the .ipynb file. It does NOT start Spark or
run the medallion pipeline. To execute the pipeline, open the generated notebook
in Jupyter/VS Code and run all cells (see README.md).
"""
from __future__ import annotations

import json
from pathlib import Path
from textwrap import dedent

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "NB_Medallion_Local_Pipeline.ipynb"


def load_code_cells(nb_path: Path) -> str:
    nb = json.loads(nb_path.read_text())
    parts: list[str] = []
    for cell in nb["cells"]:
        if cell["cell_type"] == "code":
            parts.append("".join(cell.get("source", [])))
    return "\n\n".join(parts)


def strip_runs(source: str) -> str:
    lines = [ln for ln in source.splitlines() if not ln.strip().startswith("%run")]
    return "\n".join(lines).strip()


LOCAL_SPARK = dedent(
    """
    import os
    from typing import Optional

    FABRIC_TENANT_ID = os.environ.get("FABRIC_TENANT_ID", "<TENANT_ID>")
    FABRIC_CLIENT_ID = os.environ.get("FABRIC_CLIENT_ID", "<APP_CLIENT_ID>")
    FABRIC_CLIENT_SECRET = os.environ.get("FABRIC_CLIENT_SECRET", "<APP_CLIENT_SECRET>")

    LAKEHOUSE_AI_MEDALLION_ID = os.environ.get(
        "LAKEHOUSE_AI_MEDALLION_ID", "1ee46463-2495-44f5-9dc9-d635d0067483"
    )
    LAKEHOUSE_AI_MEDALLION_BRONZE_ID = os.environ.get(
        "LAKEHOUSE_AI_MEDALLION_BRONZE_ID", "36e5350a-61f6-4120-8c32-a38f55722907"
    )
    LAKEHOUSE_AI_MEDALLION_SILVER_ID = os.environ.get(
        "LAKEHOUSE_AI_MEDALLION_SILVER_ID", LAKEHOUSE_AI_MEDALLION_ID
    )

    ONELAKE_HOST = "onelake.dfs.fabric.microsoft.com"
    DELTA_PACKAGES = "io.delta:delta-spark_2.12:3.2.0"
    AZURE_PACKAGES = "org.apache.hadoop:hadoop-azure:3.3.6,com.azure:azure-storage-blob:12.25.1"

    CURSOR_BRONZE_FILENAME: Optional[str] = os.environ.get(
        "CURSOR_BRONZE_FILENAME",
        "team-usage-events-24970505-2026-09-25.csv",
    )

    def _configure_onelake_oauth(builder):
        prefix = "fs.azure.account"
        host = ONELAKE_HOST
        return (
            builder.config(f"{prefix}.auth.type.{host}", "OAuth")
            .config(
                f"{prefix}.oauth.provider.type.{host}",
                "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
            )
            .config(f"{prefix}.oauth2.client.id.{host}", FABRIC_CLIENT_ID)
            .config(f"{prefix}.oauth2.client.secret.{host}", FABRIC_CLIENT_SECRET)
            .config(
                f"{prefix}.oauth2.client.endpoint.{host}",
                f"https://login.microsoftonline.com/{FABRIC_TENANT_ID}/oauth2/token",
            )
        )

    def build_local_spark(app_name: str = "MedallionLocal"):
        from pyspark.sql import SparkSession

        builder = (
            SparkSession.builder.appName(app_name)
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .config("spark.jars.packages", f"{DELTA_PACKAGES},{AZURE_PACKAGES}")
        )
        builder = _configure_onelake_oauth(builder)
        return builder.getOrCreate()

    def onelake_tables_root(lakehouse_id: str) -> str:
        return f"abfss://{WORKSPACEID}@{ONELAKE_HOST}/{lakehouse_id}.Lakehouse/Tables"

    def register_delta_table_if_missing(spark, table_fqn: str, abfss_path: str) -> None:
        if spark.catalog.tableExists(table_fqn):
            return
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {table_fqn} USING DELTA LOCATION '{abfss_path}'"
        )

    def register_pipeline_tables(spark) -> None:
        mappings = {
            TABLE_CURSOR_BRONZE: f"{onelake_tables_root(LAKEHOUSE_AI_MEDALLION_ID)}/Bronze/cursor_usage",
            TABLE_HC_BRONZE: f"{onelake_tables_root(LAKEHOUSE_AI_MEDALLION_BRONZE_ID)}/dbo/HC_Bronze_Historical",
            TABLE_OKTA_BRONZE: f"{onelake_tables_root(LAKEHOUSE_AI_MEDALLION_BRONZE_ID)}/dbo/CoreOktaUser",
            TABLE_COUNTRY: f"{onelake_tables_root(LAKEHOUSE_AI_MEDALLION_SILVER_ID)}/dbo/CoreCountry_Excel_Hours_Historic",
            TABLE_HC_SILVER: f"{onelake_tables_root(LAKEHOUSE_AI_MEDALLION_ID)}/Silver/HC_Silver_Historical",
            TABLE_OKTA_SILVER: f"{onelake_tables_root(LAKEHOUSE_AI_MEDALLION_ID)}/Silver/Okta_User_for_AI",
            TABLE_CURSOR_SILVER: f"{onelake_tables_root(LAKEHOUSE_AI_MEDALLION_ID)}/Silver/cursor_usage",
            TABLE_FACT_CURSOR_ACTIVE: f"{onelake_tables_root(LAKEHOUSE_AI_MEDALLION_ID)}/Gold/Fact_Cursor_Active",
        }
        for fqn, path in mappings.items():
            register_delta_table_if_missing(spark, fqn, path)

    try:
        spark  # type: ignore[name-defined]
        _USING_ATTACHED_FABRIC_SESSION = True
    except NameError:
        spark = build_local_spark()
        _USING_ATTACHED_FABRIC_SESSION = False

    if not _USING_ATTACHED_FABRIC_SESSION:
        register_pipeline_tables(spark)

    print(
        f"Spark {spark.version}; Fabric-attached session={_USING_ATTACHED_FABRIC_SESSION}"
    )
    """
)


def md(text: str, name: str) -> dict:
    return {
        "cell_type": "markdown",
        "metadata": {"name": name},
        "source": [line + "\n" for line in text.strip().split("\n")],
    }


def code(name: str, text: str) -> dict:
    body = text.strip() + "\n"
    if not body.startswith("# Cell:"):
        body = f"# Cell: {name}\n{body}"
    return {
        "cell_type": "code",
        "metadata": {"name": name},
        "source": [line + "\n" for line in body.split("\n")],
        "outputs": [],
        "execution_count": None,
    }


def build_cursor_bronze_to_silver() -> str:
    return dedent(
        """
        bronze_df = spark.table(TABLE_CURSOR_BRONZE)
        if CURSOR_BRONZE_FILENAME:
            bronze_df = bronze_df.filter(F.col("filename") == CURSOR_BRONZE_FILENAME)
        bronze = bronze_df

        okta = spark.table(TABLE_OKTA_SILVER)
        hc = spark.table(TABLE_HC_SILVER)

        user_norm = F.lower(F.trim(F.col("User")))

        base = (
            bronze.filter(is_valid_email(F.col("User")))
            .select(
                F.col("Date").cast("date").alias("CursorUsageDate"),
                user_norm.alias("CursorUsageUser"),
                F.col("`Service Account Name`").alias("CursorUsageServiceAccountName"),
                F.col("`Service Account ID`").alias("CursorUsageServiceAccountID"),
                F.col("`Cloud Agent ID`").alias("CursorUsageCloudAgentID"),
                F.col("`Automation ID`").alias("CursorUsageAutomationID"),
                F.col("Kind").alias("CursorUsageKind"),
                F.col("Model").alias("CursorUsageModel"),
                F.col("`Max Mode`").alias("CursorUsageMaxMode"),
                F.col("`Input (w/ Cache Write)`").cast("long").alias("CursorUsageInputWithCacheWrite"),
                F.col("`Input (w/o Cache Write)`").cast("long").alias("CursorUsageInputWithoutCacheWrite"),
                F.col("`Cache Read`").cast("long").alias("CursorUsageCacheRead"),
                F.col("`Output Tokens`").cast("long").alias("CursorUsageOutputTokens"),
                F.col("`Total Tokens`").cast("long").alias("CursorUsageTotalTokens"),
                F.col("Cost").cast("decimal(18,8)").alias("CursorUsageCost"),
                F.col("FileName").alias("CursorUsageFileName"),
                F.col("ExecutionDate").cast("timestamp").alias("CursorUsageIngestionDate"),
            )
            .withColumn(
                "CursorUsageMonthNo",
                (F.year("CursorUsageDate") * F.lit(100) + F.month("CursorUsageDate")).cast("int"),
            )
        )

        okta_slim = okta.select(
            "OktaUserId",
            "OktaUserEmail",
            "OktaUserFullEmail",
            "OktaUserEmployeeNumber",
            "OktaUserFullName",
            "OktaUserRegion",
            "OktaUserCountry",
        )

        joined = base.join(
            okta_slim,
            (F.col("CursorUsageUser") == F.col("OktaUserEmail"))
            | (F.col("CursorUsageUser") == F.col("OktaUserFullEmail")),
            "inner",
        )

        joined = joined.withColumn(
            "CursorUsageKeyEmail",
            F.when(
                F.col("OktaUserEmail").isNotNull(),
                F.concat(
                    F.col("CursorUsageMonthNo").cast("string"),
                    F.lit("/"),
                    F.col("OktaUserEmail"),
                ),
            ),
        ).withColumn(
            "CursorUsageKeyFullEmail",
            F.when(
                F.col("OktaUserFullEmail").isNotNull(),
                F.concat(
                    F.col("CursorUsageMonthNo").cast("string"),
                    F.lit("/"),
                    F.col("OktaUserFullEmail"),
                ),
            ),
        )

        hc_slim = hc.select(
            F.col("HCKeyEmail"),
            F.col("HCBusinessUnit"),
            F.col("HCBusinessUnitCode"),
            F.col("HCDivision"),
            F.col("HCJobTitle"),
            F.col("HCOffice"),
            F.col("HCTalentSegment"),
            F.col("Month_No").alias("HCMonthNo"),
            F.col("OktaUserCountry").alias("HCOktaUserCountry"),
            F.col("HCDivisionINC"),
        )

        joined = joined.join(
            hc_slim,
            (F.col("HCKeyEmail") == F.col("CursorUsageKeyEmail"))
            | (F.col("HCKeyEmail") == F.col("CursorUsageKeyFullEmail")),
            "left",
        ).withColumn(
            "HCDivisionINC",
            F.coalesce(F.col("HCDivisionINC"), F.lit(HC_DIVISION_INC_DEFAULT)),
        )

        write_df = joined.distinct()
        write_df = write_df.withColumn(
            FACT_CURSOR_ACTIVE_MERGE_KEY,
            cursor_usage_record_key(),
        )

        merge_incremental(
            spark,
            write_df,
            TABLE_CURSOR_SILVER,
            FACT_CURSOR_ACTIVE_MERGE_KEY,
        )

        show_sample(write_df)
        """
    )


def main() -> None:
    config_src = load_code_cells(ROOT / "medallion_config.ipynb")
    io_src = strip_runs(load_code_cells(ROOT / "medallion_io.ipynb"))
    transforms_src = strip_runs(load_code_cells(ROOT / "medallion_transforms.ipynb"))

    hc_src = strip_runs(load_code_cells(ROOT / "NB_HCHistorical_Bronze_To_Silver.ipynb"))
    okta_src = strip_runs(load_code_cells(ROOT / "NB_OktaUserforAI_Bronze_To_Silver.ipynb"))
    gold_src = strip_runs(load_code_cells(ROOT / "NB_CursorUsage_Gold.ipynb"))

    okta_src = okta_src.replace("display(okta.limit(10))", "show_sample(okta)")
    hc_src = hc_src.replace("display(hc.limit(10))", "show_sample(hc)")
    gold_src = gold_src.replace("display(fact.limit(10))", "show_sample(fact)")

    helper = dedent(
        """
        def show_sample(df, n: int = 10) -> None:
            try:
                display(df.limit(n))  # Fabric / IPython
            except NameError:
                df.limit(n).show(truncate=False)
        """
    )

    cells: list[dict] = [
        md(
            dedent(
                """
                # Medallion reporting pipeline (local Spark → Fabric OneLake)

                **How to run:** set `FABRIC_TENANT_ID`, `FABRIC_CLIENT_ID`, `FABRIC_CLIENT_SECRET`
                (PowerShell: `$env:FABRIC_TENANT_ID = "..."`) or edit **local_spark_session**, then
                **Run All** in Jupyter or VS Code. Do not use `build_local_pipeline_notebook.py` to execute
                the pipeline—that script only rebuilds this file from the split notebooks.

                **Pipeline order:** HC → Okta → Cursor usage (Silver) → Fact Cursor Active (Gold)

                Each code cell starts with `# Cell: <name>` and has matching markdown headers.
                """
            ),
            "overview",
        ),
        md("## medallion_config", "medallion_config_header"),
        code("medallion_config", config_src),
        md("## local_spark_session", "local_spark_session_header"),
        code("local_spark_session", LOCAL_SPARK),
        md("## medallion_io", "medallion_io_header"),
        code("medallion_io", io_src),
        md("## medallion_transforms", "medallion_transforms_header"),
        code("medallion_transforms", transforms_src),
        md("## helpers", "helpers_header"),
        code("helpers", helper),
        md("## hc_historical_bronze_to_silver", "hc_historical_bronze_to_silver_header"),
        code("hc_historical_bronze_to_silver", hc_src),
        md("## okta_user_bronze_to_silver", "okta_user_bronze_to_silver_header"),
        code("okta_user_bronze_to_silver", okta_src),
        md("## cursor_usage_bronze_to_silver", "cursor_usage_bronze_to_silver_header"),
        code("cursor_usage_bronze_to_silver", build_cursor_bronze_to_silver()),
        md("## cursor_usage_gold", "cursor_usage_gold_header"),
        code("cursor_usage_gold", gold_src),
    ]

    notebook = {
        "nbformat": 4,
        "nbformat_minor": 5,
        "metadata": {
            "kernelspec": {
                "display_name": "Python 3 (PySpark)",
                "language": "python",
                "name": "python3",
            },
            "language_info": {"name": "python", "pygments_lexer": "ipython3"},
        },
        "cells": cells,
    }

    OUT.write_text(json.dumps(notebook, indent=2))
    print(f"Wrote {OUT} ({len(cells)} cells)")
    print("Next: open that notebook in Jupyter/VS Code and Run All (see README.md).")


if __name__ == "__main__":
    main()
