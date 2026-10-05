# Copy to medallion/local_settings.py (gitignored) for machine-specific overrides.

SPARK_EXTRA_CONFIG = {
    # "spark.sql.shuffle.partitions": "8",
}

# Override OneLake Delta paths if Copy ABFS path differs from defaults in runtime.py
ONELAKE_TABLE_OVERRIDES = {
    # "AI_Medallion.Silver.cursor_usage": "abfss://...@onelake.dfs.fabric.microsoft.com/...",
}
