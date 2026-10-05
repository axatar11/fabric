# Copy to medallion/local_settings.py (gitignored) for machine-specific overrides.
#
# Use create_spark() when you already have working Fabric/OneLake connection code.
# medallion_entry will call it instead of building Spark from FABRIC_* env vars.

def create_spark(app_name: str = "MedallionLocal"):
    """Return an active SparkSession connected to Fabric OneLake."""
    # Paste your working session code here, for example:
    # from pyspark.sql import SparkSession
    # return SparkSession.builder.appName(app_name). ... .getOrCreate()
    raise NotImplementedError("Replace with your Spark session setup")

SPARK_EXTRA_CONFIG = {
    # Used only by the default builder when create_spark is not implemented:
    # "spark.sql.shuffle.partitions": "8",
}

# Override OneLake Delta paths if Copy ABFS path differs from defaults in runtime.py
ONELAKE_TABLE_OVERRIDES = {
    # "AI_Medallion.Silver.cursor_usage": "abfss://...@onelake.dfs.fabric.microsoft.com/...",
}
