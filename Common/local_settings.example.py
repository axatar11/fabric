# Optional — do NOT copy unless you need overrides. Default OneLake paths are in config.py.
# Copy to Common/local_settings.py (gitignored) only for custom Spark or path overrides:
#
# ONELAKE_TABLE_OVERRIDES = {"AI_Medallion.Bronze.cursor_usage": "abfss://..."}
# LAKEHOUSE_AI_MEDALLION_SILVER_DB_ID — lakehouse for AI_Medallion_Silver.dbo (country lookup).


def create_spark(app_name: str = "MedallionLocal"):
    from Common.bootstrap import create_onelake_spark

    return create_onelake_spark(app_name)


SPARK_EXTRA_CONFIG = {}
ONELAKE_TABLE_OVERRIDES = {}
