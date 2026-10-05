# Optional — do NOT copy unless you need overrides. Default OneLake paths are in config.py.
# Copy to common/local_settings.py (gitignored) only for custom Spark or path overrides:
#
# ONELAKE_TABLE_OVERRIDES = {"AI_Medallion.Bronze.cursor_usage": "abfss://..."}


def create_spark(app_name: str = "MedallionLocal"):
    from common.bootstrap import create_onelake_spark

    return create_onelake_spark(app_name)


SPARK_EXTRA_CONFIG = {}
ONELAKE_TABLE_OVERRIDES = {}
