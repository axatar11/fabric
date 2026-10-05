# Copy to common/local_settings.py (gitignored) only if you need a custom SparkSession.


def create_spark(app_name: str = "MedallionLocal"):
    from common.bootstrap import create_onelake_spark

    return create_onelake_spark(app_name)


SPARK_EXTRA_CONFIG = {}
ONELAKE_TABLE_OVERRIDES = {}
