# Copy to common/local_settings.py (gitignored).

from common.bootstrap import create_onelake_spark


def create_spark(app_name: str = "MedallionLocal"):
    """SparkSession for Fabric OneLake (Delta + abfss JARs + OAuth from env)."""
    return create_onelake_spark(app_name)


SPARK_EXTRA_CONFIG = {}
# Optional: TABLE_FQN -> full abfss://.../Tables/... path
ONELAKE_TABLE_OVERRIDES = {}
