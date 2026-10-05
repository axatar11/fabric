# Copy to common/local_settings.py (gitignored).

from common.bootstrap import create_onelake_spark

# Service principal for OneLake abfss (local Jupyter). Env vars override these.
FABRIC_TENANT_ID = ""
FABRIC_CLIENT_ID = ""
FABRIC_CLIENT_SECRET = ""


def create_spark(app_name: str = "MedallionLocal"):
    """SparkSession for Fabric OneLake (Delta + abfss JARs + OAuth)."""
    return create_onelake_spark(app_name)


SPARK_EXTRA_CONFIG = {}
ONELAKE_TABLE_OVERRIDES = {}
