# Copy to common/local_settings.py (gitignored). Put your working Fabric Spark setup here.


def create_spark(app_name: str = "MedallionLocal"):
    """Return SparkSession already configured for Fabric OneLake."""
    raise NotImplementedError("Paste your working spark session code here")


SPARK_EXTRA_CONFIG = {}
ONELAKE_TABLE_OVERRIDES = {}
