#!/usr/bin/env python3
"""Run before notebooks: checks PySpark vs SPARK_HOME alignment."""
import os
import sys
from pathlib import Path

repo = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(repo))

import pyspark

print("pip pyspark:", pyspark.__version__)
home = os.environ.get("SPARK_HOME")
if home:
    rel = Path(home) / "RELEASE"
    print("SPARK_HOME:", home, rel.read_text()[:80] if rel.is_file() else "(no RELEASE)")
else:
    print("SPARK_HOME: (not set — pip bundled jars)")

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("verify").getOrCreate()
print("SparkSession.version:", spark.version)
spark.range(1).show()
print("OK — if this fails with GenTraversableOnce, run:")
print("  pip install pyspark==3.5.4 delta-spark==3.2.0")
print("  unset SPARK_HOME  (PowerShell: Remove-Item Env:SPARK_HOME)")
spark.stop()
