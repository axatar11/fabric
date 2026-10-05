#!/usr/bin/env python3
"""Sanity-check local PySpark before running pipeline notebooks."""
from __future__ import annotations

import os
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]

FIX = """
PySpark import failed — usually a broken or mixed pip install (common after upgrading in place).

PowerShell (venv activated):

  pip uninstall -y pyspark delta-spark py4j
  pip cache purge
  pip install pyspark==3.5.4 delta-spark==3.2.0

Use Python 3.11 or 3.12 (not 3.14). If it still fails, recreate the venv:

  cd C:\\spark-dev
  Remove-Item -Recurse -Force .venv
  py -3.12 -m venv .venv
  .\\.venv\\Scripts\\Activate.ps1
  pip install -r fabric\\fabric\\requirements-local-spark.txt

Then:

  Remove-Item Env:SPARK_HOME -ErrorAction SilentlyContinue
  python fabric\\fabric\\scripts\\verify_local_spark.py
"""


def main() -> int:
    print("python:", sys.version.split()[0], sys.executable)
    try:
        import pyspark
    except ImportError as exc:
        print("FAIL:", exc)
        print(FIX)
        return 1

    print("pip pyspark:", pyspark.__version__)
    home = os.environ.get("SPARK_HOME")
    if home:
        rel = Path(home) / "RELEASE"
        extra = rel.read_text(encoding="utf-8", errors="ignore")[:80] if rel.is_file() else "(no RELEASE)"
        print("SPARK_HOME:", home, extra)
    else:
        print("SPARK_HOME: (not set — using jars bundled with pip pyspark)")

    try:
        from pyspark.sql import SparkSession
    except ImportError as exc:
        print("FAIL importing SparkSession:", exc)
        print(FIX)
        return 1

    try:
        spark = SparkSession.builder.appName("verify").getOrCreate()
        print("SparkSession.version:", spark.version)
        spark.range(1).show()
        spark.stop()
    except Exception as exc:
        print("FAIL starting Spark:", exc)
        print("If you see GenTraversableOnce, remove SPARK_HOME or match it to pyspark version.")
        return 1

    try:
        __import__("azure.identity")
    except ImportError as exc:
        print("FAIL missing azure-identity:", exc)
        print(f"  python -m pip install -r {REPO / 'requirements-local-spark.txt'}")
        return 1

    token_jar = REPO / "common" / "jars" / "onelake-cli-token-provider.jar"
    if not token_jar.is_file():
        print("FAIL missing", token_jar)
        return 1

    print(
        "OK — PySpark + azure-identity + OneLake token JAR. "
        "Run az login, then %run ./common/bootstrap"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
