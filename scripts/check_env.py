#!/usr/bin/env python3
"""Print which Python runs and whether medallion deps (PySpark) are installed."""
from __future__ import annotations

import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
REQ = REPO / "requirements-local-spark.txt"

print("Python executable:", sys.executable)
print("Version:", sys.version.replace("\n", " "))
print("Repo root:", REPO)
print("Requirements:", REQ)
print()

missing: list[str] = []
for name, mod in (
    ("pyspark", "pyspark"),
    ("delta-spark", "delta"),
    ("deltalake", "deltalake"),
    ("azure-identity", "azure.identity"),
    ("pandas", "pandas"),
    ("pyarrow", "pyarrow"),
):
    try:
        __import__(mod)
    except ImportError:
        missing.append(name)

if missing:
    print("MISSING packages:", ", ".join(missing))
    print()
    print("Install (PowerShell):")
    print(f"  pip install -r {REQ}")
    print()
    print("In Cursor: Python: Select Interpreter → pick the same python.exe as above.")
    print("Restart the notebook kernel after install.")
    sys.exit(1)

import pyspark

print("OK — pyspark", pyspark.__version__)
print("Use this interpreter as the notebook kernel.")
