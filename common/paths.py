"""Repo root detection (same layout as ``bootstrap``)."""

from __future__ import annotations

from pathlib import Path

# Typical Windows clone path for CoE local PySpark (see README).
DEFAULT_WINDOWS_REPO = Path(r"C:\spark-dev\CoE_transformation_framework")


def repo_root() -> Path:
    """Directory that contains ``common/`` and ``Notebook/``."""
    return Path(__file__).resolve().parent.parent
