from __future__ import annotations

from typing import Any


def show_sample(df: Any, n: int = 10) -> None:
    """Fabric/Databricks display() when available; otherwise Spark show()."""
    try:
        display(df.limit(n))  # type: ignore[name-defined]  # noqa: F821
    except NameError:
        df.limit(n).show(truncate=False)
