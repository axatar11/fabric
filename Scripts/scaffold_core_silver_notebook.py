#!/usr/bin/env python3
"""Generate NB_CoreReporting_BronzeToSilver.ipynb."""
from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

SETTINGS = """# Workspace folder: C:\\spark-dev\\CoE_transformation_framework (see README)
import os
from pathlib import Path
_cwd = Path.cwd()
if not (_cwd / "Common" / "bootstrap.py").is_file():
    _repo = _cwd.parent
    if (_repo / "Common" / "bootstrap.py").is_file():
        os.chdir(_repo)
REBUILD_MEDALLION_CACHE = True
os.environ.setdefault("MEDALLION_DEBUG_READ", "1")
os.environ.setdefault("MEDALLION_SCAN_CHUNK_ROWS", "75000")
from Common.io import sync_rebuild_medallion_cache_env

sync_rebuild_medallion_cache_env(REBUILD_MEDALLION_CACHE)
"""

TABLES = [
    (
        "core_asset",
        "bronze = read_table(TABLE_CORE_ASSET_BRONZE)\n"
        "silver = build_core_asset_silver(bronze)\n"
        "write_full_table(silver, TABLE_CORE_ASSET_SILVER)\n"
        "show_sample(silver.limit(10))\n",
        None,
    ),
    (
        "core_country",
        "norm_core = read_table(TABLE_NORMALIZATION_COUNTRIES_CORE_SILVER)\n"
        "bronze = read_table(TABLE_CORE_COUNTRY_BRONZE)\n"
        "silver = build_core_country_silver(bronze, norm_core)\n"
        "write_full_table(silver, TABLE_CORE_COUNTRY_SILVER)\n"
        "show_sample(silver.limit(10))\n",
        "Requires `normalization_countries_core` Silver (run reference notebook first).",
    ),
    (
        "core_billing",
        "bronze = read_table(TABLE_CORE_BILLING_BRONZE)\n"
        "silver = build_core_billing_silver(bronze)\n"
        "write_full_table(silver, TABLE_CORE_BILLING_SILVER)\n"
        "show_sample(silver.limit(10))\n",
        None,
    ),
    (
        "core_project",
        "bronze = read_table(TABLE_CORE_PROJECT_BRONZE)\n"
        "silver = build_core_project_silver(bronze)\n"
        "write_full_table(silver, TABLE_CORE_PROJECT_SILVER)\n"
        "show_sample(silver.limit(10))\n",
        None,
    ),
    (
        "core_okta_user",
        "norm_okta = read_table(TABLE_NORMALIZATION_COUNTRIES_OKTA_SILVER)\n"
        "bronze = read_table(TABLE_CORE_OKTA_USER_BRONZE)\n"
        "silver = build_core_okta_user_silver(bronze, norm_okta)\n"
        "write_full_table(silver, TABLE_CORE_OKTA_USER_SILVER)\n"
        "show_sample(silver.limit(10))\n",
        "Requires `normalization_countries_okta` Silver.",
    ),
    (
        "dim_project",
        "core_project = read_table(TABLE_CORE_PROJECT_SILVER)\n"
        "core_billing = read_table(TABLE_CORE_BILLING_SILVER)\n"
        "core_country = read_table(TABLE_CORE_COUNTRY_SILVER)\n"
        "silver = build_dim_project_silver(core_project, core_billing, core_country)\n"
        "write_full_table(silver, TABLE_DIM_PROJECT_SILVER)\n"
        "show_sample(silver.limit(10))\n",
        "Silver → Silver enrich (run `core_project`, `core_billing`, `core_country` first).",
    ),
    (
        "core_use_case_implementation",
        "bronze = read_table(TABLE_CORE_USE_CASE_IMPLEMENTATION_BRONZE)\n"
        "silver = build_core_use_case_implementation_silver(bronze)\n"
        "write_full_table(silver, TABLE_CORE_USE_CASE_IMPLEMENTATION_SILVER)\n"
        "show_sample(silver.limit(10))\n",
        None,
    ),
    (
        "core_use_case_adjustment",
        "bronze = read_table(TABLE_CORE_USE_CASE_ADJUSTMENT_BRONZE)\n"
        "silver = build_core_use_case_adjustment_silver(bronze)\n"
        "write_full_table(silver, TABLE_CORE_USE_CASE_ADJUSTMENT_SILVER)\n"
        "show_sample(silver.limit(10))\n",
        None,
    ),
]


def main() -> None:
    cells: list[dict] = [
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": [
                "# NB_CoreReporting_BronzeToSilver\n",
                "\n",
                "Core reporting entities: Bronze → Silver (**full overwrite**). "
                "One cell per table. Run **reference** normalization tables before "
                "`core_country` / `core_okta_user`; run core silvers before `dim_project`.\n",
            ],
        },
        {
            "cell_type": "code",
            "metadata": {"name": "local_medallion_settings"},
            "source": [SETTINGS],
            "outputs": [],
            "execution_count": None,
        },
        {
            "cell_type": "code",
            "metadata": {"name": "bootstrap"},
            "source": ["%run ./Common/bootstrap\n"],
            "outputs": [],
            "execution_count": None,
        },
    ]
    first = True
    for name, code, note in TABLES:
        hdr = f"## `{name}`"
        if note:
            hdr += f"\n\n{note}"
        cells.append({"cell_type": "markdown", "metadata": {}, "source": [hdr]})
        src = code
        if first:
            src = "apply_local_merge_spark_conf(spark)\n\n" + src
            first = False
        cells.append(
            {
                "cell_type": "code",
                "metadata": {"name": f"silver_{name}"},
                "source": [src],
                "outputs": [],
                "execution_count": None,
            }
        )
    nb = {
        "nbformat": 4,
        "nbformat_minor": 5,
        "metadata": {
            "language_info": {"name": "python"},
            "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
        },
        "cells": cells,
    }
    path = ROOT / "Notebook" / "Silver" / "NB_CoreReporting_BronzeToSilver.ipynb"
    path.write_text(json.dumps(nb, indent=2), encoding="utf-8")
    print(f"wrote {path.relative_to(ROOT)}")


if __name__ == "__main__":
    main()