#!/usr/bin/env python3
"""Point pipeline notebooks at medallion_entry and clean obsolete cells."""
from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

ENTRY_RUN = "%run medallion_entry\n"

COMMON_BOOTSTRAP = {
    "cells": [
        {
            "cell_type": "code",
            "metadata": {"name": "common_bootstrap"},
            "source": ["%run medallion_entry\n"],
            "outputs": [],
            "execution_count": None,
        }
    ],
    "nbformat": 4,
    "nbformat_minor": 5,
    "metadata": {
        "language_info": {"name": "python"},
        "microsoft": {"language": "python", "language_group": "synapse_pyspark"},
    },
}

MEDALLION_CONFIG_CELL = '''import sys
from pathlib import Path

_root = Path.cwd()
if not (_root / "medallion").is_dir():
    _root = _root.parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

from medallion import config as _cfg
for _name in _cfg.__all__:
    globals()[_name] = getattr(_cfg, _name)
'''

MEDALLION_IO_CELL = '''import sys
from pathlib import Path

_root = Path.cwd()
if not (_root / "medallion").is_dir():
    _root = _root.parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

from medallion import config as _cfg
from medallion import io as _io
for _name in _cfg.__all__:
    globals()[_name] = getattr(_cfg, _name)
write_full_table = _io.write_full_table
merge_incremental = _io.merge_incremental
'''

MEDALLION_TRANSFORMS_CELL = '''import sys
from pathlib import Path

_root = Path.cwd()
if not (_root / "medallion").is_dir():
    _root = _root.parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

from medallion import config as _cfg
from medallion import transforms as _tx
from pyspark.sql import functions as F
for _name in _cfg.__all__:
    globals()[_name] = getattr(_cfg, _name)
trim_lower = _tx.trim_lower
trim_col = _tx.trim_col
is_valid_email = _tx.is_valid_email
date_from_yyyymm = _tx.date_from_yyyymm
load_country_lookup = _tx.load_country_lookup
with_normalized_country = _tx.with_normalized_country
cursor_usage_record_key = _tx.cursor_usage_record_key
'''


def load_nb(path: Path) -> dict:
    return json.loads(path.read_text())


def save_nb(path: Path, nb: dict) -> None:
    path.write_text(json.dumps(nb, indent=2))


def set_first_bootstrap_cell(nb: dict) -> None:
    for cell in nb["cells"]:
        if cell["cell_type"] != "code":
            continue
        src = "".join(cell.get("source", []))
        if "%run common_bootstrap" in src or "%run medallion_entry" in src:
            cell["source"] = [ENTRY_RUN]
            cell["metadata"] = {"name": "medallion_entry"}
            return
    # insert after first markdown
    insert_at = 0
    for i, cell in enumerate(nb["cells"]):
        if cell["cell_type"] == "markdown":
            insert_at = i + 1
            break
    nb["cells"].insert(
        insert_at,
        {
            "cell_type": "code",
            "metadata": {"name": "medallion_entry"},
            "source": [ENTRY_RUN],
            "outputs": [],
            "execution_count": None,
        },
    )


def remove_run_cells(nb: dict, patterns: tuple[str, ...]) -> None:
    kept = []
    for cell in nb["cells"]:
        if cell["cell_type"] != "code":
            kept.append(cell)
            continue
        src = "".join(cell.get("source", [])).strip()
        if any(src.startswith(p) for p in patterns):
            continue
        kept.append(cell)
    nb["cells"] = kept


def clean_cursor_notebook(nb: dict) -> None:
    remove_run_cells(nb, ("%run common_bootstrap", "%run medallion_entry"))
    set_first_bootstrap_cell(nb)

    new_cells = []
    for cell in nb["cells"]:
        if cell["cell_type"] != "code":
            new_cells.append(cell)
            continue
        src = "".join(cell.get("source", []))
        if src.strip().startswith("%%sql"):
            continue
        if "drop table" in src.lower():
            continue
        if src.strip().startswith("# team-usage-events"):
            cell["source"] = [
                line + "\n"
                for line in (
                    "bronze_df = spark.table(TABLE_CURSOR_BRONZE)\n"
                    "if CURSOR_BRONZE_FILENAME:\n"
                    '    bronze_df = bronze_df.filter(F.col("filename") == CURSOR_BRONZE_FILENAME)\n'
                    "bronze = bronze_df\n"
                    "\n"
                    "okta = spark.table(TABLE_OKTA_SILVER)\n"
                    "hc = spark.table(TABLE_HC_SILVER)\n"
                    "\n"
                    'user_norm = F.lower(F.trim(F.col("User")))\n'
                )
            ]
        if "merge_incremental" in src and "write_df" in src:
            if any("merge_incremental" in "".join(c.get("source", [])) for c in new_cells if c["cell_type"] == "code"):
                continue
        new_cells.append(cell)
    nb["cells"] = new_cells

    for cell in nb["cells"]:
        if cell["cell_type"] != "code":
            continue
        src = "".join(cell.get("source", []))
        if "display(" in src:
            cell["source"] = [src.replace("display(", "show_sample(")]


def patch_simple_pipeline(path: Path) -> None:
    nb = load_nb(path)
    remove_run_cells(
        nb,
        (
            "%run common_bootstrap",
            "%run medallion_entry",
            "%run medallion_config",
            "%run medallion_io",
            "%run medallion_transforms",
        ),
    )
    set_first_bootstrap_cell(nb)
    for cell in nb["cells"]:
        if cell["cell_type"] != "code":
            continue
        src = "".join(cell.get("source", []))
        if "display(" in src:
            cell["source"] = [src.replace("display(", "show_sample(")]
    save_nb(path, nb)


def patch_library_notebook(path: Path, code: str) -> None:
    nb = load_nb(path)
    nb["cells"] = [
        {
            "cell_type": "code",
            "metadata": {"name": path.stem},
            "source": [line + "\n" for line in code.strip().split("\n")],
            "outputs": [],
            "execution_count": None,
        }
    ]
    save_nb(path, nb)


def main() -> None:
    save_nb(ROOT / "common_bootstrap.ipynb", COMMON_BOOTSTRAP)

    patch_library_notebook(ROOT / "medallion_config.ipynb", MEDALLION_CONFIG_CELL)
    patch_library_notebook(ROOT / "medallion_io.ipynb", MEDALLION_IO_CELL)
    patch_library_notebook(ROOT / "medallion_transforms.ipynb", MEDALLION_TRANSFORMS_CELL)

    for name in (
        "NB_HCHistorical_Bronze_To_Silver.ipynb",
        "NB_OktaUserforAI_Bronze_To_Silver.ipynb",
        "NB_CursorUsage_Gold.ipynb",
    ):
        patch_simple_pipeline(ROOT / name)

    nb = load_nb(ROOT / "NB_CursorUsage_Bronze_To_Silver.ipynb")
    clean_cursor_notebook(nb)
    save_nb(ROOT / "NB_CursorUsage_Bronze_To_Silver.ipynb", nb)

    print("Synced notebooks to medallion_entry")


if __name__ == "__main__":
    main()
