#!/usr/bin/env python3
"""Generate NB_Medallion_Local_Pipeline.ipynb (optional all-in-one for local Run All)."""
from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "NB_Medallion_Local_Pipeline.ipynb"

PIPELINE_NOTEBOOKS = (
    "NB_HCHistorical_Bronze_To_Silver.ipynb",
    "NB_OktaUserforAI_Bronze_To_Silver.ipynb",
    "NB_CursorUsage_Bronze_To_Silver.ipynb",
    "NB_CursorUsage_Gold.ipynb",
)


def load_nb(path: Path) -> dict:
    return json.loads(path.read_text())


def code_cells_after_entry(nb: dict) -> list[dict]:
    cells: list[dict] = []
    seen_entry = False
    for cell in nb["cells"]:
        if cell["cell_type"] != "code":
            continue
        src = "".join(cell.get("source", []))
        if "%run medallion_entry" in src or "%run common_bootstrap" in src:
            seen_entry = True
            continue
        if not seen_entry and not cells:
            continue
        cells.append(cell)
    return cells


def main() -> None:
    entry = load_nb(ROOT / "medallion_entry.ipynb")
    cells: list[dict] = [
        md(
            "overview",
            "# Medallion local pipeline (all-in-one)\n\n"
            "Same logic as `%run` pipeline notebooks, single **Run All** for local Jupyter.\n"
            "For Fabric/Databricks, prefer individual `NB_*.ipynb` with `%run medallion_entry`.\n",
        )
    ]
    cells.extend(entry["cells"])

    for nb_name in PIPELINE_NOTEBOOKS:
        nb = load_nb(ROOT / nb_name)
        title = nb_name.replace(".ipynb", "")
        cells.append(md(f"{title}_header", f"## {title}"))
        for cell in code_cells_after_entry(nb):
            meta = dict(cell.get("metadata") or {})
            meta.setdefault("name", title)
            cells.append({**cell, "metadata": meta, "outputs": [], "execution_count": None})

    notebook = {
        "nbformat": 4,
        "nbformat_minor": 5,
        "metadata": {
            "kernelspec": {
                "display_name": "Python 3 (PySpark)",
                "language": "python",
                "name": "python3",
            },
            "language_info": {"name": "python"},
        },
        "cells": cells,
    }
    OUT.write_text(json.dumps(notebook, indent=2))
    print(f"Wrote {OUT} ({len(cells)} cells)")


def md(name: str, text: str) -> dict:
    return {
        "cell_type": "markdown",
        "metadata": {"name": name},
        "source": [line + "\n" for line in text.strip().split("\n")],
    }


if __name__ == "__main__":
    main()
