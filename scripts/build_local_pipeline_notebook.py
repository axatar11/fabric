#!/usr/bin/env python3
"""Optional all-in-one notebook for local Run All."""
from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "NB_Medallion_Local_Pipeline.ipynb"
PIPELINES = (
    "NB_HCHistorical_Bronze_To_Silver.ipynb",
    "NB_OktaUserforAI_Bronze_To_Silver.ipynb",
    "NB_CursorUsage_Bronze_To_Silver.ipynb",
    "NB_CursorUsage_Gold.ipynb",
    "NB_CursorOnboard_Gold.ipynb",
)


def main() -> None:
    bootstrap = (ROOT / "common" / "bootstrap.py").read_text()
    cells = [
        {
            "cell_type": "markdown",
            "metadata": {"name": "overview"},
            "source": [
                "# Local pipeline (all-in-one)\n\n"
                "Same as running each `NB_*.ipynb`. Prefer `%run ./common/bootstrap` per notebook in Fabric.\n"
            ],
        },
        {
            "cell_type": "code",
            "metadata": {"name": "bootstrap"},
            "source": [bootstrap],
            "outputs": [],
            "execution_count": None,
        },
    ]
    for nb_name in PIPELINES:
        nb = json.loads((ROOT / nb_name).read_text())
        cells.append(
            {
                "cell_type": "markdown",
                "metadata": {"name": nb_name},
                "source": [f"## {nb_name}\n"],
            }
        )
        skip = True
        for cell in nb["cells"]:
            if cell["cell_type"] != "code":
                continue
            src = "".join(cell.get("source", []))
            if "bootstrap" in src:
                skip = False
                continue
            if skip:
                continue
            cells.append({**cell, "outputs": [], "execution_count": None})
    OUT.write_text(json.dumps({"nbformat": 4, "nbformat_minor": 5, "metadata": {}, "cells": cells}, indent=2))
    print(f"Wrote {OUT}")


if __name__ == "__main__":
    main()
