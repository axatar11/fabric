#!/usr/bin/env python3
"""Ensure pipeline notebooks start with %run ./common/bootstrap"""
from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ENTRY = "%run ./common/bootstrap\n"


def load_nb(path: Path) -> dict:
    return json.loads(path.read_text())


def save_nb(path: Path, nb: dict) -> None:
    path.write_text(json.dumps(nb, indent=2))


def set_bootstrap_cell(nb: dict) -> None:
    for cell in nb["cells"]:
        if cell["cell_type"] != "code":
            continue
        src = "".join(cell.get("source", []))
        if "bootstrap" in src or "medallion_entry" in src or "common_bootstrap" in src:
            cell["source"] = [ENTRY]
            cell["metadata"] = {"name": "bootstrap"}
            return
    idx = 1
    for i, c in enumerate(nb["cells"]):
        if c["cell_type"] == "markdown":
            idx = i + 1
    nb["cells"].insert(
        idx,
        {
            "cell_type": "code",
            "metadata": {"name": "bootstrap"},
            "source": [ENTRY],
            "outputs": [],
            "execution_count": None,
        },
    )


def main() -> None:
    for name in (
        "NB_HCHistorical_Bronze_To_Silver.ipynb",
        "NB_OktaUserforAI_Bronze_To_Silver.ipynb",
        "NB_CursorUsage_Bronze_To_Silver.ipynb",
    ):
        nb = load_nb(ROOT / name)
        set_bootstrap_cell(nb)
        save_nb(ROOT / name, nb)
    for name in ("NB_CursorUsage_Gold.ipynb", "NB_CursorOnboard_Gold.ipynb"):
        nb = load_nb(ROOT / name)
        set_bootstrap_cell(nb)
        save_nb(ROOT / name, nb)
    print("Notebooks point to ./common/bootstrap")


if __name__ == "__main__":
    main()
