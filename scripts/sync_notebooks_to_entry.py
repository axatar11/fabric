#!/usr/bin/env python3
"""Point pipeline notebooks at common/common_bootstrap (single entry)."""
from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

# Fabric: %run Python file under common/ (ipynb bootstrap notebooks removed on main)
ENTRY_RUN = "%run ./common/common_bootstrap\n"


def load_nb(path: Path) -> dict:
    return json.loads(path.read_text())


def save_nb(path: Path, nb: dict) -> None:
    path.write_text(json.dumps(nb, indent=2))


def set_first_bootstrap_cell(nb: dict) -> None:
    for cell in nb["cells"]:
        if cell["cell_type"] != "code":
            continue
        src = "".join(cell.get("source", []))
        if any(
            token in src
            for token in (
                "%run common_bootstrap",
                "%run medallion_entry",
                "%run ./common/common_bootstrap",
            )
        ):
            cell["source"] = [ENTRY_RUN]
            cell["metadata"] = {"name": "common_bootstrap"}
            return
    insert_at = 0
    for i, cell in enumerate(nb["cells"]):
        if cell["cell_type"] == "markdown":
            insert_at = i + 1
            break
    nb["cells"].insert(
        insert_at,
        {
            "cell_type": "code",
            "metadata": {"name": "common_bootstrap"},
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
    remove_run_cells(
        nb,
        (
            "%run common_bootstrap",
            "%run medallion_entry",
            "%run ./common/common_bootstrap",
        ),
    )
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
        if src.strip().startswith("# team-usage-events") or (
            "TABLE_CURSOR_BRONZE" in src and "user_norm" in src and len(src) > 400
        ):
            block = (
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
            cell["source"] = [line + "\n" for line in block.splitlines()]
            cell["metadata"] = {"name": "load_bronze_sources"}
        if "merge_incremental" in src and "write_df" in src:
            if any(
                "merge_incremental" in "".join(c.get("source", []))
                for c in new_cells
                if c["cell_type"] == "code"
            ):
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
            "%run ./common/common_bootstrap",
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


def main() -> None:
    for name in (
        "NB_HCHistorical_Bronze_To_Silver.ipynb",
        "NB_OktaUserforAI_Bronze_To_Silver.ipynb",
        "NB_CursorUsage_Gold.ipynb",
    ):
        patch_simple_pipeline(ROOT / name)

    nb = load_nb(ROOT / "NB_CursorUsage_Bronze_To_Silver.ipynb")
    clean_cursor_notebook(nb)
    save_nb(ROOT / "NB_CursorUsage_Bronze_To_Silver.ipynb", nb)

    print("Synced pipeline notebooks to ./common/common_bootstrap")


if __name__ == "__main__":
    main()
