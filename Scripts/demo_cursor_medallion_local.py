#!/usr/bin/env python3
"""Play with Cursor Okta reject + gold closed-month logic using local TSV fixtures only.

No OneLake, no Spark required. Writes under tests/cursor_demo/out/.

  python Scripts/demo_cursor_medallion_local.py
  python Scripts/demo_cursor_medallion_local.py --usage tests/cursor_demo/data/my_new_usage.tsv
  python Scripts/demo_cursor_medallion_local.py --commit-gold
"""
from __future__ import annotations

import argparse
import csv
import sys
from datetime import datetime, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from Common.cursor_demo_rules import (  # noqa: E402
    aggregate_cursor_gold_rows,
    closed_month_nos_from_lists,
    filter_rows_by_closed_months,
    split_cursor_okta_rows,
)

DEFAULT_DATA = REPO / "tests" / "cursor_demo" / "data"
DEFAULT_OUT = REPO / "tests" / "cursor_demo" / "out"


def _load_tsv(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as fh:
        return list(csv.DictReader(fh, delimiter="\t"))


def _append_tsv(path: Path, rows: list[dict[str, object]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    write_header = not path.is_file()
    with path.open("a", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames, delimiter="\t", extrasaction="ignore")
        if write_header:
            writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in fieldnames})


def _read_gold_months(path: Path) -> list[int]:
    if not path.is_file():
        return []
    text = path.read_text(encoding="utf-8").strip()
    if not text:
        return []
    return sorted(int(line.strip()) for line in text.splitlines() if line.strip())


def _write_gold_months(path: Path, months: list[int]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(str(m) for m in sorted(set(months))) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=DEFAULT_DATA,
        help="Folder with okta_demo.tsv and usage TSV(s)",
    )
    parser.add_argument(
        "--usage",
        type=Path,
        default=None,
        help="Usage TSV for this run (default: usage_demo.tsv in data-dir)",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=DEFAULT_OUT,
        help="Local output (rejects append, gold simulation)",
    )
    parser.add_argument(
        "--commit-gold",
        action="store_true",
        help="After dry-run, add merged batch months to gold_months_in_table.txt",
    )
    args = parser.parse_args()

    okta_path = args.data_dir / "okta_demo.tsv"
    usage_path = args.usage or (args.data_dir / "usage_demo.tsv")
    gold_state = args.data_dir / "gold_months_in_table.txt"

    if not okta_path.is_file():
        print(f"Missing {okta_path}", file=sys.stderr)
        return 1
    if not usage_path.is_file():
        print(f"Missing {usage_path}", file=sys.stderr)
        return 1

    okta = _load_tsv(okta_path)
    usage = _load_tsv(usage_path)
    gold_months = _read_gold_months(gold_state)

    matched, rejects = split_cursor_okta_rows(usage, okta)
    loaded_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    reject_rows = [
        {
            **r,
            "OktaRejectReason": "okta_join_miss",
            "OktaRejectLoadedAt": loaded_at,
            "SourceUsageFile": str(usage_path.name),
        }
        for r in rejects
    ]
    reject_out = args.out_dir / "cursor_usage_okta_reject.tsv"
    if reject_rows:
        reject_fields = list(reject_rows[0].keys())
        _append_tsv(reject_out, reject_rows, reject_fields)

    gold_batch = aggregate_cursor_gold_rows(matched)
    batch_months = sorted({int(r["Month_No"]) for r in gold_batch})
    closed, batch_max = closed_month_nos_from_lists(gold_months, batch_months)
    merge_batch = filter_rows_by_closed_months(gold_batch, "Month_No", closed)

    print("=== Cursor local demo (no OneLake) ===")
    print(f"Usage file:     {usage_path}")
    print(f"Okta rows:      {len(okta)}")
    print(f"Usage rows:     {len(usage)}")
    print(f"Matched:        {len(matched)}")
    print(f"Okta rejects:   {len(rejects)} -> append {reject_out}")
    print(f"Gold months:    {gold_months or '(empty)'}  ({gold_state})")
    print(f"Batch months:   {batch_months}  (max={batch_max})")
    print(f"Auto-closed:    {closed or '(none)'}")
    print(f"Rows to merge:  {len(merge_batch)}")
    for row in merge_batch:
        print(
            f"  Month_No={row['Month_No']} OktaUserId={row['OktaUserId']} "
            f"Request={row['Request']} LastModification={row['LastModification']}"
        )

    merge_out = args.out_dir / "fact_cursor_active_merge_batch.tsv"
    if merge_batch:
        _append_tsv(
            merge_out,
            merge_batch,
            ["Month_No", "LastModification", "OktaUserId", "Request"],
        )
        print(f"Merge batch logged: {merge_out}")

    if args.commit_gold and merge_batch:
        new_months = sorted(set(gold_months) | set(batch_months))
        _write_gold_months(gold_state, new_months)
        print(f"Updated gold months -> {gold_state}: {new_months}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
