#!/usr/bin/env python3
"""Dummy tests for Cursor Okta reject + gold closed-month logic (no OneLake)."""

from __future__ import annotations

import csv
import sys
import unittest
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

DATA = REPO / "tests" / "cursor_demo" / "data"


def _load_tsv(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as fh:
        return list(csv.DictReader(fh, delimiter="\t"))


class TestClosedMonthNos(unittest.TestCase):
    def test_october_in_gold_november_batch_closes_october(self) -> None:
        closed, batch_max = closed_month_nos_from_lists([202610], [202611])
        self.assertEqual(batch_max, 202611)
        self.assertEqual(closed, [202610])

    def test_same_month_batch_keeps_month_open(self) -> None:
        closed, batch_max = closed_month_nos_from_lists([202610], [202610])
        self.assertEqual(batch_max, 202610)
        self.assertEqual(closed, [])

    def test_empty_gold_nothing_closed(self) -> None:
        closed, batch_max = closed_month_nos_from_lists([], [202610])
        self.assertEqual(batch_max, 202610)
        self.assertEqual(closed, [])


class TestOktaSplit(unittest.TestCase):
    def test_fixture_usage_splits_reject(self) -> None:
        usage = _load_tsv(DATA / "usage_demo.tsv")
        okta = _load_tsv(DATA / "okta_demo.tsv")
        matched, rejects = split_cursor_okta_rows(usage, okta)
        self.assertEqual(len(rejects), 1)
        self.assertEqual(rejects[0]["CursorUsageUser"], "unknown.user@nttdata.com")
        self.assertGreaterEqual(len(matched), 3)


class TestGoldFilter(unittest.TestCase):
    def test_closed_month_dropped_from_merge_batch(self) -> None:
        rows = aggregate_cursor_gold_rows(
            [
                {"CursorUsageDate": "2026-10-05", "OktaUserId": "00uAAA"},
                {"CursorUsageDate": "2026-11-02", "OktaUserId": "00uAAA"},
            ]
        )
        closed, _ = closed_month_nos_from_lists([202610], [202611])
        merged = filter_rows_by_closed_months(rows, "Month_No", closed)
        months = {r["Month_No"] for r in merged}
        self.assertEqual(months, {202611})


if __name__ == "__main__":
    unittest.main()
