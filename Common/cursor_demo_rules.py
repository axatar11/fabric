"""Pure-Python Cursor silver/gold rules for local demos and unit tests (no OneLake)."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any, Iterable, Mapping, Sequence


def month_no_from_date(value: date | datetime | str) -> int:
    """YYYYMM int from a date, datetime, or ISO date string."""
    if isinstance(value, str):
        text = value.strip()[:10]
        dt = datetime.strptime(text, "%Y-%m-%d").date()
    elif isinstance(value, datetime):
        dt = value.date()
    else:
        dt = value
    return dt.year * 100 + dt.month


def closed_month_nos_from_lists(
    gold_months: Sequence[int],
    batch_month_nos: Sequence[int],
    extra_closed: Sequence[int] | None = None,
) -> tuple[list[int], int | None]:
    """Same rule as gold merge: close gold months strictly before batch max month."""
    batch_max = max(batch_month_nos) if batch_month_nos else None
    extra = [int(x) for x in (extra_closed or [])]
    if batch_max is None:
        return sorted(set(extra)), None
    closed = sorted(int(m) for m in gold_months if int(m) < batch_max)
    if extra:
        closed = sorted(set(closed) | set(extra))
    return closed, batch_max


def filter_rows_by_closed_months(
    rows: Sequence[Mapping[str, Any]],
    month_col: str,
    closed_month_nos: Sequence[int],
) -> list[dict[str, Any]]:
    if not closed_month_nos:
        return [dict(r) for r in rows]
    closed = {int(m) for m in closed_month_nos}
    out: list[dict[str, Any]] = []
    for row in rows:
        month = row.get(month_col)
        if month is None:
            out.append(dict(row))
            continue
        if int(month) in closed:
            continue
        out.append(dict(row))
    return out


def _okta_emails(okta_row: Mapping[str, Any]) -> set[str]:
    emails: set[str] = set()
    for key in ("OktaUserEmail", "OktaUserFullEmail"):
        val = okta_row.get(key)
        if val is not None and str(val).strip():
            emails.add(str(val).strip().lower())
    return emails


def split_cursor_okta_rows(
    usage_rows: Sequence[Mapping[str, Any]],
    okta_rows: Sequence[Mapping[str, Any]],
    *,
    user_col: str = "CursorUsageUser",
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Split usage rows into (matched with Okta attrs, rejects) — mirrors Spark join logic."""
    okta_by_email: dict[str, dict[str, Any]] = {}
    for o in okta_rows:
        row = dict(o)
        for email in _okta_emails(row):
            okta_by_email.setdefault(email, row)

    matched: list[dict[str, Any]] = []
    rejects: list[dict[str, Any]] = []
    seen_matched: set[tuple[Any, ...]] = set()

    for raw in usage_rows:
        base = dict(raw)
        user = base.get(user_col)
        if user is None:
            rejects.append(base)
            continue
        key = str(user).strip().lower()
        okta = okta_by_email.get(key)
        if okta is None:
            rejects.append(base)
            continue
        out = {**base, **okta}
        sig = tuple(sorted(out.items()))
        if sig not in seen_matched:
            seen_matched.add(sig)
            matched.append(out)
    return matched, rejects


def aggregate_cursor_gold_rows(
    matched_rows: Sequence[Mapping[str, Any]],
    *,
    date_col: str = "CursorUsageDate",
    okta_id_col: str = "OktaUserId",
) -> list[dict[str, Any]]:
    """Minimal daily gold grain: Month_No, LastModification, OktaUserId, Request count."""
    buckets: dict[tuple[str, str], dict[str, Any]] = {}
    for row in matched_rows:
        usage_date = str(row.get(date_col, ""))[:10]
        okta_id = str(row.get(okta_id_col, ""))
        key = (usage_date, okta_id)
        month_no = month_no_from_date(usage_date)
        if key not in buckets:
            buckets[key] = {
                "Month_No": month_no,
                "LastModification": usage_date,
                "OktaUserId": okta_id,
                "Request": 0,
            }
        buckets[key]["Request"] = int(buckets[key]["Request"]) + 1
    return list(buckets.values())
