# Cursor E2E — one switch

## Variable (all 5 notebooks, settings cell **before** bootstrap)

```python
CURSOR_CLOSE_MONTH_NO = None      # daily
# CURSOR_CLOSE_MONTH_NO = 202610   # month-close
```

| Value | Behavior |
|-------|----------|
| **`None`** | Daily: process bronze/silver/gold for **non-frozen** months only (`Gold.ingest_freeze_control`). |
| **`202610`** | Month-close: reload **October 2026 only** (bronze, HC, cursor silver, both gold), then **auto-freeze** after `NB_CursorOnboard_Gold`. |

Same line in:

1. `NB_OktaUserforAI_Bronze_To_Silver` (Okta still full load; switch kept for consistency)
2. `NB_HCHistorical_Bronze_To_Silver`
3. `NB_CursorUsage_Bronze_To_Silver`
4. `NB_CursorUsage_Gold`
5. `NB_CursorOnboard_Gold`

## One-time DDL

`Scripts/sql/cursor_ingest_freeze_control.sql`

## Month-close run order

1. Set **`CURSOR_CLOSE_MONTH_NO = 202610`** in all 5 settings cells.
2. Okta silver → HC silver (Oct HC in bronze) → Cursor silver → Gold active → Gold onboard.
3. Freeze row is appended automatically at end of onboard gold (no manual INSERT unless you skipped onboard).

## Daily run

**`CURSOR_CLOSE_MONTH_NO = None`** everywhere. Run pipeline as usual.

## Tables (default `_abdellah`)

| Silver | Gold |
|--------|------|
| `okta_user_for_ai` | `fact_cursor_active_abdellah` |
| `hc_silver_historical` | `fact_cursor_onboard_abdellah` |
| `cursor_usage_abdellah` | |
| `cursor_usage_okta_reject_abdellah` | |
