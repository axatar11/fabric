# Cursor E2E — one switch

## Variable (all 5 notebooks, settings cell **before** bootstrap)

```python
CURSOR_CLOSE_MONTH_NO = None      # daily
# CURSOR_CLOSE_MONTH_NO = 202610   # month-close
```

| Value | Behavior |
|-------|----------|
| **`None`** | Daily: process bronze/silver/gold for **non-frozen** months only (`Gold.ingest_freeze_control`). HC silver keeps frozen months. |
| **`202610`** | Month-close: reload **October 2026 only** (HC, cursor bronze/silver, both gold). **Okta:** full bronze → silver (all rows; no `updated_at` slice). **Freeze:** appended after `NB_CursorOnboard_Gold`. |

Same line in:

1. `NB_OktaUserforAI_Bronze_To_Silver` (full load only — `CURSOR_CLOSE_MONTH_NO` does not filter Okta)
2. `NB_HCHistorical_Bronze_To_Silver`
3. `NB_CursorUsage_Bronze_To_Silver`
4. `NB_CursorUsage_Gold`
5. `NB_CursorOnboard_Gold`

## One-time DDL

`Scripts/sql/cursor_ingest_freeze_control.sql`

## Month-close run order

1. Set **`CURSOR_CLOSE_MONTH_NO = 202610`** in all 5 settings cells.
2. In cursor silver, set **`CURSOR_BRONZE_FILENAMES = []`** (or every October file) for a full month bronze load.
3. Okta silver → HC silver (Oct HC in bronze) → Cursor silver → Gold active → Gold onboard.
4. Freeze row is appended automatically at end of onboard gold (no manual INSERT unless you skipped onboard).

## Daily run

**`CURSOR_CLOSE_MONTH_NO = None`** everywhere. Run pipeline as usual.

## Tables

| Silver | Gold |
|--------|------|
| `okta_user_for_ai` | `fact_cursor_active` |
| `hc_silver_historical` | `fact_cursor_onboard` |
| `cursor_usage` | |
| `cursor_usage_okta_reject` | |
