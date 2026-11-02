# Cursor — daily E2E and month freeze

## One-time: freeze control table

Run in **AI_Medallion** lakehouse SQL (or Spark SQL):

`Scripts/sql/cursor_ingest_freeze_control.sql`

## Daily E2E (order)

1. Okta bronze (API) → `NB_OktaUserforAI_Bronze_To_Silver`
2. HC bronze (when new month) → `NB_HCHistorical_Bronze_To_Silver`
3. Cursor bronze → `NB_CursorUsage_Bronze_To_Silver`  
   Skips `CursorUsageMonthNo` in `Gold.ingest_freeze_control` (`product` = `cursor` or `all`).
4. `NB_CursorUsage_Gold` → `fact_cursor_active_*`
5. `NB_CursorOnboard_Gold` → `fact_cursor_onboard_*`

Sandbox tables default suffix `_abdellah` (`MEDALLION_CURSOR_TABLE_SUFFIX`).

## Month-close (one last full recompute, then freeze)

Set **the same** month on all Cursor notebooks (or env once before bootstrap):

```python
CURSOR_PROCESS_MONTH_NO = 202610
# or: os.environ["MEDALLION_CURSOR_PROCESS_MONTH_NO"] = "202610"
```

| Layer | Scope when set |
|-------|----------------|
| Bronze | `Date` in that calendar month |
| HC join | `HCMonthNo` = that month only |
| Silver usage | `CursorUsageMonthNo` = that month |
| Gold | Reads/filters silver to that month only |

Okta stays **full current** (needed to match users on that month’s events).

Order: HC silver for that month → Cursor silver → both gold → then:

```sql
INSERT INTO Gold.ingest_freeze_control VALUES
  ('cursor', 202610, current_timestamp(), 'you@domain.com', 'Oct 2026 close');
```

Daily runs: leave `CURSOR_PROCESS_MONTH_NO` **unset** — only non-frozen months process.

## Optional: auto-close older months already in gold

Set before bootstrap: `MEDALLION_CURSOR_AUTO_CLOSE_FROM_GOLD=1`  
(unions with freeze control; default `0`).

## Verify

| Check | How |
|-------|-----|
| Freeze row | `SELECT * FROM Gold.ingest_freeze_control WHERE product IN ('cursor','all')` |
| Silver skip | Notebook log: `cursor silver: skip frozen Month_No [...]` |
| Gold skip | Notebook log: `excluded from merge: [...]` |
| Active gold | `SELECT Month_No, count(*) FROM Gold.fact_cursor_active_abdellah GROUP BY Month_No` |
| Rejects | `SELECT * FROM Silver.cursor_usage_okta_reject_abdellah` |
