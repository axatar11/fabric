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

## Freeze a month

```sql
INSERT INTO Gold.ingest_freeze_control VALUES
  ('cursor', 202609, current_timestamp(), 'you@domain.com', 'Sep 2026 close');
```

Next daily runs skip **202609** on silver and both gold merges.

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
