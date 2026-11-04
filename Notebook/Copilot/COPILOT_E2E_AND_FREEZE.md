# Copilot (Purview) — freeze control and Okta rejects

Same patterns as Cursor silver/gold.

## Run controls (notebook settings cell)

| Knob | Purpose |
|------|---------|
| `COPILOT_CLOSE_MONTH_NO` | Month-close: reload that YYYYMM in silver/gold. **Bronze OneLake scan reads only that calendar month** (`CreationDate` predicate), not the full table. Daily (`None`): scan from `PURVIEW_COPILOT_MIN_CREATION_DATE` onward. |
| `REBUILD_MEDALLION_CACHE` | OneLake read cache rebuild (shared with other medallion notebooks). |

Set `COPILOT_CLOSE_MONTH_NO` **before** `%run ./Common/bootstrap` via `sync_copilot_close_month_env`. There is no separate bronze month string — same knob as Cursor `CURSOR_CLOSE_MONTH_NO`.

**Reads:** Fabric bronze/silver via `read_table_broken_lineage` (deltalake scan + local cache). Purview bronze uses **`copilot_bronze_scan_predicate`** (month-close or min `CreationDate`) and column projection to limit OneLake downloads. Transient HTTP errors retry automatically; if a read fails mid-cache, delete the table folder under `~/.fabric/medallion_cache/` and re-run with `REBUILD_MEDALLION_CACHE=True`.

## Freeze (`Gold.ingest_freeze_control`)

- `product = 'copilot'` or `'all'` — daily runs skip those `Month_No` in bronze→silver and in gold merge.
- Month-close with `COPILOT_CLOSE_MONTH_NO` may still reload that month even if it is listed as frozen.
- After successful month-close gold, `append_copilot_freeze_month` adds a copilot row (deltalake append locally).

Example:

```sql
INSERT INTO Gold.ingest_freeze_control (product, Month_No, frozen_at, frozen_by, notes)
VALUES ('copilot', 202609, current_timestamp(), 'manual', 'September closed');
```

## Silver

- **Matched Okta only** in `purview_copilot_usage` (`split_purview_okta_join` inside `build_purview_copilot_usage_silver`).
- **Rejects** → `purview_copilot_usage_okta_reject`, replaced per batch month via `write_copilot_okta_rejects`. DDL: `Scripts/sql/purview_copilot_usage_okta_reject.sql`.
- **Daily:** `write_copilot_silver_daily` (merge open months; keep frozen slices from target).
- **Month-close:** `replace_one_month_in_frame` + `write_full_table`.

Notebooks: `Notebook/Silver/NB_Purview_Copilot_BronzeToSilver.ipynb`

## Gold

- Exclude frozen `Month_No` before merge (`copilot_excluded_month_nos`).
- **Daily:** incremental merge on `RecordSurrogateKey`.
- **Month-close:** replace that month in `fact_copilot_active`, then auto-freeze.

Notebook: `Notebook/Gold/NB_Purview_Copilot_SilverToGold.ipynb`
