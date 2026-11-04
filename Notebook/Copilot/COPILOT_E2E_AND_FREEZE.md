# Copilot (Purview) — freeze control and Okta rejects

Same patterns as Cursor silver/gold.

## Run controls (notebook settings cell)

| Knob | Purpose |
|------|---------|
| `COPILOT_CLOSE_MONTH_NO` | Month-close: reload that YYYYMM in silver/gold. Bronze is a **full OneLake deltalake scan** into local cache; `filter_through_close_month_ts` applies month-close / min date in Spark (same as first OneLake rollout). |
| `REBUILD_MEDALLION_CACHE` | OneLake read cache rebuild (shared with other medallion notebooks). |

Set `COPILOT_CLOSE_MONTH_NO` **before** `%run ./Common/bootstrap` via `sync_copilot_close_month_env`. There is no separate bronze month string — same knob as Cursor `CURSOR_CLOSE_MONTH_NO`.

**Reads:** Fabric bronze/silver via `read_table_broken_lineage` (full-table deltalake scan + local cache, then Spark filters). First OneLake scan can take several minutes with `MEDALLION_OBJECT_STORE_CONCURRENCY=1`; the Purview notebook defaults concurrency to `4` and `REBUILD_MEDALLION_CACHE=False` after the cache exists. Scan logs heartbeat every ~20s while rows stream. On HTTP body errors, delete partial cache and retry; transport failures fail fast (no long retry loop).

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
