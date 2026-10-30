# Cursor logic sandbox (dummy data)

Play with **Okta reject append** and **gold auto-closed months** without OneLake.

## Files

| Path | Purpose |
|------|---------|
| `data/okta_demo.tsv` | Fake Okta users |
| `data/usage_demo.tsv` | Sample usage (includes one unknown email) |
| `data/gold_months_in_table.txt` | Simulates distinct `Month_No` already in gold (`202610` by default) |
| `out/` | Appended rejects + merge batch logs (gitignored via `out/.gitkeep` if present) |

## Run

From repo root (no venv required for the demo script):

```powershell
python Scripts\demo_cursor_medallion_local.py
python -m unittest tests.test_cursor_demo_rules -v
```

Add rows to `data/usage_demo.tsv` or copy it to `my_batch.tsv` and run:

```powershell
python Scripts\demo_cursor_medallion_local.py --usage tests\cursor_demo\data\my_batch.tsv
```

Simulate landing data in gold (updates `gold_months_in_table.txt`):

```powershell
python Scripts\demo_cursor_medallion_local.py --commit-gold
```

Then run again with November usage: October should appear in **Auto-closed** when batch max is `202611`.

## Spark / real tables

Notebooks still target Fabric. Use this folder only to validate rules before running `NB_CursorUsage_*` against lakehouses.
