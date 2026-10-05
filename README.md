# Fabric medallion pipelines

Bronze → Silver → Gold for HC, Okta, and Cursor usage.

## Entry (first cell in every pipeline notebook)

```python
%run ./common/bootstrap
```

That loads **`common/`** only:

| File | Role |
|------|------|
| `bootstrap.py` | Reuse or create `spark`, inject `F`, table names, I/O + transforms |
| `config.py` | Lakehouse IDs and `TABLE_*` names |
| `io.py` | Delta overwrite + merge |
| `transforms.py` | Shared column logic |

**Local OneLake:** set `FABRIC_TENANT_ID`, `FABRIC_CLIENT_ID`, and `FABRIC_CLIENT_SECRET` (service principal with lake access). Bootstrap pulls **Delta + hadoop-azure** via Maven for `abfss://` reads.

**Local Spark (recommended):** copy `common/local_settings.example.py` → `common/local_settings.py` (delegates to `create_onelake_spark`). Only replace `create_spark()` if you need extra config; keep `spark.jars.packages` including hadoop-azure.

Table registration (`CREATE TABLE … LOCATION abfss://…`) is **off** by default (it caused catalog/Scala errors on many local setups). Turn on only if you need it:

```powershell
$env:MEDALLION_REGISTER_TABLES = "1"
```

## Local setup (Windows)

```powershell
cd C:\spark-dev
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r fabric\fabric\requirements-local-spark.txt
python fabric\fabric\scripts\verify_local_spark.py
```

If verify fails with `cannot import name '_with_origin'` (or similar), PySpark is **corrupted** — reinstall, do not upgrade in place:

```powershell
.\fabric\fabric\scripts\reinstall_local_spark.ps1
# Deletes .venv under C:\spark-dev (or nearest parent), recreates it, pip install -r requirements-local-spark.txt, runs verify.
# In-place pip only: .\reinstall_local_spark.ps1 -KeepVenv
```

Use **PySpark 3.5.x** with **Spark 3.5** (`SPARK_HOME`). If you see `GenTraversableOnce` / `scala.collection.*` on `spark.table`:

1. **Version mix** — pip PySpark 4.x with `SPARK_HOME` Spark 3.x (or the reverse). Fix:
   ```powershell
   pip install pyspark==3.5.4 delta-spark==3.2.0
   Remove-Item Env:SPARK_HOME -ErrorAction SilentlyContinue
   ```
   Or set `MEDALLION_USE_SPARK_HOME=1` only when both versions match.

2. **Stale session** — restart kernel, or before bootstrap: `$env:MEDALLION_FRESH_SPARK = "1"`.

3. **Reads** — locally we default to `read_table()` (Delta path on OneLake), not `spark.table()`, to avoid a broken local metastore. Check bootstrap output: `path_reads=True`.

4. **`SecureAzureBlobFileSystem not found`** — Spark was started without **hadoop-azure** (stale kernel or custom `create_spark()` without JARs). Fix:
   ```powershell
   $env:MEDALLION_FRESH_SPARK = "1"
   ```
   Restart the Jupyter kernel, re-run bootstrap, or use `create_onelake_spark` from `local_settings.example.py`.

Run `python scripts/verify_local_spark.py` in your venv to sanity-check Spark before opening a notebook.

Open `NB_CursorUsage_Bronze_To_Silver.ipynb` → Run All.

## Fabric

Sync repo including `common/`. First cell: `%run ./common/bootstrap`. Attached lakehouse provides tables; no local OAuth unless you run locally.

## Maintenance

```powershell
python scripts\sync_notebooks_to_entry.py
python scripts\build_local_pipeline_notebook.py
```
