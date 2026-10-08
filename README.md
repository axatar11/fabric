# Fabric medallion pipelines

Bronze → Silver → Gold for HC, Okta, and Cursor usage.

## Layout

| Folder | Notebooks |
|--------|-----------|
| `bronze/` | (ingest in Fabric — no pipeline notebooks) |
| `silver/` | `NB_HCHistorical_Bronze_To_Silver`, `NB_OktaUserforAI_Bronze_To_Silver`, `NB_CursorUsage_Bronze_To_Silver` |
| `gold/` | `NB_CursorUsage_Gold`, `NB_CursorOnboard_Gold` |
| `common/` | Shared bootstrap, config, I/O, transforms — see **`common/README.md`** |

Open notebooks from **`silver/`** or **`gold/`** in Fabric/Jupyter (repo root = notebook working directory for `%run`).

## Entry (every table notebook — run in order after kernel restart)

**Cell 1 — local read/cache settings** (must run before bootstrap):

```python
import os
os.environ.setdefault("MEDALLION_DEBUG_READ", "1")
os.environ.setdefault("MEDALLION_LOCAL_TABLE_CACHE", "1")
os.environ.setdefault("MEDALLION_SCAN_CHUNK_ROWS", "75000")
os.environ.setdefault("MEDALLION_SCAN_RETRIES", "6")
# Force OneLake read: os.environ["MEDALLION_CACHE_REFRESH"] = "1"
# Clear cache: delete %USERPROFILE%\.fabric\medallion_cache
```

**Cell 2 — bootstrap:**

```python
# Settings cell chdirs to repo root when the kernel starts in silver/ or gold/
%run ./common/bootstrap
```

Run notebooks **one table at a time** (no all-in-one orchestration notebook):  
`silver/NB_HCHistorical_Bronze_To_Silver` → `silver/NB_OktaUserforAI_Bronze_To_Silver` → `silver/NB_CursorUsage_Bronze_To_Silver` → `gold/NB_CursorUsage_Gold` → `gold/NB_CursorOnboard_Gold`.

That loads **`common/`** only:

| File | Role |
|------|------|
| `bootstrap.py` | Reuse or create `spark`, inject `F`, table names, I/O + transforms |
| `config.py` | Lakehouse IDs and `TABLE_*` names |
| `io.py` | Delta overwrite + merge |
| `transforms.py` | Shared column logic |

**Local OneLake reads**: `az login`, then `read_table()` / `load_delta_path()` using **deltalake `scan()`** (column mapping, no PySpark abfss hang) → Spark DataFrame. Force Fabric-style PySpark load: `$env:MEDALLION_PYSPARK_ABFSS_READ = "1"`.

```powershell
az login
pip install -r fabric\fabric\requirements-local-spark.txt
```

You do **not** need `local_settings.py` for default tables — paths are built in `config.py` (same as your Fabric ABFS path for cursor bronze). Copy `local_settings.example.py` → `local_settings.py` only to override paths or Spark.

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

3. **Reads** — locally `read_table()` uses **azure-cli+deltalake** (bootstrap banner). Not `spark.table()`. Run **`az login`** first.

4. **Auth errors on read** — sign in with Azure CLI (`az login`). Ensure Azure CLI is installed (bootstrap prepends the default Windows Azure CLI path when present).

5. **Force Spark ABFS reads** (optional, needs service principal + hadoop-azure JARs): `$env:MEDALLION_SPARK_DELTA_READ = "1"` plus OAuth env vars — not the default.

Run `python scripts/verify_local_spark.py` in your venv to sanity-check Spark before opening a notebook.

Open each `NB_*` notebook in pipeline order and run all cells (settings → bootstrap → reads/writes).

## Fabric

Sync repo including `common/`. Run the local settings cell (optional on Fabric), then `%run ./common/bootstrap`. Attached lakehouse provides tables; no local OAuth unless you run locally.

## Maintenance

```powershell
python scripts\sync_notebooks_to_entry.py
```
