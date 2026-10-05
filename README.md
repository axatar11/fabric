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

**Local with your own Spark code (recommended):** copy `common/local_settings.example.py` → `common/local_settings.py` and implement `create_spark()`.

Table registration (`CREATE TABLE … LOCATION abfss://…`) is **off** by default (it caused catalog/Scala errors on many local setups). Turn on only if you need it:

```powershell
$env:MEDALLION_REGISTER_TABLES = "1"
```

## Local setup (Windows)

```powershell
cd C:\spark-dev\fabric\fabric
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements-local-spark.txt
```

Use **PySpark 3.5.x** with **Spark 3.5** (`SPARK_HOME`). If you see `GenTraversableOnce` / `scala.collection.*` errors, pip PySpark and `SPARK_HOME` versions do not match, or `spark.jars.packages` pulled the wrong Delta build. Fix:

- `pip install pyspark==3.5.4 delta-spark==3.2.0`, and set `SPARK_HOME` to Spark 3.5, **or**
- Put your session in `common/local_settings.py` → `create_spark()` (no default Maven packages).

Open `NB_CursorUsage_Bronze_To_Silver.ipynb` → Run All.

## Fabric

Sync repo including `common/`. First cell: `%run ./common/bootstrap`. Attached lakehouse provides tables; no local OAuth unless you run locally.

## Maintenance

```powershell
python scripts\sync_notebooks_to_entry.py
python scripts\build_local_pipeline_notebook.py
```
