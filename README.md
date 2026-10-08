# Fabric medallion pipelines

Bronze → Silver → Gold for HC, Okta, and cursor_usage.

## Layout

| Folder | Notebooks |
|--------|-----------|
| `Notebook/Bronze/` | (ingest in Fabric — no pipeline notebooks) |
| `Notebook/Silver/` | `NB_HCHistorical_Bronze_To_Silver`, `NB_OktaUserforAI_Bronze_To_Silver`, `NB_CursorUsage_Bronze_To_Silver` |
| `Notebook/Gold/` | `NB_CursorUsage_Gold`, `NB_CursorOnboard_Gold` |
| `common/` | Bootstrap, config, I/O, transforms — see **`common/README.md`** |

Open this repository as the VS Code workspace root (the folder that contains `common` and `Notebook`). Notebook settings cells adjust the working directory when needed so `%run ./common/bootstrap` resolves correctly.

## Local development (Windows)

**Path:** clone or sync to `C:\spark-dev\CoE_transformation_framework`.

**Requirements:** Python 3.11 or 3.12, Azure CLI, Java 11+ (for local Spark).

1. Open **VS Code** → **File → Open Folder** → `C:\spark-dev\CoE_transformation_framework`.

2. Create and use a virtual environment **in this repo** (kernel must match the env where packages are installed):

```powershell
cd C:\spark-dev\CoE_transformation_framework
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements-local-spark.txt
python scripts\verify_local_spark.py
python scripts\check_env.py
az login
```

3. **Python: Select Interpreter** → `CoE_transformation_framework\.venv\Scripts\python.exe`. Restart the notebook kernel after changing interpreter.

4. Optional Jupyter kernel name:

```powershell
python -m pip install ipykernel
python -m ipykernel install --user --name coe-medallion --display-name "CoE Medallion (Py3.12)"
```

## Running notebooks

After each kernel restart: run **settings** cell → **bootstrap** → remaining cells.

**Settings (before bootstrap):**

```python
import os
os.environ.setdefault("MEDALLION_DEBUG_READ", "1")
os.environ.setdefault("MEDALLION_LOCAL_TABLE_CACHE", "1")
os.environ.setdefault("MEDALLION_SCAN_CHUNK_ROWS", "75000")
os.environ.setdefault("MEDALLION_SCAN_RETRIES", "6")
```

**Bootstrap:**

```python
%run ./common/bootstrap
```

**Order:**  
`Notebook/Silver/NB_HCHistorical_Bronze_To_Silver` → `Notebook/Silver/NB_OktaUserforAI_Bronze_To_Silver` → `Notebook/Silver/NB_CursorUsage_Bronze_To_Silver` → `Notebook/Gold/NB_CursorUsage_Gold` → `Notebook/Gold/NB_CursorOnboard_Gold`.

Local reads use **deltalake scan** and `az login` by default. Table paths and lakehouse IDs are in `common/config.py`. Optional overrides: `common/local_settings.py` (see `local_settings.example.py`).

## Troubleshooting

| Issue | Action |
|-------|--------|
| `No module named 'pyspark'` | Wrong kernel — select `.venv\Scripts\python.exe` from **this** repo, restart kernel, or run `pip install -r requirements-local-spark.txt` in that venv. |
| Corrupted PySpark | `.\scripts\reinstall_local_spark.ps1` from repo root |
| Scala / `GenTraversableOnce` | `pip install pyspark==3.5.4 delta-spark==3.2.0`; clear `SPARK_HOME` |
| Stale Spark session | Restart kernel or `$env:MEDALLION_FRESH_SPARK = "1"` before bootstrap |
| OneLake auth | `az login` |
| `PermissionError` on bootstrap (PySpark `Temp\\…`) | Restart kernel; set `$env:MEDALLION_FRESH_SPARK="1"`; optional `$env:MEDALLION_SPARK_TMP="$env:USERPROFILE\.fabric\spark_tmp"`. Close other notebooks using Spark. |

## Fabric (cloud)

Sync repo including `common/`. Run settings (optional), then `%run ./common/bootstrap`. Attached lakehouse supplies tables.

## Maintenance

```powershell
cd C:\spark-dev\CoE_transformation_framework
python scripts\sync_notebooks_to_entry.py
```
