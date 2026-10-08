# Fabric medallion pipelines

Bronze → Silver → Gold for HC, Okta, and Cursor usage.

## Layout

| Folder | Notebooks |
|--------|-----------|
| `bronze/` | (ingest in Fabric — no pipeline notebooks) |
| `silver/` | `NB_HCHistorical_Bronze_To_Silver`, `NB_OktaUserforAI_Bronze_To_Silver`, `NB_CursorUsage_Bronze_To_Silver` |
| `gold/` | `NB_CursorUsage_Gold`, `NB_CursorOnboard_Gold` |
| `common/` | Shared bootstrap, config, I/O, transforms — see **`common/README.md`** |

Open notebooks from **`silver/`** or **`gold/`**. The first settings cell moves the kernel to the **repo root** when needed so `%run ./common/bootstrap` works.

---

## New users: folder path and Python environment (Windows)

### 1. Where the repo should live

Use this clone path (CoE standard):

```text
C:\spark-dev\CoE_transformation_framework\
├── common\
├── silver\
├── gold\
├── scripts\
└── requirements-local-spark.txt
```

Clone or sync the Git repo into **`C:\spark-dev\CoE_transformation_framework`**.  
Do **not** rely on the old nested path `C:\spark-dev\fabric\fabric\`.

In **Cursor / VS Code**: **File → Open Folder** → select `C:\spark-dev\CoE_transformation_framework` (the folder that contains `common` and `silver`).

### 2. Choose a virtual environment (pick one)

| Option | Venv location | When to use |
|--------|----------------|-------------|
| **A — Shared (recommended)** | `C:\spark-dev\.venv` | One env for CoE / multiple repos under `C:\spark-dev` |
| **B — Repo-local** | `C:\spark-dev\CoE_transformation_framework\.venv` | Isolated deps only for this repo |

**Python version:** **3.11 or 3.12** only (not 3.13+). PySpark **3.5.4** + **delta-spark 3.2.0**.

**Create shared venv (option A):**

```powershell
cd C:\spark-dev
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r CoE_transformation_framework\requirements-local-spark.txt
python CoE_transformation_framework\scripts\verify_local_spark.py
```

**Create repo-local venv (option B):**

```powershell
cd C:\spark-dev\CoE_transformation_framework
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements-local-spark.txt
python scripts\verify_local_spark.py
```

### 3. Two repos side by side (old + new folder)

Copying or recloning into `CoE_transformation_framework` **does not copy Python packages**. A **new** `.venv` inside the new folder starts **empty** (no `pyspark`).

| What you see | Fix |
|--------------|-----|
| `ModuleNotFoundError: No module named 'pyspark'` on bootstrap | Kernel is wrong or venv never had `pip install -r requirements-local-spark.txt` |
| Old repo still works | Old notebook uses **`C:\spark-dev\.venv`** (or another env with PySpark) |

**Recommended:** use **one shared venv** for both folders:

```powershell
C:\spark-dev\.venv\Scripts\python.exe
```

In the **new** repo: **Python: Select Interpreter** → that same `python.exe` → **Restart kernel** → settings → bootstrap.

Check from PowerShell:

```powershell
cd C:\spark-dev\CoE_transformation_framework
C:\spark-dev\.venv\Scripts\python.exe scripts\check_env.py
```

You should see `OK — pyspark 3.5.4`. If not:

```powershell
C:\spark-dev\.venv\Scripts\Activate.ps1
pip install -r C:\spark-dev\CoE_transformation_framework\requirements-local-spark.txt
```

### 4. Point the IDE at the correct interpreter

After opening the **`CoE_transformation_framework`** folder:

1. **Ctrl+Shift+P** → **Python: Select Interpreter**
2. Choose:
   - `C:\spark-dev\.venv\Scripts\python.exe` (shared), or
   - `C:\spark-dev\CoE_transformation_framework\.venv\Scripts\python.exe` (local)

For notebooks: pick the **same** interpreter as the **Jupyter kernel** (top-right kernel picker). If kernels are missing:

```powershell
.\.venv\Scripts\Activate.ps1
python -m pip install ipykernel
python -m ipykernel install --user --name coe-medallion --display-name "CoE Medallion (Py3.12)"
```

Then select kernel **CoE Medallion (Py3.12)** in the notebook.

Optional: set **`MEDALLION_PYTHON`** to a specific `python.exe` before running `reinstall_local_spark.ps1`.

Workspace defaults (`.vscode/settings.json`) assume shared venv at `C:\spark-dev\.venv`; change that path if you use option B.

### 5. Azure login (required for local OneLake reads/writes)

```powershell
az login
```

### 6. Run pipelines

1. Open e.g. `silver\NB_HCHistorical_Bronze_To_Silver.ipynb`
2. **Kernel restart** after any env or git change
3. Run **settings** cell → **bootstrap** → remaining cells

Bootstrap prints **`Repo root:`** — it should be `C:\spark-dev\CoE_transformation_framework`. If not, you opened the wrong folder or the settings cell could not find `common\bootstrap.py`.

**Run order:**  
`silver/NB_HCHistorical_Bronze_To_Silver` → `silver/NB_OktaUserforAI_Bronze_To_Silver` → `silver/NB_CursorUsage_Bronze_To_Silver` → `gold/NB_CursorUsage_Gold` → `gold/NB_CursorOnboard_Gold`.

---

## Entry (every table notebook)

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
%run ./common/bootstrap
```

That loads **`common/`** (see **`common/README.md`**).

**Local OneLake reads**: `az login`, then `read_table()` using **deltalake `scan()`** by default.

You do **not** need `local_settings.py` for default tables — paths are in `config.py`. Copy `common/local_settings.example.py` → `common/local_settings.py` only for overrides.

Table registration (`CREATE TABLE … LOCATION abfss://…`) is **off** by default:

```powershell
$env:MEDALLION_REGISTER_TABLES = "1"
```

---

## Local setup troubleshooting

If verify fails with `cannot import name '_with_origin'` (corrupted PySpark):

```powershell
cd C:\spark-dev\CoE_transformation_framework
.\scripts\reinstall_local_spark.ps1
# Uses C:\spark-dev\.venv if present, else creates .venv near repo (see script output).
# In-place pip only: .\scripts\reinstall_local_spark.ps1 -KeepVenv
```

Use **PySpark 3.5.x**. If you see `GenTraversableOnce` / Scala errors:

```powershell
pip install pyspark==3.5.4 delta-spark==3.2.0
Remove-Item Env:SPARK_HOME -ErrorAction SilentlyContinue
```

Other tips:

- **Stale session** — restart kernel, or `$env:MEDALLION_FRESH_SPARK = "1"` before bootstrap.
- **Auth errors** — `az login`; Azure CLI must be on PATH (bootstrap prepends the default Windows install path when present).
- **Force Spark ABFS reads** (optional): `$env:MEDALLION_PYSPARK_ABFSS_READ = "1"`.

---

## Fabric (cloud)

Sync repo including `common/`. Run settings (optional), then `%run ./common/bootstrap`. Attached lakehouse provides tables; no local OAuth unless you run locally.

## Maintenance

```powershell
cd C:\spark-dev\CoE_transformation_framework
python scripts\sync_notebooks_to_entry.py
```
