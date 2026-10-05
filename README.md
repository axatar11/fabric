# Fabric medallion notebooks

HC, Okta, and Cursor usage pipelines (Bronze → Silver → Gold) for **Microsoft Fabric**, with the same notebooks runnable on **local Spark** or **Databricks**.

## One entry point (use this everywhere)

```python
%run ./common/common_bootstrap
```

(Local Jupyter can use `medallion_entry.ipynb` instead — same init.)

That loads the shared Python package under `medallion/`:

| Module | Role |
|--------|------|
| `medallion/runtime.py` | Detect Fabric / Databricks / local; create or reuse `spark`; OneLake OAuth + table registration when local |
| `medallion/config.py` | Lakehouse IDs, table names, business constants (env overrides) |
| `medallion/io.py` | Delta full write + merge |
| `medallion/transforms.py` | Shared transforms |
| `medallion/notebook_init.py` | Injects `spark`, `F`, constants, helpers into the notebook |

**Pipeline notebooks** (each starts with `%run medallion_entry`):

- `NB_HCHistorical_Bronze_To_Silver.ipynb`
- `NB_OktaUserforAI_Bronze_To_Silver.ipynb`
- `NB_CursorUsage_Bronze_To_Silver.ipynb`
- `NB_CursorUsage_Gold.ipynb`

Orchestrators: `NB_Silver_Reporting_Pipeline.ipynb`, `NB_Medallion_Reporting_Pipeline.ipynb` (unchanged `%run` of the above).

Legacy `medallion_config` / `medallion_io` / `medallion_transforms` notebooks now re-export the same Python modules for old `%run` chains.

---

## Local Spark (Windows example)

### 1. Python 3.11 or 3.12 + venv

```powershell
cd C:\spark-dev\fabric\fabric
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements-local-spark.txt
```

### 2. Connect to Fabric (pick one — you do not need all of these)

**A. You already have working Spark connection code (recommended for you)**  
Copy `medallion/local_settings.example.py` → `medallion/local_settings.py` and put your code in `create_spark()`.  
`%run medallion_entry` will call that and skip the built-in OAuth/env-var builder.

**B. Two cells in the notebook**  
Cell 1: your existing code that creates `spark`. Cell 2: `%run medallion_entry` — it **reuses** that session.

**C. Built-in builder only** (if you are not using A or B): set service principal values before starting Jupyter. In PowerShell, `$env:FABRIC_TENANT_ID = "..."` lasts only until you **close that window** (“once per session”); open a new terminal → set them again. Or put them in `local_settings` / your `create_spark()` instead so you do not rely on the shell.

### 3. Run a single pipeline notebook

Open e.g. `NB_CursorUsage_Bronze_To_Silver.ipynb` in VS Code or Jupyter. **Run All** — first cell is `%run medallion_entry` (starts Spark + registers Delta tables locally).

Set one Cursor file or all:

```powershell
$env:CURSOR_BRONZE_FILENAME = "team-usage-events-24970505-2026-09-25.csv"
# remove env var or set to "" to process all bronze files
```

### 4. Optional all-in-one notebook

```powershell
python scripts\build_local_pipeline_notebook.py
```

Then open `NB_Medallion_Local_Pipeline.ipynb` and **Run All**. This only **generates** the file; it does not execute Spark.

---

## Fabric

Upload/sync the repo folder (must include `medallion/` and `medallion_entry.ipynb`). In each pipeline notebook, first cell:

```python
%run medallion_entry
```

Fabric’s attached lakehouse supplies `spark` and table names — no OAuth in the entry cell.

---

## Databricks

Sync repo to a workspace path or wheel. First cell: `%run ./medallion_entry` (or `%run` relative path). Set `MEDALLION_SPARK_PROFILE=databricks` if auto-detection is wrong.

---

## Maintenance

After editing pipeline `.ipynb` files or `medallion/*.py`:

```powershell
python scripts\sync_notebooks_to_entry.py
python scripts\build_local_pipeline_notebook.py
```
