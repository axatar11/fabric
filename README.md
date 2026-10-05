# Fabric medallion notebooks

Databricks-style medallion pipeline notebooks for Microsoft Fabric (HC, Okta, Cursor usage → Silver → Gold).

---

## Two different commands (don’t mix them up)

| What you want | Command | Result |
|---------------|---------|--------|
| **Rebuild** the single notebook from the split Fabric notebooks | `python scripts/build_local_pipeline_notebook.py` | Writes/updates `NB_Medallion_Local_Pipeline.ipynb` only. **Does not run Spark or touch OneLake.** |
| **Run the pipeline** (Bronze → Silver → Gold) | Open `NB_Medallion_Local_Pipeline.ipynb` in Jupyter or VS Code and **Run All** | Executes PySpark against Fabric OneLake (after credentials are set). |

If you already see `Wrote ... NB_Medallion_Local_Pipeline.ipynb (19 cells)`, step 1 is done. **Go to step 2 below.**

Example (your layout):

```text
C:\spark-dev\fabric\                    ← you run commands here
C:\spark-dev\fabric\fabric\             ← git repo root (notebooks live here)
  NB_Medallion_Local_Pipeline.ipynb     ← open this file to run the pipeline
  scripts\build_local_pipeline_notebook.py
  requirements-local-spark.txt
```

From `C:\spark-dev\fabric`:

```powershell
python fabric\scripts\build_local_pipeline_notebook.py
```

---

## Step 2 — Run the pipeline on local Spark (Windows)

### Prerequisites

1. **Java 11 or 17** — `java -version` works; set `JAVA_HOME` if Spark cannot find Java.
2. **Apache Spark** installed — `SPARK_HOME` set (e.g. `C:\spark`), and `%SPARK_HOME%\bin` on `PATH`.
3. **Python 3.11 or 3.12** for the notebook kernel (PySpark/Delta often **do not** support 3.14 yet). Use a venv with 3.12 even if your default `python` is 3.14.

### One-time Python environment

In PowerShell, from the **repo root** (`fabric\fabric` in your tree):

```powershell
cd C:\spark-dev\fabric\fabric
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements-local-spark.txt
pip install jupyter ipykernel
python -m ipykernel install --user --name fabric-local-spark --display-name "Fabric local Spark"
```

### Fabric credentials (service principal)

The app registration needs **Contributor** on the Fabric workspace. In the same PowerShell session (before starting Jupyter):

```powershell
$env:FABRIC_TENANT_ID = "<your-tenant-guid>"
$env:FABRIC_CLIENT_ID = "<app-client-id>"
$env:FABRIC_CLIENT_SECRET = "<client-secret>"

# Optional — defaults match this repo’s notebook metadata:
$env:FABRIC_WORKSPACE_ID = "4cecee03-0629-4081-8ae8-b749ee04d43f"
$env:LAKEHOUSE_AI_MEDALLION_ID = "1ee46463-2495-44f5-9dc9-d635d0067483"
$env:LAKEHOUSE_AI_MEDALLION_BRONZE_ID = "36e5350a-61f6-4120-8c32-a38f55722907"

# One Cursor CSV at a time; omit or set to "" to load all bronze files:
$env:CURSOR_BRONZE_FILENAME = "team-usage-events-24970505-2026-09-25.csv"
```

Alternatively, edit placeholders in the **`local_spark_session`** cell in the notebook (`<TENANT_ID>`, etc.) instead of env vars.

### Run the notebook

**Option A — Jupyter**

```powershell
cd C:\spark-dev\fabric\fabric
.\.venv\Scripts\Activate.ps1
# env vars still set in this window
jupyter notebook NB_Medallion_Local_Pipeline.ipynb
```

Kernel: **Fabric local Spark** (or the venv’s Python). **Run → Run All Cells**.

**Option B — VS Code**

1. Open folder `C:\spark-dev\fabric\fabric`.
2. Select interpreter: `.venv\Scripts\python.exe`.
3. Open `NB_Medallion_Local_Pipeline.ipynb`.
4. **Run All**.

### What you should see

1. **`medallion_config`** — no output (defines table names).
2. **`local_spark_session`** — prints something like `Spark 3.x.x; Fabric-attached session=False`.
3. Later cells — Spark jobs; ends with sample rows from HC / Okta / Gold (`show_sample`).

If step 2 fails with auth or path errors, check OneLake paths in **`register_pipeline_tables`** inside **`local_spark_session`** (in Fabric: right-click table folder → **Copy ABFS path**).

---

## What’s in the consolidated notebook

`NB_Medallion_Local_Pipeline.ipynb` inlines:

- `medallion_config`, `medallion_io`, `medallion_transforms`
- `NB_HCHistorical_Bronze_To_Silver`, `NB_OktaUserforAI_Bronze_To_Silver`, `NB_CursorUsage_Bronze_To_Silver`, `NB_CursorUsage_Gold`

Original `%run` notebooks stay as-is for Fabric capacity.

## Regenerate after editing split notebooks

From repo root:

```powershell
python scripts\build_local_pipeline_notebook.py
```
