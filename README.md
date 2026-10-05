# Fabric medallion notebooks

Databricks-style medallion pipeline notebooks for Microsoft Fabric (HC, Okta, Cursor usage → Silver → Gold).

## Local Spark (offload from Fabric capacity)

Use **`NB_Medallion_Local_Pipeline.ipynb`** — one notebook with named cells (`# Cell: …` + markdown headers) that inlines:

- `medallion_config`, `medallion_io`, `medallion_transforms`
- `NB_HCHistorical_Bronze_To_Silver`, `NB_OktaUserforAI_Bronze_To_Silver`, `NB_CursorUsage_Bronze_To_Silver`, `NB_CursorUsage_Gold`

Original `%run` notebooks are unchanged for Fabric.

### Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements-local-spark.txt
```

Set a service principal with **Contributor** on the Fabric workspace (and OneLake ABFS OAuth):

```bash
export FABRIC_TENANT_ID="<tenant>"
export FABRIC_CLIENT_ID="<app-id>"
export FABRIC_CLIENT_SECRET="<secret>"
# optional overrides:
export FABRIC_WORKSPACE_ID="4cecee03-0629-4081-8ae8-b749ee04d43f"
export LAKEHOUSE_AI_MEDALLION_ID="1ee46463-2495-44f5-9dc9-d635d0067483"
export LAKEHOUSE_AI_MEDALLION_BRONZE_ID="36e5350a-61f6-4120-8c32-a38f55722907"
export CURSOR_BRONZE_FILENAME="team-usage-events-24970505-2026-09-25.csv"  # or empty for all files
```

Open the notebook in Jupyter / VS Code with your local PySpark kernel and run all cells.

The first session cell reuses an existing `spark` session when you run inside Fabric; otherwise it starts local Spark, configures OneLake OAuth, and registers Delta tables at the OneLake paths used by this repo.

If a table path differs in your lakehouse, edit `register_pipeline_tables` in the **local_spark_session** cell (right-click folder → **Copy ABFS path** in Fabric).

Regenerate the consolidated notebook after editing split notebooks:

```bash
python scripts/build_local_pipeline_notebook.py
```
