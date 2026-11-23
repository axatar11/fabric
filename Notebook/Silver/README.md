# Silver

Run in order:

1. `NB_HCHistorical_Bronze_To_Silver.ipynb`
2. `NB_OktaUserforAI_Bronze_To_Silver.ipynb`
3. `NB_CursorUsage_Bronze_To_Silver.ipynb` (requires HC + Okta silver)
4. `NB_Purview_Copilot_BronzeToSilver.ipynb` (requires HC + Okta silver)

**Reference tables:** `NB_ReferenceTables_BronzeToSilver.ipynb` — one code cell per table (full overwrite). Registry: `iter_reference_silver_pipelines()` in `Common/transforms.py`.

**Core reporting:** `NB_CoreReporting_BronzeToSilver.ipynb` — logic in `Common/transform_core.py` (via `transforms`). Run reference normalization silvers before country/okta; run core silvers before `dim_project`.

Then run notebooks under `Notebook/Gold/`.
