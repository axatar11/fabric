# `Common/` — shared medallion library

Loaded by **`%run ./Common/bootstrap`** in pipeline notebooks. Do not import these modules before the settings cell sets `MEDALLION_*` env vars (bootstrap reloads `io` after env is set).

**Windows clone path:** `C:\spark-dev\CoE_transformation_framework` — see root **`README.md`** for venv and kernel setup.

---

## `bootstrap.py`

| Symbol | What it does |
|--------|----------------|
| **`init_notebook(globals)`** | Creates/reuses `spark`, injects `F`, table constants, I/O helpers, transforms (runs automatically on `%run`). |
| **`create_onelake_spark()`** | Build a local Spark session with Delta + OneLake JARs (optional via `local_settings.py`). |
| **`show_sample(df)`** | `display()` in Fabric/Jupyter, else `.show()`. |
| **`reload_io_helpers(globals)`** | Reload `io` / `transforms` after `git pull` without restarting the kernel. |

---

## `config.py`

Lakehouse GUIDs, **`TABLE_*`** FQNs (bronze/silver/gold), gold `fact_cursor_*` constants, merge key column names. Override via env (`LAKEHOUSE_*`, etc.) or optional `local_settings.py`.

---

## `fabric_storage.py`

| Function | What it does |
|----------|----------------|
| **`ensure_azure_cli_on_path()`** | Prepends Windows Azure CLI to `PATH`. |
| **`azure_cli_access_token()`** | Token from `az login` for storage scope (cached; `MEDALLION_AZURE_CLI_TIMEOUT` default 120s). |
| **`refresh_abfs_token_env()`** | Writes token to env + `~/.fabric/onelake_abfs_token` for JVM. |
| **`apply_azure_cli_abfs_conf(spark)`** | Spark Hadoop config for `abfss://` reads/writes with CLI token. |
| **`fabric_storage_options()`** | Dict for **deltalake** Python (`bearer_token`, Fabric endpoint). |
| **`onelake_cli_token_jar()`** | Path URI to custom Hadoop token provider JAR. |

---

## `io.py` — reads, cache, writes

| Function | What it does |
|----------|----------------|
| **`read_table(name)`** | Local: OneLake (deltalake scan) + optional **`~/.fabric/medallion_cache`**. Fabric: `spark.table`. |
| **`read_table_broken_lineage(name, label)`** | `read_table` + materialize for joins; skips extra copy when cache already exists. |
| **`load_delta_path(abfss_path)`** | Read a Delta path with the same backend rules as `read_table`. |
| **`delta_path_for_table(fqn)`** | Map `TABLE_*` FQN → `abfss://…` OneLake path. |
| **`local_cache_path(fqn)`** | Folder under `medallion_cache` for a table. |
| **`save_table_cache` / `load_table_cache`** | Explicitly write/read local parquet cache. |
| **`apply_local_merge_spark_conf(spark)`** | Disable broadcast joins before local transforms/writes (Windows stability). |
| **`break_lineage_local(df, label)`** | Write/read local parquet under `merge_staging/_break/` to cut Spark lineage. |
| **`publish_merge_staging(df, fqn)`** | Write transform result to `merge_staging/` for merge/overwrite. |
| **`write_full_table(df, fqn)`** | Full **overwrite** silver tables (HC/Okta) via staging → deltalake. |
| **`merge_incremental(spark, df, fqn, key)`** | Delta **merge** (cursor_usage silver, gold facts) from staging parquet chunks. |
| **`append_table(df, fqn)`** | Append-only Delta write. |
| **`write_cursor_okta_rejects(spark, df, month_nos)`** | Replace `cursor_usage_okta_reject` rows for processed months (same scope as cursor silver batch). |
| **`read_cursor_frozen_month_nos(spark)`** | `Month_No` list from `Gold.ingest_freeze_control`. |
| **`sync_cursor_close_month_env(month_no)`** | Notebook helper: set `MEDALLION_CURSOR_CLOSE_MONTH_NO` before bootstrap. |
| **`cursor_frozen_month_nos_to_skip(spark, close_month_no)`** | Frozen months to skip on daily; month-close may reload `close_month_no`. |
| **`cursor_excluded_month_nos(spark, gold_table, batch_df, close_month_no=…)`** | Same skip list for gold merge (+ batch max for logs). |
| **`append_cursor_freeze_month(spark, month_no)`** | Append cursor freeze row after month-close onboard gold. |
| **`replace_one_month_in_frame(...)`** | HC month-close: replace one `Month_No` in silver target. |

---

## `transforms.py` — column logic

| Function | What it does |
|----------|----------------|
| **`trim_lower(col)`** | Lowercase trimmed column expression. |
| **`is_valid_email(col)`** | Filter expression for basic email shape. |
| **`date_from_yyyymm(col)`** | `Month_No` int → first day of month date. |
| **`filter_to_month_no(df, month_col, month_no)`** | When `month_no` is set (YYYYMM), keep only that month. |
| **`filter_through_close_month_ts(df, ts_col, month_no)`** | When `month_no` is set, keep rows with `ts_col` before the next calendar month. |
| **`load_country_lookup(spark, TABLE_COUNTRY)`** | Country → display name for normalization. |
| **`with_normalized_country(df, …)`** | Left join lookup; default `No Country`. |
| **`join_okta_for_cursor_usage` / `join_hc_for_cursor_usage`** | Equi-join helpers (optional; cursor_usage silver notebook inlines joins). |
| **`cursor_usage_record_key()`** | SHA2 merge key for **silver** cursor_usage rows. |
| **`purview_copilot_min_creation_date(close_month_no)`** | Daily: `PURVIEW_COPILOT_MIN_CREATION_DATE`; month-close: `YYYY-MM-01` from YYYYMM. |
| **`build_purview_copilot_usage_silver(...)`** | Bronze **purview_copilot_usage** → `(silver, okta_rejects)` (Okta match only in silver; HC left join). |
| **`write_copilot_okta_rejects` / `write_copilot_silver_daily`** | Same month-replace / frozen-month preserve pattern as Cursor for Copilot silver. |
| **`MEDALLION_STAGING_SPARK_PARQUET=0`** | Cursor + Copilot silver notebooks (local): driver-side staging instead of JVM `write.parquet` (avoids heap OOM). Optional **`MEDALLION_STAGING_COLLECT_PARTITIONS`**. |
| **`copilot_frozen_month_nos_to_skip` / `copilot_excluded_month_nos`** | Read `ingest_freeze_control` for `product` copilot or all. |
| **`append_copilot_freeze_month`** | Append copilot freeze row after month-close gold. |
| **`purview_active_copilot_aggregate` / `purview_active_copilot_merge_key`** | Gold **fact_copilot_active** grain (sum prompts, Request >= 1). |
| **`purview_copilot_use_case_code`** | Interaction type → `UC-9007-COP-*` use case code. |
| **`cursor_active_daily_aggregate(silver)`** | Gold active: group by date + Okta user, `COUNT(*)` Request. |
| **`cursor_active_daily_merge_key()`** | SHA2 key per (date, Okta user) for gold active. |
| **`cursor_onboard_daily_dedupe(silver)`** | One row per user/day (latest ingestion). |
| **`cursor_onboard_merge_key()`** | SHA2 key for gold onboard. |

---

## Other paths

| Path | Role |
|------|------|
| **`jars/onelake-cli-token-provider.jar`** | PySpark OneLake ABFS token (required locally). |
| **`local_settings.example.py`** | Template for optional Spark/path overrides → copy to `local_settings.py` (gitignored). |
