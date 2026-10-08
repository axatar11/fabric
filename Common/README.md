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
| **`azure_cli_access_token()`** | Token from `az login` for storage scope. |
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

---

## `transforms.py` — column logic

| Function | What it does |
|----------|----------------|
| **`trim_lower(col)`** | Lowercase trimmed column expression. |
| **`is_valid_email(col)`** | Filter expression for basic email shape. |
| **`date_from_yyyymm(col)`** | `Month_No` int → first day of month date. |
| **`load_country_lookup(spark, TABLE_COUNTRY)`** | Country → display name for normalization. |
| **`with_normalized_country(df, …)`** | Left join lookup; default `No Country`. |
| **`join_okta_for_cursor_usage` / `join_hc_for_cursor_usage`** | Equi-join helpers (optional; cursor_usage silver notebook inlines joins). |
| **`cursor_usage_record_key()`** | SHA2 merge key for **silver** cursor_usage rows. |
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
