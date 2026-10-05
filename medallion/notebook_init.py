from __future__ import annotations

import sys
from typing import Any

from medallion import config, io, runtime
from medallion.display_utils import show_sample


def init_notebook(notebook_globals: dict[str, Any], app_name: str = "Medallion") -> None:
    """
    Single entry for every pipeline notebook.

    Injects spark, config constants, F, I/O helpers, and transforms into the
    notebook namespace. Safe to call once per notebook (reuses existing spark).
    """
    root = runtime.find_repo_root()
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))

    from pyspark.sql import functions as F

    from medallion import transforms

    spark, info = runtime.get_or_create_spark(
        app_name=app_name, notebook_globals=notebook_globals
    )
    runtime.maybe_register_tables(spark, info)

    notebook_globals["spark"] = spark
    notebook_globals["F"] = F
    notebook_globals["MEDALLION_RUNTIME_INFO"] = info

    for name in config.__all__:
        notebook_globals[name] = getattr(config, name)

    notebook_globals["write_full_table"] = io.write_full_table
    notebook_globals["merge_incremental"] = io.merge_incremental
    notebook_globals["trim_lower"] = transforms.trim_lower
    notebook_globals["trim_col"] = transforms.trim_col
    notebook_globals["is_valid_email"] = transforms.is_valid_email
    notebook_globals["date_from_yyyymm"] = transforms.date_from_yyyymm
    notebook_globals["load_country_lookup"] = transforms.load_country_lookup
    notebook_globals["with_normalized_country"] = transforms.with_normalized_country
    notebook_globals["cursor_usage_record_key"] = transforms.cursor_usage_record_key
    notebook_globals["show_sample"] = show_sample

    print(
        f"Medallion init: runtime={info.get('runtime')} "
        f"spark_mode={info.get('mode')} spark={spark.version}"
    )
