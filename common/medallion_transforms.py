"""Shared transforms (re-export from medallion.transforms + config)."""

from __future__ import annotations

import sys
from pathlib import Path

_root = Path(__file__).resolve().parent.parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

from pyspark.sql import functions as F

from medallion import config as _cfg
from medallion import transforms as _tx

for _name in _cfg.__all__:
    globals()[_name] = getattr(_cfg, _name)

trim_lower = _tx.trim_lower
trim_col = _tx.trim_col
is_valid_email = _tx.is_valid_email
date_from_yyyymm = _tx.date_from_yyyymm
load_country_lookup = _tx.load_country_lookup
with_normalized_country = _tx.with_normalized_country
cursor_usage_record_key = _tx.cursor_usage_record_key
