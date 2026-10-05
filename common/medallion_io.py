"""Delta write helpers (re-export from medallion.io + config)."""

from __future__ import annotations

import sys
from pathlib import Path

_root = Path(__file__).resolve().parent.parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

from medallion import config as _cfg
from medallion import io as _io

for _name in _cfg.__all__:
    globals()[_name] = getattr(_cfg, _name)

write_full_table = _io.write_full_table
merge_incremental = _io.merge_incremental
