"""Single entry: Spark session + medallion config, I/O, and transforms.

Use from Fabric/Databricks notebooks:

    %run ./common/common_bootstrap

Same as `%run medallion_entry` (notebook) or `medallion.notebook_init.init_notebook(globals())`.
"""

from __future__ import annotations

import sys
from pathlib import Path


def _repo_root() -> Path:
    here = Path(__file__).resolve().parent.parent
    if (here / "medallion" / "notebook_init.py").is_file():
        return here
    for candidate in [Path.cwd(), *Path.cwd().parents]:
        if (candidate / "medallion" / "notebook_init.py").is_file():
            return candidate
    return here


_root = _repo_root()
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

from medallion.notebook_init import init_notebook

init_notebook(globals())
