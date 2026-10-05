"""OneLake access via Azure CLI — same pattern as NB_Cursor_Bronze.ipynb."""

from __future__ import annotations

import os
import shutil
from typing import Any

_AZURE_CLI_WIN = r"C:\Program Files\Microsoft SDKs\Azure\CLI2\wbin"


def ensure_azure_cli_on_path() -> None:
    if os.path.isdir(_AZURE_CLI_WIN):
        path = os.environ.get("PATH", "")
        if _AZURE_CLI_WIN not in path.split(os.pathsep):
            os.environ["PATH"] = _AZURE_CLI_WIN + os.pathsep + path


def fabric_storage_options() -> dict[str, Any]:
    """Token for deltalake / OneLake (run `az login` first)."""
    from azure.identity import AzureCliCredential

    ensure_azure_cli_on_path()
    if shutil.which("az") is None:
        raise RuntimeError(
            "Azure CLI not found on PATH. Install Azure CLI and run: az login"
        )
    credential = AzureCliCredential()
    token = credential.get_token("https://storage.azure.com/.default")
    return {
        "bearer_token": token.token,
        "use_fabric_endpoint": "true",
    }
