"""OneLake access via Azure CLI — same pattern as NB_Cursor_Bronze.ipynb."""

from __future__ import annotations

import os
import shutil
from pathlib import Path
from typing import Any

from common import config

_AZURE_CLI_WIN = r"C:\Program Files\Microsoft SDKs\Azure\CLI2\wbin"
_TOKEN_PROVIDER = "org.fabric.onelake.EnvAccessTokenProvider"
_ONELAKE_CLI_JAR = Path(__file__).resolve().parent / "jars" / "onelake-cli-token-provider.jar"


def onelake_cli_token_jar() -> str:
    if not _ONELAKE_CLI_JAR.is_file():
        raise FileNotFoundError(
            f"Missing {_ONELAKE_CLI_JAR}. Pull latest repo (PySpark OneLake token JAR)."
        )
    return _ONELAKE_CLI_JAR.resolve().as_uri()


def ensure_azure_cli_on_path() -> None:
    if os.path.isdir(_AZURE_CLI_WIN):
        path = os.environ.get("PATH", "")
        if _AZURE_CLI_WIN not in path.split(os.pathsep):
            os.environ["PATH"] = _AZURE_CLI_WIN + os.pathsep + path


def azure_cli_access_token() -> tuple[str, int]:
    from azure.identity import AzureCliCredential

    ensure_azure_cli_on_path()
    if shutil.which("az") is None:
        raise RuntimeError(
            "Azure CLI not found on PATH. Install Azure CLI and run: az login"
        )
    credential = AzureCliCredential()
    token = credential.get_token("https://storage.azure.com/.default")
    return token.token, int(token.expires_on)


def refresh_abfs_token_env() -> None:
    """Expose Azure CLI token to Hadoop CustomTokenProvider (EnvAccessTokenProvider)."""
    access_token, expires_on = azure_cli_access_token()
    os.environ["ONELAKE_ABFS_ACCESS_TOKEN"] = access_token
    os.environ["ONELAKE_ABFS_TOKEN_EXPIRY"] = str(expires_on)


def apply_azure_cli_abfs_conf(spark) -> None:
    """Configure abfss for spark.read.format('delta') with az login (no tenant/SP env vars)."""
    refresh_abfs_token_env()
    host = config.ONELAKE_HOST
    p = "fs.azure.account"
    spark.conf.set(f"{p}.auth.type.{host}", "Custom")
    spark.conf.set(f"{p}.oauth.provider.type.{host}", _TOKEN_PROVIDER)


def fabric_storage_options() -> dict[str, Any]:
    """Token for deltalake (optional / NB_Cursor_Bronze only)."""
    access_token, _ = azure_cli_access_token()
    return {
        "bearer_token": access_token,
        "use_fabric_endpoint": "true",
    }
