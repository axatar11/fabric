"""Azure CLI tokens and Spark/deltalake config for OneLake (Fabric) storage."""

from __future__ import annotations

import os
import shutil
from pathlib import Path
from typing import Any

from Common import config

_AZURE_CLI_WIN = r"C:\Program Files\Microsoft SDKs\Azure\CLI2\wbin"
_TOKEN_PROVIDER = "org.fabric.onelake.EnvAccessTokenProvider"
_ONELAKE_CLI_JAR = Path(__file__).resolve().parent / "jars" / "onelake-cli-token-provider.jar"
_HADOOP_CONF_TOKEN = "org.fabric.onelake.access.token"
_HADOOP_CONF_TOKEN_FILE = "org.fabric.onelake.token.file"
_HADOOP_CONF_TOKEN_EXPIRY = "org.fabric.onelake.token.expiry"


def onelake_cli_token_jar() -> str:
    """File URI of the Hadoop ABFS token provider JAR bundled under ``Common/jars/``."""
    if not _ONELAKE_CLI_JAR.is_file():
        raise FileNotFoundError(
            f"Missing {_ONELAKE_CLI_JAR}. Pull latest repo (PySpark OneLake token JAR)."
        )
    return _ONELAKE_CLI_JAR.resolve().as_uri()


def ensure_azure_cli_on_path() -> None:
    """Prepend default Windows Azure CLI install dir to ``PATH`` when present."""
    if os.path.isdir(_AZURE_CLI_WIN):
        path = os.environ.get("PATH", "")
        if _AZURE_CLI_WIN not in path.split(os.pathsep):
            os.environ["PATH"] = _AZURE_CLI_WIN + os.pathsep + path


def azure_cli_access_token() -> tuple[str, int]:
    """Return ``(access_token, expires_on_unix)`` from ``AzureCliCredential``."""
    from azure.identity import AzureCliCredential

    ensure_azure_cli_on_path()
    if shutil.which("az") is None:
        raise RuntimeError(
            "Azure CLI not found on PATH. Install Azure CLI and run: az login"
        )
    credential = AzureCliCredential()
    token = credential.get_token("https://storage.azure.com/.default")
    return token.token, int(token.expires_on)


def abfs_token_cache_path() -> Path:
    """Path where the current ABFS bearer token is written for the JVM."""
    override = os.environ.get("ONELAKE_TOKEN_CACHE")
    if override:
        return Path(override)
    return Path.home() / ".fabric" / "onelake_abfs_token"


def refresh_abfs_token_env() -> tuple[str, int, Path]:
    """Azure CLI token for JVM (file + env); PySpark JVM does not see os.environ after start."""
    access_token, expires_on = azure_cli_access_token()
    os.environ["ONELAKE_ABFS_ACCESS_TOKEN"] = access_token
    os.environ["ONELAKE_ABFS_TOKEN_EXPIRY"] = str(expires_on)
    cache = abfs_token_cache_path()
    cache.parent.mkdir(parents=True, exist_ok=True)
    cache.write_text(access_token, encoding="utf-8")
    os.environ["ONELAKE_ABFS_TOKEN_FILE"] = str(cache)
    return access_token, expires_on, cache


def apply_azure_cli_abfs_conf(spark) -> None:
    """Configure abfss for spark.read.format('delta') with az login (no tenant/SP env vars)."""
    access_token, expires_on, cache = refresh_abfs_token_env()
    host = config.ONELAKE_HOST
    p = "fs.azure.account"
    spark.conf.set(f"{p}.auth.type.{host}", "Custom")
    spark.conf.set(f"{p}.oauth.provider.type.{host}", _TOKEN_PROVIDER)
    spark.conf.set(f"spark.hadoop.{_HADOOP_CONF_TOKEN}", access_token)
    spark.conf.set(f"spark.hadoop.{_HADOOP_CONF_TOKEN_FILE}", str(cache))
    spark.conf.set(f"spark.hadoop.{_HADOOP_CONF_TOKEN_EXPIRY}", str(expires_on))
    # Fail faster when ABFS auth/network is wrong (avoid multi-minute silent hangs).
    for key, val in (
        ("fs.azure.io.retry.max.retries", "5"),
        ("fs.azure.io.retry.min.backoff.interval", "3s"),
        ("fs.azure.io.retry.max.backoff.interval", "15s"),
    ):
        spark.conf.set(f"spark.hadoop.{key}", val)


def fabric_storage_options() -> dict[str, Any]:
    """Fresh Azure CLI token for deltalake / OneLake (call again before each table read)."""
    access_token, expires_on = azure_cli_access_token()
    return {
        "bearer_token": access_token,
        "use_fabric_endpoint": "true",
        "token_expires_on": str(expires_on),
    }
