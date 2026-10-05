"""Fabric / OneLake service-principal credentials for local abfss access."""

from __future__ import annotations

import os
from pathlib import Path
from typing import NamedTuple

_REPO = Path(__file__).resolve().parent.parent
_ENV_LOADED = False


class FabricCredentials(NamedTuple):
    tenant_id: str
    client_id: str
    client_secret: str

    def missing(self) -> list[str]:
        out: list[str] = []
        if not self.tenant_id:
            out.append("FABRIC_TENANT_ID (or AZURE_TENANT_ID)")
        if not self.client_id:
            out.append("FABRIC_CLIENT_ID (or AZURE_CLIENT_ID)")
        if not self.client_secret:
            out.append("FABRIC_CLIENT_SECRET (or AZURE_CLIENT_SECRET)")
        return out


def _first_nonempty(*values: str) -> str:
    for v in values:
        if v and str(v).strip():
            return str(v).strip()
    return ""


def _first_env(*names: str) -> str:
    for name in names:
        v = os.environ.get(name, "")
        if v and v.strip():
            return v.strip()
    return ""


def _load_dotenv() -> None:
    global _ENV_LOADED
    if _ENV_LOADED:
        return
    _ENV_LOADED = True
    for path in (_REPO / ".env", _REPO.parent / ".env"):
        if not path.is_file():
            continue
        for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, val = line.partition("=")
            key = key.strip()
            val = val.strip().strip('"').strip("'")
            if key and key not in os.environ:
                os.environ[key] = val


def _from_local_settings() -> FabricCredentials:
    try:
        from common import local_settings as ls  # type: ignore

        return FabricCredentials(
            tenant_id=_first_nonempty(
                getattr(ls, "FABRIC_TENANT_ID", ""),
                getattr(ls, "AZURE_TENANT_ID", ""),
            ),
            client_id=_first_nonempty(
                getattr(ls, "FABRIC_CLIENT_ID", ""),
                getattr(ls, "AZURE_CLIENT_ID", ""),
            ),
            client_secret=_first_nonempty(
                getattr(ls, "FABRIC_CLIENT_SECRET", ""),
                getattr(ls, "AZURE_CLIENT_SECRET", ""),
            ),
        )
    except ImportError:
        return FabricCredentials("", "", "")


def load_fabric_credentials() -> FabricCredentials:
    _load_dotenv()
    from_settings = _from_local_settings()
    return FabricCredentials(
        tenant_id=_first_env("FABRIC_TENANT_ID", "AZURE_TENANT_ID", "TENANT_ID")
        or from_settings.tenant_id,
        client_id=_first_env("FABRIC_CLIENT_ID", "AZURE_CLIENT_ID", "CLIENT_ID")
        or from_settings.client_id,
        client_secret=_first_env(
            "FABRIC_CLIENT_SECRET", "AZURE_CLIENT_SECRET", "CLIENT_SECRET"
        )
        or from_settings.client_secret,
    )


def sync_fabric_credentials_to_env() -> FabricCredentials:
    """Ensure FABRIC_* env vars are set when values come from .env or local_settings."""
    creds = load_fabric_credentials()
    if creds.tenant_id:
        os.environ.setdefault("FABRIC_TENANT_ID", creds.tenant_id)
    if creds.client_id:
        os.environ.setdefault("FABRIC_CLIENT_ID", creds.client_id)
    if creds.client_secret:
        os.environ.setdefault("FABRIC_CLIENT_SECRET", creds.client_secret)
    return creds


def require_fabric_credentials() -> FabricCredentials:
    creds = sync_fabric_credentials_to_env()
    missing = creds.missing()
    if missing:
        raise RuntimeError(
            "OneLake OAuth credentials are missing: "
            + ", ".join(missing)
            + ".\n"
            "Set them before bootstrap (Jupyter does not inherit env from a separate PowerShell):\n"
            "  - common/local_settings.py (copy from local_settings.example.py), or\n"
            "  - a .env file in the repo root, or\n"
            "  - PowerShell in the same session that starts Jupyter:\n"
            "      $env:FABRIC_TENANT_ID = '<tenant-guid>'\n"
            "      $env:FABRIC_CLIENT_ID = '<app-id>'\n"
            "      $env:FABRIC_CLIENT_SECRET = '<secret>'\n"
            "Empty tenant causes login.microsoftonline.com//oauth2/token auth failures."
        )
    return creds
