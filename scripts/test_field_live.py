"""One-off live test: run the field crawler against a real Salesforce org.

Reads .env values for SF_AUTH_TOKEN + SF_DEFAULT_DOMAIN, builds a
SalesforceClient directly, and runs the production analyze_field_async
command (same code the menu and CLI use).

Bypasses the SF CLI auth flow (which requires a paired interactive
login) but exercises the full crawler code path — including the new
progress output and StorageManager-driven report path — against the
real API.

Usage:
    uv run python scripts/test_field_live.py [SEARCH_TERM]

Default search term: "OpportunityID".

SECURITY: The output JSON (asftool_downloads/.../field_impact_*.json)
contains real org field names and labels. It is gitignored — do not commit.
"""

import asyncio
import base64
import sys
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict

import asftool.cli.commands.fields as _fields_cmd
import asftool.cli.session as _session_mod
from asftool.cli.commands.fields import analyze_field_async
from asftool.core.client import SalesforceClient

# Load .env manually so we don't depend on pydantic-settings.
ENV_PATH = Path(__file__).resolve().parent.parent / ".env"
env: dict[str, str] = {}
for line in ENV_PATH.read_text(encoding="utf-8").splitlines():
    line = line.strip()
    if not line or line.startswith("#") or "=" not in line:
        continue
    k, _, v = line.partition("=")
    env[k.strip()] = v.strip()

TOKEN = env.get("SF_AUTH_TOKEN", "")
DOMAIN = env.get("SF_DEFAULT_DOMAIN", "")
API_VERSION = env.get("SF_API_VERSION", "v60.0")
SEARCH_TERM = sys.argv[1] if len(sys.argv) > 1 else "OpportunityID"

if not TOKEN or not DOMAIN:
    print("ERROR: SF_AUTH_TOKEN and SF_DEFAULT_DOMAIN must be set in .env")
    sys.exit(1)

instance_url = DOMAIN if DOMAIN.startswith("http") else f"https://{DOMAIN}`"


class TestSettings(BaseSettings):
    """Minimal settings matching what the rest of the app expects."""
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")
    app_name: str = "asftool"
    app_version: str = "0.1.0"
    encryption_key: str = base64.urlsafe_b64encode(b"x" * 32).decode()
    jwt_secret_key: str = base64.urlsafe_b64encode(b"y" * 32).decode()
    sf_api_version: str = API_VERSION
    sf_default_domain: str = DOMAIN
    field_impact_default_fuzzy_threshold: int = 85
    field_impact_max_concurrent_scans: int = 10
    field_impact_default_match_mode: str = "both"


settings = TestSettings()
client = SalesforceClient(
    access_token=TOKEN,
    instance_url=instance_url,
    settings=settings,
)


class _FakeSession:
    """Session shim that returns our pre-built client without calling SF CLI."""
    def __init__(self):
        self.alias = "default"
        self.settings = settings
        self._client = client

    def client_context(self):
        outer = self

        class _Ctx:
            async def __aenter__(self_inner):
                return outer._client

            async def __aexit__(self_inner, *args):
                return False

        return _Ctx()

    async def close(self):
        # Real client is closed in main().
        pass


# Patch Session in both modules where analyze_field_async imports it.
_session_mod.Session = _FakeSession  # type: ignore[assignment]
_fields_cmd.Session = _FakeSession  # type: ignore[assignment]


async def main() -> None:
    try:
        report_path = await analyze_field_async(
            search_term=SEARCH_TERM,
            mode="both",
            threshold=80,
            fmt="summary",
        )
        print()
        print(f"Report path: {report_path}")
    finally:
        await client.close()


asyncio.run(main())
