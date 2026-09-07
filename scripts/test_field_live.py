"""One-off live test: run the field crawler against a real Salesforce org.

Reads the .env values for SF_AUTH_TOKEN + SF_DEFAULT_DOMAIN, builds a
SalesforceClient directly, and runs FieldImpactService for the given
search term. Bypasses the SF CLI auth flow (which requires a paired
interactive login) but exercises the full crawler code path against
the real API.

Usage:
    uv run python scripts/test_field_live.py [SEARCH_TERM]

Default search term: "OpportunityID".

SECURITY: The output JSON file (scripts/field_impact_*.json) contains
real org field names and labels. It is gitignored — do not commit it.
Regenerate locally and inspect as needed.
"""

import asyncio
import json
import sys
from pathlib import Path

# Load .env manually so we don't depend on pydantic-settings.
ENV_PATH = Path(__file__).resolve().parent.parent / ".env"
env: dict[str, str] = {}
for line in ENV_PATH.read_text().splitlines():
    line = line.strip()
    if not line or line.startswith("#") or "=" not in line:
        continue
    k, _, v = line.partition("=")
    env[k.strip()] = v.strip()

TOKEN = env.get("SF_AUTH_TOKEN", "")
DOMAIN = env.get("SF_DEFAULT_DOMAIN", "")
API_VERSION = env.get("SF_API_VERSION", "v60.0")
SEARCH_TERM = sys.argv[1] if len(sys.argv) > 1 else "OpportunityID"

print("=" * 60)
print("Live Field Crawler Test")
print("=" * 60)
print(f"  Token:    {TOKEN[:20]}... (len={len(TOKEN)})")
print(f"  Domain:   {DOMAIN}")
print(f"  API ver:  {API_VERSION}")
print(f"  Search:   {SEARCH_TERM}")
print()

if not TOKEN or not DOMAIN:
    print("ERROR: SF_AUTH_TOKEN and SF_DEFAULT_DOMAIN must be set in .env")
    sys.exit(1)

# Derive a settings object that matches what the rest of the app uses.
import base64
from pydantic_settings import BaseSettings, SettingsConfigDict


class TestSettings(BaseSettings):
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


# Construct the client directly with the .env values.
from asftool.core.client import SalesforceClient
from asftool.core.services import FieldImpactService
from asftool.core.models import MatchMode

instance_url = DOMAIN if DOMAIN.startswith("http") else f"https://{DOMAIN}"

settings = TestSettings()
client = SalesforceClient(
    access_token=TOKEN,
    instance_url=instance_url,
    settings=settings,
)


async def main() -> None:
    try:
        # 1) Connectivity sanity check.
        print("--- Connectivity Check ---")
        resp = await client.get("/limits")
        limits = resp.json()
        # Pick a few interesting limits to display.
        interesting = ["DailyApiRequests", "DailyAsyncApexExecutions", "DailyStreamingApiEvents"]
        for k in interesting:
            if k in limits:
                used = limits[k].get("Max", 0) - limits[k].get("Remaining", 0)
                print(f"  {k}: {used}/{limits[k]['Max']}")
        print()

        # 2) Run the field analyzer.
        print(f"--- Field Analysis: '{SEARCH_TERM}' ---")
        service = FieldImpactService(client, settings, max_concurrent=5)
        report = await service.analyze_field_impact(
            search_term=SEARCH_TERM,
            match_mode=MatchMode.BOTH,
            fuzzy_threshold=80,
        )
        summary = report.to_summary_dict()
        print(f"  Assets scanned: {summary['total_assets_scanned']}")
        print(f"  Total matches:  {summary['total_matches']}")
        print(f"  Exact matches:  {summary['exact_matches']}")
        print(f"  Fuzzy matches:  {summary['fuzzy_matches']}")
        print(f"  By asset type:  {json.dumps(summary['by_asset_type'], indent=2)}")
        print(f"  Execution:      {summary['execution_time_ms']}ms")
        print(f"  Errors:         {summary['error_count']}")
        for err in report.errors:
            print(f"    - {err}")
        print()

        # 3) Show per-asset matches.
        def _mt(m: object) -> str:
            """Return match_type as a string (Pydantic v2 with use_enum_values
            deserializes enums to plain strings)."""
            v = getattr(m, "match_type", None)
            return v.value if hasattr(v, "value") else str(v)

        if report.details.datasets:
            print("--- Datasets ---")
            for ds in report.details.datasets:
                if ds.match_count > 0:
                    print(f"  [{ds.dataset_name}] ({ds.match_count} matches)")
                    for m in ds.matches[:5]:  # limit per asset
                        print(f"    - {m.field_api_name!r}  ({_mt(m)}, score={m.match_score})")
        if report.details.dashboards:
            print("--- Dashboards ---")
            for db in report.details.dashboards:
                if db.match_count > 0:
                    print(f"  [{db.dashboard_name}] ({db.match_count} matches)")
                    for m in db.matches[:5]:
                        print(f"    - {m.widget_id}.{m.step_id}: {m.field_api_name!r}  ({_mt(m)})")
        if report.details.dataflows:
            print("--- Dataflows ---")
            for df in report.details.dataflows:
                if df.match_count > 0:
                    print(f"  [{df.dataflow_name}] ({df.match_count} matches)")
                    for m in df.matches[:5]:
                        print(f"    - {m.node_id} ({m.node_type}): {m.field_name!r}  ({_mt(m)})")
        if report.details.replicated_datasets:
            print("--- Replicated Datasets ---")
            for rd in report.details.replicated_datasets:
                if rd.match_count > 0:
                    print(f"  [{rd.object_name}] ({rd.match_count} matches)")
                    for m in rd.matches[:5]:
                        print(f"    - {m.field_api_name!r}  ({_mt(m)}, score={m.match_score})")

        # 4) Optionally save the full report.
        out_path = Path(__file__).parent / f"field_impact_{SEARCH_TERM}.json"
        out_path.write_text(report.model_dump_json(indent=2))
        print()
        print(f"Full report saved to: {out_path}")

    finally:
        await client.close()


asyncio.run(main())
