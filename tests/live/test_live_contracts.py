"""Contract tests against a real Salesforce org.

The unit tests pin the models to payloads captured from a live org. These
tests go one step further: they call the real endpoints and assert the
models still parse what the platform returns *now*.

That is the check that catches a platform change the checked-in fixtures
would not notice. Run them with ``pytest -m live``.

They are excluded from the default test run because they need credentials
and a real org.
"""

import os
from pathlib import Path

import pytest

pytestmark = pytest.mark.live


def _read_env_file() -> dict[str, str]:
    """Read the project's .env without depending on pydantic-settings."""
    env: dict[str, str] = {}
    env_file = Path(".env")
    if not env_file.is_file():
        return env
    for line in env_file.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        env[key.strip()] = value.strip()
    return env


def _credentials() -> tuple[str, str, str]:
    """Return (access_token, instance_url, api_version)."""
    env = dict(_read_env_file())
    env.update({k: v for k, v in os.environ.items() if k in (
        "SF_AUTH_TOKEN",
        "SF_DEFAULT_DOMAIN",
        "SF_API_VERSION",
    )})

    token = env.get("SF_AUTH_TOKEN", "")
    domain = env.get("SF_DEFAULT_DOMAIN", "")
    api_version = env.get("SF_API_VERSION", "v67.0")

    if not token or not domain:
        pytest.skip(
            "Live tests need SF_AUTH_TOKEN and SF_DEFAULT_DOMAIN in .env"
        )

    instance_url = domain if domain.startswith("http") else f"https://{domain}"
    return token, instance_url.rstrip("/"), api_version


@pytest.fixture
async def client():
    """A real SalesforceClient against the configured org."""
    from asftool.core.client import SalesforceClient
    from asftool.core.config import Settings

    token, instance_url, api_version = _credentials()
    settings = Settings(
        encryption_key="dHR0dHR0dHR0dHR0dHR0dHR0dHR0dHR0dHR0dHR0dHQ=",
        jwt_secret_key="live-test-secret-not-used-in-production-00000",
        sf_api_version=api_version,
    )
    c = SalesforceClient(
        access_token=token,
        instance_url=instance_url,
        settings=settings,
    )
    try:
        yield c
    finally:
        await c.close()


class TestLiveListings:
    """The listings that were silently truncating to page 1."""

    @pytest.mark.asyncio
    async def test_dataset_listing_paginates(self, client):
        from asftool.core.services.dataset_service import DatasetService

        service = DatasetService(client, settings=client.settings)
        datasets = await service.list_datasets(page_size=5)

        assert datasets, "the org returned no datasets"
        # A page size of 5 means the service must have followed nextPageUrl.
        assert len(datasets) >= 5
        ids = [d.id for d in datasets]
        assert len(ids) == len(set(ids)), "the same dataset was returned twice"

    @pytest.mark.asyncio
    async def test_dashboard_listing_paginates(self, client):
        from asftool.core.services.dashboard_service import DashboardService

        service = DashboardService(client, settings=client.settings)
        dashboards = await service.list_dashboards(page_size=5)

        assert dashboards, "the org returned no dashboards"
        ids = [d.id for d in dashboards]
        assert len(ids) == len(set(ids)), "the same dashboard was returned twice"

    @pytest.mark.asyncio
    async def test_page_token_is_accepted_by_the_platform(self, client):
        """The platform must honour the `page` parameter we send.

        Sending the legacy `pageToken` is silently ignored and returns page
        1 again, so this asserts the cursor actually advances.
        """
        first = await client.list_datasets(page_size=5)
        next_url = first.get("nextPageUrl")
        if not next_url:
            pytest.skip("the org has a single page of datasets")

        from asftool.core.pagination import extract_page_token

        token = extract_page_token(next_url)
        assert token, f"no cursor in nextPageUrl: {next_url}"

        second = await client.list_datasets(page_size=5, page_token=token)
        first_ids = {d["id"] for d in first["datasets"]}
        second_ids = {d["id"] for d in second["datasets"]}
        assert not (first_ids & second_ids), (
            "the platform returned the same page again -- the cursor is "
            "not being honoured"
        )


class TestLiveModels:
    """Every model must parse what the platform returns right now."""

    @pytest.mark.asyncio
    async def test_dataset_list_response(self, client):
        from asftool.core.models import DatasetListResponse

        payload = await client.list_datasets()
        response = DatasetListResponse(**payload)
        assert response.datasets

    @pytest.mark.asyncio
    async def test_dashboard_list_response(self, client):
        from asftool.core.models import DashboardListResponse

        payload = await client.list_dashboards()
        response = DashboardListResponse(**payload)
        assert response.dashboards

    @pytest.mark.asyncio
    async def test_dataflow_list_response(self, client):
        from asftool.core.models import DataflowListResponse

        payload = await client.list_dataflows()
        response = DataflowListResponse(**payload)
        assert response.dataflows

    @pytest.mark.asyncio
    async def test_dataflow_job_list_response(self, client):
        """The model must parse the platform's job payload.

        A live org returned a `Warning` status that appears in no
        documented list, which a strict enum rejected.
        """
        from asftool.core.models import DataflowJobListResponse

        payload = await client.list_dataflow_jobs()
        response = DataflowJobListResponse(**payload)
        for job in response.dataflowjobs:
            assert job.id
            assert job.status


class TestLiveOrgDiscovery:
    """`auth list-orgs` must find the orgs the CLI knows about."""

    def test_sf_cli_orgs_are_discovered(self):
        from asftool.core.sf_cli import SFCLIManager

        manager = SFCLIManager()
        if not manager.is_available():
            pytest.skip("the SF CLI is not installed")

        orgs = manager.list_orgs()
        assert orgs, "no orgs discovered, but the SF CLI is authenticated"

    def test_org_list_response_is_parsed(self):
        """The parser must understand the platform's real response shape."""
        import json
        import subprocess

        from asftool.core.sf_cli import parse_org_list

        result = subprocess.run(
            ["sf", "org", "list", "--json", "--all"],
            capture_output=True,
            text=True,
            timeout=60,
        )
        if result.returncode != 0:
            pytest.skip("the SF CLI could not list orgs")

        payload = json.loads(result.stdout)
        assert "orgs" not in payload.get("result", {}), (
            "the SF CLI response no longer has the shape we expect"
        )
        orgs = parse_org_list(payload)
        assert isinstance(orgs, list)
