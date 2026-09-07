"""Unit tests for FieldImpactService."""

import base64
from unittest.mock import AsyncMock

import pytest

from asftool.core.client import SalesforceClient
from asftool.core.config import Settings
from asftool.core.models import (
    AssetType,
    MatchMode,
    MatchType,
)
from asftool.core.services.field_impact_service import FieldImpactService


def _settings():
    encryption_key = base64.urlsafe_b64encode(b"x" * 32).decode()
    jwt_secret_key = base64.urlsafe_b64encode(b"y" * 32).decode()
    return Settings(
        encryption_key=encryption_key,
        jwt_secret_key=jwt_secret_key,
        sf_api_version="v60.0",
    )


def _client(settings):
    c = SalesforceClient(
        access_token="test", instance_url="https://test.salesforce.com", settings=settings
    )
    c._client = AsyncMock()
    return c


# ----------------------------------------------------------------------------
# Service-level happy path with mocked client responses
# ----------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_analyze_with_exact_match_in_dataset():
    settings = _settings()
    client = _client(settings)

    client.list_datasets = AsyncMock(
        return_value={
            "datasets": [
                {
                    "id": "ds1",
                    "name": "SalesData",
                    "label": "Sales Data",
                    "currentVersionId": "v1",
                }
            ],
            "nextPageUrl": None,
        }
    )
    client.get_dataset_xmd = AsyncMock(
        return_value={
            "measures": [{"field": "Amount", "label": "Amount", "type": "Numeric"}],
            "dimensions": [{"field": "Stage", "label": "Stage", "type": "Text"}],
            "dates": [],
        }
    )
    client.list_dashboards = AsyncMock(return_value={"dashboards": [], "nextPageUrl": None})
    client.list_dataflows = AsyncMock(return_value={"dataflows": []})
    client.list_replicated_datasets = AsyncMock(return_value={"replicatedDatasets": []})

    service = FieldImpactService(client, settings, max_concurrent=5)
    report = await service.analyze_field_impact(
        search_term="Amount", match_mode=MatchMode.EXACT
    )

    assert report.summary.total_matches == 1
    assert report.summary.exact_matches == 1
    assert report.summary.fuzzy_matches == 0
    assert len(report.details.datasets) == 1
    ds = report.details.datasets[0]
    assert ds.match_count == 1
    assert ds.matches[0].field_api_name == "Amount"
    assert ds.matches[0].match_type == MatchType.EXACT
    assert report.errors == []


@pytest.mark.asyncio
async def test_analyze_with_fuzzy_match_in_dataset():
    settings = _settings()
    client = _client(settings)

    client.list_datasets = AsyncMock(
        return_value={
            "datasets": [
                {"id": "ds1", "name": "D", "label": "D", "currentVersionId": "v1"}
            ],
            "nextPageUrl": None,
        }
    )
    client.get_dataset_xmd = AsyncMock(
        return_value={
            "measures": [],
            "dimensions": [{"field": "AccountName", "label": "Account Name", "type": "Text"}],
            "dates": [],
        }
    )
    client.list_dashboards = AsyncMock(return_value={"dashboards": [], "nextPageUrl": None})
    client.list_dataflows = AsyncMock(return_value={"dataflows": []})
    client.list_replicated_datasets = AsyncMock(return_value={"replicatedDatasets": []})

    service = FieldImpactService(client, settings, max_concurrent=5)
    report = await service.analyze_field_impact(
        search_term="AcctNm", match_mode=MatchMode.FUZZY, fuzzy_threshold=70
    )

    assert report.summary.total_matches == 1
    assert report.summary.fuzzy_matches == 1
    assert report.details.datasets[0].matches[0].match_type == MatchType.FUZZY


@pytest.mark.asyncio
async def test_analyze_no_match():
    settings = _settings()
    client = _client(settings)

    client.list_datasets = AsyncMock(
        return_value={
            "datasets": [
                {"id": "ds1", "name": "D", "label": "D", "currentVersionId": "v1"}
            ],
            "nextPageUrl": None,
        }
    )
    client.get_dataset_xmd = AsyncMock(
        return_value={
            "measures": [{"field": "Quantity", "label": "Quantity", "type": "Numeric"}],
            "dimensions": [],
            "dates": [],
        }
    )
    client.list_dashboards = AsyncMock(return_value={"dashboards": [], "nextPageUrl": None})
    client.list_dataflows = AsyncMock(return_value={"dataflows": []})
    client.list_replicated_datasets = AsyncMock(return_value={"replicatedDatasets": []})

    service = FieldImpactService(client, settings, max_concurrent=5)
    report = await service.analyze_field_impact(
        search_term="ZzzNotAMatch", match_mode=MatchMode.BOTH, fuzzy_threshold=90
    )

    assert report.summary.total_matches == 0
    assert report.summary.datasets_scanned == 1
    assert report.details.datasets[0].match_count == 0


@pytest.mark.asyncio
async def test_analyze_dashboard_field_match():
    settings = _settings()
    client = _client(settings)

    client.list_datasets = AsyncMock(return_value={"datasets": [], "nextPageUrl": None})
    client.list_dashboards = AsyncMock(
        return_value={
            "dashboards": [
                {"id": "db1", "name": "SalesDash", "label": "Sales Dashboard"},
            ],
            "nextPageUrl": None,
        }
    )
    client.get_dashboard_full = AsyncMock(
        return_value={
            "id": "db1",
            "name": "SalesDash",
            "label": "Sales Dashboard",
            "state": {
                "widgets": [
                    {
                        "id": "w1",
                        "type": "chart",
                        "steps": [
                            {
                                "id": "s1",
                                "label": "Step 1",
                                "query": {
                                    "measures": [{"field": "Amount"}]
                                }
                            }
                        ]
                    }
                ]
            }
        }
    )
    client.list_dataflows = AsyncMock(return_value={"dataflows": []})
    client.list_replicated_datasets = AsyncMock(return_value={"replicatedDatasets": []})

    service = FieldImpactService(client, settings, max_concurrent=5)
    report = await service.analyze_field_impact(
        search_term="Amount", match_mode=MatchMode.EXACT
    )

    assert report.summary.total_matches == 1
    assert report.summary.dashboards_scanned == 1
    db_match = report.details.dashboards[0].matches[0]
    assert db_match.field_api_name == "Amount"
    assert db_match.widget_id == "w1"
    assert db_match.step_id == "s1"


@pytest.mark.asyncio
async def test_analyze_dataflow_field_match():
    settings = _settings()
    client = _client(settings)

    client.list_datasets = AsyncMock(return_value={"datasets": [], "nextPageUrl": None})
    client.list_dashboards = AsyncMock(return_value={"dashboards": [], "nextPageUrl": None})
    client.list_dataflows = AsyncMock(
        return_value={"dataflows": [{"id": "df1", "name": "ETL", "label": "ETL"}]}
    )
    client.get_dataflow_definition = AsyncMock(
        return_value={
            "id": "df1",
            "name": "ETL",
            "definition": {
                "nodes": [
                    {
                        "id": "n1",
                        "type": "sfdcDigest",
                        "fields": [
                            {"field": "AccountId", "type": "Text"},
                            {"field": "Amount", "type": "Numeric"},
                        ],
                    }
                ]
            }
        }
    )
    client.list_replicated_datasets = AsyncMock(return_value={"replicatedDatasets": []})

    service = FieldImpactService(client, settings, max_concurrent=5)
    report = await service.analyze_field_impact(
        search_term="Amount", match_mode=MatchMode.EXACT
    )

    assert report.summary.dataflows_scanned == 1
    assert report.summary.total_matches == 1
    df_match = report.details.dataflows[0].matches[0]
    assert df_match.field_name == "Amount"
    assert df_match.node_id == "n1"
    assert df_match.node_type == "sfdcDigest"


@pytest.mark.asyncio
async def test_analyze_replicated_dataset_field_match():
    settings = _settings()
    client = _client(settings)

    client.list_datasets = AsyncMock(return_value={"datasets": [], "nextPageUrl": None})
    client.list_dashboards = AsyncMock(return_value={"dashboards": [], "nextPageUrl": None})
    client.list_dataflows = AsyncMock(return_value={"dataflows": []})
    client.list_replicated_datasets = AsyncMock(
        return_value={"replicatedDatasets": [{"id": "rd1", "name": "Account", "label": "Account"}]}
    )
    client.get_replicated_dataset_fields = AsyncMock(
        return_value={
            "fields": [
                {"field": "AccountId", "label": "Account ID", "type": "Text"},
                {"field": "Name", "label": "Account Name", "type": "Text"},
            ]
        }
    )

    service = FieldImpactService(client, settings, max_concurrent=5)
    report = await service.analyze_field_impact(
        search_term="AccountId", match_mode=MatchMode.EXACT
    )

    assert report.summary.replicated_datasets_scanned == 1
    assert report.summary.total_matches == 1
    rd_match = report.details.replicated_datasets[0].matches[0]
    assert rd_match.field_api_name == "AccountId"
    assert rd_match.object_name == "Account"


# ----------------------------------------------------------------------------
# Service-level error handling
# ----------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_dataset_scan_error_doesnt_fail_whole_report():
    settings = _settings()
    client = _client(settings)

    client.list_datasets = AsyncMock(side_effect=Exception("API down"))
    client.list_dashboards = AsyncMock(return_value={"dashboards": [], "nextPageUrl": None})
    client.list_dataflows = AsyncMock(return_value={"dataflows": []})
    client.list_replicated_datasets = AsyncMock(return_value={"replicatedDatasets": []})

    service = FieldImpactService(client, settings, max_concurrent=5)
    report = await service.analyze_field_impact(search_term="Amount")

    assert len(report.errors) > 0
    assert any("Datasets" in e for e in report.errors)


@pytest.mark.asyncio
async def test_dataset_with_no_version_skipped():
    settings = _settings()
    client = _client(settings)

    client.list_datasets = AsyncMock(
        return_value={
            "datasets": [
                {"id": "ds1", "name": "EmptyDS", "label": "Empty", "currentVersionId": None}
            ],
            "nextPageUrl": None,
        }
    )
    client.list_dashboards = AsyncMock(return_value={"dashboards": [], "nextPageUrl": None})
    client.list_dataflows = AsyncMock(return_value={"dataflows": []})
    client.list_replicated_datasets = AsyncMock(return_value={"replicatedDatasets": []})

    service = FieldImpactService(client, settings, max_concurrent=5)
    report = await service.analyze_field_impact(search_term="Amount")

    assert report.summary.datasets_scanned == 1
    assert report.details.datasets[0].match_count == 0


@pytest.mark.asyncio
async def test_include_flags_skip_asset_types():
    settings = _settings()
    client = _client(settings)

    client.list_datasets = AsyncMock(return_value={"datasets": [], "nextPageUrl": None})
    client.list_dashboards = AsyncMock(return_value={"dashboards": [], "nextPageUrl": None})
    client.list_dataflows = AsyncMock(return_value={"dataflows": []})
    client.list_replicated_datasets = AsyncMock(return_value={"replicatedDatasets": []})

    service = FieldImpactService(client, settings, max_concurrent=5)
    # Only scan datasets.
    report = await service.analyze_field_impact(
        search_term="Amount",
        include_datasets=True,
        include_dashboards=False,
        include_dataflows=False,
        include_replicated=False,
    )
    assert report.summary.dashboards_scanned == 0
    assert report.summary.dataflows_scanned == 0
    assert report.summary.replicated_datasets_scanned == 0


# ----------------------------------------------------------------------------
# Pagination
# ----------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_pagination_iterates_through_pages():
    settings = _settings()
    client = _client(settings)

    client.list_datasets = AsyncMock(
        side_effect=[
            {
                "datasets": [{"id": "ds1", "name": "D1", "label": "D1", "currentVersionId": "v1"}],
                "nextPageUrl": "/services/data/v60.0/wave/datasets?pageToken=abc",
            },
            {
                "datasets": [{"id": "ds2", "name": "D2", "label": "D2", "currentVersionId": "v2"}],
                "nextPageUrl": None,
            },
        ]
    )
    client.get_dataset_xmd = AsyncMock(
        return_value={"measures": [], "dimensions": [], "dates": []}
    )
    client.list_dashboards = AsyncMock(return_value={"dashboards": [], "nextPageUrl": None})
    client.list_dataflows = AsyncMock(return_value={"dataflows": []})
    client.list_replicated_datasets = AsyncMock(return_value={"replicatedDatasets": []})

    service = FieldImpactService(client, settings, max_concurrent=5)
    report = await service.analyze_field_impact(search_term="Amount")

    assert report.summary.datasets_scanned == 2
    assert client.list_datasets.call_count == 2


# ----------------------------------------------------------------------------
# Progress callback
# ----------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_progress_callback_invoked():
    settings = _settings()
    client = _client(settings)

    client.list_datasets = AsyncMock(return_value={"datasets": [], "nextPageUrl": None})
    client.list_dashboards = AsyncMock(return_value={"dashboards": [], "nextPageUrl": None})
    client.list_dataflows = AsyncMock(return_value={"dataflows": []})
    client.list_replicated_datasets = AsyncMock(return_value={"replicatedDatasets": []})

    stages: list[str] = []

    async def progress_cb(stage: str, current: int, total: int) -> None:
        stages.append(stage)

    service = FieldImpactService(client, settings, max_concurrent=5)
    await service.analyze_field_impact(
        search_term="Amount", progress_callback=progress_cb
    )

    assert "datasets" in stages
    assert "dashboards" in stages
    assert "dataflows" in stages
    assert "replicated_datasets" in stages


# ----------------------------------------------------------------------------
# Edge cases
# ----------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_extract_page_token():
    s = FieldImpactService.__new__(FieldImpactService)
    assert s._extract_page_token("") is None
    assert s._extract_page_token("/path?other=x") is None
    assert s._extract_page_token("/path?pageToken=abc") == "abc"
    assert s._extract_page_token("/path?pageToken=abc&sort=name") == "abc"


@pytest.mark.asyncio
async def test_analyze_with_application_id_builds_graph():
    """When application_id is provided, the service should build a graph
    and only scan assets present in that graph."""
    settings = _settings()
    client = _client(settings)

    # Only one dataset is in the graph; the list call should be filtered.
    client.get_application_dependencies = AsyncMock(
        return_value={
            "dependencies": [
                {"id": "ds_in", "name": "InDS", "type": "dataset"},
            ]
        }
    )
    client.list_datasets = AsyncMock(
        return_value={
            "datasets": [
                {"id": "ds_in", "name": "InDS", "label": "InDS", "currentVersionId": "v1"},
                {"id": "ds_out", "name": "OutDS", "label": "OutDS", "currentVersionId": "v2"},
            ],
            "nextPageUrl": None,
        }
    )
    client.get_dataset_xmd = AsyncMock(
        return_value={
            "measures": [{"field": "Amount", "label": "Amount", "type": "Numeric"}],
            "dimensions": [],
            "dates": [],
        }
    )
    client.list_dashboards = AsyncMock(return_value={"dashboards": [], "nextPageUrl": None})
    client.list_dataflows = AsyncMock(return_value={"dataflows": []})
    client.list_replicated_datasets = AsyncMock(return_value={"replicatedDatasets": []})

    service = FieldImpactService(client, settings, max_concurrent=5)
    report = await service.analyze_field_impact(
        search_term="Amount", application_id="app1"
    )

    # Only ds_in should be scanned.
    assert report.summary.datasets_scanned == 1
    assert report.summary.total_matches == 1
