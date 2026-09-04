"""Integration tests for API endpoints with mocked responses."""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

import httpx

from tcrm_toolkit.core.client import SalesforceClient
from tcrm_toolkit.core.config import Settings
from tcrm_toolkit.core.services.dataset_service import DatasetService
from tcrm_toolkit.core.services.dashboard_service import DashboardService
from tcrm_toolkit.core.services.dataflow_service import DataflowService
from tcrm_toolkit.core.models import Dataset, Dashboard, Dataflow


class MockResponse:
    """Mock HTTP response."""

    def __init__(self, json_data, status_code=200, headers=None):
        self._json_data = json_data
        self.status_code = status_code
        self.headers = headers or {}

    def json(self):
        return self._json_data

    def raise_for_status(self):
        if self.status_code >= 400:
            raise httpx.HTTPStatusError(
                f"HTTP {self.status_code}",
                request=MagicMock(),
                response=self,
            )


@pytest.fixture
def settings():
    """Create test settings."""
    import base64
    encryption_key = base64.urlsafe_b64encode(b"x" * 32).decode()
    jwt_secret_key = base64.urlsafe_b64encode(b"y" * 32).decode()
    return Settings(
        encryption_key=encryption_key,
        jwt_secret_key=jwt_secret_key,
        sf_api_version="v60.0",
        sf_default_domain="test.salesforce.com",
    )


@pytest.fixture
def mock_client(settings):
    """Create a client with mocked HTTP client."""
    client = SalesforceClient(
        access_token="test_token",
        instance_url="https://test.salesforce.com",
        settings=settings,
    )
    client._client = AsyncMock()
    return client


class TestDatasetEndpoints:
    """Integration tests for dataset endpoints."""

    @pytest.mark.asyncio
    async def test_list_datasets(self, mock_client):
        """Test listing datasets."""
        mock_response = MockResponse({
            "datasets": [
                {
                    "id": "0Fb000000000001",
                    "name": "TestDataset",
                    "label": "Test Dataset",
                    "currentVersionId": "0Fc000000000001",
                    "currentVersionUrl": "/services/data/v60.0/wave/datasets/0Fb000000000001/versions/0Fc000000000001",
                    "versionsUrl": "/services/data/v60.0/wave/datasets/0Fb000000000001/versions",
                    "historiesUrl": "/services/data/v60.0/wave/datasets/0Fb000000000001/histories",
                    "createdDate": "2024-01-01T00:00:00.000Z",
                    "createdById": "005000000000001",
                    "lastModifiedDate": "2024-01-01T00:00:00.000Z",
                    "lastModifiedById": "005000000000001",
                    "rowCount": 1000,
                    "status": "Active",
                    "type": "Edgemart",
                }
            ],
            "nextPageUrl": None,
        })
        mock_client._client.request.return_value = mock_response

        datasets = await mock_client.list_datasets()

        assert len(datasets["datasets"]) == 1
        assert datasets["datasets"][0]["name"] == "TestDataset"

    @pytest.mark.asyncio
    async def test_get_dataset(self, mock_client):
        """Test getting a single dataset."""
        mock_response = MockResponse({
            "id": "0Fb000000000001",
            "name": "TestDataset",
            "label": "Test Dataset",
            "currentVersionId": "0Fc000000000001",
            "createdDate": "2024-01-01T00:00:00.000Z",
            "createdById": "005000000000001",
            "lastModifiedDate": "2024-01-01T00:00:00.000Z",
            "lastModifiedById": "005000000000001",
            "rowCount": 1000,
            "status": "Active",
            "type": "Edgemart",
        })
        mock_client._client.request.return_value = mock_response

        dataset = await mock_client.get_dataset("0Fb000000000001")

        assert dataset["id"] == "0Fb000000000001"
        assert dataset["name"] == "TestDataset"

    @pytest.mark.asyncio
    async def test_saql_query(self, mock_client):
        """Test SAQL query execution."""
        mock_response = MockResponse({
            "results": {
                "records": [
                    {"count": 1000},
                ]
            }
        })
        mock_client._client.request.return_value = mock_response

        result = await mock_client.saql_query('q = load "test"; q = group q by all; q = foreach q generate count() as "count";')

        assert result["results"]["records"][0]["count"] == 1000


class TestDashboardEndpoints:
    """Integration tests for dashboard endpoints."""

    @pytest.mark.asyncio
    async def test_list_dashboards(self, mock_client):
        """Test listing dashboards."""
        mock_response = MockResponse({
            "dashboards": [
                {
                    "id": "0FK000000000001",
                    "name": "TestDashboard",
                    "label": "Test Dashboard",
                    "folderId": "00l000000000001",
                    "folderName": "Test Folder",
                    "createdDate": "2024-01-01T00:00:00.000Z",
                    "createdById": "005000000000001",
                    "lastModifiedDate": "2024-01-01T00:00:00.000Z",
                    "lastModifiedById": "005000000000001",
                    "historiesUrl": "/services/data/v60.0/wave/dashboards/0FK000000000001/histories",
                    "datasetsUrl": "/services/data/v60.0/wave/dashboards/0FK000000000001/datasets",
                }
            ],
            "nextPageUrl": None,
        })
        mock_client._client.request.return_value = mock_response

        dashboards = await mock_client.list_dashboards()

        assert len(dashboards["dashboards"]) == 1
        assert dashboards["dashboards"][0]["name"] == "TestDashboard"


class TestDataflowEndpoints:
    """Integration tests for dataflow endpoints."""

    @pytest.mark.asyncio
    async def test_list_dataflows(self, mock_client):
        """Test listing dataflows."""
        mock_response = MockResponse({
            "dataflows": [
                {
                    "id": "03C000000000001",
                    "name": "TestDataflow",
                    "label": "Test Dataflow",
                    "status": "Active",
                    "createdDate": "2024-01-01T00:00:00.000Z",
                    "createdById": "005000000000001",
                    "lastModifiedDate": "2024-01-01T00:00:00.000Z",
                    "lastModifiedById": "005000000000001",
                    "historiesUrl": "/services/data/v60.0/wave/dataflows/03C000000000001/histories",
                }
            ],
        })
        mock_client._client.request.return_value = mock_response

        dataflows = await mock_client.list_dataflows()

        assert len(dataflows["dataflows"]) == 1
        assert dataflows["dataflows"][0]["name"] == "TestDataflow"

    @pytest.mark.asyncio
    async def test_start_dataflow(self, mock_client):
        """Test starting a dataflow."""
        mock_response = MockResponse({
            "id": "03D000000000001",
            "dataflowId": "03C000000000001",
            "command": "start",
            "status": "Queued",
        })
        mock_client._client.request.return_value = mock_response

        job = await mock_client.start_dataflow("03C000000000001")

        assert job["id"] == "03D000000000001"
        assert job["command"] == "start"


class TestDatasetServiceIntegration:
    """Integration tests for DatasetService."""

    @pytest.mark.asyncio
    async def test_extract_fields_from_xmd(self, mock_client, settings):
        """Test field extraction from XMD."""
        service = DatasetService(mock_client, settings=settings)

        from tcrm_toolkit.core.models import DatasetXMD
        xmd = DatasetXMD(
            measures=[
                {"field": "Amount", "label": "Amount"},
                {"field": "Quantity_epoch", "label": "Quantity Epoch"},  # Should be excluded
            ],
            dimensions=[
                {"field": "Account_Name", "label": "Account Name"},
                {"field": "Close_Date_Day", "label": "Close Date Day"},  # Should be excluded
                {"field": "Stage", "label": "Stage"},
            ],
            dates=[],
        )

        fields = service._extract_fields_from_xmd(xmd)

        assert "Amount" in fields
        assert "Account_Name" in fields
        assert "Stage" in fields
        assert "Quantity_epoch" not in fields
        assert "Close_Date_Day" not in fields


class TestDashboardServiceIntegration:
    """Integration tests for DashboardService."""

    @pytest.mark.asyncio
    async def test_backup_dashboard(self, mock_client, settings):
        """Test dashboard backup."""
        service = DashboardService(mock_client, settings=settings)

        from datetime import datetime
        mock_response = MockResponse({
            "id": "0FK000000000001",
            "name": "TestDashboard",
            "label": "Test Dashboard",
            "state": {"widgets": []},
            "created_date": datetime.utcnow().isoformat(),
            "created_by_id": "005000000000001",
            "last_modified_date": datetime.utcnow().isoformat(),
            "last_modified_by_id": "005000000000001",
        })
        mock_client._client.request.return_value = mock_response

        backup = await service.backup_dashboard("0FK000000000001")

        assert backup.dashboard_id == "0FK000000000001"
        assert backup.dashboard_name == "TestDashboard"
        assert "widgets" in backup.json_definition.get("state", {})


class TestDataflowServiceIntegration:
    """Integration tests for DataflowService."""

    @pytest.mark.asyncio
    async def test_wait_for_dataflow_job(self, mock_client, settings):
        """Test waiting for dataflow job completion."""
        service = DataflowService(mock_client, settings=settings)

        # Mock job status progression
        call_count = 0

        async def mock_list_jobs():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                return {
                    "dataflowjobs": [{
                        "id": "03D000000000001",
                        "dataflow_id": "03C000000000001",
                        "dataflow_name": "TestDataflow",
                        "command": "start",
                        "status": "Running",
                    }]
                }
            return {
                "dataflowjobs": [{
                    "id": "03D000000000001",
                    "dataflow_id": "03C000000000001",
                    "dataflow_name": "TestDataflow",
                    "command": "start",
                    "status": "Success",
                }]
            }

        mock_client.list_dataflow_jobs = mock_list_jobs

        job = await service.wait_for_dataflow_job("03D000000000001", poll_interval=0, timeout=10)

        assert job.status == "Success"