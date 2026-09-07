"""Unit tests for AssetGraphBuilder."""

import base64
from unittest.mock import AsyncMock

import pytest

from asftool.core.client import SalesforceClient
from asftool.core.config import Settings
from asftool.core.models import AssetType
from asftool.core.services.asset_graph import AssetGraphBuilder


@pytest.fixture
def settings():
    encryption_key = base64.urlsafe_b64encode(b"x" * 32).decode()
    jwt_secret_key = base64.urlsafe_b64encode(b"y" * 32).decode()
    return Settings(
        encryption_key=encryption_key,
        jwt_secret_key=jwt_secret_key,
        sf_api_version="v60.0",
    )


@pytest.fixture
def mock_client(settings):
    client = SalesforceClient(
        access_token="test",
        instance_url="https://test.salesforce.com",
        settings=settings,
    )
    client._client = AsyncMock()
    return client


class TestBuildFromApplication:
    """Tests for build_from_application."""

    @pytest.mark.asyncio
    async def test_empty_dependencies(self, mock_client):
        mock_client.get_application_dependencies = AsyncMock(
            return_value={"dependencies": []}
        )
        builder = AssetGraphBuilder(mock_client)
        graph = await builder.build_from_application("app1")
        assert "app1" in graph.nodes
        assert graph.edges == {}

    @pytest.mark.asyncio
    async def test_single_level_dependencies(self, mock_client):
        mock_client.get_application_dependencies = AsyncMock(
            return_value={
                "dependencies": [
                    {"id": "ds1", "name": "SalesData", "type": "dataset"},
                    {"id": "db1", "name": "SalesDash", "type": "dashboard"},
                ]
            }
        )
        builder = AssetGraphBuilder(mock_client)
        graph = await builder.build_from_application("app1")
        assert "ds1" in graph.nodes
        assert "db1" in graph.nodes
        assert graph.edges["app1"] == ["ds1", "db1"]
        assert graph.nodes["ds1"].type == AssetType.DATASET
        assert graph.nodes["db1"].type == AssetType.DASHBOARD

    @pytest.mark.asyncio
    async def test_recursive_dependencies(self, mock_client):
        """Application -> dataset1 -> dataflow1, dashboard1"""
        responses = [
            {  # app1 deps
                "dependencies": [
                    {"id": "ds1", "name": "D1", "type": "dataset"},
                ]
            },
            {  # ds1 deps
                "dependencies": [
                    {"id": "df1", "name": "F1", "type": "dataflow"},
                    {"id": "db1", "name": "B1", "type": "dashboard"},
                ]
            },
            {"dependencies": []},  # df1 deps
            {"dependencies": []},  # db1 deps
        ]
        mock_client.get_application_dependencies = AsyncMock(side_effect=responses)
        builder = AssetGraphBuilder(mock_client)
        graph = await builder.build_from_application("app1")
        assert "df1" in graph.nodes
        assert "db1" in graph.nodes
        assert graph.edges["ds1"] == ["df1", "db1"]

    @pytest.mark.asyncio
    async def test_unknown_type_skipped(self, mock_client):
        mock_client.get_application_dependencies = AsyncMock(
            return_value={
                "dependencies": [
                    {"id": "ds1", "name": "D1", "type": "dataset"},
                    {"id": "weird1", "name": "W1", "type": "alien_artifact"},
                ]
            }
        )
        builder = AssetGraphBuilder(mock_client)
        graph = await builder.build_from_application("app1")
        assert "ds1" in graph.nodes
        assert "weird1" not in graph.nodes

    @pytest.mark.asyncio
    async def test_max_depth_limits_recursion(self, mock_client):
        """After MAX_DEPTH levels, recursion should stop."""
        # Make a chain 10 levels deep; only the first MAX_DEPTH levels fetched.
        responses = []
        for i in range(AssetGraphBuilder.MAX_DEPTH + 2):
            responses.append({
                "dependencies": [{"id": f"n{i}", "name": f"N{i}", "type": "dashboard"}]
            })
        # Tail with empty to prevent actual recursion past depth.
        responses.extend([{"dependencies": []}] * 5)

        mock_client.get_application_dependencies = AsyncMock(side_effect=responses)
        builder = AssetGraphBuilder(mock_client)
        graph = await builder.build_from_application("app1")

        # We should have stopped before exploring all 10 levels.
        # So we should have at most MAX_DEPTH+1 children explored.
        assert len(mock_client.get_application_dependencies.call_args_list) <= AssetGraphBuilder.MAX_DEPTH + 1

    @pytest.mark.asyncio
    async def test_error_in_dependency_fetch_continues(self, mock_client):
        call_count = 0

        async def maybe_fail(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return {"dependencies": [{"id": "ds1", "name": "D1", "type": "dataset"}]}
            raise Exception("Network error")

        mock_client.get_application_dependencies = AsyncMock(side_effect=maybe_fail)
        builder = AssetGraphBuilder(mock_client)
        graph = await builder.build_from_application("app1")
        # ds1 should be in graph but no further recursion succeeded.
        assert "ds1" in graph.nodes


class TestBuildFromDataset:
    """Tests for build_from_dataset."""

    @pytest.mark.asyncio
    async def test_basic(self, mock_client):
        mock_client.get_dataset_dependencies = AsyncMock(
            return_value={
                "dependencies": [
                    {"id": "db1", "name": "SalesDash", "type": "dashboard"},
                ]
            }
        )
        builder = AssetGraphBuilder(mock_client)
        graph = await builder.build_from_dataset("ds1")
        assert "ds1" in graph.nodes
        assert "db1" in graph.nodes
        assert graph.edges["ds1"] == ["db1"]
        assert graph.nodes["ds1"].type == AssetType.DATASET

    @pytest.mark.asyncio
    async def test_no_dependencies(self, mock_client):
        mock_client.get_dataset_dependencies = AsyncMock(
            return_value={"dependencies": []}
        )
        builder = AssetGraphBuilder(mock_client)
        graph = await builder.build_from_dataset("ds1")
        assert "ds1" in graph.nodes
        assert graph.edges == {}

    @pytest.mark.asyncio
    async def test_fetch_error(self, mock_client):
        mock_client.get_dataset_dependencies = AsyncMock(
            side_effect=Exception("API error")
        )
        builder = AssetGraphBuilder(mock_client)
        graph = await builder.build_from_dataset("ds1")
        # Root node still added even if dependencies fetch fails.
        assert "ds1" in graph.nodes


class TestTypeMapping:
    """Tests for TCRM type string mapping."""

    @pytest.mark.asyncio
    async def test_lens_mapped_to_dashboard(self, mock_client):
        mock_client.get_application_dependencies = AsyncMock(
            return_value={
                "dependencies": [
                    {"id": "l1", "name": "Lens1", "type": "lens"},
                ]
            }
        )
        builder = AssetGraphBuilder(mock_client)
        graph = await builder.build_from_application("app1")
        assert graph.nodes["l1"].type == AssetType.DASHBOARD

    @pytest.mark.asyncio
    async def test_replicated_dataset_type(self, mock_client):
        mock_client.get_application_dependencies = AsyncMock(
            return_value={
                "dependencies": [
                    {"id": "r1", "name": "Repl1", "type": "replicatedDataset"},
                ]
            }
        )
        builder = AssetGraphBuilder(mock_client)
        graph = await builder.build_from_application("app1")
        assert graph.nodes["r1"].type == AssetType.REPLICATED_DATASET
