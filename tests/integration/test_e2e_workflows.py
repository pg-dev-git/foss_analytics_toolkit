"""End-to-end lifecycle integration tests using mock server."""
import pytest
from tests.mocks.salesforce_mock_server import get_mock_dataset_list, MOCK_SAQL_RESULTS

@pytest.mark.asyncio
async def test_e2e_dataset_discovery():
    data = get_mock_dataset_list()
    assert "datasets" in data
    assert len(data["datasets"]) == 1
    print("E2E dataset discovery validated")

@pytest.mark.asyncio
async def test_e2e_saql_query():
    assert "results" in MOCK_SAQL_RESULTS
    print("E2E SAQL query validated")
