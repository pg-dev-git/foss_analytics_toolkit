"""Pytest configuration and fixtures."""

import pytest


@pytest.fixture(scope="session")
def event_loop():
    """Create event loop for async tests."""
    import asyncio
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
def sample_dataset():
    """Sample dataset for testing."""
    from tcrm_toolkit.core.models import Dataset
    return Dataset(
        id="0Fb000000000001",
        name="TestDataset",
        label="Test Dataset",
        currentVersionId="0Fc000000000001",
        createdDate="2024-01-01T00:00:00.000Z",
        createdById="005000000000001",
        lastModifiedDate="2024-01-01T00:00:00.000Z",
        lastModifiedById="005000000000001",
        rowCount=1000,
        status="Active",
        type="Edgemart",
    )


@pytest.fixture
def sample_dashboard():
    """Sample dashboard for testing."""
    from tcrm_toolkit.core.models import Dashboard
    return Dashboard(
        id="0FK000000000001",
        name="TestDashboard",
        label="Test Dashboard",
        folderId="00l000000000001",
        folderName="Test Folder",
        createdDate="2024-01-01T00:00:00.000Z",
        createdById="005000000000001",
        lastModifiedDate="2024-01-01T00:00:00.000Z",
        lastModifiedById="005000000000001",
    )


@pytest.fixture
def sample_dataflow():
    """Sample dataflow for testing."""
    from tcrm_toolkit.core.models import Dataflow
    return Dataflow(
        id="03C000000000001",
        name="TestDataflow",
        label="Test Dataflow",
        status="Active",
        createdDate="2024-01-01T00:00:00.000Z",
        createdById="005000000000001",
        lastModifiedDate="2024-01-01T00:00:00.000Z",
        lastModifiedById="005000000000001",
    )
