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


@pytest.fixture(autouse=True)
def _set_test_env(monkeypatch):
    """Set default test environment variables for settings."""
    import base64
    test_enc_key = base64.urlsafe_b64encode(b"12345678901234567890123456789012").decode()
    monkeypatch.setenv("ENCRYPTION_KEY", test_enc_key)
    monkeypatch.setenv("JWT_SECRET_KEY", "super_secret_jwt_key_that_is_long_enough_12345")


@pytest.fixture
def settings():
    """Return Settings instance configured for tests."""
    import base64
    from tcrm_toolkit.core.config import Settings
    return Settings(
        encryption_key=base64.urlsafe_b64encode(b"12345678901234567890123456789012").decode(),
        jwt_secret_key="super_secret_jwt_key_that_is_long_enough_12345",
    )
