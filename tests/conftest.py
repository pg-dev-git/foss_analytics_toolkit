"""Pytest configuration and fixtures."""

import base64

import pytest

# Deterministic, obviously-fake secrets for the test session. Tests must not
# depend on the developer's ambient shell or on a local .env file.
_TEST_ENCRYPTION_KEY = base64.urlsafe_b64encode(b"\x01" * 32).decode()
_TEST_JWT_SECRET_KEY = "test-jwt-secret-key-not-used-in-production-000"


@pytest.fixture(autouse=True)
def isolate_settings(monkeypatch):
    """Give every test a deterministic, self-contained Settings environment.

    Without this, ``Settings()`` raises ``ValidationError`` when the machine
    has no ``.env`` (and picks up whatever the developer has in their shell
    when it does). The autouse fixture pins the two required secrets and
    clears the ``get_settings`` cache so no state leaks between tests.
    """
    monkeypatch.setenv("ENCRYPTION_KEY", _TEST_ENCRYPTION_KEY)
    monkeypatch.setenv("JWT_SECRET_KEY", _TEST_JWT_SECRET_KEY)

    from asftool.core.config import get_settings

    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


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
    from asftool.core.models import Dataset
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
    from asftool.core.models import Dashboard
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
    from asftool.core.models import Dataflow
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
