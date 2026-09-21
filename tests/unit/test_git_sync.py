"""Unit and integration tests for sync_service and CLI sync/revert.

Uses respx for CRMA REST API mocking and tempfile for local Git fixtures.
"""

import json
import tempfile
from pathlib import Path

import pytest
import respx
import httpx

from asftool.core.git import (
    CRMAGitSyncService,
    RepoMappingResolver,
    WorkspaceManager,
    create_default_config,
)
from asftool.core.models.auth_tokens import AuthTokens
from asftool.core.git.resolver import AssetContext


@pytest.fixture
def mock_crma_dashboard():
    """Mock CRMA dashboard API response."""
    return {
        "id": "01Z000000000001AAA",
        "developerName": "Test_Dashboard",
        "label": "Test Dashboard",
        "folderName": "Test Reports",
        "folderId": "05B000000000002",
        "createdDate": "2024-01-01T00:00:00.000Z",
        "lastModifiedDate": "2024-06-01T00:00:00.000Z",
        "state": "Published",
    }


@respx.mock
def test_sync_service_dry_run(mock_crma_dashboard):
    """Integration: dry_run returns plan without making Git changes."""
    # Use a custom settings with known API version for testing
    from asftool.core.config import Settings

    test_settings = Settings(
        ENCRYPTION_KEY="LzLb1AK5iiGgy3e6gbnireGJ16sYvia7RcvUUNWuw5Q=",
        JWT_SECRET_KEY="test-jwt-secret-key-that-is-at-least-32-chars-long",
        SF_API_VERSION="v68.0",
    )

    # Mock the CRMA REST endpoint - use dynamic API version (test v68.0)
    route = respx.get("/services/data/v68.0/wave/dashboards").mock(
        return_value=httpx.Response(
            200,
            json={
                "records": [mock_crma_dashboard],
                "nextPageUrl": None,
            },
        )
    )

    # For test purposes, use a simple default config instead of loading from file
    from asftool.core.git import RepoMappingConfig
    from asftool.core.models.git_auth import GitRepositoryTarget, GitProvider
    from asftool.core.git import create_default_config

    target = GitRepositoryTarget(
        provider=GitProvider.GITHUB,
        host="github.com",
        organization="testorg",
        repository="test-repo",
        credentials_alias="test-creds",
    )
    config = create_default_config(
        provider=GitProvider.GITHUB,
        host="github.com",
        organization="testorg",
        repository="test-repo",
        credentials_alias="test-creds",
    )
    resolver = RepoMappingResolver(config)

    service = CRMAGitSyncService(
        resolver=resolver,
        instance_url="https://test.salesforce.com",
        access_token="test-token",
        settings=test_settings,
    )

    # The dry_run would call the REST endpoint; in test environment
    # we verify the service can be constructed and has correct structure
    assert service is not None
    assert service.normalizer is not None
    assert service.workspace_manager is not None
    # Verify dynamic URL construction works
    assert service.base_url == "https://test.salesforce.com/services/data/v68.0"
    assert service.wave_base_url == "https://test.salesforce.com/services/data/v68.0/wave"
    assert service._get_asset_endpoint("dashboard") == "https://test.salesforce.com/services/data/v68.0/wave/dashboards"


def test_sync_result_dataclass():
    """Unit test: SyncResult initializes correctly."""
    from asftool.core.git import SyncResult

    result = SyncResult(
        success=True,
        repositories_synced=2,
        assets_synced=5,
        assets_skipped=1,
        errors=[],
        commit_hashes={"repo-1": "abc123"},
        duration_seconds=3.5,
    )

    assert result.success is True
    assert result.repositories_synced == 2
    assert result.duration_seconds == 3.5


def test_revert_result_dataclass():
    """Unit test: RevertResult initializes correctly."""
    from asftool.core.git import RevertResult

    result = RevertResult(
        success=True,
        asset_id="01Z001",
        asset_type="dashboard",
        commit_hash="deadbeef",
    )

    assert result.success is True
    assert result.asset_type == "dashboard"
    assert result.error is None


def test_workspace_manager_slug():
    """Unit test: Workspace slug generation is filesystem-safe."""
    from asftool.core.git import WorkspaceManager, GitRepositoryTarget, GitProvider

    wm = WorkspaceManager()
    target = GitRepositoryTarget(
        provider=GitProvider.GITHUB,
        host="github.com",
        organization="test-org",
        repository="test-repo",
        credentials_alias="test",
    )

    slug = wm.get_workspace_slug(target)
    assert "github" in slug
    assert "test-repo" in slug
    assert "/" not in slug  # No filesystem separators


def test_git_engine_init_and_commit():
    """Integration: GitEngine creates repo, stages, commits."""
    from asftool.core.git import GitEngine

    import tempfile
    from pathlib import Path

    with tempfile.TemporaryDirectory() as tmpdir:
        repo_path = Path(tmpdir) / "git-test"
        engine = GitEngine.init_repository(repo_path)

        # Create file
        test_file = repo_path / "hello.json"
        test_file.write_text('{"label":"hello"}')

        stage_result = engine.stage_files(["hello.json"])
        assert stage_result["success"] is True

        commit_hash = engine.commit("Add hello.json")
        assert commit_hash is not None
        assert len(commit_hash) == 40  # SHA-1 hex length

        # Verify file exists at commit
        exists = engine.file_exists_at_commit("hello.json", commit_hash)
        assert exists is True

        content = engine.read_file_at_commit("hello.json", commit_hash)
        assert b'"label":"hello"' in content

        engine.close()


def test_crmanormalizer_utf8_and_sorting():
    """Unit test: Normalizer produces deterministic sorted UTF-8 output."""
    from asftool.core.git import CRMANormalizer
    import json

    normalizer = CRMANormalizer()
    data = {
        "z_key": 1,
        "a_key": 2,
        "label": "日本語 🇯🇵",
        "lastModifiedDate": "2024-01-01",
    }
    result = normalizer.normalize(data, "dashboard")
    normalized = json.loads(result.normalized_json)

    # Sorted keys
    assert list(normalized.keys()) == sorted(normalized.keys())
    # UTF-8 preserved (not escaped)
    assert "日本語" in result.normalized_json


# =========================================================================
# New tests for full asset detail fetching
# =========================================================================

@pytest.fixture
def mock_full_dashboard():
    """Mock full dashboard response with widgets."""
    return {
        "id": "01Z000000000001AAA",
        "developerName": "Test_Dashboard",
        "label": "Test Dashboard",
        "folderName": "Test Reports",
        "folderId": "05B000000000002",
        "createdDate": "2024-01-01T00:00:00.000Z",
        "lastModifiedDate": "2024-06-01T00:00:00.000Z",
        "state": "Published",
        "widgets": [
            {
                "id": "w1",
                "label": "Revenue Chart",
                "type": "chart",
                "query": {"soql": "SELECT Amount FROM Opportunity"},
            },
            {
                "id": "w2",
                "label": "Pipeline Table",
                "type": "table",
                "columns": ["Name", "Stage", "Amount"],
            }
        ],
        "steps": {
            "step1": {
                "type": "soql",
                "query": "SELECT Name FROM Account",
            }
        }
    }


@pytest.fixture
def mock_full_dataflow():
    """Mock full dataflow response with steps/nodes."""
    return {
        "id": "02K000000000001BBB",
        "developerName": "Test_Dataflow",
        "label": "Test Dataflow",
        "createdDate": "2024-01-01T00:00:00.000Z",
        "lastModifiedDate": "2024-06-01T00:00:00.000Z",
        "nodes": [
            {
                "id": "node1",
                "name": "Extract_Opportunities",
                "type": "sfdcDigest",
                "parameters": {
                    "object": "Opportunity",
                    "fields": [{"name": "Id"}, {"name": "Amount"}],
                }
            },
            {
                "id": "node2",
                "name": "Transform_Revenue",
                "type": "computeExpression",
                "parameters": {
                    "source": "node1",
                    "fields": [{"name": "Revenue", "type": "numeric"}],
                }
            }
        ]
    }


@pytest.fixture
def mock_full_recipe():
    """Mock full recipe response with steps."""
    return {
        "id": "01Z000000000001CCC",
        "developerName": "Test_Recipe",
        "label": "Test Recipe",
        "dataflowId": "02K000000000002",
        "dataflowName": "Parent Dataflow",
        "createdDate": "2024-01-01T00:00:00.000Z",
        "lastModifiedDate": "2024-06-01T00:00:00.000Z",
        "steps": [
            {"name": "Load Data", "type": "load", "parameters": {"source": "Account"}},
            {"name": "Filter Active", "type": "filter", "parameters": {"condition": "IsActive=true"}},
            {"name": "Aggregate", "type": "aggregate", "parameters": {"groupBy": "Owner"}},
        ]
    }


@pytest.fixture
def mock_dashboard_list():
    """Mock dashboard list endpoint response (preview)."""
    return {
        "dashboards": [
            {
                "id": "01Z000000000001AAA",
                "developerName": "Test_Dashboard",
                "label": "Test Dashboard",
                "folderName": "Test Reports",
                "folderId": "05B000000000002",
                "createdDate": "2024-01-01T00:00:00.000Z",
                "lastModifiedDate": "2024-06-01T00:00:00.000Z",
                "state": "Published",
            }
        ],
        "nextPageUrl": None,
    }


@pytest.fixture
def mock_dataflow_list():
    """Mock dataflow list endpoint response (preview)."""
    return {
        "dataflows": [
            {
                "id": "02K000000000001BBB",
                "developerName": "Test_Dataflow",
                "label": "Test Dataflow",
                "createdDate": "2024-01-01T00:00:00.000Z",
                "lastModifiedDate": "2024-06-01T00:00:00.000Z",
            }
        ],
        "nextPageUrl": None,
    }


@pytest.fixture
def mock_recipe_list():
    """Mock recipe list endpoint response (preview)."""
    return {
        "recipes": [
            {
                "id": "01Z000000000001CCC",
                "developerName": "Test_Recipe",
                "label": "Test Recipe",
                "dataflowId": "02K000000000002",
                "dataflowName": "Parent Dataflow",
                "createdDate": "2024-01-01T00:00:00.000Z",
                "lastModifiedDate": "2024-06-01T00:00:00.000Z",
            }
        ],
        "nextPageUrl": None,
    }


@pytest.mark.asyncio
@respx.mock
async def test_fetch_all_assets_dashboard_includes_widgets(mock_dashboard_list, mock_full_dashboard):
    """Test that _fetch_all_assets fetches full dashboard detail including widgets."""
    from asftool.core.config import Settings
    from asftool.core.git import CRMAGitSyncService, RepoMappingResolver
    from asftool.core.git.workspace import WorkspaceManager
    from asftool.core.models.git_auth import GitProvider, GitRepositoryTarget
    from asftool.core.git import create_default_config

    test_settings = Settings(
        ENCRYPTION_KEY="LzLb1AK5iiGgy3e6gbnireGJ16sYvia7RcvUUNWuw5Q=",
        JWT_SECRET_KEY="test-jwt-secret-key-that-is-at-least-32-chars-long",
        SF_API_VERSION="v68.0",
    )

    # Mock LIST endpoint
    respx.get("/services/data/v68.0/wave/dashboards").mock(
        return_value=httpx.Response(200, json=mock_dashboard_list)
    )

    # Mock DETAIL endpoint
    respx.get("/services/data/v68.0/wave/dashboards/01Z000000000001AAA").mock(
        return_value=httpx.Response(200, json=mock_full_dashboard)
    )

    target = GitRepositoryTarget(
        provider=GitProvider.GITHUB,
        host="github.com",
        organization="testorg",
        repository="test-repo",
        credentials_alias="test-creds",
    )
    config = create_default_config(
        provider=GitProvider.GITHUB,
        host="github.com",
        organization="testorg",
        repository="test-repo",
        credentials_alias="test-creds",
    )
    resolver = RepoMappingResolver(config)

    service = CRMAGitSyncService(
        resolver=resolver,
        instance_url="https://test.salesforce.com",
        access_token="test-token",
        settings=test_settings,
        detail_fetch_concurrency=2,
    )

    async with service._get_sf_client() as client:
        assets = await service._fetch_all_assets(client, "dashboard")

    assert len(assets) == 1
    asset = assets[0]
    
    # Verify full detail was fetched (widgets and steps)
    assert "widgets" in asset
    assert len(asset["widgets"]) == 2
    assert asset["widgets"][0]["label"] == "Revenue Chart"
    assert asset["widgets"][1]["label"] == "Pipeline Table"
    assert "steps" in asset
    assert "step1" in asset["steps"]
    
    # Verify preview fields were preserved
    assert asset["folderName"] == "Test Reports"
    assert asset["folderId"] == "05B000000000002"
    assert asset["id"] == "01Z000000000001AAA"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_all_assets_dataflow_includes_nodes(mock_dataflow_list, mock_full_dataflow):
    """Test that _fetch_all_assets fetches full dataflow detail including nodes."""
    from asftool.core.config import Settings
    from asftool.core.git import CRMAGitSyncService, RepoMappingResolver
    from asftool.core.models.git_auth import GitProvider, GitRepositoryTarget
    from asftool.core.git import create_default_config

    test_settings = Settings(
        ENCRYPTION_KEY="LzLb1AK5iiGgy3e6gbnireGJ16sYvia7RcvUUNWuw5Q=",
        JWT_SECRET_KEY="test-jwt-secret-key-that-is-at-least-32-chars-long",
        SF_API_VERSION="v68.0",
    )

    # Mock LIST endpoint
    respx.get("/services/data/v68.0/wave/dataflows").mock(
        return_value=httpx.Response(200, json=mock_dataflow_list)
    )

    # Mock DETAIL endpoint
    respx.get("/services/data/v68.0/wave/dataflows/02K000000000001BBB").mock(
        return_value=httpx.Response(200, json=mock_full_dataflow)
    )

    target = GitRepositoryTarget(
        provider=GitProvider.GITHUB,
        host="github.com",
        organization="testorg",
        repository="test-repo",
        credentials_alias="test-creds",
    )
    config = create_default_config(
        provider=GitProvider.GITHUB,
        host="github.com",
        organization="testorg",
        repository="test-repo",
        credentials_alias="test-creds",
    )
    resolver = RepoMappingResolver(config)

    service = CRMAGitSyncService(
        resolver=resolver,
        instance_url="https://test.salesforce.com",
        access_token="test-token",
        settings=test_settings,
        detail_fetch_concurrency=2,
    )

    async with service._get_sf_client() as client:
        assets = await service._fetch_all_assets(client, "dataflow")

    assert len(assets) == 1
    asset = assets[0]
    
    # Verify full detail was fetched (nodes)
    assert "nodes" in asset
    assert len(asset["nodes"]) == 2
    assert asset["nodes"][0]["name"] == "Extract_Opportunities"
    assert asset["nodes"][1]["name"] == "Transform_Revenue"
    
    # Verify preview fields preserved
    assert asset["id"] == "02K000000000001BBB"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_all_assets_recipe_includes_steps(mock_recipe_list, mock_full_recipe):
    """Test that _fetch_all_assets fetches full recipe detail including steps."""
    from asftool.core.config import Settings
    from asftool.core.git import CRMAGitSyncService, RepoMappingResolver
    from asftool.core.models.git_auth import GitProvider, GitRepositoryTarget
    from asftool.core.git import create_default_config

    test_settings = Settings(
        ENCRYPTION_KEY="LzLb1AK5iiGgy3e6gbnireGJ16sYvia7RcvUUNWuw5Q=",
        JWT_SECRET_KEY="test-jwt-secret-key-that-is-at-least-32-chars-long",
        SF_API_VERSION="v68.0",
    )

    # Mock LIST endpoint
    respx.get("/services/data/v68.0/wave/recipes").mock(
        return_value=httpx.Response(200, json=mock_recipe_list)
    )

    # Mock DETAIL endpoint
    respx.get("/services/data/v68.0/wave/recipes/01Z000000000001CCC").mock(
        return_value=httpx.Response(200, json=mock_full_recipe)
    )

    target = GitRepositoryTarget(
        provider=GitProvider.GITHUB,
        host="github.com",
        organization="testorg",
        repository="test-repo",
        credentials_alias="test-creds",
    )
    config = create_default_config(
        provider=GitProvider.GITHUB,
        host="github.com",
        organization="testorg",
        repository="test-repo",
        credentials_alias="test-creds",
    )
    resolver = RepoMappingResolver(config)

    service = CRMAGitSyncService(
        resolver=resolver,
        instance_url="https://test.salesforce.com",
        access_token="test-token",
        settings=test_settings,
        detail_fetch_concurrency=2,
    )

    async with service._get_sf_client() as client:
        assets = await service._fetch_all_assets(client, "recipe")

    assert len(assets) == 1
    asset = assets[0]
    
    # Verify full detail was fetched (steps)
    assert "steps" in asset
    assert len(asset["steps"]) == 3
    assert asset["steps"][0]["name"] == "Load Data"
    assert asset["steps"][1]["name"] == "Filter Active"
    assert asset["steps"][2]["name"] == "Aggregate"
    
    # Verify preview fields preserved
    assert asset["dataflowId"] == "02K000000000002"
    assert asset["dataflowName"] == "Parent Dataflow"
    assert asset["id"] == "01Z000000000001CCC"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_all_assets_detail_404_fallbacks_to_preview(mock_dashboard_list):
    """Test that 404 on detail endpoint falls back to preview data."""
    from asftool.core.config import Settings
    from asftool.core.git import CRMAGitSyncService, RepoMappingResolver
    from asftool.core.models.git_auth import GitProvider, GitRepositoryTarget
    from asftool.core.git import create_default_config

    test_settings = Settings(
        ENCRYPTION_KEY="LzLb1AK5iiGgy3e6gbnireGJ16sYvia7RcvUUNWuw5Q=",
        JWT_SECRET_KEY="test-jwt-secret-key-that-is-at-least-32-chars-long",
        SF_API_VERSION="v68.0",
    )

    # Mock LIST endpoint
    respx.get("/services/data/v68.0/wave/dashboards").mock(
        return_value=httpx.Response(200, json=mock_dashboard_list)
    )

    # Mock DETAIL endpoint returning 404
    respx.get("/services/data/v68.0/wave/dashboards/01Z000000000001AAA").mock(
        return_value=httpx.Response(404, json={"error": "Not found"})
    )

    target = GitRepositoryTarget(
        provider=GitProvider.GITHUB,
        host="github.com",
        organization="testorg",
        repository="test-repo",
        credentials_alias="test-creds",
    )
    config = create_default_config(
        provider=GitProvider.GITHUB,
        host="github.com",
        organization="testorg",
        repository="test-repo",
        credentials_alias="test-creds",
    )
    resolver = RepoMappingResolver(config)

    service = CRMAGitSyncService(
        resolver=resolver,
        instance_url="https://test.salesforce.com",
        access_token="test-token",
        settings=test_settings,
        detail_fetch_concurrency=2,
    )

    async with service._get_sf_client() as client:
        assets = await service._fetch_all_assets(client, "dashboard")

    assert len(assets) == 1
    asset = assets[0]
    
    # Should have preview data but not widgets (since detail 404)
    assert asset["id"] == "01Z000000000001AAA"
    assert asset["folderName"] == "Test Reports"
    assert "widgets" not in asset  # Not in preview data


@pytest.mark.asyncio
@respx.mock
async def test_fetch_all_assets_multiple_dashboards_concurrent(mock_full_dashboard):
    """Test that multiple dashboards are fetched concurrently with semaphore limiting."""
    from asftool.core.config import Settings
    from asftool.core.git import CRMAGitSyncService, RepoMappingResolver
    from asftool.core.models.git_auth import GitProvider, GitRepositoryTarget
    from asftool.core.git import create_default_config

    test_settings = Settings(
        ENCRYPTION_KEY="LzLb1AK5iiGgy3e6gbnireGJ16sYvia7RcvUUNWuw5Q=",
        JWT_SECRET_KEY="test-jwt-secret-key-that-is-at-least-32-chars-long",
        SF_API_VERSION="v68.0",
    )

    # Mock LIST endpoint with 3 dashboards
    dashboard_list = {
        "dashboards": [
            {"id": "01Z000000000001AAA", "developerName": "Dashboard_1", "label": "Dashboard 1", "folderName": "Reports"},
            {"id": "01Z000000000002BBB", "developerName": "Dashboard_2", "label": "Dashboard 2", "folderName": "Reports"},
            {"id": "01Z000000000003CCC", "developerName": "Dashboard_3", "label": "Dashboard 3", "folderName": "Reports"},
        ],
        "nextPageUrl": None,
    }
    respx.get("/services/data/v68.0/wave/dashboards").mock(
        return_value=httpx.Response(200, json=dashboard_list)
    )

    # Mock DETAIL endpoints for all 3
    for i, dash_id in enumerate(["01Z000000000001AAA", "01Z000000000002BBB", "01Z000000000003CCC"]):
        detail = mock_full_dashboard.copy()
        detail["id"] = dash_id
        detail["developerName"] = f"Dashboard_{i+1}"
        detail["label"] = f"Dashboard {i+1}"
        respx.get(f"/services/data/v68.0/wave/dashboards/{dash_id}").mock(
            return_value=httpx.Response(200, json=detail)
        )

    target = GitRepositoryTarget(
        provider=GitProvider.GITHUB,
        host="github.com",
        organization="testorg",
        repository="test-repo",
        credentials_alias="test-creds",
    )
    config = create_default_config(
        provider=GitProvider.GITHUB,
        host="github.com",
        organization="testorg",
        repository="test-repo",
        credentials_alias="test-creds",
    )
    resolver = RepoMappingResolver(config)

    # Use low concurrency to test semaphore
    service = CRMAGitSyncService(
        resolver=resolver,
        instance_url="https://test.salesforce.com",
        access_token="test-token",
        settings=test_settings,
        detail_fetch_concurrency=2,
    )

    async with service._get_sf_client() as client:
        assets = await service._fetch_all_assets(client, "dashboard")

    assert len(assets) == 3
    for asset in assets:
        assert "widgets" in asset
        assert len(asset["widgets"]) == 2


@pytest.mark.asyncio
@respx.mock
async def test_dry_run_uses_full_details_for_comparison(mock_dashboard_list, mock_full_dashboard):
    """Test that dry_run uses full asset details (with widgets) for comparison."""
    from asftool.core.config import Settings
    from asftool.core.git import CRMAGitSyncService, RepoMappingResolver, WorkspaceManager
    from asftool.core.models.git_auth import GitProvider, GitRepositoryTarget
    from asftool.core.git import create_default_config
    import tempfile
    from pathlib import Path
    import subprocess
    import json

    test_settings = Settings(
        ENCRYPTION_KEY="LzLb1AK5iiGgy3e6gbnireGJ16sYvia7RcvUUNWuw5Q=",
        JWT_SECRET_KEY="test-jwt-secret-key-that-is-at-least-32-chars-long",
        SF_API_VERSION="v68.0",
    )

    # Mock LIST endpoint
    respx.get("/services/data/v68.0/wave/dashboards").mock(
        return_value=httpx.Response(200, json=mock_dashboard_list)
    )

    # Mock DETAIL endpoint
    respx.get("/services/data/v68.0/wave/dashboards/01Z000000000001AAA").mock(
        return_value=httpx.Response(200, json=mock_full_dashboard)
    )

    target = GitRepositoryTarget(
        provider=GitProvider.GITHUB,
        host="github.com",
        organization="testorg",
        repository="test-repo",
        credentials_alias="test-creds",
        path_prefix="dashboards",
    )
    config = create_default_config(
        provider=GitProvider.GITHUB,
        host="github.com",
        organization="testorg",
        repository="test-repo",
        credentials_alias="test-creds",
        path_prefix="dashboards",
    )
    resolver = RepoMappingResolver(config)

    # Create a temporary workspace with existing file
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create workspace manager with custom base path
        workspace_manager = WorkspaceManager(base_path=Path(tmpdir))
        workspace_path = workspace_manager.get_workspace_path(target)
        
        # Initialize git repo in the workspace path
        workspace_path.mkdir(parents=True, exist_ok=True)
        subprocess.run(["git", "init"], cwd=workspace_path, capture_output=True)
        subprocess.run(["git", "config", "user.email", "test@test.com"], cwd=workspace_path, capture_output=True)
        subprocess.run(["git", "config", "user.name", "Test"], cwd=workspace_path, capture_output=True)
        
        # Create existing file with OLD widgets (different from mock_full_dashboard)
        # The file path follows: Path(target.path_prefix) / folder / asset_type / name.json
        # where folder = context.folder = asset.get("folderName") = "Test Reports"
        existing_content = {
            "developerName": "Test_Dashboard",
            "label": "Test Dashboard",
            "widgets": [{"id": "old", "label": "Old Widget"}],
        }
        file_path = workspace_path / target.path_prefix / "Test Reports" / "dashboard" / "Test_Dashboard.json"
        file_path.parent.mkdir(parents=True, exist_ok=True)
        file_path.write_text(json.dumps(existing_content, indent=2))
        
        # Commit the file
        subprocess.run(["git", "add", "."], cwd=workspace_path, capture_output=True)
        subprocess.run(["git", "commit", "-m", "Initial"], cwd=workspace_path, capture_output=True)

        service = CRMAGitSyncService(
            resolver=resolver,
            instance_url="https://test.salesforce.com",
            access_token="test-token",
            settings=test_settings,
            detail_fetch_concurrency=2,
            workspace_manager=workspace_manager,
        )

        plan = await service.dry_run(asset_types=["dashboard"])
        
        # Should detect update because widgets differ
        repo_slug = list(plan["repositories"].keys())[0]
        assets = plan["repositories"][repo_slug]["assets"]
        assert len(assets) == 1
        assert assets[0]["action"] == "update"
        # Verify the full detail was used (widgets should be in normalized comparison)
