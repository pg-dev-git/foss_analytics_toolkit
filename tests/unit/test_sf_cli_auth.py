"""Unit tests for asftool.core.auth.sf_cli_auth (SF CLI authentication service)."""

import pytest
from unittest.mock import AsyncMock, Mock, patch
from datetime import UTC, datetime, timedelta

from asftool.core.auth.sf_cli_auth import SFCLIAuthService
from asftool.core.auth.token_store import StoredToken
from asftool.core.sf_cli import SFCLIAuthResult, SFCLIManager, SFCLIError
from asftool.core.config import Settings
from asftool.core.crypto import CryptoManager


@pytest.fixture
def mock_settings():
    return Settings()


@pytest.fixture
def mock_crypto():
    return CryptoManager("test_key_base64_encoded_32_chars==")


@pytest.fixture
def mock_sf_cli():
    return Mock(spec=SFCLIManager)


@pytest.fixture
def auth_service(mock_settings, mock_crypto, mock_sf_cli):
    return SFCLIAuthService(mock_settings, mock_crypto, mock_sf_cli)


class TestCheckSfCliAuth:
    """Tests for check_sf_cli_auth method."""

    @pytest.mark.asyncio
    async def test_check_sf_cli_auth_returns_false_when_cli_not_available(
        self, auth_service, mock_sf_cli
    ):
        """When SF CLI is not available, check should return authenticated=False."""
        mock_sf_cli.is_available.return_value = False

        result = await auth_service.check_sf_cli_auth("default")

        assert result["authenticated"] is False
        assert result["alias"] == "default"
        assert result["username"] is None
        assert result["instance_url"] is None
        assert "SF CLI not available" in result["message"]

    @pytest.mark.asyncio
    async def test_check_sf_cli_auth_returns_false_when_org_not_authenticated(
        self, auth_service, mock_sf_cli
    ):
        """When org is not in SF CLI's authenticated list, return False."""
        mock_sf_cli.is_available.return_value = True
        mock_sf_cli.is_org_authenticated_async = AsyncMock(return_value=False)

        result = await auth_service.check_sf_cli_auth("default")

        assert result["authenticated"] is False
        assert "No authenticated session found" in result["message"]

    @pytest.mark.asyncio
    async def test_check_sf_cli_auth_returns_true_with_details_when_authenticated(
        self, auth_service, mock_sf_cli
    ):
        """When org is authenticated, return True with username and instance_url."""
        mock_sf_cli.is_available.return_value = True
        mock_sf_cli.is_org_authenticated_async = AsyncMock(return_value=True)

        # Mock get_org_info
        org_info = SFCLIAuthResult(
            access_token="",
            instance_url="https://example.my.salesforce.com",
            username="user@example.com",
            alias="default",
        )
        mock_sf_cli.get_org_info = AsyncMock(return_value=org_info)

        # Mock get_access_token
        mock_sf_cli.get_access_token = AsyncMock(return_value="access-token-123")

        result = await auth_service.check_sf_cli_auth("default")

        assert result["authenticated"] is True
        assert result["alias"] == "default"
        assert result["username"] == "user@example.com"
        assert result["instance_url"] == "https://example.my.salesforce.com"
        assert result["has_valid_token"] is True
        assert "SF CLI has authenticated session" in result["message"]

    @pytest.mark.asyncio
    async def test_check_sf_cli_auth_handles_exception_gracefully(
        self, auth_service, mock_sf_cli
    ):
        """Exception during check should return authenticated=False with error message."""
        mock_sf_cli.is_available.return_value = True
        mock_sf_cli.is_org_authenticated_async = AsyncMock(
            side_effect=Exception("CLI error")
        )

        result = await auth_service.check_sf_cli_auth("default")

        assert result["authenticated"] is False
        assert "Failed to check SF CLI auth" in result["message"]


class TestImportSfCliSession:
    """Tests for import_sf_cli_session method."""

    @pytest.mark.asyncio
    async def test_import_sf_cli_session_raises_when_cli_not_available(
        self, auth_service, mock_sf_cli
    ):
        """Should raise SFCLIAuthError when SF CLI not available."""
        mock_sf_cli.is_available.return_value = False

        with pytest.raises(Exception, match="SF CLI not available"):
            await auth_service.import_sf_cli_session("default")

    @pytest.mark.asyncio
    async def test_import_sf_cli_session_raises_when_org_not_authenticated(
        self, auth_service, mock_sf_cli
    ):
        """Should raise SFCLIAuthError when org not authenticated in SF CLI."""
        mock_sf_cli.is_available.return_value = True
        mock_sf_cli.is_org_authenticated_async = AsyncMock(return_value=False)

        with pytest.raises(Exception, match="No authenticated session"):
            await auth_service.import_sf_cli_session("default")

    @pytest.mark.asyncio
    async def test_import_sf_cli_session_successfully_imports_token(
        self, auth_service, mock_sf_cli, mock_crypto
    ):
        """Should import token and save to token store."""
        mock_sf_cli.is_available.return_value = True
        mock_sf_cli.is_org_authenticated_async = AsyncMock(return_value=True)

        org_info = SFCLIAuthResult(
            access_token="",
            instance_url="https://example.my.salesforce.com",
            refresh_token="refresh-token",
            expires_at=datetime.now(UTC) + timedelta(hours=1),
            alias="default",
            username="user@example.com",
        )
        mock_sf_cli.get_org_info = AsyncMock(return_value=org_info)
        mock_sf_cli.get_access_token = AsyncMock(return_value="access-token-123")

        # Mock token store save
        with patch.object(auth_service.token_store, "save_token", new_callable=AsyncMock) as mock_save:
            token = await auth_service.import_sf_cli_session("default")

            assert token == "access-token-123"
            mock_save.assert_called_once()
            saved_token = mock_save.call_args[0][0]
            assert isinstance(saved_token, StoredToken)
            assert saved_token.access_token == "access-token-123"
            assert saved_token.instance_url == "https://example.my.salesforce.com"
            assert saved_token.username == "user@example.com"
            assert saved_token.alias == "default"


class TestGetSfCliOrgs:
    """Tests for get_sf_cli_orgs method."""

    @pytest.mark.asyncio
    async def test_get_sf_cli_orgs_returns_empty_when_cli_not_available(
        self, auth_service, mock_sf_cli
    ):
        """Should return empty list when SF CLI not available."""
        mock_sf_cli.is_available.return_value = False

        result = await auth_service.get_sf_cli_orgs()
        assert result == []

    @pytest.mark.asyncio
    async def test_get_sf_cli_orgs_returns_orgs_from_sf_cli(
        self, auth_service, mock_sf_cli
    ):
        """Should return orgs from SF CLI manager."""
        mock_sf_cli.is_available.return_value = True
        mock_sf_cli.get_sf_cli_orgs = AsyncMock(return_value=[
            {"alias": "prod", "username": "user@prod.com", "instance_url": "https://prod.my.salesforce.com", "connected_status": "Connected"},
            {"alias": "dev", "username": "user@dev.com", "instance_url": "https://dev.my.salesforce.com", "connected_status": "Connected"},
        ])

        result = await auth_service.get_sf_cli_orgs()

        assert len(result) == 2
        assert result[0]["alias"] == "prod"
        assert result[1]["alias"] == "dev"
        mock_sf_cli.get_sf_cli_orgs.assert_called_once()