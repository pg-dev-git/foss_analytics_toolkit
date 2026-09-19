"""Integration tests for the ``asftool auth`` command group."""

import pytest
from typer.testing import CliRunner
from unittest.mock import AsyncMock, Mock, patch

from asftool.cli.commands.auth import app as auth_app
from asftool.core.sf_cli import SFCLIAuthResult, SFCLIManager


@pytest.fixture
def runner():
    return CliRunner()


@pytest.fixture
def mock_sf_cli_manager():
    """Create a mock SFCLIManager with common setup."""
    with patch("asftool.cli.commands.auth.Session") as mock_session_class:
        mock_session = Mock()
        mock_session_class.return_value = mock_session
        
        # Mock auth_service
        mock_auth_service = Mock()
        mock_session.auth_service = mock_auth_service
        mock_session.close = AsyncMock()
        
        yield mock_auth_service, mock_session


class TestAuthLogin:
    """Tests for auth login command."""

    def test_login_help(self, runner):
        result = runner.invoke(auth_app, ["login", "--help"])
        assert result.exit_code == 0
        assert "Authenticate via SF CLI web/device login" in result.stdout
        assert "--force" in result.stdout
        assert "-f" in result.stdout

    def test_login_checks_sf_cli_availability(self, runner):
        """Login should fail gracefully when SF CLI not available."""
        with patch("asftool.cli.commands.auth.Session") as mock_session_class:
            mock_session = Mock()
            mock_session_class.return_value = mock_session
            mock_session.auth_service.sf_cli.is_available.return_value = False
            mock_session.close = AsyncMock()
            
            result = runner.invoke(auth_app, ["login"])
            
            assert result.exit_code == 1
            assert "SF CLI not found" in result.stdout


class TestAuthCheckAuth:
    """Tests for auth check-auth command."""

    def test_check_auth_help(self, runner):
        result = runner.invoke(auth_app, ["check-auth", "--help"])
        assert result.exit_code == 0
        assert "Check if SF CLI has an authenticated session" in result.stdout

    def test_check_auth_success(self, runner, mock_sf_cli_manager):
        """Check auth should show success when SF CLI has session."""
        mock_auth_service, mock_session = mock_sf_cli_manager
        mock_auth_service.check_sf_cli_auth = AsyncMock(return_value={
            "authenticated": True,
            "alias": "default",
            "username": "user@example.com",
            "instance_url": "https://example.my.salesforce.com",
            "has_valid_token": True,
            "message": "SF CLI has authenticated session for 'default'",
        })
        
        result = runner.invoke(auth_app, ["check-auth"])
        
        assert result.exit_code == 0
        assert "SF CLI authenticated: default" in result.stdout
        assert "user@example.com" in result.stdout
        assert "https://example.my.salesforce.com" in result.stdout
        assert "Valid access token available" in result.stdout

    def test_check_auth_not_authenticated(self, runner, mock_sf_cli_manager):
        """Check auth should show warning when no session exists."""
        mock_auth_service, mock_session = mock_sf_cli_manager
        mock_auth_service.check_sf_cli_auth = AsyncMock(return_value={
            "authenticated": False,
            "alias": "default",
            "username": None,
            "instance_url": None,
            "has_valid_token": False,
            "message": "No authenticated session found for alias 'default' in SF CLI",
        })
        
        result = runner.invoke(auth_app, ["check-auth"])
        
        assert result.exit_code == 0
        assert "No authenticated session found" in result.stdout


class TestAuthImportSf:
    """Tests for auth import-sf command."""

    def test_import_sf_help(self, runner):
        result = runner.invoke(auth_app, ["import-sf", "--help"])
        assert result.exit_code == 0
        assert "SF CLI alias" in result.stdout

    def test_import_sf_success(self, runner, mock_sf_cli_manager):
        """Import should succeed when SF CLI has session."""
        mock_auth_service, mock_session = mock_sf_cli_manager
        mock_auth_service.import_sf_cli_session = AsyncMock(return_value="access-token-123")
        mock_auth_service.get_instance_url = AsyncMock(return_value="https://example.my.salesforce.com")
        mock_auth_service.get_username = AsyncMock(return_value="user@example.com")
        
        result = runner.invoke(auth_app, ["import-sf"])
        
        assert result.exit_code == 0
        assert "Successfully imported SF CLI session for 'default'" in result.stdout
        assert "https://example.my.salesforce.com" in result.stdout
        assert "user@example.com" in result.stdout
        assert "Session is now available" in result.stdout

    def test_import_sf_failure(self, runner, mock_sf_cli_manager):
        """Import should fail gracefully when SF CLI has no session."""
        from asftool.core.auth import SFCLIAuthError
        mock_auth_service, mock_session = mock_sf_cli_manager
        mock_auth_service.import_sf_cli_session = AsyncMock(
            side_effect=SFCLIAuthError("No authenticated session for alias 'default' in SF CLI")
        )
        
        result = runner.invoke(auth_app, ["import-sf"])
        
        assert result.exit_code == 1
        assert "Import failed" in result.stdout
        assert "No authenticated session" in result.stdout


class TestAuthStatus:
    """Tests for auth status command."""

    def test_status_help(self, runner):
        result = runner.invoke(auth_app, ["status", "--help"])
        assert result.exit_code == 0
        assert "Check authentication status" in result.stdout

    def test_status_authenticated(self, runner, mock_sf_cli_manager):
        """Status should show authenticated info."""
        mock_auth_service, mock_session = mock_sf_cli_manager
        mock_auth_service.status = AsyncMock(return_value={
            "authenticated": True,
            "alias": "default",
            "username": "user@example.com",
            "instance_url": "https://example.my.salesforce.com",
            "token_expired": False,
            "expires_at": "2026-12-31T23:59:59+00:00",
            "sf_cli_available": True,
            "message": "Authenticated",
        })
        
        result = runner.invoke(auth_app, ["status"])
        
        assert result.exit_code == 0
        assert "Authenticated: default" in result.stdout
        assert "user@example.com" in result.stdout
        assert "https://example.my.salesforce.com" in result.stdout
        assert "Token valid" in result.stdout

    def test_status_not_authenticated(self, runner, mock_sf_cli_manager):
        """Status should show not authenticated."""
        mock_auth_service, mock_session = mock_sf_cli_manager
        mock_auth_service.status = AsyncMock(return_value={
            "authenticated": False,
            "alias": "default",
            "message": "Not authenticated. Run 'asftool auth login'.",
            "sf_cli_available": True,
        })
        
        result = runner.invoke(auth_app, ["status"])
        
        assert result.exit_code == 0
        assert "Not authenticated" in result.stdout


class TestAuthListOrgs:
    """Tests for auth list-orgs command."""

    def test_list_orgs_help(self, runner):
        result = runner.invoke(auth_app, ["list-orgs", "--help"])
        assert result.exit_code == 0
        assert "List all authorized orgs" in result.stdout

    def test_list_orgs_empty(self, runner, mock_sf_cli_manager):
        """List orgs should handle empty list."""
        mock_auth_service, mock_session = mock_sf_cli_manager
        mock_auth_service.list_orgs = AsyncMock(return_value=[])
        
        result = runner.invoke(auth_app, ["list-orgs"])
        
        assert result.exit_code == 0
        assert "No authorized orgs found" in result.stdout


class TestAuthLogout:
    """Tests for auth logout command."""

    def test_logout_help(self, runner):
        result = runner.invoke(auth_app, ["logout", "--help"])
        assert result.exit_code == 0
        assert "Remove stored authentication" in result.stdout

    def test_logout_success(self, runner, mock_sf_cli_manager):
        """Logout should succeed when token exists."""
        mock_auth_service, mock_session = mock_sf_cli_manager
        mock_auth_service.logout = AsyncMock(return_value=True)
        
        result = runner.invoke(auth_app, ["logout", "default"])
        
        assert result.exit_code == 0
        assert "Logged out alias 'default'" in result.stdout

    def test_logout_no_token(self, runner, mock_sf_cli_manager):
        """Logout should warn when no token exists."""
        mock_auth_service, mock_session = mock_sf_cli_manager
        mock_auth_service.logout = AsyncMock(return_value=False)
        
        result = runner.invoke(auth_app, ["logout", "default"])
        
        assert result.exit_code == 0
        assert "No stored credentials for alias 'default'" in result.stdout

class TestAuthStartupOrgSelection:
    """Integration tests for startup SF CLI org selection flow."""

    def test_startup_flow_with_three_orgs(self, runner, mock_sf_cli_manager):
        mock_auth_service, mock_session = mock_sf_cli_manager
        mock_auth_service.get_sf_cli_orgs = AsyncMock(return_value=[
            {"alias": "prod", "username": "user@prod.com", "instance_url": "https://prod.my.salesforce.com", "connected_status": "Connected"},
            {"alias": "dev", "username": "user@dev.com", "instance_url": "https://dev.my.salesforce.com", "connected_status": "Connected"},
            {"alias": "sandbox", "username": "user@sandbox.com", "instance_url": "https://sandbox.my.salesforce.com", "connected_status": "Connected"},
        ])
        mock_auth_service.import_sf_cli_session = AsyncMock(return_value="token-123")
        result = runner.invoke(auth_app, ["check-auth"])
        assert result.exit_code == 0
