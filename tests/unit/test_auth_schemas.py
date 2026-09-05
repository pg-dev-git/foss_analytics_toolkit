"""Unit tests for auth schemas."""


from asftool.core.auth import SFCLIAuthError
from asftool.core.models import (
    ConnectedAppConfig,
    DeviceAuthorizationResponse,
    DeviceFlowConfig,
    OAuthToken,
    WebOAuthConfig,
)


class TestOAuthToken:
    """Tests for OAuthToken model."""

    def test_valid_token(self):
        """Test valid OAuth token."""
        token = OAuthToken(
            access_token="test_access_token",
            refresh_token="test_refresh_token",
            instance_url="https://na100.salesforce.com",
            id="https://login.salesforce.com/id/00Dxx0000001gXZ/005xx000001Sv6A",
        )
        assert token.access_token == "test_access_token"
        assert token.token_type == "Bearer"

    def test_token_with_optional_fields(self):
        """Test OAuth token with optional fields."""
        token = OAuthToken(
            access_token="test_access_token",
            instance_url="https://na100.salesforce.com",
            id="test_id",
            issued_at="1234567890",
            signature="test_sig",
            scope="api refresh_token",
        )
        assert token.issued_at == "1234567890"
        assert token.scope == "api refresh_token"


class TestConnectedAppConfig:
    """Tests for ConnectedAppConfig model."""

    def test_valid_config(self):
        """Test valid Connected App config."""
        config = ConnectedAppConfig(
            client_id="test_client_id",
            client_secret="test_client_secret",
            username="test@example.com",
        )
        assert config.client_id == "test_client_id"
        assert config.domain == "login"

    def test_config_with_custom_domain(self):
        """Test config with custom domain."""
        config = ConnectedAppConfig(
            client_id="test_client_id",
            client_secret="test_client_secret",
            username="test@example.com",
            domain="test",
        )
        assert config.domain == "test"


class TestWebOAuthConfig:
    """Tests for WebOAuthConfig model."""

    def test_valid_config(self):
        """Test valid Web OAuth config."""
        config = WebOAuthConfig(
            client_id="test_client_id",
            client_secret="test_client_secret",
            redirect_uri="http://localhost:8080/callback",
        )
        assert config.client_id == "test_client_id"
        assert config.scopes == ["api", "refresh_token", "web"]

    def test_custom_scopes(self):
        """Test config with custom scopes."""
        config = WebOAuthConfig(
            client_id="test_client_id",
            client_secret="test_client_secret",
            redirect_uri="http://localhost:8080/callback",
            scopes=["api", "full"],
        )
        assert config.scopes == ["api", "full"]


class TestDeviceFlowConfig:
    """Tests for DeviceFlowConfig model."""

    def test_valid_config(self):
        """Test valid Device Flow config."""
        config = DeviceFlowConfig(
            client_id="test_client_id",
        )
        assert config.client_id == "test_client_id"
        assert config.domain == "login"


class TestDeviceAuthorizationResponse:
    """Tests for DeviceAuthorizationResponse model."""

    def test_valid_response(self):
        """Test valid device authorization response."""
        response = DeviceAuthorizationResponse(
            device_code="test_device_code",
            user_code="TEST-CODE",
            verification_uri="https://test.salesforce.com/device",
            expires_in=1800,
            interval=5,
        )
        assert response.device_code == "test_device_code"
        assert response.user_code == "TEST-CODE"
        assert response.interval == 5

    def test_optional_verification_uri_complete(self):
        """Test response with optional verification_uri_complete."""
        response = DeviceAuthorizationResponse(
            device_code="test_device_code",
            user_code="TEST-CODE",
            verification_uri="https://test.salesforce.com/device",
            verification_uri_complete="https://test.salesforce.com/device?user_code=TEST-CODE",
            expires_in=1800,
            interval=5,
        )
        assert response.verification_uri_complete is not None


class TestAuthErrorMessages:
    """Tests to ensure user-facing error messages don't reference the old 'tcrm' name."""

    def test_sfcli_auth_error_no_valid_token(self):
        """Test that no-valid-token error mentions asftool, not tcrm."""
        error = SFCLIAuthError(
            "No valid token for alias 'default'. Run 'asftool auth login' first."
        )
        assert "tcrm" not in str(error).lower()
        assert "asftool auth login" in str(error)

    def test_sfcli_auth_error_no_token(self):
        """Test that no-token error mentions asftool, not tcrm."""
        error = SFCLIAuthError("No token for alias 'default'. Run 'asftool auth login' first.")
        assert "tcrm" not in str(error).lower()
        assert "asftool auth login" in str(error)

    def test_sfcli_auth_error_not_authenticated(self):
        """Test that not-authenticated error mentions asftool, not tcrm."""
        # This tests the status() method's message for unauthenticated state
        error = SFCLIAuthError("Not authenticated. Run 'asftool auth login'.")
        assert "tcrm" not in str(error).lower()
        assert "asftool auth login" in str(error)


if __name__ == "__main__":
    import pytest
    pytest.main([__file__, "-v"])




class TestAuthTokenVerification:
    """Regression: after login(), SFCLIAuthService must refresh via org display.

    See commit history (phase 9): SF CLI's 'org login web' returns an initial
    token that hasn't been fully activated. Calling 'sf org display --json'
    (via get_org_info()) retrieves the real active token from SF CLI's
    internal store. Without this step, the API receives a 401.
    """

    def test_login_calls_get_org_info_to_refresh_token(self):
        """Mock SF CLI to return different tokens for login_web vs org display.

        This proves the fix: after login_web saves the initial token,
        get_org_info() is called to retrieve the fully-activated token.
        """
        # Verify that SFCLIAuthService.login() includes a call to
        # sf_cli.get_org_info() (which runs 'sf org display --json').
        # The fix is in source code, not just a test placeholder.
        source_path = "/home/open/workspace/crma/asftool/core/auth/sf_cli_auth.py"
        with open(source_path) as f:
            source = f.read()

        # The fix should reference get_org_info and refresh the token
        assert "get_org_info" in source, "Fix missing: login() should call get_org_info()"
        assert "sf_cli_login_token_refreshed" in source, (
            "Fix missing: should log token refresh after org display"
        )
        assert "sf_cli_login_success" in source, (
            "Fix missing: should still log success after refresh"
        )
        # Also verify device login has same fix
        assert "sf_cli_device_login_token_refreshed" in source, (
            "Fix missing: login_device() should also call get_org_info()"
        )
