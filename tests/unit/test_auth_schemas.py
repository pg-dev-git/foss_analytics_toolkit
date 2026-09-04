"""Unit tests for auth schemas."""

import pytest
from pydantic import ValidationError

from asftool.core.models import (
    OAuthToken,
    ConnectedAppConfig,
    WebOAuthConfig,
    DeviceFlowConfig,
    DeviceAuthorizationResponse,
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