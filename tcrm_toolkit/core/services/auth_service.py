"""Authentication service with pure Python OAuth flows."""

import asyncio
import base64
import hashlib
import secrets
import time
import urllib.parse
from contextlib import asynccontextmanager
from dataclasses import dataclass
from urllib.parse import parse_qs, urlparse

import httpx
import structlog
from authlib.integrations.httpx_client import AsyncOAuth2Client
from jose import jwt

from tcrm_toolkit.core.config import Settings, get_settings
from tcrm_toolkit.core.crypto import CryptoManager
from tcrm_toolkit.core.exceptions import (
    OAuthError,
    TokenExpiredError,
    TokenNotFoundError,
)
from tcrm_toolkit.core.models import (
    ConnectedAppConfig,
    DeviceAuthorizationResponse,
    DeviceFlowConfig,
    OAuthToken,
    WebOAuthConfig,
)

logger = structlog.get_logger(__name__)


@dataclass
class PKCEChallenge:
    """PKCE code verifier and challenge pair."""
    code_verifier: str
    code_challenge: str
    code_challenge_method: str = "S256"


class AuthService:
    """Authentication service supporting multiple OAuth 2.0 flows."""

    def __init__(
        self,
        settings: Settings | None = None,
        crypto: CryptoManager | None = None,
    ):
        """Initialize the auth service."""
        self.settings = settings or get_settings()
        self.crypto = crypto or CryptoManager()
        self._http_client: httpx.AsyncClient | None = None

    @property
    def http_client(self) -> httpx.AsyncClient:
        """Get or create the HTTP client."""
        if self._http_client is None:
            self._http_client = httpx.AsyncClient(
                timeout=httpx.Timeout(30.0),
                follow_redirects=True,
            )
        return self._http_client

    async def close(self) -> None:
        """Close the HTTP client."""
        if self._http_client:
            await self._http_client.aclose()
            self._http_client = None

    # =========================================================================
    # PKCE Utilities
    # =========================================================================

    @staticmethod
    def generate_pkce_challenge() -> PKCEChallenge:
        """Generate PKCE code verifier and challenge."""
        code_verifier = secrets.token_urlsafe(32)
        code_challenge = base64.urlsafe_b64encode(
            hashlib.sha256(code_verifier.encode()).digest()
        ).decode().rstrip("=")
        return PKCEChallenge(
            code_verifier=code_verifier,
            code_challenge=code_challenge,
        )

    @staticmethod
    def build_authorize_url(
        config: WebOAuthConfig,
        code_challenge: str,
        state: str | None = None,
    ) -> str:
        """Build the authorization URL for Web PKCE flow."""
        params = {
            "response_type": "code",
            "client_id": config.client_id,
            "redirect_uri": config.redirect_uri,
            "code_challenge": code_challenge,
            "code_challenge_method": "S256",
            "scope": " ".join(config.scopes),
        }
        if state:
            params["state"] = state

        base_url = f"https://{config.domain}.salesforce.com/services/oauth2/authorize"
        return f"{base_url}?{urllib.parse.urlencode(params)}"

    # =========================================================================
    # JWT Bearer Flow (Connected App)
    # =========================================================================

    async def jwt_bearer_login(self, config: ConnectedAppConfig) -> OAuthToken:
        """Authenticate using JWT Bearer flow for Connected Apps.

        This is the recommended flow for server-to-server automation.
        """
        if not all([config.client_id, config.client_secret, config.username]):
            raise OAuthError("Missing required Connected App credentials")

        # Create JWT assertion
        now = int(time.time())
        claim = {
            "iss": config.client_id,
            "sub": config.username,
            "aud": f"https://{config.domain}.salesforce.com/services/oauth2/token",
            "exp": now + 300,  # 5 minutes
            "iat": now,
        }

        # Sign with client_secret (HS256) - for production, use RS256 with certificate
        assertion = jwt.encode(claim, config.client_secret, algorithm="HS256")

        token_url = f"https://{config.domain}.salesforce.com/services/oauth2/token"
        data = {
            "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
            "assertion": assertion,
        }

        async with AsyncOAuth2Client() as client:
            response = await client.post(token_url, data=data)
            response.raise_for_status()
            token_data = response.json()

        return OAuthToken(**token_data)

    # =========================================================================
    # Web PKCE Flow
    # =========================================================================

    async def start_web_pkce_flow(
        self,
        config: WebOAuthConfig,
    ) -> tuple[str, PKCEChallenge, str]:
        """Start Web PKCE flow and return authorize URL, PKCE challenge, and state."""
        pkce = self.generate_pkce_challenge()
        state = secrets.token_urlsafe(16)
        authorize_url = self.build_authorize_url(config, pkce.code_challenge, state)
        return authorize_url, pkce, state

    async def exchange_pkce_code(
        self,
        config: WebOAuthConfig,
        code: str,
        pkce: PKCEChallenge,
    ) -> OAuthToken:
        """Exchange authorization code for tokens in PKCE flow."""
        token_url = f"https://{config.domain}.salesforce.com/services/oauth2/token"
        data = {
            "grant_type": "authorization_code",
            "client_id": config.client_id,
            "client_secret": config.client_secret,
            "code": code,
            "redirect_uri": config.redirect_uri,
            "code_verifier": pkce.code_verifier,
        }

        async with AsyncOAuth2Client() as client:
            response = await client.post(token_url, data=data)
            response.raise_for_status()
            token_data = response.json()

        return OAuthToken(**token_data)

    async def run_web_pkce_flow(
        self,
        config: WebOAuthConfig,
        port: int = 8080,
    ) -> OAuthToken:
        """Run complete Web PKCE flow with local callback server."""
        authorize_url, pkce, state = await self.start_web_pkce_flow(config)

        # Start local server to receive callback
        code_future: asyncio.Future[str] = asyncio.get_event_loop().create_future()
        received_state: asyncio.Future[str] = asyncio.get_event_loop().create_future()

        async def callback_handler(request: httpx.Request) -> httpx.Response:
            query = parse_qs(urlparse(str(request.url)).query)
            if "code" in query:
                code_future.set_result(query["code"][0])
            if "state" in query:
                received_state.set_result(query["state"][0])
            return httpx.Response(
                200,
                text="Authentication successful! You can close this window.",
                headers={"Content-Type": "text/html"},
            )

        # Note: In production, use a proper ASGI server like uvicorn
        # For CLI, we'll use a simple approach with a temporary server
        logger.info("web_pkce_started", authorize_url=authorize_url)
        print(f"\nPlease open this URL in your browser:\n{authorize_url}\n")

        # For now, we'll prompt for the code manually
        # A full implementation would start a local HTTP server
        code = input("Enter the authorization code from the callback URL: ").strip()
        if not code:
            raise OAuthError("No authorization code provided")

        return await self.exchange_pkce_code(config, code, pkce)

    # =========================================================================
    # Device Authorization Flow
    # =========================================================================

    async def start_device_flow(self, config: DeviceFlowConfig) -> DeviceAuthorizationResponse:
        """Start Device Authorization Flow."""
        token_url = f"https://{config.domain}.salesforce.com/services/oauth2/device_authorization"
        data = {"client_id": config.client_id}

        async with AsyncOAuth2Client() as client:
            response = await client.post(token_url, data=data)
            response.raise_for_status()
            return DeviceAuthorizationResponse(**response.json())

    async def poll_device_token(
        self,
        config: DeviceFlowConfig,
        device_code: str,
    ) -> OAuthToken:
        """Poll for device token."""
        token_url = f"https://{config.domain}.salesforce.com/services/oauth2/token"
        data = {
            "grant_type": "urn:ietf:params:oauth:grant-type:device_code",
            "client_id": config.client_id,
            "device_code": device_code,
        }

        async with AsyncOAuth2Client() as client:
            response = await client.post(token_url, data=data)

            if response.status_code == 400:
                error_data = response.json()
                error = error_data.get("error")
                if error == "authorization_pending":
                    raise OAuthError("Authorization pending", code="pending")
                elif error == "slow_down":
                    raise OAuthError("Polling too fast", code="slow_down")
                elif error == "expired_token":
                    raise OAuthError("Device code expired", code="expired")
                elif error == "access_denied":
                    raise OAuthError("User denied authorization", code="access_denied")
                else:
                    raise OAuthError(f"Device flow error: {error}")

            response.raise_for_status()
            return OAuthToken(**response.json())

    async def run_device_flow(
        self,
        config: DeviceFlowConfig,
    ) -> OAuthToken:
        """Run complete Device Authorization Flow."""
        device_auth = await self.start_device_flow(config)

        logger.info(
            "device_flow_started",
            user_code=device_auth.user_code,
            verification_uri=device_auth.verification_uri,
        )

        print(f"\nPlease go to: {device_auth.verification_uri}")
        print(f"And enter code: {device_auth.user_code}")
        print(f"\nWaiting for authorization... (expires in {device_auth.expires_in}s)")

        interval = device_auth.interval or 5
        start_time = time.time()

        while time.time() - start_time < device_auth.expires_in:
            try:
                token = await self.poll_device_token(config, device_auth.device_code)
                logger.info("device_flow_completed")
                return token
            except OAuthError as e:
                if e.error_code == "pending":
                    await asyncio.sleep(interval)
                    continue
                elif e.error_code == "slow_down":
                    interval += 5
                    await asyncio.sleep(interval)
                    continue
                else:
                    raise

        raise OAuthError("Device authorization timed out", code="timeout")

    # =========================================================================
    # Token Refresh
    # =========================================================================

    async def refresh_access_token(
        self,
        client_id: str,
        client_secret: str,
        refresh_token: str,
        domain: str = "login",
    ) -> OAuthToken:
        """Refresh an expired access token using refresh token."""
        token_url = f"https://{domain}.salesforce.com/services/oauth2/token"
        data = {
            "grant_type": "refresh_token",
            "client_id": client_id,
            "client_secret": client_secret,
            "refresh_token": refresh_token,
        }

        async with AsyncOAuth2Client() as client:
            response = await client.post(token_url, data=data)
            response.raise_for_status()
            return OAuthToken(**response.json())

    # =========================================================================
    # Token Storage (Keyring)
    # =========================================================================

    def store_tokens(self, username: str, token: OAuthToken) -> None:
        """Store OAuth tokens securely in keyring."""
        token_data = {
            "access_token": token.access_token,
            "refresh_token": token.refresh_token,
            "instance_url": token.instance_url,
            "id": token.id,
            "token_type": token.token_type,
            "issued_at": token.issued_at,
            "scope": token.scope,
        }
        self.crypto.store_token(username, token_data)
        logger.info("tokens_stored", username=username)

    def retrieve_tokens(self, username: str) -> OAuthToken | None:
        """Retrieve OAuth tokens from keyring."""
        token_data = self.crypto.retrieve_token(username)
        if not token_data:
            return None
        return OAuthToken(**token_data)

    def delete_tokens(self, username: str) -> bool:
        """Delete stored tokens from keyring."""
        return self.crypto.delete_token(username)

    # =========================================================================
    # Auto-Refresh Logic
    # =========================================================================

    def is_token_expired(self, token: OAuthToken, buffer_seconds: int = 300) -> bool:
        """Check if token is expired or near expiry."""
        if not token.issued_at:
            return True
        try:
            issued_at = int(token.issued_at) / 1000  # Convert from milliseconds
            expires_in = 7200  # Default 2 hours for access token
            return (time.time() - issued_at) > (expires_in - buffer_seconds)
        except (ValueError, TypeError):
            return True

    async def ensure_valid_token(
        self,
        username: str,
        config: ConnectedAppConfig | WebOAuthConfig | DeviceFlowConfig,
    ) -> OAuthToken:
        """Get valid token, refreshing if necessary."""
        token = self.retrieve_tokens(username)
        if not token:
            raise TokenNotFoundError(f"No stored token for user: {username}")

        if not self.is_token_expired(token):
            return token

        if not token.refresh_token:
            raise TokenExpiredError("Token expired and no refresh token available")

        logger.info("refreshing_token", username=username)

        # Determine flow type and refresh
        if isinstance(config, ConnectedAppConfig):
            new_token = await self.jwt_bearer_login(config)
        elif isinstance(config, WebOAuthConfig):
            new_token = await self.refresh_access_token(
                config.client_id,
                config.client_secret,
                token.refresh_token,
                config.domain,
            )
        elif isinstance(config, DeviceFlowConfig):
            new_token = await self.refresh_access_token(
                config.client_id,
                config.client_secret,
                token.refresh_token,
                config.domain,
            )
        else:
            raise OAuthError("Unknown config type for token refresh")

        # Store new tokens
        self.store_tokens(username, new_token)
        return new_token


@asynccontextmanager
async def create_auth_service(
    settings: Settings | None = None,
    crypto: CryptoManager | None = None,
) -> AuthService:
    """Context manager for creating and closing an AuthService."""
    service = AuthService(settings, crypto)
    try:
        yield service
    finally:
        await service.close()
