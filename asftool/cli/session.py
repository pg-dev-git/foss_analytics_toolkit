"""Session helper for CLI commands.

Thin bridge between CLI/menu handlers and the auth + service layer.
Manages a single SFCLIAuthService + SalesforceClient per command.
"""

from contextlib import asynccontextmanager
from typing import Any
from contextvars import ContextVar

import structlog

import os
from asftool.core.auth import SFCLIAuthService
from asftool.core.auth.token_store import TokenStore
from asftool.core.client import SalesforceClient
from asftool.core.config import Settings, get_settings
from asftool.core.crypto import CryptoManager, create_crypto_manager
from asftool.core.models.auth_tokens import AuthTokens

logger = structlog.get_logger(__name__)

# Context variable to store the current session alias for the interactive menu loop
_current_session_alias: ContextVar[str | None] = ContextVar("_current_session_alias", default=None)


def get_current_session_alias() -> str | None:
    """Get the current session alias from context."""
    return _current_session_alias.get()


def set_current_session_alias(alias: str | None) -> None:
    """Set the current session alias in context."""
    _current_session_alias.set(alias)


class Session:
    """Manages authenticated session for a single CLI command or menu action.

    The Session is intentionally short-lived: one CLI invocation = one Session.
    The Session owns the SFCLIAuthService and (when needed) SalesforceClient,
    and ensures both are closed properly via the async context manager.
    """

    def __init__(
        self,
        alias: str | None = None,
        settings: Settings | None = None,
        crypto: CryptoManager | None = None,
    ):
        # Use context alias if available, otherwise fallback to "default"
        self.alias = alias or get_current_session_alias() or "default"
        self.settings = settings or get_settings()
        self.crypto = crypto or create_crypto_manager()
        # Ensure crypto manager is valid before creating token store
        if self.crypto is None:
            from asftool.core.crypto import CryptoManager
            import base64
            temp_key = base64.urlsafe_b64encode(os.urandom(32)).decode()
            self.crypto = CryptoManager(temp_key)
        self._auth_service: SFCLIAuthService | None = None
        self._client: SalesforceClient | None = None
        self.token_store = TokenStore(self.crypto)

    @property
    def auth_service(self) -> SFCLIAuthService:
        """Lazy-init auth service."""
        if self._auth_service is None:
            self._auth_service = SFCLIAuthService(
                settings=self.settings,
                crypto_manager=self.crypto,
            )
        return self._auth_service

    async def get_client(self) -> SalesforceClient:
        """Get authenticated SalesforceClient (with auto-refresh)."""
        if self._client is not None:
            return self._client

        token = await self.auth_service.get_access_token(
            alias=self.alias,
            auto_refresh=True,
        )
        instance_url = await self.auth_service.get_instance_url(alias=self.alias)
        
        # Get stored token to retrieve API version if available
        from asftool.core.auth.token_store import TokenStore
        stored_token = await self.auth_service.token_store.load_token(self.alias)
        api_version = stored_token.api_version if stored_token else None
        
        # Create settings with the stored API version if available
        from asftool.core.config import Settings
        client_settings = self.settings
        if api_version:
            # Create a copy of settings with the org's API version
            client_settings = Settings(
                **{**self.settings.model_dump(), "sf_api_version": api_version}
            )

        self._client = SalesforceClient(
            access_token=token,
            instance_url=instance_url,
            settings=client_settings,
        )
        return self._client

    async def close(self) -> None:
        """Cleanup SalesforceClient. Auth service has no async resources."""
        if self._client is not None:
            await self._client.close()
            self._client = None
        self._auth_service = None

    @asynccontextmanager
    async def client_context(self):
        """Async context manager for SalesforceClient with auto-cleanup."""
        client = await self.get_client()
        try:
            yield client
        finally:
            await self.close()

    async def get_auth_tokens(self, alias: str = "default") -> AuthTokens:
        """Get typed authentication tokens for the session alias."""
        # First try stored token (from previous auth login) without triggering SF CLI login
        token = await self.token_store.load_token(alias)
        if token and token.access_token:
            return AuthTokens(
                access_token=token.access_token,
                instance_url=token.instance_url,
                username=token.username,
                alias=alias,
                token_expired=token.is_expired() if hasattr(token, 'is_expired') else False,
                api_version=token.api_version,
            )
        # Fallback: retrieve from SF CLI auth service
        return await self.auth_service.get_auth_tokens(alias=alias)

    async def check_sf_cli_auth(self, alias: str = "default") -> dict[str, Any]:
        """Check if SF CLI has an authenticated session for the alias."""
        return await self.auth_service.check_sf_cli_auth(alias=alias)

    async def import_sf_cli_session(self, alias: str = "default") -> str:
        """Import an existing SF CLI authenticated session into the token store."""
        return await self.auth_service.import_sf_cli_session(alias=alias)

    async def get_sf_cli_orgs(self) -> list[dict[str, Any]]:
        """Get detailed list of all authenticated orgs from SF CLI."""
        return await self.auth_service.get_sf_cli_orgs()

    async def __aenter__(self) -> "Session":
        """Enter async context manager."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Exit async context manager, clean up resources."""
        await self.close()
