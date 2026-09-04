"""Session helper for CLI commands.

Thin bridge between CLI/menu handlers and the auth + service layer.
Manages a single SFCLIAuthService + SalesforceClient per command.
"""

from contextlib import asynccontextmanager

import structlog

from asftool.core.auth import SFCLIAuthService
from asftool.core.client import SalesforceClient
from asftool.core.config import Settings, get_settings
from asftool.core.crypto import CryptoManager, create_crypto_manager

logger = structlog.get_logger(__name__)


class Session:
    """Manages authenticated session for a single CLI command or menu action.

    The Session is intentionally short-lived: one CLI invocation = one Session.
    The Session owns the SFCLIAuthService and (when needed) SalesforceClient,
    and ensures both are closed properly via the async context manager.
    """

    def __init__(
        self,
        alias: str = "default",
        settings: Settings | None = None,
        crypto: CryptoManager | None = None,
    ):
        self.alias = alias
        self.settings = settings or get_settings()
        self.crypto = crypto or create_crypto_manager()
        self._auth_service: SFCLIAuthService | None = None
        self._client: SalesforceClient | None = None

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

        self._client = SalesforceClient(
            access_token=token,
            instance_url=instance_url,
            settings=self.settings,
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
