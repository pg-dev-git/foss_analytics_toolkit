"""Session management for Interactive TUI - wraps SFCLIAuthService."""

from contextlib import asynccontextmanager
from dataclasses import dataclass

from tcrm_toolkit.core.auth import SFCLIAuthError, SFCLIAuthService
from tcrm_toolkit.core.client import SalesforceClient
from tcrm_toolkit.core.config import Settings, get_settings
from tcrm_toolkit.core.crypto import CryptoManager, create_crypto_manager
from tcrm_toolkit.interactive.safety import SafetyError, SafetyMonitor


@dataclass
class OrgSession:
    """Represents an authenticated org session."""
    alias: str
    username: str | None
    instance_url: str
    is_default: bool = False


class SessionManager:
    """
    Manages authenticated sessions across the TUI lifecycle.
    
    Responsibilities:
    - Multi-org credential management via SF CLI aliases
    - Auto-refresh tokens before expiry
    - Session persistence across TUI restarts
    - Quick org switching (Ctrl+O)
    - Safety gate: blocks client creation if VPN/Proxy detected
    """

    def __init__(
        self,
        settings: Settings | None = None,
        crypto: CryptoManager | None = None,
        safety_monitor: SafetyMonitor | None = None,
    ):
        self.settings = settings or get_settings()
        self.crypto = crypto or create_crypto_manager()
        self.safety = safety_monitor or SafetyMonitor(self.settings)
        self._auth_service: SFCLIAuthService | None = None
        self._current_alias: str = "default"
        self._client: SalesforceClient | None = None
        self._org_sessions: dict[str, OrgSession] = {}

    @property
    def auth_service(self) -> SFCLIAuthService:
        """Lazy-initialize SFCLIAuthService."""
        if self._auth_service is None:
            self._auth_service = SFCLIAuthService(
                settings=self.settings,
                crypto_manager=self.crypto,
            )
        return self._auth_service

    @property
    def current_alias(self) -> str:
        return self._current_alias

    @property
    def current_org(self) -> OrgSession | None:
        return self._org_sessions.get(self._current_alias)

    async def initialize(self) -> None:
        """Initialize session - load orgs, check safety, auto-login if needed."""
        safety_result = await self.safety.check_connection_safety()
        if not safety_result.is_safe and self.settings.safety_block_on_critical:
            raise SafetyError(f"Unsafe connection: {safety_result.details}")

        await self.refresh_org_list()

        try:
            await self.ensure_valid_token()
        except SFCLIAuthError:
            pass

    async def refresh_org_list(self) -> list[OrgSession]:
        """Refresh list of authorized orgs from SF CLI."""
        orgs = await self.auth_service.list_orgs()
        self._org_sessions = {}

        for org in orgs:
            alias = org.get("alias", "unknown")
            username = org.get("username")
            instance_url = org.get("instanceUrl", "")
            connected = org.get("connectedStatus") == "Connected"

            if connected and username and instance_url:
                self._org_sessions[alias] = OrgSession(
                    alias=alias,
                    username=username,
                    instance_url=instance_url.rstrip("/"),
                    is_default=(alias == "default"),
                )

        return list(self._org_sessions.values())

    async def ensure_valid_token(self, alias: str | None = None) -> str:
        """Get valid access token for alias, auto-refresh if needed."""
        alias = alias or self._current_alias

        safety_result = await self.safety.check_connection_safety()
        if not safety_result.is_safe and self.settings.safety_block_on_critical:
            raise SafetyError(f"Unsafe connection: {safety_result.details}")

        token = await self.auth_service.get_access_token(alias=alias, auto_refresh=True)
        return token

    async def get_client(self, alias: str | None = None) -> SalesforceClient:
        """Get authenticated SalesforceClient for alias."""
        alias = alias or self._current_alias

        safety_result = await self.safety.check_connection_safety()
        if not safety_result.is_safe and self.settings.safety_block_on_critical:
            raise SafetyError(f"Unsafe connection: {safety_result.details}")

        access_token = await self.ensure_valid_token(alias)
        instance_url = await self.auth_service.get_instance_url(alias)

        self._client = SalesforceClient(
            access_token=access_token,
            instance_url=instance_url,
            settings=self.settings,
        )

        return self._client

    @asynccontextmanager
    async def client_context(self, alias: str | None = None):
        """Context manager for SalesforceClient with auto-cleanup."""
        client = await self.get_client(alias)
        try:
            yield client
        finally:
            await client.close()
            self._client = None

    async def switch_org(self, alias: str) -> OrgSession:
        """Switch to different org alias."""
        if alias not in self._org_sessions:
            await self.refresh_org_list()

        if alias not in self._org_sessions:
            raise SFCLIAuthError(f"Org alias '{alias}' not found. Run 'sf org list' first.")

        self._current_alias = alias
        self._client = None

        await self.ensure_valid_token(alias)

        return self._org_sessions[alias]

    async def login(self, alias: str = "default", instance_url: str | None = None) -> str:
        """Run SF CLI web login flow."""
        token = await self.auth_service.login(alias=alias, instance_url=instance_url)
        await self.refresh_org_list()
        self._current_alias = alias
        return token

    async def logout(self, alias: str = "default") -> bool:
        """Logout and remove stored auth for alias."""
        result = await self.auth_service.logout(alias)
        await self.refresh_org_list()

        if alias == self._current_alias:
            self._current_alias = "default"
            self._client = None

        return result

    async def get_status(self, alias: str | None = None) -> dict:
        """Get authentication status for alias."""
        alias = alias or self._current_alias
        return await self.auth_service.status(alias)

    def list_orgs(self) -> list[OrgSession]:
        """List all known org sessions."""
        return list(self._org_sessions.values())

    async def close(self) -> None:
        """Cleanup resources."""
        if self._client:
            await self._client.close()
            self._client = None
        if self._auth_service:
            await self._auth_service.close()
            self._auth_service = None
