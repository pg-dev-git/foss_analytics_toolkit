"""SF CLI-based authentication service."""

from typing import Any

import structlog

from tcrm_toolkit.core.auth.token_store import StoredToken, TokenStore
from tcrm_toolkit.core.config import Settings
from tcrm_toolkit.core.crypto import CryptoManager
from tcrm_toolkit.core.sf_cli import SFCLIError, SFCLIManager, SFCLINotFoundError

logger = structlog.get_logger(__name__)


class SFCLIAuthError(Exception):
    """SF CLI authentication error."""
    pass


class SFCLIAuthService:
    """High-level SF CLI authentication service."""

    def __init__(
        self,
        settings: Settings,
        crypto_manager: CryptoManager,
        sf_cli_manager: SFCLIManager | None = None,
    ):
        """
        Initialize SF CLI auth service.

        Args:
            settings: Application settings
            crypto_manager: CryptoManager for token encryption
            sf_cli_manager: Optional SFCLIManager instance (created if not provided)
        """
        self.settings = settings
        self.crypto = crypto_manager
        self.sf_cli = sf_cli_manager or SFCLIManager()
        self.token_store = TokenStore(crypto_manager)

    async def login(
        self,
        alias: str = "default",
        instance_url: str | None = None,
        timeout: int = 300,
    ) -> str:
        """
        Run full web login flow via SF CLI.

        Args:
            alias: Org alias to use
            instance_url: Optional custom instance URL
            timeout: Timeout in seconds for login flow

        Returns:
            Access token

        Raises:
            SFCLIAuthError: If login fails
        """
        if not self.sf_cli.is_available():
            raise SFCLIAuthError(
                "SF CLI not found. Install from https://developer.salesforce.com/tools/sfdxcli"
            )

        logger.info("starting_sf_cli_login", alias=alias)

        try:
            # Run SF CLI web login
            auth_result = await self.sf_cli.login_web(
                alias=alias,
                instance_url=instance_url,
                timeout=timeout,
            )

            # Store token
            stored_token = StoredToken(
                access_token=auth_result.access_token,
                instance_url=auth_result.instance_url,
                refresh_token=auth_result.refresh_token,
                expires_at=auth_result.expires_at.isoformat() if auth_result.expires_at else None,
                alias=auth_result.alias,
                username=auth_result.username,
            )
            await self.token_store.save_token(stored_token)

            logger.info("sf_cli_login_success", alias=alias, username=auth_result.username)
            return auth_result.access_token

        except SFCLINotFoundError as e:
            raise SFCLIAuthError(str(e)) from e
        except SFCLIError as e:
            raise SFCLIAuthError(f"SF CLI login failed: {e}") from e
        except Exception as e:
            raise SFCLIAuthError(f"Unexpected error during login: {e}") from e

    async def login_device(
        self,
        alias: str = "default",
        instance_url: str | None = None,
        timeout: int = 300,
    ) -> str:
        """
        Run device login flow via SF CLI (for headless environments).

        Args:
            alias: Org alias to use
            instance_url: Optional custom instance URL
            timeout: Timeout in seconds for login flow

        Returns:
            Access token

        Raises:
            SFCLIAuthError: If login fails
        """
        if not self.sf_cli.is_available():
            raise SFCLIAuthError(
                "SF CLI not found. Install from https://developer.salesforce.com/tools/sfdxcli"
            )

        logger.info("starting_sf_cli_device_login", alias=alias)

        try:
            auth_result = await self.sf_cli.login_device(
                alias=alias,
                instance_url=instance_url,
                timeout=timeout,
            )

            stored_token = StoredToken(
                access_token=auth_result.access_token,
                instance_url=auth_result.instance_url,
                refresh_token=auth_result.refresh_token,
                expires_at=auth_result.expires_at.isoformat() if auth_result.expires_at else None,
                alias=auth_result.alias,
                username=auth_result.username,
            )
            await self.token_store.save_token(stored_token)

            logger.info("sf_cli_device_login_success", alias=alias, username=auth_result.username)
            return auth_result.access_token

        except SFCLINotFoundError as e:
            raise SFCLIAuthError(str(e)) from e
        except SFCLIError as e:
            raise SFCLIAuthError(f"SF CLI device login failed: {e}") from e
        except Exception as e:
            raise SFCLIAuthError(f"Unexpected error during device login: {e}") from e

    async def get_access_token(
        self,
        alias: str = "default",
        auto_refresh: bool = True,
    ) -> str:
        """
        Get valid access token, auto-refresh if needed.

        Args:
            alias: Org alias
            auto_refresh: Whether to attempt auto-refresh

        Returns:
            Valid access token

        Raises:
            SFCLIAuthError: If no valid token available
        """
        # Try to get valid token from store
        token = await self.token_store.get_valid_token(alias, self.sf_cli, auto_refresh)

        if token and not token.is_expired():
            return token.access_token

        # If we have a token but it's expired and auto_refresh failed,
        # or no token at all, try to re-login
        if auto_refresh:
            logger.info("attempting_re_login", alias=alias)
            return await self.login(alias=alias)

        raise SFCLIAuthError(
            f"No valid token for alias '{alias}'. Run 'tcrm auth login' first."
        )

    async def get_instance_url(self, alias: str = "default") -> str:
        """
        Get instance URL for alias.

        Args:
            alias: Org alias

        Returns:
            Instance URL

        Raises:
            SFCLIAuthError: If no token available
        """
        token = await self.token_store.load_token(alias)
        if not token:
            raise SFCLIAuthError(f"No token for alias '{alias}'. Run 'tcrm auth login' first.")
        return token.instance_url

    async def get_username(self, alias: str = "default") -> str | None:
        """
        Get username for alias.

        Args:
            alias: Org alias

        Returns:
            Username if available
        """
        token = await self.token_store.load_token(alias)
        return token.username if token else None

    async def logout(self, alias: str = "default") -> bool:
        """
        Logout and remove stored auth.

        Args:
            alias: Org alias

        Returns:
            True if logged out, False if no token was stored
        """
        # Remove from SF CLI
        try:
            await self.sf_cli.logout(alias)
        except Exception as e:
            logger.warning("sf_cli_logout_failed", alias=alias, error=str(e))

        # Remove from token store
        return await self.token_store.delete_token(alias)

    async def status(self, alias: str = "default") -> dict[str, Any]:
        """
        Get authentication status.

        Args:
            alias: Org alias

        Returns:
            Status dictionary
        """
        token = await self.token_store.load_token(alias)

        if not token:
            return {
                "authenticated": False,
                "alias": alias,
                "message": "Not authenticated. Run 'tcrm auth login'.",
            }

        is_expired = token.is_expired()
        sf_cli_available = self.sf_cli.is_available()

        return {
            "authenticated": True,
            "alias": token.alias,
            "username": token.username,
            "instance_url": token.instance_url,
            "token_expired": is_expired,
            "expires_at": token.expires_at,
            "created_at": token.created_at,
            "updated_at": token.updated_at,
            "sf_cli_available": sf_cli_available,
            "message": "Token expired" if is_expired else "Authenticated",
        }

    async def list_orgs(self) -> list[dict[str, Any]]:
        """List all authorized orgs from SF CLI."""
        if not self.sf_cli.is_available():
            return []

        try:
            return self.sf_cli.list_orgs()
        except Exception as e:
            logger.error("list_orgs_failed", error=str(e))
            return []

    async def close(self) -> None:
        """Cleanup resources."""
        pass
