"""SF CLI-based authentication service."""

from typing import Any

import structlog

from asftool.core.auth.token_store import StoredToken, TokenStore
from asftool.core.config import Settings
from asftool.core.crypto import CryptoManager
from asftool.core.exceptions import TokenStoreUnavailableError
from asftool.core.sf_cli import SFCLIError, SFCLIManager, SFCLINotFoundError

logger = structlog.get_logger(__name__)


class SFCLIAuthError(Exception):
    """SF CLI authentication error."""
    pass


class SFCLIAuthService:
    """High-level SF CLI authentication service."""

    def __init__(
        self,
        settings: Settings,
        crypto_manager: CryptoManager | None,
        sf_cli_manager: SFCLIManager | None = None,
    ):
        """
        Initialize SF CLI auth service.

        Args:
            settings: Application settings
            crypto_manager: CryptoManager for token encryption. May be None
                when no encryption key is configured -- the service can then
                report status and list orgs, but not store a token.
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

            # Get the actual access token using 'sf org auth show-access-token -p'
            # The 'org display' command redacts the token for security.
            # Also get org info for instance_url, username, etc.
            # This step is MANDATORY - the login_web token is often redacted/invalid.
            actual_token = await self.sf_cli.get_access_token(alias=alias)

            # Get org info for metadata (instance_url, username, etc.).
            # If SF CLI rejects version format, fall back to auth_result.
            org_info = await self.sf_cli.get_org_info(alias=alias)
            # If version was rejected, org_info is partial; fall back to auth_result.
            if not org_info.instance_url:
                org_info = auth_result
            refreshed_token = StoredToken(
                access_token=actual_token,
                instance_url=org_info.instance_url or auth_result.instance_url,
                refresh_token=org_info.refresh_token,
                expires_at=org_info.expires_at.isoformat() if org_info.expires_at else None,
                alias=org_info.alias,
                username=org_info.username,
            )
            await self.token_store.save_token(refreshed_token)
            logger.info("sf_cli_login_token_refreshed", alias=alias)

            logger.info("sf_cli_login_success", alias=alias, username=auth_result.username)
            return actual_token

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
        Run full device login flow via SF CLI (for headless environments).

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

            # Get the actual access token using 'sf org auth show-access-token -p'
            # The 'org display' command redacts the token for security.
            # Also get org info for instance_url, username, etc.
            # This step is MANDATORY - the login_device token is often redacted/invalid.
            actual_token = await self.sf_cli.get_access_token(alias=alias)

            # Get org info for metadata (instance_url, username, etc.).
            # If SF CLI rejects version format, fall back to auth_result.
            org_info = await self.sf_cli.get_org_info(alias=alias)
            # If version was rejected, org_info is partial; fall back to auth_result.
            if not org_info.instance_url:
                org_info = auth_result
            refreshed_token = StoredToken(
                access_token=actual_token,
                instance_url=org_info.instance_url or auth_result.instance_url,
                refresh_token=org_info.refresh_token,
                expires_at=org_info.expires_at.isoformat() if org_info.expires_at else None,
                alias=org_info.alias,
                username=org_info.username,
            )
            await self.token_store.save_token(refreshed_token)
            logger.info("sf_cli_device_login_token_refreshed", alias=alias)

            logger.info("sf_cli_device_login_success", alias=alias, username=auth_result.username)
            return actual_token

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
            TokenRefreshError: If a refresh was attempted and failed. The
                failure is reported rather than silently starting an
                interactive browser login, which the user did not ask for
                and which cannot work on a headless host.
            SFCLIAuthError: If no valid token available
        """
        # Try to get valid token from store. A refresh failure propagates:
        # it is not the same as "there is no token".
        token = await self.token_store.get_valid_token(alias, self.sf_cli, auto_refresh)

        if token and not token.is_expired():
            return token.access_token

        # No token at all (or auto_refresh is off). Only now is an
        # interactive login appropriate.
        if auto_refresh:
            logger.info("attempting_re_login", alias=alias)
            return await self.login(alias=alias)

        raise SFCLIAuthError(
            f"No valid token for alias '{alias}'. Run 'asftool auth login' first."
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
            raise SFCLIAuthError(f"No token for alias '{alias}'. Run 'asftool auth login' first.")
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
        try:
            token = await self.token_store.load_token(alias)
        except TokenStoreUnavailableError as e:
            # A host with no keyring is not authenticated, but it is also
            # not broken -- report it plainly instead of raising a
            # traceback out of the first command a user runs.
            logger.warning("token_store_unavailable", alias=alias)
            return {
                "authenticated": False,
                "alias": alias,
                "token_store_available": False,
                "message": str(e),
            }

        if not token:
            return {
                "authenticated": False,
                "alias": alias,
                "token_store_available": True,
                "message": "Not authenticated. Run 'asftool auth login'.",
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
            "observed_at": token.observed_at,
            "sf_cli_available": sf_cli_available,
            "token_store_available": True,
            "message": (
                "Token expired (will auto-refresh on next use)"
                if is_expired
                else "Authenticated"
            ),
        }

    async def list_orgs(self) -> list[dict[str, Any]]:
        """List all authorized orgs from SF CLI.

        Raises:
            SFCLIError: If SF CLI is installed but its output cannot be
                parsed. Returning an empty list in that case made a parsing
                bug indistinguishable from "you have no orgs", which is how
                the org-discovery bug went unnoticed.
        """
        if not self.sf_cli.is_available():
            return []

        try:
            return self.sf_cli.list_orgs()
        except SFCLIError:
            logger.error("list_orgs_parse_failed")
            raise
        except Exception as e:
            logger.error("list_orgs_failed", error=str(e))
            raise SFCLIError(f"Could not list orgs via SF CLI: {e}") from e
