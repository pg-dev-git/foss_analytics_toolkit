"""Secure token storage with encryption and keyring integration."""

import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import keyring
import structlog

from asftool.core.crypto import CryptoManager, EncryptedData
from asftool.core.exceptions import (
    CryptoError,
    TokenRefreshError,
    TokenStoreUnavailableError,
)
from asftool.core.sf_cli import SFCLIManager

logger = structlog.get_logger(__name__)

#: How long a token without an explicit expiry is trusted after it was last
#: observed. The SF CLI does not report a token expiry, so this bounds how
#: long an unverified token is used before it is refreshed.
MAX_TOKEN_AGE = 3600  # seconds

#: Actionable message for a host with no OS keyring backend.
_NO_KEYRING_MESSAGE = (
    "No OS keyring backend is available on this machine. "
    "Install one with: pip install 'keyrings.alt' "
    "(or: pip install 'asftool[headless]'), "
    "or use a container that provides a keyring."
)


@dataclass
class StoredToken:
    """Stored token data with metadata."""
    access_token: str
    instance_url: str
    refresh_token: str | None = None
    expires_at: str | None = None  # ISO format string
    alias: str = "default"
    username: str | None = None
    api_version: str | None = None  # API version for this org (e.g., "67.0")
    created_at: str = ""  # ISO format string
    updated_at: str = ""  # ISO format string
    observed_at: str | None = None  # ISO format string

    def __post_init__(self):
        now = datetime.now(UTC).isoformat()
        if not self.created_at:
            self.created_at = now
        if not self.updated_at:
            self.updated_at = now
        if not self.observed_at:
            self.observed_at = now

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "StoredToken":
        """Create from dictionary."""
        return cls(**data)

    def is_expired(self, buffer_seconds: int = 60) -> bool:
        """Check if the token is expired (with buffer).

        The SF CLI does not report a token expiry -- `sf org display` has
        no such field, and the access token is opaque rather than a
        decodable JWT. So there is no authoritative expiry to read.

        Two signals are used instead:

        * ``expires_at``, when the platform did provide one.
        * ``observed_at`` plus :data:`MAX_TOKEN_AGE`, as a bounded fallback
          for the case where no expiry is available. Salesforce session
          tokens are valid for a limited time, so a token we have not
          refreshed within that window is treated as expired rather than
          trusted forever.

        Returns:
            True if the token should be considered expired.
        """
        if self.expires_at:
            try:
                expires = datetime.fromisoformat(self.expires_at.replace("Z", "+00:00"))
                now = datetime.now(UTC) if expires.tzinfo is not None else datetime.now()
                return now >= (expires - timedelta(seconds=buffer_seconds))
            except (ValueError, TypeError):
                pass

        if self.observed_at:
            try:
                observed = datetime.fromisoformat(self.observed_at.replace("Z", "+00:00"))
                now = datetime.now(UTC) if observed.tzinfo is not None else datetime.now()
                return now >= observed + timedelta(seconds=MAX_TOKEN_AGE)
            except (ValueError, TypeError):
                return False

        return False

    def update_timestamp(self) -> None:
        """Update the updated_at timestamp."""
        self.updated_at = datetime.now(UTC).isoformat()


class TokenStore:
    """Secure token storage with encryption and keyring integration."""

    KEYRING_SERVICE = "asftool-sfcli"

    def __init__(self, crypto_manager: CryptoManager | None):
        """
        Initialize token store.

        Args:
            crypto_manager: CryptoManager instance for encryption. May be
                None when no encryption key is configured; saving a token
                then raises rather than storing it unprotected.
        """
        self.crypto = crypto_manager

    def _get_keyring_key(self, alias: str) -> str:
        """Get keyring key for alias."""
        return f"sfcli_token:{alias}"

    async def save_token(self, token: StoredToken) -> None:
        """
        Encrypt and store token in keyring.

        Args:
            token: StoredToken to save

        Raises:
            CryptoError: If no encryption key is configured. Storing a token
                unprotected would defeat the point of the keyring.
        """
        if self.crypto is None:
            raise CryptoError(
                "Cannot store a token: no ENCRYPTION_KEY is configured. "
                "Run 'asftool init' or copy .env.example to .env."
            )

        token.update_timestamp()

        # Serialize to JSON
        json_data = json.dumps(token.to_dict())

        # Encrypt
        encrypted = self.crypto.encrypt(json_data)

        # Store in keyring
        keyring.set_password(
            self.KEYRING_SERVICE,
            self._get_keyring_key(token.alias),
            encrypted.to_json(),
        )

        logger.info("token_saved", alias=token.alias, username=token.username)

    async def load_token(self, alias: str = "default") -> StoredToken | None:
        """
        Load and decrypt token from keyring.

        Args:
            alias: Org alias

        Returns:
            StoredToken if found, None otherwise.

        Raises:
            TokenStoreUnavailableError: If the host has no keyring backend.
        """
        try:
            stored = keyring.get_password(
                self.KEYRING_SERVICE, self._get_keyring_key(alias)
            )
        except keyring.errors.NoKeyringError as e:
            logger.error("keyring_unavailable", alias=alias)
            raise TokenStoreUnavailableError(_NO_KEYRING_MESSAGE) from e

        if not stored:
            return None

        if self.crypto is None:
            raise CryptoError(
                "Cannot decrypt the stored token: no ENCRYPTION_KEY is configured."
            )

        try:
            encrypted = EncryptedData.from_json(stored)
            json_data = self.crypto.decrypt(encrypted)
            data = json.loads(json_data)
            return StoredToken.from_dict(data)
        except Exception as e:
            logger.error("token_load_failed", alias=alias, error=str(e))
            # If decryption fails, remove corrupted entry
            await self.delete_token(alias)
            return None

    async def delete_token(self, alias: str = "default") -> bool:
        """
        Delete stored token from keyring.

        Args:
            alias: Org alias

        Returns:
            True if deleted, False if not found.

        Raises:
            TokenStoreUnavailableError: If the host has no keyring backend.
        """
        try:
            keyring.delete_password(self.KEYRING_SERVICE, self._get_keyring_key(alias))
            logger.info("token_deleted", alias=alias)
            return True
        except keyring.errors.NoKeyringError as e:
            logger.error("keyring_unavailable", alias=alias)
            raise TokenStoreUnavailableError(_NO_KEYRING_MESSAGE) from e
        except keyring.errors.PasswordDeleteError:
            # The entry does not exist.
            return False

    async def get_valid_token(
        self,
        alias: str,
        sf_cli_manager: SFCLIManager,
        auto_refresh: bool = True,
    ) -> StoredToken | None:
        """
        Get valid token, auto-refresh if needed.

        Args:
            alias: Org alias
            sf_cli_manager: SFCLIManager instance for refresh
            auto_refresh: Whether to attempt auto-refresh

        Returns:
            Valid StoredToken, or None when there is no token stored at all
            (or auto-refresh was disabled).

        Raises:
            TokenRefreshError: If a refresh was attempted and failed. This
                is deliberately distinct from returning None: a failed
                refresh must not be mistaken for "no token", because the
                caller would then start an interactive browser login that
                the user never asked for.
            TokenStoreUnavailableError: If the host has no keyring backend.
        """
        token = await self.load_token(alias)
        if not token:
            return None

        if not token.is_expired():
            return token

        logger.info("token_expired_attempting_refresh", alias=alias)

        if not auto_refresh:
            return None

        # Try to refresh via SF CLI
        try:
            auth_result = await sf_cli_manager.refresh_token(alias)
            new_token = StoredToken(
                access_token=auth_result.access_token,
                instance_url=auth_result.instance_url,
                refresh_token=auth_result.refresh_token,
                expires_at=auth_result.expires_at.isoformat() if auth_result.expires_at else None,
                alias=auth_result.alias,
                username=auth_result.username,
            )
            await self.save_token(new_token)
            return new_token
        except Exception as e:
            logger.error("token_refresh_failed", alias=alias, error=str(e))
            raise TokenRefreshError(
                f"Failed to refresh the token for alias '{alias}': {e}. "
                "Run 'asftool auth login' to re-authenticate."
            ) from e

    async def list_aliases(self) -> list[str]:
        """List all stored aliases (limited by keyring capabilities)."""
        # Note: keyring doesn't have a direct list method
        # This is a placeholder for future implementation
        return []
