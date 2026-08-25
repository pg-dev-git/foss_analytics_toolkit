"""Secure token storage with encryption and keyring integration."""

import json
import keyring
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from typing import Any, Optional

import structlog

from tcrm_toolkit.core.crypto import CryptoManager, EncryptedData

logger = structlog.get_logger(__name__)


@dataclass
class StoredToken:
    """Stored token data with metadata."""
    access_token: str
    instance_url: str
    refresh_token: Optional[str] = None
    expires_at: Optional[str] = None  # ISO format string
    alias: str = "default"
    username: Optional[str] = None
    created_at: str = ""  # ISO format string
    updated_at: str = ""  # ISO format string

    def __post_init__(self):
        now = datetime.utcnow().isoformat()
        if not self.created_at:
            self.created_at = now
        if not self.updated_at:
            self.updated_at = now

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "StoredToken":
        """Create from dictionary."""
        return cls(**data)

    def is_expired(self, buffer_seconds: int = 60) -> bool:
        """Check if token is expired (with buffer)."""
        if not self.expires_at:
            return True
        try:
            expires = datetime.fromisoformat(self.expires_at.replace("Z", "+00:00"))
            return datetime.utcnow() >= (expires - timedelta(seconds=buffer_seconds))
        except (ValueError, TypeError):
            return True

    def update_timestamp(self) -> None:
        """Update the updated_at timestamp."""
        self.updated_at = datetime.utcnow().isoformat()


class TokenStore:
    """Secure token storage with encryption and keyring integration."""

    KEYRING_SERVICE = "tcrm-toolkit-sfcli"

    def __init__(self, crypto_manager: CryptoManager):
        """
        Initialize token store.

        Args:
            crypto_manager: CryptoManager instance for encryption
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
        """
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

    async def load_token(self, alias: str = "default") -> Optional[StoredToken]:
        """
        Load and decrypt token from keyring.

        Args:
            alias: Org alias

        Returns:
            StoredToken if found, None otherwise
        """
        stored = keyring.get_password(self.KEYRING_SERVICE, self._get_keyring_key(alias))
        if not stored:
            return None

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
            True if deleted, False if not found
        """
        try:
            keyring.delete_password(self.KEYRING_SERVICE, self._get_keyring_key(alias))
            logger.info("token_deleted", alias=alias)
            return True
        except keyring.errors.PasswordDeleteError:
            return False

    async def get_valid_token(
        self,
        alias: str,
        sf_cli_manager: "SFCLIManager",
        auto_refresh: bool = True,
    ) -> Optional[StoredToken]:
        """
        Get valid token, auto-refresh if needed.

        Args:
            alias: Org alias
            sf_cli_manager: SFCLIManager instance for refresh
            auto_refresh: Whether to attempt auto-refresh

        Returns:
            Valid StoredToken or None if not available
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
            return None

    async def list_aliases(self) -> list[str]:
        """List all stored aliases (limited by keyring capabilities)."""
        # Note: keyring doesn't have a direct list method
        # This is a placeholder for future implementation
        return []