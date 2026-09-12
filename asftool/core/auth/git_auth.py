"""Git authentication service with keyring storage and secret masking.

Provides secure storage and retrieval of Git credentials using OS keyring
with dynamic PBKDF2 encryption via CryptoManager.
"""

import json
import os
from typing import Any, Optional

import keyring
from pydantic import SecretStr

from asftool.core.crypto import CryptoManager, create_crypto_manager
from asftool.core.models.git_auth import (
    GitAuthType,
    GitCredentialSummary,
    GitCredentials,
    GitProvider,
    SSHKeyFormat,
)


class GitAuthError(Exception):
    """Base exception for Git authentication errors."""

    pass


class GitCredentialsNotFound(GitAuthError):
    """Raised when credentials are not found."""

    pass


class GitAuthService:
    """Service for managing Git credentials with encrypted keyring storage.

    Features:
    - Multi-provider support (GitHub, GitLab, Bitbucket, Azure DevOps)
    - Encrypted storage via OS keyring with PBKDF2
    - Environment variable fallbacks
    - Secret masking in logs/output
    - Credential validation
    """

    KEYRING_SERVICE = "asftool-git"
    KEY_PREFIX = "git-creds:"

    def __init__(self, crypto_manager: Optional[CryptoManager] = None):
        """Initialize with optional crypto manager (uses default if None)."""
        self._crypto = crypto_manager or create_crypto_manager()

    def _keyring_key(self, alias: str) -> str:
        """Generate keyring key for alias."""
        return f"{self.KEY_PREFIX}{alias}"

    def _mask_secret(self, secret: Optional[SecretStr]) -> str:
        """Mask secret for logging/display."""
        if secret is None:
            return "***"
        value = secret.get_secret_value()
        if len(value) <= 8:
            return "****"
        return f"{value[:4]}****{value[-4:]}"

    def _credentials_to_dict(self, creds: GitCredentials) -> dict[str, Any]:
        """Convert credentials to storable dict (secrets as plain strings for encryption)."""
        data = creds.model_dump(mode="json", exclude={"_env_token", "_env_username", "_env_ssh_key"})
        # Convert SecretStr to plain strings for encryption
        if data.get("token"):
            data["token"] = creds.token.get_secret_value()
        if data.get("ssh_private_key"):
            data["ssh_private_key"] = creds.ssh_private_key.get_secret_value()
        if data.get("ssh_passphrase"):
            data["ssh_passphrase"] = creds.ssh_passphrase.get_secret_value()
        return data

    def _dict_to_credentials(self, data: dict[str, Any]) -> GitCredentials:
        """Convert stored dict back to GitCredentials with SecretStr."""
        # Convert plain strings back to SecretStr
        if data.get("token"):
            data["token"] = SecretStr(data["token"])
        if data.get("ssh_private_key"):
            data["ssh_private_key"] = SecretStr(data["ssh_private_key"])
        if data.get("ssh_passphrase"):
            data["ssh_passphrase"] = SecretStr(data["ssh_passphrase"])
        return GitCredentials(**data)

    def store_credentials(self, credentials: GitCredentials) -> None:
        """Store Git credentials in encrypted keyring.

        Args:
            credentials: GitCredentials to store

        Raises:
            GitAuthError: If storage fails
        """
        try:
            data = self._credentials_to_dict(credentials)
            encrypted = self._crypto.encrypt_json(data)
            keyring.set_password(
                self.KEYRING_SERVICE,
                self._keyring_key(credentials.alias),
                encrypted.to_json(),
            )
        except Exception as e:
            raise GitAuthError(f"Failed to store credentials: {e}") from e

    def retrieve_credentials(self, alias: str) -> GitCredentials:
        """Retrieve Git credentials from keyring.

        Args:
            alias: Credential alias

        Returns:
            GitCredentials object

        Raises:
            GitCredentialsNotFound: If credentials not found
            GitAuthError: If retrieval/decryption fails
        """
        try:
            stored = keyring.get_password(self.KEYRING_SERVICE, self._keyring_key(alias))
            if not stored:
                raise GitCredentialsNotFound(f"Credentials not found for alias: {alias}")

            from asftool.core.crypto import EncryptedData

            encrypted = EncryptedData.from_json(stored)
            data = self._crypto.decrypt_json(encrypted)
            return self._dict_to_credentials(data)
        except GitCredentialsNotFound:
            raise
        except Exception as e:
            raise GitAuthError(f"Failed to retrieve credentials: {e}") from e

    def delete_credentials(self, alias: str) -> bool:
        """Delete stored credentials from keyring.

        Args:
            alias: Credential alias

        Returns:
            True if deleted, False if not found
        """
        try:
            keyring.delete_password(self.KEYRING_SERVICE, self._keyring_key(alias))
            return True
        except keyring.errors.PasswordDeleteError:
            return False
        except Exception as e:
            raise GitAuthError(f"Failed to delete credentials: {e}") from e

    def list_credentials(self) -> list[GitCredentialSummary]:
        """List all stored credential summaries (no secrets).

        Note: keyring doesn't support listing directly, so we track
        known aliases in a separate index.
        """
        # We maintain an index of known aliases
        index_key = f"{self.KEY_PREFIX}__index__"
        try:
            stored = keyring.get_password(self.KEYRING_SERVICE, index_key)
            if not stored:
                return []
            aliases = json.loads(stored)
        except Exception:
            return []

        summaries = []
        for alias in aliases:
            try:
                creds = self.retrieve_credentials(alias)
                summaries.append(GitCredentialSummary.from_credentials(creds))
            except Exception:
                # Skip corrupted entries
                continue
        return summaries

    def _update_index(self, alias: str, add: bool = True) -> None:
        """Update the alias index in keyring."""
        index_key = f"{self.KEY_PREFIX}__index__"
        try:
            stored = keyring.get_password(self.KEYRING_SERVICE, index_key)
            aliases = json.loads(stored) if stored else []
        except Exception:
            aliases = []

        if add and alias not in aliases:
            aliases.append(alias)
        elif not add and alias in aliases:
            aliases.remove(alias)

        try:
            keyring.set_password(self.KEYRING_SERVICE, index_key, json.dumps(aliases))
        except Exception:
            # Index is best-effort
            pass

    def store_credentials_with_index(self, credentials: GitCredentials) -> None:
        """Store credentials and update index."""
        self.store_credentials(credentials)
        self._update_index(credentials.alias, add=True)

    def delete_credentials_with_index(self, alias: str) -> bool:
        """Delete credentials and update index."""
        result = self.delete_credentials(alias)
        if result:
            self._update_index(alias, add=False)
        return result

    def validate_credentials(self, credentials: GitCredentials) -> tuple[bool, list[str]]:
        """Validate credentials for completeness and basic format.

        Args:
            credentials: Credentials to validate

        Returns:
            Tuple of (is_valid, list_of_errors)
        """
        errors = []

        # Provider-specific validation
        if credentials.provider == GitProvider.AZURE_DEVOPS:
            if not credentials.project:
                errors.append("Azure DevOps requires 'project' field")
            if not credentials.organization:
                errors.append("Azure DevOps requires 'organization' field")

        if credentials.provider == GitProvider.GITLAB and credentials.auth_type == GitAuthType.PROJECT_TOKEN:
            if not credentials.project:
                errors.append("GitLab project token requires 'project' field")

        # Auth type validation
        if credentials.auth_type == GitAuthType.SSH_KEY:
            if not credentials.ssh_private_key:
                errors.append("SSH auth requires 'ssh_private_key'")
        else:
            if not credentials.token:
                # Check env fallback
                if not os.environ.get("ASFTOOL_GIT_TOKEN"):
                    errors.append(f"{credentials.auth_type.value} auth requires 'token'")

        # Host validation
        if not credentials.host:
            errors.append("Host is required")

        return len(errors) == 0, errors

    def get_connection_config(self, alias: str) -> dict:
        """Get connection config for Git client libraries.

        Args:
            alias: Credential alias

        Returns:
            Dict suitable for GitPython, dulwich, or httpx
        """
        creds = self.retrieve_credentials(alias)
        return creds.to_connection_config()

    def create_credentials_interactive(
        self,
        alias: str,
        provider: GitProvider,
        host: str,
        auth_type: GitAuthType = GitAuthType.PAT,
    ) -> GitCredentials:
        """Create credentials interactively (for CLI use).

        This is a helper that prompts for required fields.
        Actual prompting is done in CLI layer.
        """
        # This is a placeholder - CLI will collect inputs
        # and call store_credentials_with_index directly
        raise NotImplementedError("Use CLI for interactive creation")


def get_git_auth_service() -> GitAuthService:
    """Factory function to get GitAuthService instance."""
    return GitAuthService()