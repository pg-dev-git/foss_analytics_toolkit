"""Cryptography utilities with dynamic salt and keyring integration."""

import base64
import json
import os
from dataclasses import dataclass
from typing import Any

import keyring
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC


@dataclass
class EncryptedData:
    """Container for encrypted data with metadata."""
    ciphertext: str  # base64 encoded
    salt: str  # base64 encoded
    iterations: int
    algorithm: str = "PBKDF2-HMAC-SHA256"

    def to_json(self) -> str:
        """Serialize to JSON string."""
        return json.dumps({
            "ciphertext": self.ciphertext,
            "salt": self.salt,
            "iterations": self.iterations,
            "algorithm": self.algorithm,
        })

    @classmethod
    def from_json(cls, json_str: str) -> "EncryptedData":
        """Deserialize from JSON string."""
        data = json.loads(json_str)
        return cls(
            ciphertext=data["ciphertext"],
            salt=data["salt"],
            iterations=data["iterations"],
            algorithm=data.get("algorithm", "PBKDF2-HMAC-SHA256"),
        )


class CryptoManager:
    """Manages encryption/decryption with dynamic salts and keyring storage."""

    # Keyring service name
    KEYRING_SERVICE = "tcrm-toolkit"

    # PBKDF2 iterations (adjust based on security requirements)
    DEFAULT_ITERATIONS = 214322

    def __init__(self, master_key: str):
        """Initialize with master key (base64 encoded 32 bytes)."""
        self._master_key = base64.urlsafe_b64decode(master_key.encode())

    def _derive_key(self, salt: bytes, iterations: int | None = None) -> bytes:
        """Derive encryption key from master key and salt."""
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=salt,
            iterations=iterations or self.DEFAULT_ITERATIONS,
        )
        return base64.urlsafe_b64encode(kdf.derive(self._master_key))

    def encrypt(self, plaintext: str, iterations: int | None = None) -> EncryptedData:
        """Encrypt plaintext with a random salt."""
        # Generate random salt
        salt = os.urandom(16)

        # Derive key from salt
        key = self._derive_key(salt, iterations)

        # Encrypt with Fernet
        fernet = Fernet(key)
        ciphertext = fernet.encrypt(plaintext.encode())

        return EncryptedData(
            ciphertext=base64.urlsafe_b64encode(ciphertext).decode(),
            salt=base64.urlsafe_b64encode(salt).decode(),
            iterations=iterations or self.DEFAULT_ITERATIONS,
        )

    def decrypt(self, encrypted_data: EncryptedData) -> str:
        """Decrypt ciphertext using stored salt."""
        # Decode salt and ciphertext
        salt = base64.urlsafe_b64decode(encrypted_data.salt.encode())
        ciphertext = base64.urlsafe_b64decode(encrypted_data.ciphertext.encode())

        # Derive key from salt
        key = self._derive_key(salt, encrypted_data.iterations)

        # Decrypt with Fernet
        fernet = Fernet(key)
        plaintext = fernet.decrypt(ciphertext)

        return plaintext.decode()

    def encrypt_json(self, data: dict[str, Any], iterations: int | None = None) -> EncryptedData:
        """Encrypt a JSON-serializable dictionary."""
        return self.encrypt(json.dumps(data), iterations)

    def decrypt_json(self, encrypted_data: EncryptedData) -> dict[str, Any]:
        """Decrypt and parse JSON."""
        return json.loads(self.decrypt(encrypted_data))

    # Keyring integration for OAuth tokens
    def store_token(self, username: str, token_data: dict[str, Any]) -> None:
        """Store OAuth token data in system keyring."""
        encrypted = self.encrypt_json(token_data)
        keyring.set_password(
            self.KEYRING_SERVICE,
            f"token:{username}",
            encrypted.to_json(),
        )

    def retrieve_token(self, username: str) -> dict[str, Any] | None:
        """Retrieve OAuth token data from system keyring."""
        stored = keyring.get_password(self.KEYRING_SERVICE, f"token:{username}")
        if not stored:
            return None

        try:
            encrypted = EncryptedData.from_json(stored)
            return self.decrypt_json(encrypted)
        except Exception:
            # If decryption fails, remove corrupted entry
            keyring.delete_password(self.KEYRING_SERVICE, f"token:{username}")
            return None

    def delete_token(self, username: str) -> bool:
        """Delete stored token from keyring."""
        try:
            keyring.delete_password(self.KEYRING_SERVICE, f"token:{username}")
            return True
        except keyring.errors.PasswordDeleteError:
            return False

    def list_stored_tokens(self) -> list[str]:
        """List usernames with stored tokens."""
        # Note: keyring doesn't have a direct list method
        # This is a placeholder for future implementation
        return []


def create_crypto_manager() -> CryptoManager:
    """Factory function to create CryptoManager from settings."""
    from tcrm_toolkit.core.config import get_settings
    settings = get_settings()
    return CryptoManager(settings.encryption_key)