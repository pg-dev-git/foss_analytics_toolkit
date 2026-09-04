"""Unit tests for crypto module."""

import base64
import pytest

from asftool.core.crypto import CryptoManager, EncryptedData
from asftool.core.config import Settings


@pytest.fixture
def test_settings():
    """Create test settings with valid keys."""
    encryption_key = base64.urlsafe_b64encode(b"x" * 32).decode()
    jwt_secret = base64.urlsafe_b64encode(b"y" * 32).decode()
    # Use model_construct to bypass .env file loading and validation
    return Settings.model_construct(
        encryption_key=encryption_key,
        jwt_secret_key=jwt_secret,
    )


@pytest.fixture
def crypto_manager(test_settings):
    """Create a crypto manager for testing."""
    return CryptoManager(test_settings.encryption_key)


class TestCryptoManager:
    """Tests for CryptoManager."""

    def test_encrypt_decrypt_roundtrip(self, crypto_manager):
        """Test that encrypt/decrypt works correctly."""
        plaintext = "test secret data"

        encrypted = crypto_manager.encrypt(plaintext)
        decrypted = crypto_manager.decrypt(encrypted)

        assert decrypted == plaintext

    def test_encrypt_produces_different_ciphertext(self, crypto_manager):
        """Test that encrypting same plaintext twice produces different ciphertext."""
        plaintext = "test secret data"

        encrypted1 = crypto_manager.encrypt(plaintext)
        encrypted2 = crypto_manager.encrypt(plaintext)

        # Different salts should produce different ciphertext
        assert encrypted1.ciphertext != encrypted2.ciphertext
        assert encrypted1.salt != encrypted2.salt

    def test_decrypt_with_wrong_key_fails(self, test_settings):
        """Test that decrypting with wrong key fails."""
        crypto1 = CryptoManager(test_settings.encryption_key)
        crypto2 = CryptoManager(base64.urlsafe_b64encode(b"z" * 32).decode())

        plaintext = "test secret data"
        encrypted = crypto1.encrypt(plaintext)

        with pytest.raises(Exception):
            crypto2.decrypt(encrypted)

    def test_encrypt_json_decrypt_json(self, crypto_manager):
        """Test JSON encryption/decryption."""
        data = {"key": "value", "number": 42, "nested": {"a": 1}}

        encrypted = crypto_manager.encrypt_json(data)
        decrypted = crypto_manager.decrypt_json(encrypted)

        assert decrypted == data

    def test_encrypted_data_serialization(self):
        """Test EncryptedData JSON serialization."""
        encrypted = EncryptedData(
            ciphertext="dGVzdA==",
            salt="c2FsdA==",
            iterations=100000,
        )

        json_str = encrypted.to_json()
        restored = EncryptedData.from_json(json_str)

        assert restored.ciphertext == encrypted.ciphertext
        assert restored.salt == encrypted.salt
        assert restored.iterations == encrypted.iterations

    def test_generate_encryption_key(self):
        """Test encryption key generation."""
        from asftool.core.config import generate_encryption_key

        key = generate_encryption_key()
        decoded = base64.urlsafe_b64decode(key + "=" * (-len(key) % 4))
        assert len(decoded) == 32

    def test_generate_jwt_secret(self):
        """Test JWT secret generation."""
        from asftool.core.config import generate_jwt_secret

        secret = generate_jwt_secret()
        assert len(secret) >= 32