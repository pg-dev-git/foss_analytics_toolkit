"""Unit tests for crypto module."""

import base64
import pytest

from tcrm_toolkit.core.crypto import CryptoManager, EncryptedData


class TestCryptoManager:
    """Tests for CryptoManager."""

    def test_encrypt_decrypt_roundtrip(self):
        """Test that encrypt/decrypt works correctly."""
        crypto = CryptoManager("dGVzdC1tYXN0ZXJrZXktdGhhdC1pcy0zMi1ieXRlcw==")
        plaintext = "test secret data"

        encrypted = crypto.encrypt(plaintext)
        decrypted = crypto.decrypt(encrypted)

        assert decrypted == plaintext

    def test_encrypt_produces_different_ciphertext(self):
        """Test that encrypting same plaintext twice produces different ciphertext."""
        crypto = CryptoManager("dGVzdC1tYXN0ZXJrZXktdGhhdC1pcy0zMi1ieXRlcw==")
        plaintext = "test secret data"

        encrypted1 = crypto.encrypt(plaintext)
        encrypted2 = crypto.encrypt(plaintext)

        # Different salts should produce different ciphertext
        assert encrypted1.ciphertext != encrypted2.ciphertext
        assert encrypted1.salt != encrypted2.salt

    def test_decrypt_with_wrong_key_fails(self):
        """Test that decrypting with wrong key fails."""
        crypto1 = CryptoManager("dGVzdC1tYXN0ZXJrZXktdGhhdC1pcy0zMi1ieXRlcw==")
        crypto2 = CryptoManager("dGVzdC1tYXN0ZXJrZXktdGhhdC1pcy0zMi1ieXRlcg==")

        plaintext = "test secret data"
        encrypted = crypto1.encrypt(plaintext)

        with pytest.raises(Exception):
            crypto2.decrypt(encrypted)

    def test_encrypt_json_decrypt_json(self):
        """Test JSON encryption/decryption."""
        crypto = CryptoManager("dGVzdC1tYXN0ZXJrZXktdGhhdC1pcy0zMi1ieXRlcw==")
        data = {"key": "value", "number": 42, "nested": {"a": 1}}

        encrypted = crypto.encrypt_json(data)
        decrypted = crypto.decrypt_json(encrypted)

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
        from tcrm_toolkit.core.config import generate_encryption_key

        key = generate_encryption_key()
        decoded = base64.urlsafe_b64decode(key + "=" * (-len(key) % 4))
        assert len(decoded) == 32

    def test_generate_jwt_secret(self):
        """Test JWT secret generation."""
        from tcrm_toolkit.core.config import generate_jwt_secret

        secret = generate_jwt_secret()
        assert len(secret) >= 32