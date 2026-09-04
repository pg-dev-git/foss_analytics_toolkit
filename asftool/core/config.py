"""Configuration management using Pydantic Settings."""

import base64
import os
from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
        populate_by_name=True,
    )

    # Application Settings
    app_name: str = "asftool"
    app_version: str = "0.1.0"
    debug: bool = False
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"

    # Salesforce API Settings
    sf_api_version: str = Field(default="v60.0", alias="SF_API_VERSION")
    sf_default_domain: str = Field(default="login.salesforce.com", alias="SF_DEFAULT_DOMAIN")

    # Encryption Settings
    encryption_key: str = Field(alias="ENCRYPTION_KEY")

    # JWT Settings
    jwt_secret_key: str = Field(alias="JWT_SECRET_KEY")
    jwt_algorithm: str = Field(default="HS256", alias="JWT_ALGORITHM")
    access_token_expire_minutes: int = Field(default=30, alias="ACCESS_TOKEN_EXPIRE_MINUTES")
    refresh_token_expire_days: int = Field(default=30, alias="REFRESH_TOKEN_EXPIRE_DAYS")

    # Connected App Credentials (JWT Bearer flow)
    sf_connected_app_client_id: str | None = Field(default=None, alias="SF_CONNECTED_APP_CLIENT_ID")
    sf_connected_app_client_secret: str | None = Field(default=None, alias="SF_CONNECTED_APP_CLIENT_SECRET")
    sf_connected_app_username: str | None = Field(default=None, alias="SF_CONNECTED_APP_USERNAME")

    # Web OAuth Settings (PKCE flow)
    sf_web_oauth_client_id: str | None = Field(default=None, alias="SF_WEB_OAUTH_CLIENT_ID")
    sf_web_oauth_client_secret: str | None = Field(default=None, alias="SF_WEB_OAUTH_CLIENT_SECRET")
    sf_web_oauth_redirect_uri: str = Field(default="http://localhost:8080/callback", alias="SF_WEB_OAUTH_REDIRECT_URI")

    # Device Flow Settings
    sf_device_flow_client_id: str | None = Field(default=None, alias="SF_DEVICE_FLOW_CLIENT_ID")

    @field_validator("encryption_key")
    @classmethod
    def validate_encryption_key(cls, v: str) -> str:
        """Validate that encryption key is a valid base64-encoded 32-byte key."""
        try:
            decoded = base64.urlsafe_b64decode(v + "=" * (-len(v) % 4))
            if len(decoded) != 32:
                raise ValueError("Encryption key must decode to exactly 32 bytes")
        except Exception as e:
            raise ValueError(f"Invalid encryption key: {e}") from e
        return v

    @field_validator("jwt_secret_key")
    @classmethod
    def validate_jwt_secret(cls, v: str) -> str:
        """Validate JWT secret key length."""
        if len(v) < 32:
            raise ValueError("JWT secret key must be at least 32 characters")
        return v

    @property
    def sf_base_url(self) -> str:
        """Get the base Salesforce API URL."""
        return f"https://{self.sf_default_domain}/services/data/{self.sf_api_version}"

    @property
    def wave_base_url(self) -> str:
        """Get the Wave/Analytics API base URL."""
        return f"{self.sf_base_url}/wave"

    @property
    def has_connected_app_credentials(self) -> bool:
        """Check if Connected App credentials are configured."""
        return all([
            self.sf_connected_app_client_id,
            self.sf_connected_app_client_secret,
            self.sf_connected_app_username,
        ])

    @property
    def has_web_oauth_credentials(self) -> bool:
        """Check if Web OAuth credentials are configured."""
        return all([
            self.sf_web_oauth_client_id,
            self.sf_web_oauth_client_secret,
        ])

    @property
    def has_device_flow_credentials(self) -> bool:
        """Check if Device Flow credentials are configured."""
        return self.sf_device_flow_client_id is not None

    @property
    def config_dir(self) -> Path:
        """Get the user config directory (`~/.asftool/`)."""
        return Path.home() / ".asftool"

    @property
    def log_file(self) -> Path:
        """Get the log file path (`~/.asftool/asftool.log`)."""
        return self.config_dir / "asftool.log"


@lru_cache
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()


def generate_encryption_key() -> str:
    """Generate a new secure encryption key."""
    return base64.urlsafe_b64encode(os.urandom(32)).decode()


def generate_jwt_secret() -> str:
    """Generate a new JWT secret key."""
    return base64.urlsafe_b64encode(os.urandom(32)).decode()
