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

    # Salesforce API Settings.
    # Default = current latest stable (Winter '27 = v68.0). Override via
    # SF_API_VERSION env var, ~/.asftool/config.json (Phase 2), or
    # --api-version CLI flag.
    sf_api_version: str = Field(default="v68.0", alias="SF_API_VERSION")
    sf_default_domain: str = Field(default="login.salesforce.com", alias="SF_DEFAULT_DOMAIN")

    # Encryption Settings.
    # Optional so the tool can start on a machine with no .env file; the
    # preflight in cli/main.py tells the user what to do instead of the
    # process dying with a pydantic traceback.
    encryption_key: str | None = Field(default=None, alias="ENCRYPTION_KEY")

    # NOTE: the JWT-bearer (Connected App), Web OAuth (PKCE) and Device Flow
    # settings that used to live here were never read by any code path --
    # the only implemented authentication is the SF CLI. They were removed
    # rather than left as dead configuration. See CHANGELOG.md.

    # Field Impact Analysis Settings
    field_impact_default_fuzzy_threshold: int = Field(
        default=85, alias="FIELD_IMPACT_DEFAULT_FUZZY_THRESHOLD", ge=0, le=100
    )
    field_impact_max_concurrent_scans: int = Field(
        default=10, alias="FIELD_IMPACT_MAX_CONCURRENT_SCANS", ge=1, le=50
    )
    field_impact_default_match_mode: Literal["exact", "fuzzy", "both"] = Field(
        default="both", alias="FIELD_IMPACT_DEFAULT_MATCH_MODE"
    )

    # SF CLI Auto-Import Settings
    auto_import_sf_cli_session: bool = Field(default=False, alias="AUTO_IMPORT_SF_CLI_SESSION")

    @field_validator("sf_api_version")
    @classmethod
    def validate_sf_api_version(cls, v: str) -> str:
        """Validate Salesforce API version format (v<major>.<minor>)."""
        import re
        if not re.match(r"^v\d+\.\d+$", v):
            raise ValueError(
                f"Invalid SF_API_VERSION {v!r}: must match v<major>.<minor> "
                f"(e.g. 'v60.0', 'v68.0')"
            )
        return v

    @field_validator("encryption_key")
    @classmethod
    def validate_encryption_key(cls, v: str | None) -> str | None:
        """Validate that encryption key is a valid base64-encoded 32-byte key."""
        if v is None:
            return None
        try:
            decoded = base64.urlsafe_b64decode(v + "=" * (-len(v) % 4))
            if len(decoded) != 32:
                raise ValueError("Encryption key must decode to exactly 32 bytes")
        except Exception as e:
            raise ValueError(f"Invalid encryption key: {e}") from e
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
    def config_dir(self) -> Path:
        """Get the user config directory (`~/.asftool/`)."""
        return Path.home() / ".asftool"

    @property
    def log_file(self) -> Path:
        """Get the log file path (`~/.asftool/asftool.log`)."""
        return self.config_dir / "asftool.log"


@lru_cache
def get_settings() -> Settings:
    """Get cached settings instance.

    Precedence (highest first):
      1. Environment variable (SF_API_VERSION, ENCRYPTION_KEY, ...)
      2. Persisted user config at ~/.asftool/config.json (sf_api_version only)
      3. Hardcoded default

    The persisted config is applied AFTER pydantic-settings has read
    the env vars, so env vars always win.
    """
    from asftool.core.config_store import get_user_config  # avoid circular

    settings = Settings()  # type: ignore[call-arg]
    settings.sf_api_version = _resolve_sf_api_version(
        from_settings=settings.sf_api_version,
        from_user_config=get_user_config().sf_api_version,
    )
    return settings


def _sf_api_version_from_env_sources() -> str | None:
    """Check if SF_API_VERSION is set in os.environ OR the project .env file.

    Returns the value if found, None if neither source provides it.
    pydantic-settings consumes these sources when constructing Settings,
    but doesn't tell us *which* one it picked. For the merge precedence
    we just need to know if any of them had a value.
    """
    # 1) Real env var.
    if "SF_API_VERSION" in os.environ:
        return os.environ["SF_API_VERSION"]
    # 2) Project .env file. pydantic-settings already loaded it; we re-read
    #    only to check for the key. This is a one-line parse, not a re-load.
    env_file = Path(".env")
    if env_file.is_file():
        for line in env_file.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, _, v = line.partition("=")
            if k.strip() == "SF_API_VERSION":
                return v.strip()
    return None


def _resolve_sf_api_version(
    from_settings: str | None,
    from_user_config: str | None,
) -> str:
    """Apply the merge rule: env > persisted > default.

    Args:
        from_settings: The value pydantic-settings resolved (from .env
            or os.environ). May equal the hardcoded default if neither
            source provided a value.
        from_user_config: The value from ~/.asftool/config.json, or None.

    The rule: if any env-level source provided a value, that wins.
    Otherwise, the persisted config wins. Otherwise, the default.
    """
    if _sf_api_version_from_env_sources() is not None:
        return from_settings or "v68.0"
    if from_user_config:
        return from_user_config
    return "v68.0"


def generate_encryption_key() -> str:
    """Generate a new secure encryption key."""
    return base64.urlsafe_b64encode(os.urandom(32)).decode()
