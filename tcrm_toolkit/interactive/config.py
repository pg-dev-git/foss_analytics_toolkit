"""TUI Configuration settings (Placeholder for Phase 4)."""

from pydantic_settings import BaseSettings, SettingsConfigDict


class TUIConfig(BaseSettings):
    """TUI-specific configuration."""
    model_config = SettingsConfigDict(env_prefix="TCRM_TUI_")
    theme: str = "default"
    refresh_interval: int = 5
