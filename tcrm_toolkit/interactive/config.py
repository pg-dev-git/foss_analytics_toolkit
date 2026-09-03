"""TUI Configuration settings."""

from __future__ import annotations

from pathlib import Path
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class TUIConfig(BaseSettings):
    """Configuration for Interactive TUI."""

    model_config = SettingsConfigDict(
        env_prefix="TCRM_TUI_",
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # Appearance
    theme: Literal["dark", "light", "auto"] = "dark"
    keybindings: Literal["vim", "standard"] = "standard"
    show_line_numbers: bool = False

    # Layout
    sidebar_width: int = 25
    detail_panel_width: int = 30
    status_bar_height: int = 1

    # Behavior
    confirm_destructive: bool = True
    auto_refresh_interval: int = 10  # seconds for job monitoring
    max_history_items: int = 100

    # Performance
    browser_page_size: int = 50
    search_debounce_ms: int = 300

    # Paths
    config_dir: Path = Field(default_factory=lambda: Path.home() / ".tcrm")
    history_file: Path = Field(default_factory=lambda: Path.home() / ".tcrm" / "history.json")

    def __init__(self, **values):
        super().__init__(**values)
        self.config_dir.mkdir(parents=True, exist_ok=True)

