"""Configuration persistence for TUI settings."""

import json
from pathlib import Path
from typing import Any

from tcrm_toolkit.interactive.config import TUIConfig


class ConfigManager:
    """Manages persistent TUI configuration."""

    def __init__(self, config_dir: Path | None = None):
        self.config_dir = config_dir or (Path.home() / ".tcrm")
        self.config_file = self.config_dir / "config.json"
        self._config: TUIConfig | None = None

    def load(self) -> TUIConfig:
        """Load configuration from disk or create default."""
        if self._config is not None:
            return self._config

        self.config_dir.mkdir(parents=True, exist_ok=True)
        if self.config_file.exists():
            try:
                data = json.loads(self.config_file.read_text(encoding="utf-8"))
                # Convert path strings back if present
                if "config_dir" in data:
                    data["config_dir"] = Path(data["config_dir"])
                if "history_file" in data:
                    data["history_file"] = Path(data["history_file"])
                self._config = TUIConfig(**data)
            except Exception:
                self._config = TUIConfig()
        else:
            self._config = TUIConfig()
            self.save(self._config)

        return self._config

    def save(self, config: TUIConfig) -> None:
        """Save configuration to disk."""
        self._config = config
        self.config_dir.mkdir(parents=True, exist_ok=True)
        data = config.model_dump(mode="json")
        self.config_file.write_text(json.dumps(data, indent=2), encoding="utf-8")
