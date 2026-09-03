"""Window state and UI preference persistence."""

import json
from pathlib import Path
from typing import Any


class WindowManager:
    """Manages window state, column widths, and UI preferences."""

    def __init__(self, config_dir: Path | None = None):
        self.config_dir = config_dir or (Path.home() / ".tcrm")
        self.state_file = self.config_dir / "window_state.json"
        self._state: dict[str, Any] = {}
        self.load()

    def load(self) -> dict[str, Any]:
        """Load window state from disk."""
        self.config_dir.mkdir(parents=True, exist_ok=True)
        if self.state_file.exists():
            try:
                self._state = json.loads(self.state_file.read_text(encoding="utf-8"))
            except Exception:
                self._state = {}
        else:
            self._state = {}
        return self._state

    def save(self) -> None:
        """Save window state to disk."""
        self.config_dir.mkdir(parents=True, exist_ok=True)
        self.state_file.write_text(json.dumps(self._state, indent=2), encoding="utf-8")

    def get(self, key: str, default: Any = None) -> Any:
        """Get state value."""
        return self._state.get(key, default)

    def set(self, key: str, value: Any) -> None:
        """Set state value and save."""
        self._state[key] = value
        self.save()
