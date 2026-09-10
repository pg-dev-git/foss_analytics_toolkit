"""Persistent user configuration store.

Stores small, user-tweakable settings (e.g. preferred API version) at
``~/.asftool/config.json`` so they survive across sessions without
needing environment variables.

This is the *user preference* layer; environment variables and CLI
flags always take precedence (see :func:`get_user_config` and
:func:`get_settings` in ``core/config.py`` for the merge logic).
"""

import json
import os
import re
import tempfile
from pathlib import Path

import structlog
from pydantic import BaseModel, ConfigDict, Field, field_validator

logger = structlog.get_logger(__name__)


class UserConfig(BaseModel):
    """User-tweakable settings persisted to ~/.asftool/config.json.

    ``None`` means "no preference set" — fall through to env var or
    hardcoded default. We do NOT mirror the full Settings model here
    on purpose: only values the user actually changes.
    """

    sf_api_version: str | None = Field(
        default=None,
        description="Preferred Salesforce API version (e.g. 'v68.0').",
    )

    model_config = ConfigDict(extra="ignore")

    @field_validator("sf_api_version")
    @classmethod
    def _validate_version(cls, v: str | None) -> str | None:
        """Reject anything that isn't a v<major>.<minor> string.

        Same rule as ``Settings.sf_api_version``. Returning ``None`` short-circuits
        the regex check so the field can stay unset.
        """
        if v is None:
            return v
        if not re.match(r"^v\d+\.\d+$", v):
            raise ValueError(
                f"Invalid sf_api_version {v!r}: must match v<major>.<minor>"
            )
        return v


class ConfigStore:
    """Atomic-load / atomic-save wrapper for UserConfig.

    Default location: ``~/.asftool/config.json``. Atomic writes via
    ``tempfile`` + ``os.replace`` to avoid partial files if the process
    is killed mid-write.
    """

    DEFAULT_PATH = Path.home() / ".asftool" / "config.json"

    def __init__(self, path: Path | None = None):
        self.path = path or self.DEFAULT_PATH

    def load(self) -> UserConfig:
        """Read the config from disk. Empty/missing/corrupt -> empty config."""
        if not self.path.exists():
            return UserConfig()
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError) as e:
            logger.warning(
                "config_store_read_failed",
                path=str(self.path),
                error=str(e),
            )
            return UserConfig()
        try:
            return UserConfig(**data)
        except Exception as e:
            # Pydantic validation failure (e.g. bad version format) —
            # treat as empty config and let the user re-set it.
            logger.warning(
                "config_store_invalid",
                path=str(self.path),
                error=str(e),
            )
            return UserConfig()

    def save(self, cfg: UserConfig) -> None:
        """Atomically write the config to disk."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = cfg.model_dump(exclude_none=True)
        # Atomic write: write to temp file in same dir, then os.replace.
        fd, tmp_path = tempfile.mkstemp(
            dir=self.path.parent,
            prefix=".config_",
            suffix=".json.tmp",
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(payload, f, indent=2, ensure_ascii=False)
            os.replace(tmp_path, self.path)
            logger.info("config_store_saved", path=str(self.path), keys=list(payload.keys()))
        except Exception:
            # Best-effort cleanup of the temp file.
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise

    def reset(self) -> None:
        """Delete the config file. Silently ignores missing files."""
        try:
            self.path.unlink()
        except FileNotFoundError:
            pass

    def update(self, **kwargs) -> UserConfig:
        """Load, apply changes, save, return the new config."""
        cfg = self.load()
        updated = cfg.model_copy(update=kwargs)
        self.save(updated)
        return updated


# -----------------------------------------------------------------------------
# Convenience helpers
# -----------------------------------------------------------------------------

# Cached at module level so the file is read at most once per process.
_cached_config: UserConfig | None = None
_cached_path: Path | None = None


def get_user_config() -> UserConfig:
    """Return the persisted user config, with simple in-process caching."""
    global _cached_config, _cached_path
    store = ConfigStore()
    if _cached_config is None or _cached_path != store.path:
        _cached_config = store.load()
        _cached_path = store.path
    return _cached_config


def get_config_store() -> ConfigStore:
    """Return a ConfigStore instance for the standard location."""
    return ConfigStore()
