"""Storage management for downloaded data.

Provides organized directory structure and consistent file naming
for downloaded datasets, dashboards, dataflows, and backups.
"""

import re
from datetime import datetime
from pathlib import Path
from typing import Literal

from asftool.core.config import Settings, get_settings


class StorageManager:
    """Manages organized storage for downloaded data."""

    def __init__(self, settings: Settings | None = None):
        self.settings = settings or get_settings()

    @property
    def base_dir(self) -> Path:
        """Base directory for all downloads (in current working directory)."""
        return Path.cwd() / "asftool_downloads"

    def _sanitize(self, name: str) -> str:
        """Sanitize a string for use in filenames."""
        # Replace spaces and special chars with underscores
        # Keep alphanumeric, dash, underscore, dot
        sanitized = re.sub(r"[^\w\-.]", "_", name)
        # Collapse multiple underscores
        sanitized = re.sub(r"_+", "_", sanitized)
        # Strip leading/trailing underscores
        return sanitized.strip("_")

    def _timestamp(self) -> str:
        """Generate timestamp string for filenames."""
        return datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    def _org_dir(self, alias: str) -> Path:
        """Get the org-specific download directory."""
        org_dir = self.base_dir / self._sanitize(alias)
        org_dir.mkdir(parents=True, exist_ok=True)
        return org_dir

    def _type_dir(
        self,
        alias: str,
        type_: Literal["datasets", "dashboards", "dataflows", "backups", "field_impact"],
    ) -> Path:
        """Get the type-specific directory within org directory."""
        type_dir = self._org_dir(alias) / type_
        type_dir.mkdir(parents=True, exist_ok=True)
        return type_dir

    def field_impact_path(
        self,
        alias: str,
        search_term: str,
        extension: str = "json",
    ) -> Path:
        """Generate path for a field impact analysis report."""
        timestamp = self._timestamp()
        safe_term = self._sanitize(search_term) or "field"
        filename = f"{timestamp}_{safe_term}.{extension}"
        return self._type_dir(alias, "field_impact") / filename

    def dataset_path(
        self,
        alias: str,
        dataset_name: str,
        dataset_id: str,
        extension: str = "csv",
    ) -> Path:
        """Generate path for a dataset download."""
        timestamp = self._timestamp()
        safe_name = self._sanitize(dataset_name)
        safe_id = self._sanitize(dataset_id)
        filename = f"{timestamp}_{safe_name}_{safe_id}.{extension}"
        return self._type_dir(alias, "datasets") / filename

    def dashboard_path(
        self,
        alias: str,
        dashboard_name: str,
        dashboard_id: str,
        extension: str = "json",
    ) -> Path:
        """Generate path for a dashboard download/backup."""
        timestamp = self._timestamp()
        safe_name = self._sanitize(dashboard_name)
        safe_id = self._sanitize(dashboard_id)
        filename = f"{timestamp}_{safe_name}_{safe_id}.{extension}"
        return self._type_dir(alias, "dashboards") / filename

    def dataflow_path(
        self,
        alias: str,
        dataflow_name: str,
        dataflow_id: str,
        extension: str = "json",
    ) -> Path:
        """Generate path for a dataflow download."""
        timestamp = self._timestamp()
        safe_name = self._sanitize(dataflow_name)
        safe_id = self._sanitize(dataflow_id)
        filename = f"{timestamp}_{safe_name}_{safe_id}.{extension}"
        return self._type_dir(alias, "dataflows") / filename

    def backup_path(
        self,
        alias: str,
        backup_name: str = "full_backup",
        extension: str = "tar.gz",
    ) -> Path:
        """Generate path for a full org backup."""
        timestamp = self._timestamp()
        safe_name = self._sanitize(backup_name)
        filename = f"{timestamp}_{safe_name}.{extension}"
        return self._type_dir(alias, "backups") / filename

    def list_downloads(self, alias: str) -> dict[str, list[Path]]:
        """List all downloads for an org, organized by type."""
        org_dir = self._org_dir(alias)
        result = {}
        for type_ in ["datasets", "dashboards", "dataflows", "backups"]:
            type_dir = org_dir / type_
            if type_dir.exists():
                files = sorted(type_dir.glob("*"), key=lambda p: p.stat().st_mtime, reverse=True)
                result[type_] = files
            else:
                result[type_] = []
        return result

    def get_org_downloads_size(self, alias: str) -> int:
        """Get total size of all downloads for an org in bytes."""
        org_dir = self._org_dir(alias)
        if not org_dir.exists():
            return 0
        return sum(f.stat().st_size for f in org_dir.rglob("*") if f.is_file())

    def cleanup_old_downloads(self, alias: str, keep_last: int = 10) -> int:
        """Remove old downloads, keeping only the most recent N per type."""
        removed = 0
        for type_ in ["datasets", "dashboards", "dataflows", "backups"]:
            type_dir = self._org_dir(alias) / type_
            if not type_dir.exists():
                continue
            files = sorted(type_dir.glob("*"), key=lambda p: p.stat().st_mtime, reverse=True)
            for old_file in files[keep_last:]:
                old_file.unlink()
                removed += 1
        return removed


def get_storage_manager(settings: Settings | None = None) -> StorageManager:
    """Get a StorageManager instance."""
    return StorageManager(settings)
