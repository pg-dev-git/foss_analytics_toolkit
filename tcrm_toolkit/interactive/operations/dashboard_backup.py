"""Dashboard backup and restore operations."""

from collections.abc import Callable
from pathlib import Path
from typing import Any

import structlog

from tcrm_toolkit.core.services.dashboard_service import DashboardService
from tcrm_toolkit.interactive.tasks import TaskRunner

logger = structlog.get_logger(__name__)


class DashboardBackupManager:
    """Manager for dashboard backup and restore operations."""

    def __init__(
        self,
        session,
        task_runner: TaskRunner,
        progress_callback: Callable[[dict[str, Any]], None] | None = None,
    ):
        self.session = session
        self.task_runner = task_runner
        self.progress_callback = progress_callback

    async def backup_dashboard(
        self,
        dashboard_id: str,
        output_path: Path,
    ) -> dict[str, Any]:
        """Backup single dashboard to JSON file."""
        async with self.session.client_context() as client:
            service = DashboardService(client, self.session.settings)

            if self.progress_callback:
                self.progress_callback({"status": "fetching", "dashboard_id": dashboard_id})

            backup = await service.backup_dashboard(dashboard_id, output_path)

            if self.progress_callback:
                self.progress_callback({"status": "completed", "path": str(output_path)})

            return {
                "dashboard_id": dashboard_id,
                "dashboard_name": backup.dashboard_name,
                "path": str(output_path),
            }

    async def backup_all_dashboards(
        self,
        output_dir: Path,
        pattern: str | None = None,
    ) -> dict[str, Any]:
        """Backup all dashboards to directory."""
        output_dir.mkdir(parents=True, exist_ok=True)

        async with self.session.client_context() as client:
            service = DashboardService(client, self.session.settings)
            dashboards = await service.list_dashboards()

            if pattern:
                import fnmatch
                dashboards = [d for d in dashboards if fnmatch.fnmatch(d.label, pattern)]

            results = []
            for i, dashboard in enumerate(dashboards):
                if self.progress_callback:
                    self.progress_callback({
                        "status": "backing_up",
                        "current": i + 1,
                        "total": len(dashboards),
                        "dashboard": dashboard.label,
                    })

                try:
                    output_path = output_dir / f"{dashboard.name}.json"
                    await service.backup_dashboard(dashboard.id, output_path)
                    results.append({
                        "id": dashboard.id,
                        "name": dashboard.name,
                        "label": dashboard.label,
                        "path": str(output_path),
                        "status": "success",
                    })
                except Exception as e:
                    results.append({
                        "id": dashboard.id,
                        "name": dashboard.name,
                        "label": dashboard.label,
                        "error": str(e),
                        "status": "failed",
                    })

            if self.progress_callback:
                self.progress_callback({"status": "completed", "results": results})

            return {
                "total": len(dashboards),
                "successful": sum(1 for r in results if r["status"] == "success"),
                "failed": sum(1 for r in results if r["status"] == "failed"),
                "results": results,
            }

    async def restore_dashboard(
        self,
        backup_path: Path,
        new_name: str | None = None,
    ) -> dict[str, Any]:
        """Restore dashboard from backup file."""
        async with self.session.client_context() as client:
            service = DashboardService(client, self.session.settings)

            if self.progress_callback:
                self.progress_callback({"status": "restoring", "path": str(backup_path)})

            dashboard = await service.restore_dashboard(backup_path, new_name)

            if self.progress_callback:
                self.progress_callback({"status": "completed", "dashboard_id": dashboard.id})

            return {
                "dashboard_id": dashboard.id,
                "dashboard_name": dashboard.name,
                "source": str(backup_path),
            }
