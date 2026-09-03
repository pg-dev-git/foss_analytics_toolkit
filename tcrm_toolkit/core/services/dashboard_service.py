"""Dashboard service for TCRM Toolkit."""

import json
from pathlib import Path

import structlog

from tcrm_toolkit.core.client import SalesforceClient
from tcrm_toolkit.core.config import Settings, get_settings
from tcrm_toolkit.core.models import (
    Dashboard,
    DashboardBackup,
    DashboardDataset,
    DashboardListResponse,
)

logger = structlog.get_logger(__name__)


class DashboardService:
    """Service for dashboard operations."""

    def __init__(
        self,
        client: SalesforceClient,
        settings: Settings | None = None,
    ):
        """Initialize the dashboard service."""
        self.client = client
        self.settings = settings or get_settings()

    # =========================================================================
    # Listing and Retrieval
    # =========================================================================

    async def list_dashboards(
        self,
        page_size: int = 50,
        sort: str = "Mru",
    ) -> list[Dashboard]:
        """List all dashboards with pagination."""
        all_dashboards = []
        page_token = None

        while True:
            response = await self.client.list_dashboards(
                page_size=page_size,
                sort=sort,
                page_token=page_token,
            )
            data = DashboardListResponse(**response)
            all_dashboards.extend(data.dashboards)

            if not data.next_page_url:
                break

            page_token = self._extract_page_token(data.next_page_url)

        return all_dashboards

    async def get_dashboard(self, dashboard_id: str) -> Dashboard:
        """Get dashboard details by ID."""
        response = await self.client.get_dashboard(dashboard_id)
        return Dashboard(**response)

    async def get_dashboard_datasets(self, dashboard_id: str) -> list[DashboardDataset]:
        """Get datasets used in a dashboard."""
        response = await self.client.get_dashboard_datasets(dashboard_id)
        datasets = response.get("datasets", [])
        return [DashboardDataset(**d) for d in datasets]

    # =========================================================================
    # Backup and Restore
    # =========================================================================

    async def backup_dashboard(
        self,
        dashboard_id: str,
        output_path: Path | None = None,
    ) -> DashboardBackup:
        """Backup dashboard JSON definition."""
        dashboard = await self.get_dashboard(dashboard_id)

        # Get the full dashboard JSON
        response = await self.client.get(f"{self.client.wave_base_url}/dashboards/{dashboard_id}")
        json_definition = response.json()

        backup = DashboardBackup(
            dashboard_id=dashboard_id,
            dashboard_name=dashboard.name,
            dashboard_label=dashboard.label,
            json_definition=json_definition,
        )

        if output_path:
            output_path.write_text(json.dumps(json_definition, indent=2))
            logger.info("dashboard_backup_saved", path=str(output_path))

        return backup

    async def restore_dashboard(
        self,
        backup_path: Path,
        new_name: str | None = None,
    ) -> Dashboard:
        """Restore dashboard from backup file."""
        json_definition = json.loads(backup_path.read_text())

        if new_name:
            json_definition["name"] = new_name
            json_definition["label"] = new_name

        # Create new dashboard
        response = await self.client.post(
            f"{self.client.wave_base_url}/dashboards",
            json=json_definition,
        )
        return Dashboard(**response.json())

    # =========================================================================
    # Deletion
    # =========================================================================

    async def delete_dashboard(self, dashboard_id: str) -> bool:
        """Delete a dashboard."""
        await self.client.delete_dashboard(dashboard_id)
        return True

    # =========================================================================
    # Helper Methods
    # =========================================================================

    def _extract_page_token(self, url: str) -> str | None:
        """Extract page token from next page URL."""
        from urllib.parse import parse_qs, urlparse
        parsed = urlparse(url)
        params = parse_qs(parsed.query)
        return params.get("pageToken", [None])[0]
