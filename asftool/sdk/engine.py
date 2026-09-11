"""ASFToolSDK — importable facade for agent integration."""

from __future__ import annotations

from asftool.core.services.dataset_service import DatasetService
from asftool.core.services.dashboard_service import DashboardService
from asftool.core.services.dataflow_service import DataflowService


class ASFToolSDK:
    """Programmatic SDK entry point with service facades."""

    def __init__(self, client):
        self.client = client
        self.datasets = DatasetService(client)
        self.dashboards = DashboardService(client)
        self.dataflows = DataflowService(client)
        self.jobs = None  # Placeholder for job orchestration
        self.crawler = None  # Placeholder for impact crawler

    async def __aenter__(self) -> "ASFToolSDK":
        return self

    async def __aexit__(self, *args) -> None:
        await self.client.close()
