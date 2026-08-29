"""Dataflow operations for Interactive TUI."""

import asyncio
from typing import Any

from textual.widgets import DataTable

from tcrm_toolkit.core.models import Dataflow, DataflowJob
from tcrm_toolkit.core.services.dataflow_service import DataflowService
from tcrm_toolkit.interactive.widgets.data_table import ColumnConfig, DataBrowser


def create_dataflow_browser(session) -> DataBrowser[Dataflow]:
    """Create configured dataflow browser."""

    columns = [
        ColumnConfig(key="id", title="ID", width=18, formatter=lambda x: x[:15] + "..." if len(str(x)) > 18 else str(x)),
        ColumnConfig(key="name", title="Name", width=30),
        ColumnConfig(key="label", title="Label", width=30),
        ColumnConfig(key="status", title="Status", width=15),
        ColumnConfig(key="created_date", title="Created", width=20, formatter=lambda x: x.strftime("%Y-%m-%d") if x else "N/A"),
    ]

    async def load_data(offset: int, limit: int, search: str | None, sort: str | None):
        async with session.client_context() as client:
            service = DataflowService(client, session.settings)
            all_dataflows = await service.list_dataflows()

            if search:
                search_lower = search.lower()
                all_dataflows = [
                    df for df in all_dataflows
                    if search_lower in df.name.lower()
                    or search_lower in df.label.lower()
                    or search_lower in df.id.lower()
                ]

            total = len(all_dataflows)
            page_data = all_dataflows[offset:offset + limit]
            return page_data, total

    def get_row_id(dataflow: Dataflow) -> str:
        return dataflow.id

    def get_row_data(dataflow: Dataflow) -> dict[str, Any]:
        return {
            "id": dataflow.id,
            "name": dataflow.name,
            "label": dataflow.label,
            "status": dataflow.status,
            "created_date": dataflow.created_date,
        }

    return DataBrowser(
        columns=columns,
        load_data=load_data,
        get_row_id=get_row_id,
        get_row_data=get_row_data,
        title="🔄 Dataflows",
        page_size=50,
        id="dataflows-browser",
    )


def create_dataflow_job_browser(session) -> DataBrowser[DataflowJob]:
    """Create configured dataflow job browser with live polling."""

    columns = [
        ColumnConfig(key="id", title="Job ID", width=18, formatter=lambda x: x[:15] + "..." if len(str(x)) > 18 else str(x)),
        ColumnConfig(key="dataflow_name", title="Dataflow", width=30),
        ColumnConfig(key="command", title="Command", width=12),
        ColumnConfig(key="status", title="Status", width=15),
        ColumnConfig(key="start_time", title="Started", width=20, formatter=lambda x: x.strftime("%Y-%m-%d %H:%M") if x else "N/A"),
        ColumnConfig(key="end_time", title="Ended", width=20, formatter=lambda x: x.strftime("%Y-%m-%d %H:%M") if x else "N/A"),
    ]

    async def load_data(offset: int, limit: int, search: str | None, sort: str | None):
        async with session.client_context() as client:
            service = DataflowService(client, session.settings)
            all_jobs = await service.list_dataflow_jobs()

            if search:
                search_lower = search.lower()
                all_jobs = [
                    job for job in all_jobs
                    if search_lower in job.dataflow_name.lower()
                    or search_lower in job.command.lower()
                    or search_lower in job.status.lower()
                    or search_lower in job.id.lower()
                ]

            all_jobs.sort(key=lambda j: j.start_time or "", reverse=True)

            total = len(all_jobs)
            page_data = all_jobs[offset:offset + limit]
            return page_data, total

    def get_row_id(job: DataflowJob) -> str:
        return job.id

    def get_row_data(job: DataflowJob) -> dict[str, Any]:
        return {
            "id": job.id,
            "dataflow_name": job.dataflow_name,
            "command": job.command,
            "status": job.status,
            "start_time": job.start_time,
            "end_time": job.end_time,
        }

    browser = DataBrowser(
        columns=columns,
        load_data=load_data,
        get_row_id=get_row_id,
        get_row_data=get_row_data,
        title="📋 Dataflow Jobs",
        page_size=50,
        id="jobs-browser",
    )

    original_on_mount = browser.on_mount

    async def on_mount_with_polling(self) -> None:
        await original_on_mount()
        self._poll_task = asyncio.create_task(self._poll_running_jobs())

    async def _poll_running_jobs(self) -> None:
        while True:
            try:
                await asyncio.sleep(10)
                table = self.query_one("#data-table", DataTable)
                has_running = any(
                    "running" in str(table.get_cell_at(row, 3)).lower()
                    for row in range(table.row_count)
                )
                if has_running:
                    await self.refresh()
            except Exception:
                pass

    browser.on_mount = on_mount_with_polling.__get__(browser, DataBrowser)

    return browser

