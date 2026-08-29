"""Dashboard operations for Interactive TUI."""

from typing import Any

from tcrm_toolkit.core.models import Dashboard
from tcrm_toolkit.core.services.dashboard_service import DashboardService
from tcrm_toolkit.interactive.widgets.data_table import ColumnConfig, DataBrowser


def create_dashboard_browser(session) -> DataBrowser[Dashboard]:
    """Create configured dashboard browser."""

    columns = [
        ColumnConfig(key="id", title="ID", width=18, formatter=lambda x: x[:15] + "..." if len(str(x)) > 18 else str(x)),
        ColumnConfig(key="name", title="Name", width=30),
        ColumnConfig(key="label", title="Label", width=30),
        ColumnConfig(key="folder_name", title="Folder", width=25, formatter=lambda x: x or "N/A"),
        ColumnConfig(key="created_date", title="Created", width=20, formatter=lambda x: x.strftime("%Y-%m-%d") if x else "N/A"),
    ]

    async def load_data(offset: int, limit: int, search: str | None, sort: str | None):
        async with session.client_context() as client:
            service = DashboardService(client, session.settings)
            sort_key = sort.split(":")[0] if sort else "Mru"
            all_dashboards = await service.list_dashboards(page_size=1000, sort=sort_key)

            if search:
                search_lower = search.lower()
                all_dashboards = [
                    db for db in all_dashboards
                    if search_lower in db.name.lower()
                    or search_lower in db.label.lower()
                    or search_lower in (db.folder_name or "").lower()
                    or search_lower in db.id.lower()
                ]

            total = len(all_dashboards)
            page_data = all_dashboards[offset:offset + limit]
            return page_data, total

    def get_row_id(dashboard: Dashboard) -> str:
        return dashboard.id

    def get_row_data(dashboard: Dashboard) -> dict[str, Any]:
        return {
            "id": dashboard.id,
            "name": dashboard.name,
            "label": dashboard.label,
            "folder_name": dashboard.folder_name or "N/A",
            "created_date": dashboard.created_date,
        }

    return DataBrowser(
        columns=columns,
        load_data=load_data,
        get_row_id=get_row_id,
        get_row_data=get_row_data,
        title="📈 Dashboards",
        page_size=50,
        id="dashboards-browser",
    )

