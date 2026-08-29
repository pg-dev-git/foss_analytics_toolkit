"""Dataset operations for Interactive TUI."""

from typing import Any

from tcrm_toolkit.core.models import Dataset
from tcrm_toolkit.core.services.dataset_service import DatasetService
from tcrm_toolkit.interactive.widgets.data_table import ColumnConfig, DataBrowser


def create_dataset_browser(session) -> DataBrowser[Dataset]:
    """Create configured dataset browser."""

    columns = [
        ColumnConfig(key="id", title="ID", width=18, formatter=lambda x: x[:15] + "..." if len(str(x)) > 18 else str(x)),
        ColumnConfig(key="name", title="Name", width=30),
        ColumnConfig(key="label", title="Label", width=30),
        ColumnConfig(key="row_count", title="Rows", width=12, formatter=lambda x: f"{x:,}" if x else "N/A"),
        ColumnConfig(key="status", title="Status", width=12),
        ColumnConfig(key="type", title="Type", width=15),
    ]

    async def load_data(offset: int, limit: int, search: str | None, sort: str | None):
        """Load datasets with pagination."""
        async with session.client_context() as client:
            service = DatasetService(client, session.settings)

            sort_key = sort.split(":")[0] if sort else "Mru"
            all_datasets = await service.list_datasets(page_size=1000, sort=sort_key)

            if search:
                search_lower = search.lower()
                all_datasets = [
                    ds for ds in all_datasets
                    if search_lower in ds.name.lower()
                    or search_lower in ds.label.lower()
                    or search_lower in ds.id.lower()
                ]

            total = len(all_datasets)
            page_data = all_datasets[offset:offset + limit]

            return page_data, total

    def get_row_id(dataset: Dataset) -> str:
        return dataset.id

    def get_row_data(dataset: Dataset) -> dict[str, Any]:
        return {
            "id": dataset.id,
            "name": dataset.name,
            "label": dataset.label,
            "row_count": dataset.row_count or 0,
            "status": dataset.status,
            "type": dataset.type,
        }

    return DataBrowser(
        columns=columns,
        load_data=load_data,
        get_row_id=get_row_id,
        get_row_data=get_row_data,
        title="📊 Datasets",
        page_size=50,
        id="datasets-browser",
    )

