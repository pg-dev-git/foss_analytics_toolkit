"""Detail panel widget for showing entity details."""

from textual.app import ComposeResult
from textual.containers import Vertical
from textual.widget import Widget
from textual.widgets import Label, Static


class DetailPanel(Widget):
    """Right-side detail panel for showing selected item details."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._content = Static("Select an item to view details", id="detail-content")

    def compose(self) -> ComposeResult:
        yield Vertical(
            Label("Details", id="detail-title"),
            self._content,
            id="detail-container"
        )

    def show_dataset(self, dataset) -> None:
        from tcrm_toolkit.core.models import Dataset
        if not isinstance(dataset, Dataset):
            return

        row_count_str = f"{dataset.row_count:,}" if dataset.row_count is not None else 'N/A'
        created_str = dataset.created_date.strftime('%Y-%m-%d %H:%M') if dataset.created_date else 'N/A'
        modified_str = dataset.last_modified_date.strftime('%Y-%m-%d %H:%M') if dataset.last_modified_date else 'N/A'

        content = f"""[bold]Dataset Details[/bold]

[cyan]ID:[/cyan] {dataset.id}
[cyan]Name:[/cyan] {dataset.name}
[cyan]Label:[/cyan] {dataset.label}
[cyan]Description:[/cyan] {dataset.description or 'N/A'}
[cyan]Status:[/cyan] {dataset.status}
[cyan]Type:[/cyan] {dataset.type}
[cyan]Row Count:[/cyan] {row_count_str}
[cyan]Created:[/cyan] {created_str}
[cyan]Last Modified:[/cyan] {modified_str}
[cyan]Current Version:[/cyan] {dataset.current_version_id or 'N/A'}
"""
        self._content.update(content)

    def show_dashboard(self, dashboard) -> None:
        from tcrm_toolkit.core.models import Dashboard
        if not isinstance(dashboard, Dashboard):
            return

        created_str = dashboard.created_date.strftime('%Y-%m-%d %H:%M') if dashboard.created_date else 'N/A'
        modified_str = dashboard.last_modified_date.strftime('%Y-%m-%d %H:%M') if dashboard.last_modified_date else 'N/A'

        content = f"""[bold]Dashboard Details[/bold]

[cyan]ID:[/cyan] {dashboard.id}
[cyan]Name:[/cyan] {dashboard.name}
[cyan]Label:[/cyan] {dashboard.label}
[cyan]Description:[/cyan] {dashboard.description or 'N/A'}
[cyan]Folder:[/cyan] {dashboard.folder_name or 'N/A'}
[cyan]Created:[/cyan] {created_str}
[cyan]Last Modified:[/cyan] {modified_str}
"""
        self._content.update(content)

    def show_dataflow(self, dataflow) -> None:
        from tcrm_toolkit.core.models import Dataflow
        if not isinstance(dataflow, Dataflow):
            return

        created_str = dataflow.created_date.strftime('%Y-%m-%d %H:%M') if dataflow.created_date else 'N/A'
        modified_str = dataflow.last_modified_date.strftime('%Y-%m-%d %H:%M') if dataflow.last_modified_date else 'N/A'

        content = f"""[bold]Dataflow Details[/bold]

[cyan]ID:[/cyan] {dataflow.id}
[cyan]Name:[/cyan] {dataflow.name}
[cyan]Label:[/cyan] {dataflow.label}
[cyan]Description:[/cyan] {dataflow.description or 'N/A'}
[cyan]Status:[/cyan] {dataflow.status}
[cyan]Created:[/cyan] {created_str}
[cyan]Last Modified:[/cyan] {modified_str}
"""
        self._content.update(content)

    def show_dataflow_job(self, job) -> None:
        """Show dataflow job details."""
        from tcrm_toolkit.core.models import DataflowJob
        if not isinstance(job, DataflowJob):
            return

        content = f"""[bold]Dataflow Job Details[/bold]

[cyan]Job ID:[/cyan] {job.id}
[cyan]Dataflow:[/cyan] {job.dataflow_name}
[cyan]Command:[/cyan] {job.command}
[cyan]Status:[/cyan] {job.status}
[cyan]Start Time:[/cyan] {job.start_time.strftime('%Y-%m-%d %H:%M') if job.start_time else 'N/A'}
[cyan]End Time:[/cyan] {job.end_time.strftime('%Y-%m-%d %H:%M') if job.end_time else 'N/A'}
[cyan]Duration:[/cyan] {self._format_duration(job.start_time, job.end_time) if job.start_time else 'N/A'}
"""
        self._content.update(content)

    def _format_duration(self, start, end) -> str:
        if not start or not end:
            return "N/A"
        delta = end - start
        hours = delta.seconds // 3600
        minutes = (delta.seconds % 3600) // 60
        return f"{hours}h {minutes}m"

    def clear(self) -> None:
        self._content.update("Select an item to view details")

