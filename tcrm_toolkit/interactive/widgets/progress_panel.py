"""Progress panel for showing running task progress."""

from textual.app import ComposeResult
from textual.containers import Vertical
from textual.widgets import DataTable, Label, Static

from tcrm_toolkit.interactive.tasks import TaskRunner


class ProgressPanel(Static):
    """Panel showing active task progress bars."""

    def __init__(self, task_runner: TaskRunner):
        super().__init__(id="progress-panel")
        self.task_runner = task_runner

    def compose(self) -> ComposeResult:
        yield Vertical(
            Label("Active Tasks", id="progress-title"),
            DataTable(id="progress-table", cursor_type="row"),
            id="progress-container"
        )

    async def on_mount(self) -> None:
        table = self.query_one("#progress-table", DataTable)
        table.add_columns("Task", "Status", "Progress", "Details")
        self.set_interval(1.0, self.update_progress)

    def update_progress(self) -> None:
        """Update progress table from task runner."""
        table = self.query_one("#progress-table", DataTable)
        table.clear()

        for progress in self.task_runner.get_all_progress():
            if progress.is_finished:
                continue

            pct = progress.percent
            bar = "█" * int(pct / 5) + "░" * (20 - int(pct / 5))

            table.add_row(
                progress.task_id,
                progress.status.value,
                f"{bar} {pct:.1f}%",
                progress.message,
            )

