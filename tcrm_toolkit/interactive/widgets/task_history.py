"""Task history panel for viewing past operations."""

from textual import on
from textual.app import ComposeResult
from textual.containers import Vertical
from textual.widgets import DataTable, Label, Static, TabbedContent, TabPane

from tcrm_toolkit.interactive.tasks import TaskRunner, TaskStatus


class TaskHistory(Static):
    """Panel showing task history with filtering."""

    def __init__(self, task_runner: TaskRunner):
        super().__init__(id="task-history")
        self.task_runner = task_runner
        self._filter_status: TaskStatus | None = None

    def compose(self) -> ComposeResult:
        yield Vertical(
            Label("Task History", id="history-title"),
            TabbedContent(
                TabPane("All", id="tab-all"),
                TabPane("Running", id="tab-running"),
                TabPane("Completed", id="tab-completed"),
                TabPane("Failed", id="tab-failed"),
                id="history-tabs"
            ),
            DataTable(id="history-table", cursor_type="row", zebra_stripes=True),
            id="history-container"
        )

    async def on_mount(self) -> None:
        table = self.query_one("#history-table", DataTable)
        table.add_columns("Time", "Task", "Status", "Duration", "Details")
        table.zebra_stripes = True
        await self.refresh_history()

    @on(TabbedContent.TabActivated, "#history-tabs")
    async def on_tab_changed(self, event: TabbedContent.TabActivated) -> None:
        tab_map = {
            "tab-all": None,
            "tab-running": TaskStatus.RUNNING,
            "tab-completed": TaskStatus.COMPLETED,
            "tab-failed": TaskStatus.FAILED,
        }
        self._filter_status = tab_map.get(event.tab.id)
        await self.refresh_history()

    async def refresh_history(self) -> None:
        """Refresh history table."""
        table = self.query_one("#history-table", DataTable)
        table.clear()

        history = self.task_runner.get_history()

        if self._filter_status:
            history = [r for r in history if r.status == self._filter_status]

        for result in reversed(history[-100:]):
            duration = ""
            if result.completed_at and result.started_at:
                delta = result.completed_at - result.started_at
                duration = f"{delta.total_seconds():.1f}s"

            status_style = {
                TaskStatus.COMPLETED: "[green]",
                TaskStatus.FAILED: "[red]",
                TaskStatus.CANCELLED: "[yellow]",
                TaskStatus.RUNNING: "[blue]",
            }.get(result.status, "")

            details = result.error or str(result.result)[:50] if result.result else ""

            table.add_row(
                result.started_at.strftime("%H:%M:%S"),
                result.task_id,
                f"{status_style}{result.status.value}[/]",
                duration,
                details,
            )


TaskHistoryPanel = TaskHistory

