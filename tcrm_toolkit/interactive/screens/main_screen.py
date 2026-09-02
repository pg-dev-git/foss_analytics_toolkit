"""Main screen with sidebar navigation and content area."""

from typing import Any

from textual import on
from textual.app import ComposeResult
from textual.containers import Container, Horizontal, Vertical
from textual.screen import Screen
from textual.widgets import DataTable, Label, ListItem, ListView, Static

from tcrm_toolkit.interactive.safety import SafetyMonitor
from tcrm_toolkit.interactive.screens.help_screen import HelpScreen
from tcrm_toolkit.interactive.session import SessionManager
from tcrm_toolkit.interactive.widgets.data_table import DataBrowser
from tcrm_toolkit.interactive.widgets.detail_panel import DetailPanel


class MainScreen(Screen):
    """Main screen with navigation sidebar and content area."""

    BINDINGS = [
        ("ctrl+p", "command_palette", "Command Palette"),
        ("ctrl+o", "org_picker", "Switch Org"),
        ("ctrl+r", "refresh", "Refresh"),
        ("escape", "escape", "Back"),
    ]

    def __init__(self, session: SessionManager, safety: SafetyMonitor, task_runner: Any | None = None):
        super().__init__()
        self.session = session
        self.safety = safety
        self.task_runner = task_runner
        self._current_view = "datasets"

    def compose(self) -> ComposeResult:
        yield Horizontal(
            Vertical(
                Static("📊 TCRM Toolkit", id="sidebar-title"),
                ListView(
                    ListItem(Label("📊 Datasets"), id="nav-datasets"),
                    ListItem(Label("📈 Dashboards"), id="nav-dashboards"),
                    ListItem(Label("🔄 Dataflows"), id="nav-dataflows"),
                    ListItem(Label("📋 Jobs"), id="nav-jobs"),
                    ListItem(Label("🔐 Orgs"), id="nav-orgs"),
                    ListItem(Label("⚙️ Config"), id="nav-config"),
                    ListItem(Label("📋 History"), id="nav-history"),
                    id="nav-list"
                ),
                Static("[dim]Ctrl+P: Commands  Ctrl+O: Orgs  Ctrl+R: Refresh[/dim]", id="sidebar-hints"),
                id="sidebar"
            ),
            Vertical(
                Static("Select a navigation item", id="content-title"),
                Container(id="content-area"),
                id="content"
            ),
            DetailPanel(id="detail-panel"),
            id="main-layout"
        )

    async def on_mount(self) -> None:
        nav_list = self.query_one("#nav-list", ListView)
        nav_list.index = 0
        await self._switch_view("datasets")

    @on(ListView.Selected, "#nav-list")
    async def on_nav_selected(self, event: ListView.Selected) -> None:
        item = event.item
        if item.id and item.id.startswith("nav-"):
            view = item.id[4:]
            await self._switch_view(view)

    async def _switch_view(self, view: str) -> None:
        self._current_view = view
        titles = {
            "datasets": "📊 Datasets",
            "dashboards": "📈 Dashboards",
            "dataflows": "🔄 Dataflows",
            "jobs": "📋 Dataflow Jobs",
            "orgs": "🔐 Organizations",
            "config": "⚙️ Configuration",
            "history": "📋 Task History",
        }
        self.query_one("#content-title", Static).update(titles.get(view, view))
        await self._load_view(view)

    async def _load_view(self, view: str) -> None:
        container = self.query_one("#content-area", Container)
        await container.remove_children()

        if view == "datasets":
            await self._load_datasets_view(container)
        elif view == "dashboards":
            await self._load_dashboards_view(container)
        elif view == "dataflows":
            await self._load_dataflows_view(container)
        elif view == "jobs":
            await self._load_jobs_view(container)
        elif view == "orgs":
            await self._load_orgs_view(container)
        elif view == "config":
            await self._load_config_view(container)
        elif view == "history":
            await self._load_history_view(container)

    async def _load_datasets_view(self, container: Container) -> None:
        """Load datasets browser."""
        from tcrm_toolkit.interactive.operations.dataset_ops import create_dataset_browser
        browser = create_dataset_browser(self.session)
        await container.mount(browser)

    async def _load_dashboards_view(self, container: Container) -> None:
        """Load dashboards browser."""
        from tcrm_toolkit.interactive.operations.dashboard_ops import create_dashboard_browser
        browser = create_dashboard_browser(self.session)
        await container.mount(browser)

    async def _load_dataflows_view(self, container: Container) -> None:
        """Load dataflows browser."""
        from tcrm_toolkit.interactive.operations.dataflow_ops import create_dataflow_browser
        browser = create_dataflow_browser(self.session)
        await container.mount(browser)

    async def _load_jobs_view(self, container: Container) -> None:
        """Load jobs browser with auto-refresh."""
        from tcrm_toolkit.interactive.operations.dataflow_ops import create_dataflow_job_browser
        browser = create_dataflow_job_browser(self.session)
        await container.mount(browser)

    @on(DataBrowser.RowSelected)
    async def on_data_browser_row_selected(self, event: DataBrowser.RowSelected) -> None:
        """Handle row selection from any data browser."""
        detail = self.query_one("#detail-panel", DetailPanel)
        if self._current_view == "datasets":
            detail.show_dataset(event.row)
        elif self._current_view == "dashboards":
            detail.show_dashboard(event.row)
        elif self._current_view == "dataflows":
            detail.show_dataflow(event.row)
        elif self._current_view == "jobs":
            detail.show_dataflow_job(event.row)

    async def _load_orgs_view(self, container: Container) -> None:
        orgs = self.session.list_orgs()
        table = DataTable(id="orgs-table", cursor_type="row")
        table.add_columns("#", "Alias", "Username", "Instance URL", "Current")
        table.zebra_stripes = True
        await container.mount(table)

        for i, org in enumerate(orgs, 1):
            current = "●" if org.alias == self.session.current_alias else ""
            table.add_row(str(i), org.alias, org.username or "N/A", org.instance_url, current)

    async def _load_config_view(self, container: Container) -> None:
        from tcrm_toolkit.interactive.config_manager import ConfigManager
        cfg = ConfigManager().load()
        table = DataTable(id="config-table", cursor_type="row")
        table.add_column("Setting", style="cyan")
        table.add_column("Value", style="white")
        table.zebra_stripes = True
        await container.mount(table)

        for field_name, value in cfg.model_dump().items():
            table.add_row(field_name, str(value))

    async def _load_history_view(self, container: Container) -> None:
        from tcrm_toolkit.interactive.widgets.task_history import TaskHistory
        from tcrm_toolkit.interactive.tasks import TaskRunner
        runner = self.task_runner or TaskRunner()
        history_widget = TaskHistory(runner)
        await container.mount(history_widget)

    async def refresh_data(self) -> None:
        await self._load_view(self._current_view)

    async def action_escape(self) -> None:
        detail = self.query_one("#detail-panel", DetailPanel)
        detail.clear()

    async def action_command_palette(self) -> None:
        from tcrm_toolkit.interactive.widgets.command_palette import CommandPaletteScreen
        from tcrm_toolkit.interactive.screens.help_screen import HelpScreen

        commands = [
            ("📊 Datasets", "view-datasets"),
            ("📈 Dashboards", "view-dashboards"),
            ("🔄 Dataflows", "view-dataflows"),
            ("📋 Dataflow Jobs", "view-jobs"),
            ("🔐 Organizations", "view-orgs"),
            ("⚙️ Configuration", "view-config"),
            ("📋 Task History", "view-history"),
            ("🔄 Refresh View", "refresh"),
            ("🔐 Switch Organization", "org_picker"),
            ("❓ Help & Shortcuts", "help"),
            ("🚪 Quit", "quit"),
        ]

        def on_command_selected(action_id: str | None) -> None:
            if action_id:
                self.run_worker(self._handle_command(action_id))

        self.app.push_screen(CommandPaletteScreen(commands), on_command_selected)

    async def _handle_command(self, action_id: str) -> None:
        if action_id.startswith("view-"):
            view = action_id[5:]
            await self._switch_view(view)
        elif action_id == "refresh":
            await self.app.action_refresh()
        elif action_id == "org_picker":
            await self.app.action_org_picker()
        elif action_id == "help":
            self.app.push_screen(HelpScreen())
        elif action_id == "quit":
            self.app.exit()

