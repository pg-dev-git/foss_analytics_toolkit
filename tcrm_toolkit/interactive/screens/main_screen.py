"""Main screen with sidebar navigation and content area."""

from textual import on
from textual.app import ComposeResult
from textual.containers import Container, Horizontal, Vertical
from textual.screen import Screen
from textual.widgets import DataTable, Label, ListItem, ListView, Static

from tcrm_toolkit.interactive.safety import SafetyMonitor
from tcrm_toolkit.interactive.session import SessionManager
from tcrm_toolkit.interactive.widgets.detail_panel import DetailPanel


class MainScreen(Screen):
    """Main screen with navigation sidebar and content area."""

    BINDINGS = [
        ("ctrl+p", "command_palette", "Command Palette"),
        ("ctrl+o", "org_picker", "Switch Org"),
        ("ctrl+r", "refresh", "Refresh"),
        ("escape", "escape", "Back"),
    ]

    def __init__(self, session: SessionManager, safety: SafetyMonitor):
        super().__init__()
        self.session = session
        self.safety = safety
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

    async def _load_datasets_view(self, container: Container) -> None:
        """Load datasets browser."""
        from tcrm_toolkit.interactive.operations.dataset_ops import create_dataset_browser
        from tcrm_toolkit.interactive.widgets.data_table import DataBrowser
        browser = create_dataset_browser(self.session)
        await container.mount(browser)

        @browser.on(DataBrowser.RowSelected)
        async def on_dataset_selected(event: DataBrowser.RowSelected) -> None:
            detail = self.query_one("#detail-panel", DetailPanel)
            detail.show_dataset(event.row)

    async def _load_dashboards_view(self, container: Container) -> None:
        """Load dashboards browser."""
        from tcrm_toolkit.interactive.operations.dashboard_ops import create_dashboard_browser
        from tcrm_toolkit.interactive.widgets.data_table import DataBrowser
        browser = create_dashboard_browser(self.session)
        await container.mount(browser)

        @browser.on(DataBrowser.RowSelected)
        async def on_dashboard_selected(event: DataBrowser.RowSelected) -> None:
            detail = self.query_one("#detail-panel", DetailPanel)
            detail.show_dashboard(event.row)

    async def _load_dataflows_view(self, container: Container) -> None:
        """Load dataflows browser."""
        from tcrm_toolkit.interactive.operations.dataflow_ops import create_dataflow_browser
        from tcrm_toolkit.interactive.widgets.data_table import DataBrowser
        browser = create_dataflow_browser(self.session)
        await container.mount(browser)

        @browser.on(DataBrowser.RowSelected)
        async def on_dataflow_selected(event: DataBrowser.RowSelected) -> None:
            detail = self.query_one("#detail-panel", DetailPanel)
            detail.show_dataflow(event.row)

    async def _load_jobs_view(self, container: Container) -> None:
        """Load jobs browser with auto-refresh."""
        from tcrm_toolkit.interactive.operations.dataflow_ops import create_dataflow_job_browser
        from tcrm_toolkit.interactive.widgets.data_table import DataBrowser
        browser = create_dataflow_job_browser(self.session)
        await container.mount(browser)

        @browser.on(DataBrowser.RowSelected)
        async def on_job_selected(event: DataBrowser.RowSelected) -> None:
            detail = self.query_one("#detail-panel", DetailPanel)
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
        await container.mount(Static("Configuration view - TODO"))

    async def refresh_data(self) -> None:
        await self._load_view(self._current_view)

    async def action_escape(self) -> None:
        detail = self.query_one("#detail-panel", DetailPanel)
        detail.clear()

    async def action_command_palette(self) -> None:
        self.notify("Command palette coming in Phase 4", severity="information")

