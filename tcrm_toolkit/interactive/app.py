"""Main Textual App for CRMA Toolkit Interactive TUI."""


from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Container
from textual.widgets import Footer, Header

from tcrm_toolkit.core.config import get_settings
from tcrm_toolkit.core.crypto import create_crypto_manager
from tcrm_toolkit.interactive.safety import RiskLevel, SafetyMonitor, SafetyResult
from tcrm_toolkit.interactive.screens.login_screen import LoginScreen
from tcrm_toolkit.interactive.screens.main_screen import MainScreen
from tcrm_toolkit.interactive.screens.org_picker import OrgPickerScreen
from tcrm_toolkit.interactive.screens.safety_modal import SafetyModalScreen
from tcrm_toolkit.interactive.session import SessionManager
from tcrm_toolkit.interactive.widgets.status_bar import StatusBar


class TCRMApp(App):
    """
    Main Interactive TUI Application for CRMA Toolkit.
    """

    CSS_PATH = "styles/dark.css"

    BINDINGS = [
        Binding("ctrl+q", "quit", "Quit", show=True),
        Binding("ctrl+p", "command_palette", "Command Palette", show=True),
        Binding("ctrl+o", "org_picker", "Switch Org", show=True),
        Binding("ctrl+r", "refresh", "Refresh", show=True),
        Binding("f1", "help", "Help", show=False),
        Binding("escape", "escape", "Back/Cancel", show=False),
    ]

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.settings = get_settings()
        self.crypto = create_crypto_manager()
        self.safety = SafetyMonitor(self.settings)
        self.session = SessionManager(
            settings=self.settings,
            crypto=self.crypto,
            safety_monitor=self.safety,
        )
        self._main_screen: MainScreen | None = None
        self._safety_check_interval = self.settings.safety_check_interval

    def compose(self) -> ComposeResult:
        """Compose the app layout."""
        yield Header(show_clock=True)
        yield Container(id="main-container")
        yield StatusBar(id="status-bar")
        yield Footer()

    async def on_mount(self) -> None:
        """Initialize app on mount."""
        self.safety.start_monitoring(callback=self._on_safety_update)

        try:
            await self.session.initialize()
        except Exception as e:
            self.notify(f"Session init failed: {e}", severity="error")

        safety_result = await self.safety.check_connection_safety()
        if safety_result.risk_level == RiskLevel.CRITICAL and self.settings.safety_block_on_critical:
            self.push_screen(SafetyModalScreen(safety_result), self._on_safety_modal_dismiss)
        else:
            await self._show_main_screen()

    async def _on_safety_update(self, result: SafetyResult) -> None:
        """Handle safety monitor updates."""
        status_bar = self.query_one("#status-bar", StatusBar)
        status_bar.update_safety(result)

        if result.risk_level == RiskLevel.CRITICAL and self.settings.safety_block_on_critical:
            if not self.screen_stack or not isinstance(self.screen_stack[-1], SafetyModalScreen):
                self.push_screen(SafetyModalScreen(result), self._on_safety_modal_dismiss)

    def _on_safety_modal_dismiss(self, action: str) -> None:
        """Handle safety modal dismissal."""
        if action == "retry":
            self.run_worker(self._recheck_safety_and_continue())
        else:
            self.exit()

    async def _recheck_safety_and_continue(self) -> None:
        result = await self.safety.check_connection_safety(force=True)
        if result.risk_level == RiskLevel.CRITICAL:
            self.push_screen(SafetyModalScreen(result), self._on_safety_modal_dismiss)
        else:
            await self._show_main_screen()

    async def _show_main_screen(self) -> None:
        while len(self.screen_stack) > 1:
            self.pop_screen()

        if not self.session.current_org:
            self.push_screen(LoginScreen(self.session), self._on_login_complete)
        else:
            await self._mount_main_screen()

    def _on_login_complete(self, success: bool) -> None:
        if success:
            self.run_worker(self._mount_main_screen())
        else:
            self.notify("Login failed", severity="error")
            self.push_screen(LoginScreen(self.session), self._on_login_complete)

    async def _mount_main_screen(self) -> None:
        if self._main_screen is None:
            self._main_screen = MainScreen(self.session, self.safety)

        container = self.query_one("#main-container", Container)
        await container.mount(self._main_screen)
        self._main_screen.focus()

    async def action_command_palette(self) -> None:
        if self._main_screen:
            await self._main_screen.action_command_palette()

    async def action_org_picker(self) -> None:
        orgs = self.session.list_orgs()
        if not orgs:
            self.notify("No orgs configured. Run 'sf org login web' first.", severity="warning")
            return

        def on_org_selected(alias: str) -> None:
            self.run_worker(self._switch_org(alias))

        self.push_screen(OrgPickerScreen(orgs, self.session.current_alias), on_org_selected)

    async def _switch_org(self, alias: str) -> None:
        try:
            await self.session.switch_org(alias)
            self.notify(f"Switched to org: {alias}", severity="information")
            if self._main_screen:
                await self._main_screen.refresh_data()
            status_bar = self.query_one("#status-bar", StatusBar)
            status_bar.update_org(self.session.current_org)
        except Exception as e:
            self.notify(f"Failed to switch org: {e}", severity="error")

    async def action_refresh(self) -> None:
        if self._main_screen:
            await self._main_screen.refresh_data()
            self.notify("Refreshed", severity="information", timeout=2)

    async def action_escape(self) -> None:
        if self._main_screen:
            await self._main_screen.action_escape()

    async def on_unmount(self) -> None:
        self.safety.stop_monitoring()
        await self.session.close()
        await self.safety.close()


def main() -> None:
    app = TCRMApp()
    app.run()


if __name__ == "__main__":
    main()

