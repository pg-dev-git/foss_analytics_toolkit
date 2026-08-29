"""Login screen for initial authentication."""

from textual import on, work
from textual.app import ComposeResult
from textual.containers import Container, Vertical
from textual.screen import ModalScreen
from textual.widgets import Button, Input, Static

from tcrm_toolkit.interactive.session import SessionManager


class LoginScreen(ModalScreen[bool]):
    """Modal screen for SF CLI web login."""

    BINDINGS = [
        ("escape", "cancel", "Cancel"),
    ]

    def __init__(self, session: SessionManager):
        super().__init__()
        self.session = session
        self._alias = "default"
        self._instance_url = None

    def compose(self) -> ComposeResult:
        yield Container(
            Vertical(
                Static("🔐 Salesforce Authentication", id="login-title"),
                Static(
                    "Choose authentication method:\n"
                    "• Browser Login: Opens local browser (desktop environments).\n"
                    "• Device Code Login: Generates code & URL for headless VMs / SSH sessions.",
                    id="login-info"
                ),
                Input(placeholder="Org alias (default)", id="alias-input", value="default"),
                Input(placeholder="Custom instance URL (optional)", id="instance-input"),
                Static("", id="login-status"),
                Button("Login with Browser (Web)", id="login-btn", variant="primary"),
                Button("Login with Device Code (Headless)", id="device-login-btn", variant="warning"),
                Button("Cancel", id="cancel-btn", variant="default"),
                id="login-form"
            ),
            id="login-container"
        )

    @on(Button.Pressed, "#login-btn")
    async def on_login_pressed(self) -> None:
        alias_input = self.query_one("#alias-input", Input)
        instance_input = self.query_one("#instance-input", Input)
        status = self.query_one("#login-status", Static)
        login_btn = self.query_one("#login-btn", Button)
        device_btn = self.query_one("#device-login-btn", Button)

        self._alias = alias_input.value or "default"
        self._instance_url = instance_input.value or None

        login_btn.disabled = True
        device_btn.disabled = True
        status.update("🔄 Opening browser for authentication...")

        try:
            await self._run_login()
            self.dismiss(True)
        except Exception as e:
            status.update(f"❌ Login failed: {e}")
            login_btn.disabled = False
            device_btn.disabled = False

    @on(Button.Pressed, "#device-login-btn")
    async def on_device_login_pressed(self) -> None:
        alias_input = self.query_one("#alias-input", Input)
        instance_input = self.query_one("#instance-input", Input)
        status = self.query_one("#login-status", Static)
        login_btn = self.query_one("#login-btn", Button)
        device_btn = self.query_one("#device-login-btn", Button)

        self._alias = alias_input.value or "default"
        self._instance_url = instance_input.value or None

        login_btn.disabled = True
        device_btn.disabled = True
        status.update("🔄 Requesting device code... Check terminal output for URL/code.")

        try:
            await self._run_device_login()
            self.dismiss(True)
        except Exception as e:
            status.update(f"❌ Device login failed: {e}")
            login_btn.disabled = False
            device_btn.disabled = False

    @work(exclusive=True)
    async def _run_login(self) -> None:
        status = self.query_one("#login-status", Static)

        token = await self.session.login(
            alias=self._alias,
            instance_url=self._instance_url,
        )

        status.update(f"✅ Authenticated as {self.session.current_org.username if self.session.current_org else 'user'}")

    @work(exclusive=True)
    async def _run_device_login(self) -> None:
        status = self.query_one("#login-status", Static)

        token = await self.session.login_device(
            alias=self._alias,
            instance_url=self._instance_url,
        )

        status.update(f"✅ Authenticated via device flow as {self.session.current_org.username if self.session.current_org else 'user'}")

    @on(Button.Pressed, "#cancel-btn")
    def on_cancel_pressed(self) -> None:
        self.dismiss(False)

    def action_cancel(self) -> None:
        self.dismiss(False)

