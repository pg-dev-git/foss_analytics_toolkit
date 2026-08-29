"""Safety modal for critical VPN/Proxy detection."""

from textual import on
from textual.app import ComposeResult
from textual.containers import Container, Vertical
from textual.screen import ModalScreen
from textual.widgets import Button, Checkbox, Label, Static

from tcrm_toolkit.interactive.safety import SafetyResult, RiskLevel


class SafetyModalScreen(ModalScreen[str]):
    """Modal dialog for critical safety alerts."""
    
    BINDINGS = [
        ("escape", "cancel", "Cancel"),
    ]
    
    def __init__(self, safety_result: SafetyResult):
        super().__init__()
        self.safety_result = safety_result
        self._dont_show_again = False
    
    def compose(self) -> ComposeResult:
        details_lines = []
        for check in self.safety_result.checks.values():
            if not check.passed:
                icon = "🔴" if check.risk_level == RiskLevel.CRITICAL else "🟡"
                details_lines.append(f"{icon} {check.name.value}: {check.details}")
                if check.remediation:
                    details_lines.append(f"   → {check.remediation}")
        
        details_text = "\n".join(details_lines) if details_lines else "Unknown risk detected"
        
        yield Container(
            Vertical(
                Static("⚠️ CONNECTION SAFETY ALERT", id="safety-title"),
                Static(
                    "Salesforce detects VPN/Proxy connections and will IMMEDIATELY disable your user.\n"
                    "Continuing risks permanent org lockout.",
                    id="safety-warning"
                ),
                Static(details_text, id="safety-details"),
                Checkbox("Don't show again this session", id="dont-show-checkbox"),
                Container(
                    Button("Disconnect VPN & Retry", id="retry-btn", variant="primary"),
                    Button("I Understand Risks - Continue", id="continue-btn", variant="warning"),
                    Button("Quit", id="quit-btn", variant="error"),
                    id="safety-buttons"
                ),
                id="safety-container"
            ),
            id="safety-dialog"
        )
    
    @on(Button.Pressed, "#retry-btn")
    def on_retry(self) -> None:
        self.dismiss("retry")
    
    @on(Button.Pressed, "#continue-btn")
    def on_continue(self) -> None:
        checkbox = self.query_one("#dont-show-checkbox", Checkbox)
        self._dont_show_again = checkbox.value
        self.dismiss("continue")
    
    @on(Button.Pressed, "#quit-btn")
    def on_quit(self) -> None:
        self.dismiss("quit")
    
    def action_cancel(self) -> None:
        self.dismiss("quit")

