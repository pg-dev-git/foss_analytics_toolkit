"""Safety modal for critical VPN/Proxy detection with hard block."""

from __future__ import annotations

from textual import on
from textual.app import ComposeResult
from textual.containers import Container, Vertical
from textual.screen import ModalScreen
from textual.widgets import Button, Static

from tcrm_toolkit.interactive.safety import RiskLevel, SafetyResult


class SafetyModalScreen(ModalScreen[str]):
    """Modal dialog for critical safety alerts with hard block."""

    BINDINGS = [
        ("escape", "cancel", "Quit"),
    ]

    def __init__(self, safety_result: SafetyResult):
        super().__init__()
        self.safety_result = safety_result

    def compose(self) -> ComposeResult:
        details_lines = []
        for check in self.safety_result.checks.values():
            if not check.passed:
                icon = "🔴" if check.risk_level == RiskLevel.CRITICAL else "🟡"
                details_lines.append(f"{icon} {check.name.value}: {check.details}")
                if check.remediation:
                    details_lines.append(f"   → {check.remediation}")

        details_text = "\n".join(details_lines) if details_lines else "Critical security risk detected"

        yield Container(
            Vertical(
                Static("⚠️ CRITICAL CONNECTION SAFETY ALERT", id="safety-title"),
                Static(
                    "Salesforce immediately disables users detected on VPN/Proxy.\n"
                    "All API operations are strictly blocked until your connection is secure.",
                    id="safety-warning"
                ),
                Static(details_text, id="safety-details"),
                Container(
                    Button("Disconnect VPN & Retry", id="retry-btn", variant="primary"),
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

    @on(Button.Pressed, "#quit-btn")
    def on_quit(self) -> None:
        self.dismiss("quit")

    def action_cancel(self) -> None:
        self.dismiss("quit")

