"""Status bar widget for bottom of TUI."""

from textual.widgets import Static

from tcrm_toolkit.interactive.safety import RiskLevel, SafetyResult
from tcrm_toolkit.interactive.session import OrgSession


class StatusBar(Static):
    """Bottom status bar showing org, safety, API usage, background tasks."""

    def __init__(self, **kwargs):
        super().__init__("", **kwargs)
        self._org: OrgSession | None = None
        self._safety: SafetyResult | None = None
        self._api_usage = "0/15,000"
        self._bg_tasks = 0

    def update_org(self, org: OrgSession | None) -> None:
        self._org = org
        self._render()

    def update_safety(self, safety: SafetyResult) -> None:
        self._safety = safety
        self._render()

    def update_api_usage(self, used: int, limit: int) -> None:
        self._api_usage = f"{used:,}/{limit:,}"
        self._render()

    def update_bg_tasks(self, count: int) -> None:
        self._bg_tasks = count
        self._render()

    def _render(self) -> None:
        parts = []

        if self._org:
            parts.append(f"Org: {self._org.alias} ({self._org.username})")
        else:
            parts.append("Org: Not connected")

        if self._safety:
            if self._safety.risk_level == RiskLevel.CRITICAL:
                parts.append("🔴 UNSAFE")
            elif self._safety.risk_level == RiskLevel.WARNING:
                parts.append("🟡 WARNING")
            else:
                parts.append("🟢 SAFE")
        else:
            parts.append("🟢 SAFE")

        parts.append(f"API: {self._api_usage}")

        if self._bg_tasks > 0:
            parts.append(f"BG: {self._bg_tasks} running")

        self.update("  •  ".join(parts))

