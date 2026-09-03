"""Enhanced notification manager with history and severity."""

from datetime import datetime
from typing import NamedTuple


class NotificationRecord(NamedTuple):
    message: str
    severity: str
    timestamp: datetime
    timeout: float | None


class NotificationManager:
    """Manages notification history and presentation."""

    def __init__(self, max_history: int = 100):
        self.max_history = max_history
        self.history: list[NotificationRecord] = []

    def record(self, message: str, severity: str = "information", timeout: float | None = None) -> NotificationRecord:
        """Record and return a notification item."""
        rec = NotificationRecord(
            message=message,
            severity=severity,
            timestamp=datetime.utcnow(),
            timeout=timeout,
        )
        self.history.append(rec)
        if len(self.history) > self.max_history:
            self.history.pop(0)
        return rec

    def get_history(self) -> list[NotificationRecord]:
        """Get notification history."""
        return list(self.history)

    def clear(self) -> None:
        """Clear notification history."""
        self.history.clear()
