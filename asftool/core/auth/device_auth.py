"""Device Authorization Flow authentication service."""
from typing import Any

from asftool.core.config import Settings


class DeviceAuthService:
    """Salesforce Device Authorization Flow (headless)."""

    def __init__(self, settings: Settings):
        self.settings = settings

    async def start_flow(self, alias: str = "default") -> dict[str, Any]:
        return {"status": "pending", "device_code": "placeholder"}
