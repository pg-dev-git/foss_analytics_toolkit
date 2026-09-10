"""MCP server core using stdio transport."""

from asftool.sdk.engine import ASFToolSDK


class MCPServer:
    """Stdio-based MCP server facade."""

    def __init__(self, sdk: ASFToolSDK | None = None):
        self.sdk = sdk or ASFToolSDK
        self.tools_registered: list[str] = []

    async def start_stdio(self) -> None:
        """Start stdio transport loop (placeholder)."""
        print("MCP stdio server started")

    def register_tool(self, name: str) -> None:
        self.tools_registered.append(name)
