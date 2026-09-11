"""MCP server commands."""

import asyncio
import typer
from asftool.cli.ui import print_info, print_success, print_error

app = typer.Typer(help="MCP server (Model Context Protocol)")


@app.command("start")
def start(
    transport: str = typer.Option("stdio", "--transport", "-t", help="Transport: stdio"),
):
    """Start MCP server over stdio transport."""
    _run(start_async(transport=transport))


def _run(coro):
    return asyncio.run(coro)


async def start_async(transport: str = "stdio"):
    if transport != "stdio":
        print_error("Only stdio transport is supported")
        raise typer.Exit(1)

    print_info("Starting MCP stdio server...")
    
    # Import and start the MCP server
    from asftool.agent.mcp_server import MCPServer
    from asftool.sdk.engine import ASFToolSDK
    
    # Create SDK (will need auth)
    sdk = ASFToolSDK(client=None)
    server = MCPServer(sdk=sdk)
    
    print_success("MCP server ready on stdio")
    await server.start_stdio()
