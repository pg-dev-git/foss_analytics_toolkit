"""Authentication commands — SF CLI only.

The only authentication path is via the Salesforce CLI (`sf` or `sfdx`).
Web login (default) opens a browser; device login (`--device`) works
on headless/SSH environments.
"""

import asyncio

import typer
from rich.console import Console
from rich.table import Table

from asftool.cli.session import Session
from asftool.cli.ui import print_error, print_info, print_success, print_warning
from asftool.core.auth import SFCLIAuthError

app = typer.Typer(help="Authentication commands (SF CLI)")
console = Console()


def _run(coro):
    """Run async coroutine in sync context."""
    return asyncio.run(coro)


@app.command("login")
def login(
    alias: str = typer.Option("default", "--alias", "-a", help="Org alias"),
    instance_url: str | None = typer.Option(
        None, "--instance-url", "-r", help="Custom instance URL"
    ),
    device: bool = typer.Option(
        False, "--device", "-d", help="Use device code flow (headless)"
    ),
    timeout: int = typer.Option(
        300, "--timeout", "-t", help="Login timeout in seconds"
    ),
):
    """Authenticate via SF CLI web/device login."""
    session = Session(alias=alias)

    if not session.auth_service.sf_cli.is_available():
        print_error("SF CLI not found")
        print_info(
            "Install from: https://developer.salesforce.com/tools/sfdxcli"
        )
        raise typer.Exit(1)

    async def _login() -> None:
        try:
            if device:
                print_info(f"Starting SF CLI device login (alias: {alias})...")
                await session.auth_service.login_device(
                    alias=alias, instance_url=instance_url, timeout=timeout
                )
            else:
                print_info(f"Starting SF CLI web login (alias: {alias})...")
                print_warning("A browser window will open for authentication")
                await session.auth_service.login(
                    alias=alias, instance_url=instance_url, timeout=timeout
                )

            print_success("Authenticated successfully")
            instance = await session.auth_service.get_instance_url(alias=alias)
            username = await session.auth_service.get_username(alias=alias)
            print_info(f"Instance: {instance}")
            if username:
                print_info(f"User: {username}")
        except SFCLIAuthError as e:
            print_error(f"Login failed: {e}")
            raise typer.Exit(1) from e
        finally:
            await session.close()

    _run(_login())


@app.command("logout")
def logout(
    alias: str = typer.Argument("default", help="Org alias to logout"),
):
    """Remove stored authentication for an org."""
    session = Session(alias=alias)

    async def _logout() -> None:
        try:
            if await session.auth_service.logout(alias=alias):
                print_success(f"Logged out alias '{alias}'")
            else:
                print_warning(f"No stored credentials for alias '{alias}'")
        except SFCLIAuthError as e:
            print_error(f"Logout failed: {e}")
            raise typer.Exit(1) from e
        finally:
            await session.close()

    _run(_logout())


@app.command("status")
def status(
    alias: str = typer.Option("default", "--alias", "-a", help="Org alias to check"),
):
    """Check authentication status."""
    session = Session(alias=alias)

    async def _status() -> None:
        try:
            status_info = await session.auth_service.status(alias=alias)
            if status_info["authenticated"]:
                print_success(f"Authenticated: {status_info['alias']}")
                if status_info.get("username"):
                    print_info(f"User: {status_info['username']}")
                print_info(f"Instance: {status_info['instance_url']}")
                if status_info["token_expired"]:
                    print_warning("Token expired (will auto-refresh on next use)")
                else:
                    print_info("Token valid")
                if status_info.get("expires_at"):
                    print_info(f"Expires: {status_info['expires_at']}")
            else:
                print_warning(status_info["message"])
            if not status_info.get("sf_cli_available", True):
                print_warning("SF CLI not available")
        except Exception as e:
            print_error(f"Status check failed: {e}")
            raise typer.Exit(1) from e
        finally:
            await session.close()

    _run(_status())


@app.command("list-orgs")
def list_orgs():
    """List all authorized orgs."""
    session = Session()

    async def _list() -> None:
        try:
            orgs = await session.auth_service.list_orgs()
            if not orgs:
                print_info("No authorized orgs found")
                return

            table = Table(title="Authorized Orgs", show_header=True)
            table.add_column("✓", style="green", width=3)
            table.add_column("Alias", style="cyan")
            table.add_column("Username", style="white")
            table.add_column("Instance", style="blue")

            for org in orgs:
                connected = "✓" if org.get("connectedStatus") == "Connected" else "✗"
                table.add_row(
                    connected,
                    org.get("alias", "N/A"),
                    org.get("username", "N/A"),
                    org.get("instanceUrl", "N/A"),
                )
            console.print(table)
        except Exception as e:
            print_error(f"Failed to list orgs: {e}")
            raise typer.Exit(1) from e
        finally:
            await session.close()

    _run(_list())
