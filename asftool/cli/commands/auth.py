"""Authentication commands — SF CLI only.

The only authentication path is via the Salesforce CLI (`sf` or `sfdx`).
Web login (default) opens a browser; device login (`--device`) works
on headless/SSH environments.

Each command exposes both:
  - A Typer command (`login`, `logout`, ...) callable from the CLI.
  - An `*_async` wrapper with the actual coroutine, callable from
    the interactive menu (which already runs an asyncio event loop).

The Typer command bodies are thin shims that call `_run(_async_fn())`
so the async logic lives in exactly one place.
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
    """Run async coroutine in a fresh event loop (used only by Typer commands)."""
    return asyncio.run(coro)


# ---------------------------------------------------------------------------
# Async wrappers (used by the interactive menu and by the Typer commands below)
# ---------------------------------------------------------------------------


async def login_async(
    alias: str = "default",
    instance_url: str | None = None,
    device: bool = False,
    timeout: int = 300,
    force: bool = False,
) -> None:
    """Authenticate via SF CLI web (default) or device flow.

    If SF CLI already has an authenticated session for the alias, it will be
    imported automatically unless --force is specified.
    """
    session = Session(alias=alias)

    if not session.auth_service.sf_cli.is_available():
        print_error("SF CLI not found")
        print_info(
            "Install from: https://developer.salesforce.com/tools/sfdxcli"
        )
        raise typer.Exit(1)

    # Check if SF CLI already has an authenticated session
    if not force:
        sf_cli_auth = await session.auth_service.check_sf_cli_auth(alias=alias)
        if sf_cli_auth["authenticated"]:
            print_info(f"Found existing SF CLI session for '{alias}' (User: {sf_cli_auth['username']})")
            print_info("Importing existing session...")
            try:
                await session.auth_service.import_sf_cli_session(alias=alias)
                print_success("Successfully imported existing SF CLI session")
                instance = await session.auth_service.get_instance_url(alias=alias)
                username = await session.auth_service.get_username(alias=alias)
                print_info(f"Instance: {instance}")
                if username:
                    print_info(f"User: {username}")
                return
            except SFCLIAuthError as e:
                print_warning(f"Could not import existing session: {e}")
                print_info("Proceeding with new login...")

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


async def logout_async(alias: str = "default") -> None:
    """Remove stored authentication for an org."""
    session = Session(alias=alias)

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


async def status_async(alias: str = "default") -> None:
    """Check authentication status."""
    session = Session(alias=alias)

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


async def list_orgs_async() -> None:
    """List all authorized orgs."""
    session = Session()

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


async def check_auth_async(alias: str = "default") -> None:
    """Check if SF CLI has an authenticated session for the alias."""
    session = Session(alias=alias)

    try:
        result = await session.auth_service.check_sf_cli_auth(alias=alias)
        if result["authenticated"]:
            print_success(f"SF CLI authenticated: {result['alias']}")
            if result.get("username"):
                print_info(f"User: {result['username']}")
            print_info(f"Instance: {result['instance_url']}")
            if result.get("has_valid_token"):
                print_info("Valid access token available")
            else:
                print_warning("Access token could not be retrieved")
        else:
            print_warning(result["message"])
    except Exception as e:
        print_error(f"Auth check failed: {e}")
        raise typer.Exit(1) from e
    finally:
        await session.close()


# ---------------------------------------------------------------------------
# Typer commands (thin shims that call the async wrappers)
# ---------------------------------------------------------------------------


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
    force: bool = typer.Option(
        False, "--force", "-f", help="Force new login even if SF CLI session exists"
    ),
):
    """Authenticate via SF CLI web/device login."""
    _run(
        login_async(
            alias=alias, instance_url=instance_url, device=device, timeout=timeout, force=force
        )
    )


@app.command("logout")
def logout(
    alias: str = typer.Argument("default", help="Org alias to logout"),
):
    """Remove stored authentication for an org."""
    _run(logout_async(alias=alias))


@app.command("status")
def status(
    alias: str = typer.Option("default", "--alias", "-a", help="Org alias to check"),
):
    """Check authentication status."""
    _run(status_async(alias=alias))


@app.command("list-orgs")
def list_orgs():
    """List all authorized orgs."""
    _run(list_orgs_async())

@app.command("import-sf")
def import_sf(alias: str = typer.Option("default", "--alias", "-a", help="SF CLI alias")):
    _run(import_sf_async(alias=alias))


@app.command("check-auth")
def check_auth(
    alias: str = typer.Option("default", "--alias", "-a", help="Org alias to check"),
):
    """Check if SF CLI has an authenticated session for the alias."""
    _run(check_auth_async(alias=alias))

async def import_sf_async(alias: str = "default") -> None:
    """Import an existing SF CLI authenticated session into the token store."""
    session = Session(alias=alias)
    try:
        # Import from SF CLI session using the new method
        access_token = await session.auth_service.import_sf_cli_session(alias=alias)
        print_success(f"Successfully imported SF CLI session for '{alias}'")
        instance = await session.auth_service.get_instance_url(alias=alias)
        username = await session.auth_service.get_username(alias=alias)
        print_info(f"Instance: {instance}")
        if username:
            print_info(f"User: {username}")
        print_info("Session is now available for use with asftool commands")
    except SFCLIAuthError as e:
        print_error(f"Import failed: {e}")
        raise typer.Exit(1) from e
    except Exception as e:
        print_error(f"Import failed: {e}")
        raise typer.Exit(1) from e
    finally:
        await session.close()

