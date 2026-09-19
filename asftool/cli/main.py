"""ASFTool — main CLI entry point.

Two modes:
  - With a subcommand (`asftool auth login`, `asftool datasets list`, ...)
    → Typer dispatches to the matching command group.
  - Without a subcommand (`asftool` alone) → interactive menu loop
    ("always running OS" style, like the legacy FOSS_Toolkit.py).

The interactive menu loop is fully async. We detect "no subcommand" before
calling Typer so we can run the async menu in a fresh event loop.
"""

# ponytail: PYTHONIOENCODING (env layer) + sys.stdout.reconfigure (stdlib).
# If emoji (Analytics REST API response label: 'Canadian Sales 🇨🇦') causes encoding errors,
# root fix = force UTF-8 at CLI startup, not filter the data.
import os
import sys

from dotenv import load_dotenv

if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except (AttributeError, TypeError):
        pass  # Python < 3.7: PYTHONIOENCODING handles it.

# Load .env and set PYTHONIOENCODING before any module creates a console
load_dotenv(dotenv_path=".env", override=False)
os.environ.setdefault("PYTHONIOENCODING", "utf-8")

# ruff: noqa: E402 - imports below must come after encoding setup
import asyncio
from typing import Literal

import typer
from rich.panel import Panel

# Subcommand groups
from asftool.cli.commands.auth import app as auth_app
from asftool.cli.commands.config import app as config_app
from asftool.cli.commands.dashboards import app as dashboards_app
from asftool.cli.commands.dataflows import app as dataflows_app
from asftool.cli.commands.datasets import app as datasets_app
from asftool.cli.commands.doctor import app as doctor_app
from asftool.cli.commands.fields import app as fields_app
from asftool.cli.commands.git_auth import app as git_auth_app
from asftool.cli.commands.git import app as git_app
from asftool.cli.commands.jobs import app as jobs_app
from asftool.cli.commands.lineage import app as lineage_app
from asftool.cli.commands.mcp import app as mcp_app
from asftool.cli.menu import Menu, create_menus
from asftool.cli.session import Session, get_current_session_alias, set_current_session_alias
from asftool.cli.ui import console, print_error, print_header, print_info, print_warning

app = typer.Typer(
    name="asftool",
    help="ASFTool — Analytics REST API Software Tool (ASFT): lightweight CLI for Salesforce Analytics REST API",
    add_completion=False,
    no_args_is_help=True,  # subcommands-only when no menu loop
)

app.add_typer(auth_app, name="auth")
app.add_typer(datasets_app, name="datasets")
app.add_typer(dashboards_app, name="dashboards")
app.add_typer(dataflows_app, name="dataflows")
app.add_typer(jobs_app, name="jobs")
app.add_typer(doctor_app, name="doctor")
app.add_typer(fields_app, name="fields")
app.add_typer(config_app, name="config")
app.add_typer(lineage_app, name="lineage")
app.add_typer(mcp_app, name="mcp")
app.add_typer(git_auth_app, name="git-auth")
app.add_typer(git_app, name="git")


# ---------------------------------------------------------------------------
# Menu rendering (async, runs inside one event loop)
# ---------------------------------------------------------------------------


from dataclasses import dataclass


@dataclass
class SelectedOrg:
    """Result of org selection prompt."""
    alias: str
    username: str | None
    instance_url: str | None


async def _prompt_sf_cli_org_selection(session: Session) -> SelectedOrg | None | Literal["login"]:
    """
    Prompt user to select an SF CLI authenticated org on startup.

    Returns:
        - SelectedOrg: user selected an existing org
        - None: user chose to continue without selecting
        - "login": user wants to start a new web login flow
    """
    # Check if SF CLI is available
    if not session.auth_service.sf_cli.is_available():
        return None

    # Get authenticated orgs from SF CLI
    orgs = await session.get_sf_cli_orgs()
    if not orgs:
        console.print()
        print_warning("No authenticated orgs found in SF CLI")
        console.print("Options:")
        console.print("  [l] Start new web login")
        console.print("  [c] Continue without SF CLI session")
        console.print()
        try:
            choice = console.input("Select [l/c]: ").strip().lower()
        except (EOFError, KeyboardInterrupt):
            console.print("\n[yellow]Exiting...[/yellow]")
            return None
        if choice == "l":
            return "login"
        return None

    # Display numbered list of orgs
    console.print()
    print_header("SF CLI Authenticated Orgs")
    console.print("Found the following authenticated orgs in SF CLI:")
    console.print()

    for i, org in enumerate(orgs, 1):
        username = org.get("username", "unknown")
        instance = org.get("instance_url", "unknown")
        console.print(f"  [cyan]{i}[/cyan]  [bold]{org['alias']}[/bold] — {username} — {instance}")

    console.print()
    console.print("Options:")
    for i in range(1, len(orgs) + 1):
        console.print(f"  [{i}] Use this org")
    console.print("  [c] Continue without choosing")
    console.print("  [l] Start new web login")
    console.print()

    while True:
        try:
            choice = console.input("Select option: ").strip().lower()
        except (EOFError, KeyboardInterrupt):
            console.print("\n[yellow]Exiting...[/yellow]")
            return None
        
        if choice == "c":
            return None
        elif choice == "l":
            return "login"
        elif choice.isdigit():
            idx = int(choice) - 1
            if 0 <= idx < len(orgs):
                selected = orgs[idx]
                return SelectedOrg(
                    alias=selected["alias"],
                    username=selected.get("username"),
                    instance_url=selected.get("instance_url"),
                )
        
        console.print("[red]Invalid choice. Please try again.[/red]")


async def _session_status_text(session: Session) -> str:
    """Get a one-line status string for the current session."""
    try:
        s = await session.auth_service.status(session.alias)
        if s["authenticated"]:
            username = s.get("username") or "unknown"
            instance = s.get("instance_url") or "N/A"
            token = "✓ Valid" if not s["token_expired"] else "⚠ Expired"
            return f"[green]{username}[/green] • {instance} • Token: {token}"
        return f"[yellow]{s.get('message', 'Not authenticated')}[/yellow]"
    except Exception as e:
        return f"[red]Status error: {e}[/red]"


def _render_session_header(status_text: str, alias: str) -> None:
    """Print the session panel. (Sync — no I/O.)"""
    console.print(
        Panel.fit(
            f"[bold]ASFTool[/bold] — Analytics REST API Software Tool (ASFT)\n"
            f"Session: {status_text}\n"
            f"Org alias: [cyan]{alias}[/cyan]",
            title="[cyan]Session[/cyan]",
            border_style="cyan",
        )
    )


def _render_menu(menu: Menu) -> None:
    """Print a menu with its items. (Sync.)"""
    print_header(menu.title)
    if menu.subtitle:
        print_info(menu.subtitle)
    console.print()

    for item in menu.items:
        if item.submenu:
            console.print(f"  [cyan]{item.key}[/cyan]  {item.label} ▸")
        else:
            console.print(f"  [cyan]{item.key}[/cyan]  {item.label}")
    console.print()


async def _prompt_choice(menu: Menu) -> str:
    """Async-safe prompt using Rich's async console."""
    valid_keys = [item.key for item in menu.items]
    prompt = f"Select [{'/'.join(valid_keys)}] (b=back, q=quit): "
    # Rich's console.input is sync but doesn't block the loop in any meaningful
    # way for an interactive terminal. If we ever embed in pytest, we'll want
    # a non-blocking variant.
    return console.input(prompt)


async def _run_menu_loop(main_menu: Menu) -> None:
    """The interactive menu loop (fully async)."""
    session = Session()
    current: Menu = main_menu
    first_run = True

    while True:
        # On first run, check for SF CLI authenticated orgs and prompt user (config-controlled)
        if first_run:
            from asftool.core.config import get_settings
            first_run = False
            if get_settings().auto_import_sf_cli_session:
                selection = await _prompt_sf_cli_org_selection(session)
                
                if selection == "login":
                    # User wants to start a new web login
                    from asftool.cli.commands.auth import login_async
                    try:
                        await login_async(alias=session.alias)
                    except KeyboardInterrupt:
                        console.print("\n[yellow]Login cancelled[/yellow]")
                    except Exception as e:
                        print_error(f"Login failed: {e}")
                elif isinstance(selection, SelectedOrg):
                    # User selected an existing org - import it
                    session.alias = selection.alias
                    set_current_session_alias(selection.alias)  # Set context for menu handlers
                    try:
                        await session.auth_service.import_sf_cli_session(selection.alias)
                        console.print(f"[green]Imported SF CLI session for '{selection.alias}'[/green]")
                    except Exception as e:
                        print_warning(f"Could not import SF CLI session: {e}")
                # If None (continue without choosing), just proceed with current session

        status_text = await _session_status_text(session)
        _render_session_header(status_text, session.alias)
        _render_menu(current)

        try:
            raw = await _prompt_choice(current)
        except (EOFError, KeyboardInterrupt):
            console.print("\n[yellow]Exiting...[/yellow]")
            return

        choice = raw.strip().lower()
        if not choice:
            continue

        # Root-level "q" → exit
        if choice == "q" and current.parent is None:
            console.print("[dim]Goodbye.[/dim]")
            return

        # "b" → back to parent (only if there's a parent)
        if choice == "b" and current.parent is not None:
            current = current.parent
            continue

        # Submenu navigation
        sub = next(
            (i.submenu for i in current.items if i.submenu and i.key.lower() == choice),
            None,
        )
        if sub is not None:
            current = sub
            continue

        # Leaf handler
        item = next(
            (
                i
                for i in current.items
                if not i.submenu and i.key.lower() == choice
            ),
            None,
        )
        if item is None:
            console.print(f"[red]Invalid choice: {choice}[/red]")
            continue

        if item.handler is not None:
            try:
                await item.handler()
            except KeyboardInterrupt:
                console.print("\n[yellow]Cancelled[/yellow]")
            except Exception as e:
                print_error(f"Error: {e}")
            console.print()

        if item.exit_after and current.parent is not None:
            current = current.parent


def _has_subcommand(argv: list[str]) -> bool:
    """True if argv contains a known subcommand (not just --flags)."""
    known = {"auth", "datasets", "dashboards", "dataflows", "jobs", "doctor", "fields", "config", "lineage", "mcp", "git-auth", "git"}
    for arg in argv[1:]:
        if arg in known:
            return True
        if not arg.startswith("-"):
            # First non-flag arg that's not a known subcommand = treat as menu
            return False
    return False


def main() -> None:
    """Entry point: dispatch to subcommand or run interactive menu loop."""
    if not _has_subcommand(sys.argv):
        main_menu, _ = create_menus()
        asyncio.run(_run_menu_loop(main_menu))
        return

    app()


if __name__ == "__main__":
    main()
