"""ASFTool — main CLI entry point.

Two modes:
  - With a subcommand (`asftool auth login`, `asftool datasets list`, ...)
    → Typer dispatches to the matching command group.
  - Without a subcommand (`asftool` alone) → interactive menu loop
    ("always running OS" style, like the legacy FOSS_Toolkit.py).

The interactive menu loop is fully async. We detect "no subcommand" before
calling Typer so we can run the async menu in a fresh event loop.
"""

import asyncio
import sys

import typer
from rich.panel import Panel

# Subcommand groups
from asftool.cli.commands.auth import app as auth_app
from asftool.cli.commands.dashboards import app as dashboards_app
from asftool.cli.commands.dataflows import app as dataflows_app
from asftool.cli.commands.datasets import app as datasets_app
from asftool.cli.commands.jobs import app as jobs_app
from asftool.cli.menu import Menu, create_menus
from asftool.cli.session import Session
from asftool.cli.ui import console, print_error, print_header, print_info

app = typer.Typer(
    name="asftool",
    help="FOSS Analytics Tool for Salesforce TCRM",
    add_completion=False,
    no_args_is_help=True,  # subcommands-only when no menu loop
)

app.add_typer(auth_app, name="auth")
app.add_typer(datasets_app, name="datasets")
app.add_typer(dashboards_app, name="dashboards")
app.add_typer(dataflows_app, name="dataflows")
app.add_typer(jobs_app, name="jobs")


# ---------------------------------------------------------------------------
# Menu rendering (async, runs inside one event loop)
# ---------------------------------------------------------------------------


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
            f"[bold]ASFTool[/bold] — FOSS Analytics Tool for TCRM\n"
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

    while True:
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
    known = {"auth", "datasets", "dashboards", "dataflows", "jobs"}
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
