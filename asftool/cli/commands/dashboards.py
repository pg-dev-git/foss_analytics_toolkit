"""Dashboard CLI commands.

List, backup, and inspect dashboards. Delegates to DashboardService for
all API work. The same async functions are reused by the interactive
menu (asftool/cli/menus/dashboards.py).

Each command exposes both:
  - A Typer command (``list``, ``backup``, ...) callable from the CLI.
  - An ``*_async`` wrapper with the actual coroutine, callable from
    the interactive menu (which already runs an asyncio event loop).

The Typer command bodies are thin shims that call ``_run(_async_fn())``
so the async logic lives in exactly one place.
"""

import asyncio
from pathlib import Path

import typer
from rich.table import Table

from asftool.cli.session import Session
from asftool.cli.ui import console, print_error, print_header, print_info, print_success
from asftool.core.exceptions import DashboardError
from asftool.core.services import DashboardService

app = typer.Typer(help="Dashboard operations")


def _run(coro):
    """Run async coroutine in a fresh event loop (used only by Typer commands)."""
    return asyncio.run(coro)


# ---------------------------------------------------------------------------
# Async wrappers (used by the interactive menu and by the Typer commands below)
# ---------------------------------------------------------------------------


async def list_dashboards_async(
    page_size: int = 50, sort: str = "Mru"
) -> None:
    """List all dashboards."""
    session = Session()
    try:
        async with session.client_context() as client:
            service = DashboardService(client, session.settings)
            dashboards = await service.list_dashboards(
                page_size=page_size, sort=sort
            )

            if not dashboards:
                print_info("No dashboards found")
                return

            table = Table(title="Dashboards", show_header=True)
            table.add_column("ID", style="cyan")
            table.add_column("Name", style="white")
            table.add_column("Folder", style="blue")
            table.add_column("Updated", style="dim")

            for db in dashboards:
                table.add_row(
                    db.id,
                    db.name,
                    db.folder_name or "N/A",
                    db.last_modified_date.strftime("%Y-%m-%d")
                    if db.last_modified_date
                    else "N/A",
                )
            console.print(table)
            print_info(f"Total: {len(dashboards)} dashboards")
    except Exception as e:
        print_error(f"List failed: {e}")
        raise typer.Exit(1) from e
    finally:
        await session.close()


async def backup_dashboard_async(dashboard_id: str, output: Path) -> None:
    """Backup dashboard JSON definition."""
    session = Session()
    try:
        async with session.client_context() as client:
            service = DashboardService(client, session.settings)
            backup = await service.backup_dashboard(
                dashboard_id=dashboard_id, output_path=output
            )
            print_success(
                f"Dashboard '{backup.dashboard_name}' backed up to {output}"
            )
    except DashboardError as e:
        print_error(f"Backup failed: {e}")
        raise typer.Exit(1) from e
    except Exception as e:
        print_error(f"Backup failed: {e}")
        raise typer.Exit(1) from e
    finally:
        await session.close()


async def show_dashboard_async(dashboard_id: str) -> None:
    """Show dashboard details."""
    session = Session()
    try:
        async with session.client_context() as client:
            service = DashboardService(client, session.settings)
            db = await service.get_dashboard(dashboard_id)
            datasets = await service.get_dashboard_datasets(dashboard_id)

            print_header(f"Dashboard: {db.name}")
            print_info(f"ID: {db.id}")
            print_info(f"Label: {db.label}")
            print_info(f"Folder: {db.folder_name or 'N/A'}")
            if datasets:
                names = ", ".join(d.name for d in datasets)
                print_info(f"Datasets: {names}")
            else:
                print_info("Datasets: (none)")
            print_info(f"Created: {db.created_date}")
            print_info(f"Updated: {db.last_modified_date}")
    except Exception as e:
        print_error(f"Show failed: {e}")
        raise typer.Exit(1) from e
    finally:
        await session.close()


# ---------------------------------------------------------------------------
# Typer commands (thin shims that call the async wrappers)
# ---------------------------------------------------------------------------


@app.command("list")
def list_dashboards(
    page_size: int = typer.Option(50, "--page-size", help="Page size"),
    sort: str = typer.Option("Mru", "--sort", help="Sort order: Mru, Name, CreatedDate"),
):
    """List all dashboards."""
    _run(list_dashboards_async(page_size=page_size, sort=sort))


@app.command("backup")
def backup_dashboard(
    dashboard_id: str = typer.Argument(..., help="Dashboard ID"),
    output: Path = typer.Option(
        Path.cwd() / "dashboard_backup.json",
        "--output",
        "-o",
        help="Output JSON file path",
    ),
):
    """Backup dashboard JSON definition."""
    _run(backup_dashboard_async(dashboard_id=dashboard_id, output=output))


@app.command("show")
def show_dashboard(
    dashboard_id: str = typer.Argument(..., help="Dashboard ID"),
):
    """Show dashboard details."""
    _run(show_dashboard_async(dashboard_id=dashboard_id))
