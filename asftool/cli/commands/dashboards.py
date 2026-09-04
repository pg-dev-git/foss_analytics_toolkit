"""Dashboard CLI commands.

List, backup, and inspect dashboards. Delegates to DashboardService for
all API work. The same functions are reused by the interactive menu
(asftool/cli/menus/dashboards.py).
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
    return asyncio.run(coro)


@app.command("list")
def list_dashboards(
    page_size: int = typer.Option(50, "--page-size", help="Page size"),
    sort: str = typer.Option("Mru", "--sort", help="Sort order: Mru, Name, CreatedDate"),
):
    """List all dashboards."""
    session = Session()

    async def _list() -> None:
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

    _run(_list())


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
    session = Session()

    async def _backup() -> None:
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

    _run(_backup())


@app.command("show")
def show_dashboard(
    dashboard_id: str = typer.Argument(..., help="Dashboard ID"),
):
    """Show dashboard details."""
    session = Session()

    async def _show() -> None:
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

    _run(_show())


# Async wrappers for the menu layer -----------------------------------------


async def list_dashboards_async() -> None:
    list_dashboards()


async def backup_dashboard_async(dashboard_id: str, output: Path) -> None:
    backup_dashboard(dashboard_id=dashboard_id, output=output)


async def show_dashboard_async(dashboard_id: str) -> None:
    show_dashboard(dashboard_id=dashboard_id)
