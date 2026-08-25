"""Dashboard CLI commands."""

import asyncio
from pathlib import Path

import typer

from tcrm_toolkit.cli.ui import (
    console,
    print_header,
    print_success,
    print_error,
    print_info,
    print_warning,
    prompt_confirm,
    create_dashboard_table,
    print_dashboard_details,
)
from tcrm_toolkit.core import get_settings

app = typer.Typer(name="dashboards", help="Dashboard commands")


@app.command("list")
def list_dashboards(
    page_size: int = typer.Option(50, "--page-size", "-n", help="Number of dashboards per page"),
    sort: str = typer.Option("Mru", "--sort", "-s", help="Sort order: Mru, Name, CreatedDate"),
) -> None:
    """List all dashboards."""
    asyncio.run(_list_dashboards_async(page_size, sort))


async def _list_dashboards_async(page_size: int, sort: str) -> None:
    """Async list dashboards implementation."""
    print_header("Dashboards", "Listing all TCRM dashboards")
    print_warning("Authentication integration pending - use 'tcrm auth login' first")


@app.command("get")
def get_dashboard(
    dashboard_id: str = typer.Argument(help="Dashboard ID"),
) -> None:
    """Get dashboard details."""
    asyncio.run(_get_dashboard_async(dashboard_id))


async def _get_dashboard_async(dashboard_id: str) -> None:
    """Async get dashboard implementation."""
    print_header("Dashboard Details", f"Dashboard ID: {dashboard_id}")
    print_warning("Authentication integration pending")


@app.command("backup")
def backup_dashboard(
    dashboard_id: str = typer.Argument(help="Dashboard ID"),
    output: Path = typer.Option(
        None,
        "--output",
        "-o",
        help="Output JSON file path",
    ),
) -> None:
    """Backup dashboard JSON definition."""
    asyncio.run(_backup_dashboard_async(dashboard_id, output))


async def _backup_dashboard_async(dashboard_id: str, output: Path | None) -> None:
    """Async backup dashboard implementation."""
    if output is None:
        output = Path(f"{dashboard_id}_backup.json")

    print_header("Backup Dashboard", f"Dashboard: {dashboard_id} -> {output}")
    print_warning("Authentication integration pending")


@app.command("restore")
def restore_dashboard(
    backup_file: Path = typer.Argument(help="Backup JSON file"),
    new_name: str = typer.Option(None, "--name", "-n", help="New dashboard name"),
) -> None:
    """Restore dashboard from backup."""
    asyncio.run(_restore_dashboard_async(backup_file, new_name))


async def _restore_dashboard_async(backup_file: Path, new_name: str | None) -> None:
    """Async restore dashboard implementation."""
    print_header("Restore Dashboard", f"Backup: {backup_file}")
    print_warning("Authentication integration pending")


@app.command("delete")
def delete_dashboard(
    dashboard_id: str = typer.Argument(help="Dashboard ID"),
    force: bool = typer.Option(False, "--force", "-f", help="Skip confirmation"),
) -> None:
    """Delete a dashboard."""
    asyncio.run(_delete_dashboard_async(dashboard_id, force))


async def _delete_dashboard_async(dashboard_id: str, force: bool) -> None:
    """Async delete dashboard implementation."""
    if not force:
        confirm = prompt_confirm(f"Delete dashboard {dashboard_id}? This cannot be undone.")
        if not confirm:
            print_info("Cancelled")
            return

    print_header("Delete Dashboard", f"Dashboard: {dashboard_id}")
    print_warning("Authentication integration pending")


@app.command("datasets")
def dashboard_datasets(
    dashboard_id: str = typer.Argument(help="Dashboard ID"),
) -> None:
    """List datasets used in a dashboard."""
    asyncio.run(_dashboard_datasets_async(dashboard_id))


async def _dashboard_datasets_async(dashboard_id: str) -> None:
    """Async dashboard datasets implementation."""
    print_header("Dashboard Datasets", f"Dashboard: {dashboard_id}")
    print_warning("Authentication integration pending")