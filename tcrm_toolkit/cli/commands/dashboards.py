"""Dashboard CLI commands."""

import asyncio
from pathlib import Path

import typer

from tcrm_toolkit.cli.ui import (
    console,
    create_dashboard_table,
    print_dashboard_details,
    print_error,
    print_header,
    print_info,
    print_success,
    prompt_confirm,
)
from tcrm_toolkit.core import SalesforceClient, get_settings
from tcrm_toolkit.core.auth import SFCLIAuthService
from tcrm_toolkit.core.crypto import create_crypto_manager
from tcrm_toolkit.core.services.dashboard_service import DashboardService

app = typer.Typer(name="dashboards", help="Dashboard commands")


async def _get_client(alias: str = "default") -> SalesforceClient:
    """Get authenticated SalesforceClient."""
    settings = get_settings()
    crypto = create_crypto_manager()
    auth_service = SFCLIAuthService(settings, crypto)
    try:
        token = await auth_service.get_access_token(alias, auto_refresh=False)
        instance_url = await auth_service.get_instance_url(alias)
    except Exception as e:
        print_error(f"Authentication failed: {e}. Run 'tcrm auth login' first.")
        raise typer.Exit(1)
    return SalesforceClient(access_token=token, instance_url=instance_url, settings=settings)


@app.command("list")
def list_dashboards(
    page_size: int = typer.Option(50, "--page-size", "-n", help="Number of dashboards per page"),
    sort: str = typer.Option("Mru", "--sort", "-s", help="Sort order: Mru, Name, CreatedDate"),
    alias: str = typer.Option("default", "--alias", "-a", help="Org alias"),
) -> None:
    """List all dashboards."""
    asyncio.run(_list_dashboards_async(page_size, sort, alias))


async def _list_dashboards_async(page_size: int, sort: str, alias: str) -> None:
    """Async list dashboards implementation."""
    client = await _get_client(alias)
    async with client:
        service = DashboardService(client, get_settings())
        dashboards = await service.list_dashboards(page_size=page_size, sort=sort)
        table = create_dashboard_table(dashboards)
        console.print(table)


@app.command("get")
def get_dashboard(
    dashboard_id: str = typer.Argument(help="Dashboard ID"),
    alias: str = typer.Option("default", "--alias", "-a", help="Org alias"),
) -> None:
    """Get dashboard details."""
    asyncio.run(_get_dashboard_async(dashboard_id, alias))


async def _get_dashboard_async(dashboard_id: str, alias: str) -> None:
    """Async get dashboard implementation."""
    client = await _get_client(alias)
    async with client:
        service = DashboardService(client, get_settings())
        dashboard = await service.get_dashboard(dashboard_id)
        print_dashboard_details(dashboard)


@app.command("backup")
def backup_dashboard(
    dashboard_id: str = typer.Argument(help="Dashboard ID"),
    output: Path = typer.Option(
        None,
        "--output",
        "-o",
        help="Output JSON file path",
    ),
    alias: str = typer.Option("default", "--alias", "-a", help="Org alias"),
) -> None:
    """Backup dashboard JSON definition."""
    asyncio.run(_backup_dashboard_async(dashboard_id, output, alias))


async def _backup_dashboard_async(dashboard_id: str, output: Path | None, alias: str) -> None:
    """Async backup dashboard implementation."""
    if output is None:
        output = Path(f"{dashboard_id}_backup.json")

    print_header("Backup Dashboard", f"Dashboard: {dashboard_id} -> {output}")
    client = await _get_client(alias)
    async with client:
        service = DashboardService(client, get_settings())
        await service.backup_dashboard(dashboard_id, output)
        print_success(f"Dashboard backed up successfully to {output}")


@app.command("restore")
def restore_dashboard(
    backup_file: Path = typer.Argument(help="Backup JSON file"),
    new_name: str = typer.Option(None, "--name", "-n", help="New dashboard name"),
    alias: str = typer.Option("default", "--alias", "-a", help="Org alias"),
) -> None:
    """Restore dashboard from backup."""
    asyncio.run(_restore_dashboard_async(backup_file, new_name, alias))


async def _restore_dashboard_async(backup_file: Path, new_name: str | None, alias: str) -> None:
    """Async restore dashboard implementation."""
    print_header("Restore Dashboard", f"Backup: {backup_file}")
    client = await _get_client(alias)
    async with client:
        service = DashboardService(client, get_settings())
        dashboard = await service.restore_dashboard(backup_file, new_name)
        print_success(f"Dashboard restored successfully as {dashboard.label} (ID: {dashboard.id})")


@app.command("delete")
def delete_dashboard(
    dashboard_id: str = typer.Argument(help="Dashboard ID"),
    force: bool = typer.Option(False, "--force", "-f", help="Skip confirmation"),
    alias: str = typer.Option("default", "--alias", "-a", help="Org alias"),
) -> None:
    """Delete a dashboard."""
    asyncio.run(_delete_dashboard_async(dashboard_id, force, alias))


async def _delete_dashboard_async(dashboard_id: str, force: bool, alias: str) -> None:
    """Async delete dashboard implementation."""
    if not force:
        confirm = prompt_confirm(f"Delete dashboard {dashboard_id}? This cannot be undone.")
        if not confirm:
            print_info("Cancelled")
            return

    client = await _get_client(alias)
    async with client:
        service = DashboardService(client, get_settings())
        await service.delete_dashboard(dashboard_id)
        print_success(f"Dashboard {dashboard_id} deleted successfully")


@app.command("datasets")
def dashboard_datasets(
    dashboard_id: str = typer.Argument(help="Dashboard ID"),
    alias: str = typer.Option("default", "--alias", "-a", help="Org alias"),
) -> None:
    """List datasets used in a dashboard."""
    asyncio.run(_dashboard_datasets_async(dashboard_id, alias))


async def _dashboard_datasets_async(dashboard_id: str, alias: str) -> None:
    """Async dashboard datasets implementation."""
    client = await _get_client(alias)
    async with client:
        service = DashboardService(client, get_settings())
        datasets = await service.get_dashboard_datasets(dashboard_id)
        print_header("Dashboard Datasets", f"Dashboard: {dashboard_id}")
        if not datasets:
            print_info("No datasets found for this dashboard")
        else:
            for ds in datasets:
                print_info(f" - {ds.name} ({ds.id})")

