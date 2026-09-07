"""Dashboard menu — wired to Phase 5 dashboard commands."""

from pathlib import Path

import typer

from asftool.cli.menu import Menu, MenuItem
from asftool.cli.ui import prompt_text


async def list_dashboards() -> None:
    from asftool.cli.commands.dashboards import list_dashboards_async

    try:
        await list_dashboards_async()
    except typer.Exit:
        pass


async def backup_dashboard() -> None:
    from asftool.cli.commands.dashboards import backup_dashboard_async
    from asftool.cli.ui import prompt_confirm
    from asftool.core.config import get_settings
    from asftool.core.storage import get_storage_manager

    dashboard_id = prompt_text("Dashboard ID")
    if not dashboard_id:
        return

    # Get dashboard name for path generation
    from asftool.cli.session import Session
    from asftool.core.services import DashboardService
    session = Session()
    try:
        async with session.client_context() as client:
            service = DashboardService(client, session.settings)
            dashboard = await service.get_dashboard(dashboard_id)
            dashboard_name = dashboard.name
    except Exception:
        dashboard_name = "dashboard"
    finally:
        await session.close()

    # Generate default path using StorageManager
    settings = get_settings()
    storage = get_storage_manager(settings)
    default_path = storage.dashboard_path(
        alias=session.alias,
        dashboard_name=dashboard_name,
        dashboard_id=dashboard_id,
    )

    # Present path to user and ask for confirmation/override
    from asftool.cli.ui import print_info
    print_info(f"Default output path: {default_path}")
    output = prompt_text("Output JSON path (press Enter to use default)", default=str(default_path))

    if not prompt_confirm(f"Save to {output}?"):
        print_info("Cancelled.")
        return

    try:
        await backup_dashboard_async(
            dashboard_id=dashboard_id, output=Path(output), alias=session.alias
        )
    except typer.Exit:
        pass


async def show_dashboard() -> None:
    from asftool.cli.commands.dashboards import show_dashboard_async

    dashboard_id = prompt_text("Dashboard ID")
    if not dashboard_id:
        return
    try:
        await show_dashboard_async(dashboard_id=dashboard_id)
    except typer.Exit:
        pass


def dashboard_operations(menu: Menu) -> None:
    menu.add(MenuItem("1", "List all dashboards", handler=list_dashboards))
    menu.add(MenuItem("2", "Backup dashboard JSON", handler=backup_dashboard))
    menu.add(MenuItem("3", "Show dashboard details", handler=show_dashboard))
    menu.add(MenuItem("b", "Back", exit_after=True))
