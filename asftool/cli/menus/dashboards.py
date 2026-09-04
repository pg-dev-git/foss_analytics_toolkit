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

    dashboard_id = prompt_text("Dashboard ID")
    if not dashboard_id:
        return
    output = prompt_text("Output JSON file path", default="./dashboard_backup.json")
    try:
        await backup_dashboard_async(
            dashboard_id=dashboard_id, output=Path(output)
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
