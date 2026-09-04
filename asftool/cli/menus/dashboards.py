"""Dashboard menu — stub for Phase 3. Full implementation in Phase 5."""

from asftool.cli.menu import Menu, MenuItem
from asftool.cli.ui import print_warning


async def _stub(operation: str) -> None:
    print_warning(f"{operation} — coming in Phase 5 (Dashboard Operations)")


async def list_dashboards() -> None:
    await _stub("List dashboards")


async def backup_dashboard() -> None:
    await _stub("Backup dashboard JSON")


async def show_dashboard() -> None:
    await _stub("Show dashboard details")


def dashboard_operations(menu: Menu) -> None:
    menu.add(MenuItem("1", "List all dashboards", handler=list_dashboards))
    menu.add(MenuItem("2", "Backup dashboard JSON", handler=backup_dashboard))
    menu.add(MenuItem("3", "Show dashboard details", handler=show_dashboard))
    menu.add(MenuItem("b", "Back", exit_after=True))
