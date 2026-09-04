"""Dataflow menu — stub for Phase 3. Full implementation in Phase 6."""

from asftool.cli.menu import Menu, MenuItem
from asftool.cli.ui import print_warning


async def _stub(operation: str) -> None:
    print_warning(f"{operation} — coming in Phase 6 (Dataflow Operations)")


async def list_dataflows() -> None:
    await _stub("List dataflows")


async def backup_dataflow() -> None:
    await _stub("Backup dataflow JSON")


async def start_dataflow() -> None:
    await _stub("Start dataflow")


async def stop_dataflow() -> None:
    await _stub("Stop dataflow")


async def show_dataflow() -> None:
    await _stub("Show dataflow details")


def dataflow_operations(menu: Menu) -> None:
    menu.add(MenuItem("1", "List all dataflows", handler=list_dataflows))
    menu.add(MenuItem("2", "Backup dataflow JSON", handler=backup_dataflow))
    menu.add(MenuItem("3", "Start dataflow", handler=start_dataflow))
    menu.add(MenuItem("4", "Stop dataflow", handler=stop_dataflow))
    menu.add(MenuItem("5", "Show dataflow details", handler=show_dataflow))
    menu.add(MenuItem("b", "Back", exit_after=True))
