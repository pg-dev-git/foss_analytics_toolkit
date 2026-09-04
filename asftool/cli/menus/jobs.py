"""Data Manager Jobs menu — stub for Phase 3. Full implementation in Phase 7."""

from asftool.cli.menu import Menu, MenuItem
from asftool.cli.ui import print_warning


async def _stub(operation: str) -> None:
    print_warning(f"{operation} — coming in Phase 7 (Data Manager Jobs)")


async def list_jobs() -> None:
    await _stub("List jobs")


async def show_job() -> None:
    await _stub("Show job details")


def jobs_operations(menu: Menu) -> None:
    menu.add(MenuItem("1", "List all jobs", handler=list_jobs))
    menu.add(MenuItem("2", "Show job details", handler=show_job))
    menu.add(MenuItem("b", "Back", exit_after=True))
