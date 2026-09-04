"""Menu definitions for the interactive "always running OS" mode.

A Menu holds MenuItems. Each MenuItem is either:
  - a leaf with an async `handler` to invoke, or
  - a submenu (children) to navigate into.

Navigation is simple: a keypress selects an item by its `key`. Pressing `b`
goes back to the parent menu; pressing `q` at the root exits the loop.
"""

from collections.abc import Awaitable, Callable
from dataclasses import dataclass


@dataclass
class MenuItem:
    """A single menu option."""

    key: str  # Short key for selection (e.g., "1", "b", "q")
    label: str  # Display label (e.g., "📊  Datasets")
    handler: Callable[[], Awaitable[None]] | None = None
    submenu: "Menu | None" = None
    exit_after: bool = False  # If True, return to parent menu after running


class Menu:
    """A menu with items and optional parent."""

    def __init__(
        self,
        title: str,
        subtitle: str | None = None,
        items: list[MenuItem] | None = None,
        parent: "Menu | None" = None,
    ):
        self.title = title
        self.subtitle = subtitle
        self.items: list[MenuItem] = items or []
        self.parent = parent

    def add(self, item: MenuItem) -> "Menu":
        """Append a MenuItem and return self for chaining."""
        self.items.append(item)
        return self

    def add_submenu(self, key: str, label: str) -> "Menu":
        """Create a child submenu and add it as an item."""
        submenu = Menu(title=label, parent=self)
        self.add(MenuItem(key=key, label=label, submenu=submenu))
        return submenu


def create_menus() -> tuple[Menu, dict[str, Menu]]:
    """Create all menus. Returns (main_menu, {name: menu})."""
    # Imports done inside to avoid circular imports during package init.
    from asftool.cli.menus.auth import auth_operations
    from asftool.cli.menus.dashboards import dashboard_operations
    from asftool.cli.menus.dataflows import dataflow_operations
    from asftool.cli.menus.datasets import dataset_operations
    from asftool.cli.menus.jobs import jobs_operations

    main = Menu(
        title="ASFTool — FOSS Analytics Tool for TCRM",
        subtitle="Select an operation",
    )

    datasets_menu = main.add_submenu("1", "📊  Datasets")
    dashboards_menu = main.add_submenu("2", "📈  Dashboards")
    dataflows_menu = main.add_submenu("3", "⚙️  Dataflows")
    jobs_menu = main.add_submenu("4", "📋  Data Manager Jobs")
    auth_menu = main.add_submenu("5", "🔐  Authentication")

    async def _run_doctor() -> None:
        from asftool.cli.commands.doctor import run_diagnostics

        run_diagnostics()

    main.add(MenuItem(key="6", label="🩺  Doctor / Diagnostics", handler=_run_doctor))
    main.add(MenuItem(key="q", label="❌  Exit"))

    # Wire submenu operations (phases 4-7 fill in handlers).
    dataset_operations(datasets_menu)
    dashboard_operations(dashboards_menu)
    dataflow_operations(dataflows_menu)
    jobs_operations(jobs_menu)
    auth_operations(auth_menu)

    all_menus: dict[str, Menu] = {
        "main": main,
        "datasets": datasets_menu,
        "dashboards": dashboards_menu,
        "dataflows": dataflows_menu,
        "jobs": jobs_menu,
        "auth": auth_menu,
    }
    return main, all_menus
