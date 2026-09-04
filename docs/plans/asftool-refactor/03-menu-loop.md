# Phase 3: Interactive Menu Loop ("Always Running OS")

**Goal:** Replace TUI with Rich + questionary menu loop that feels like the original `FOSS_Toolkit.py` but modern async.

---

## Prerequisites

- Phase 0-2 complete
- `asftool cli auth` commands work
- `Session` class available

---

## Files to Create/Modify

```
asftool/cli/ui.py              # Rich console helpers (already exists, may need updates)
asftool/cli/menu.py            # NEW: Menu definitions and rendering
asftool/cli/main.py            # Main entry point with menu loop
```

---

## Step 3.1: Create `asftool/cli/menu.py`

Central place for all menu definitions — easy to maintain.

```python
"""Menu definitions for interactive mode."""

from dataclasses import dataclass
from typing import Callable, Awaitable


@dataclass
class MenuItem:
    """A single menu option."""
    key: str                    # Short key for display (e.g., "1")
    label: str                  # Display label (e.g., "📊  Datasets")
    handler: Callable[[], Awaitable[None]] | None = None
    submenu: "Menu" | None = None
    exit_after: bool = False    # If True, return to parent after handler


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
        self.items = items or []
        self.parent = parent

    def add(self, item: MenuItem) -> "Menu":
        self.items.append(item)
        return self

    def add_submenu(self, key: str, label: str) -> "Menu":
        submenu = Menu(title=label, parent=self)
        self.add(MenuItem(key=key, label=label, submenu=submenu))
        return submenu


# --- Menu Handlers (imported from cli modules) ---
# These will be wired up in main.py


def create_menus() -> tuple[Menu, dict[str, Menu]]:
    """Create all menus. Returns (main_menu, all_menus_by_name)."""
    from asftool.cli.menus.datasets import dataset_operations
    from asftool.cli.menus.dashboards import dashboard_operations
    from asftool.cli.menus.dataflows import dataflow_operations
    from asftool.cli.menus.jobs import jobs_operations
    from asftool.cli.menus.auth import auth_operations

    main = Menu(
        title="ASFTool — FOSS Analytics Tool for TCRM",
        subtitle="Select an operation",
    )

    datasets_menu = main.add_submenu("1", "📊  Datasets")
    dashboards_menu = main.add_submenu("2", "📈  Dashboards")
    dataflows_menu = main.add_submenu("3", "⚙️  Dataflows")
    jobs_menu = main.add_submenu("4", "📋  Data Manager Jobs")
    auth_menu = main.add_submenu("5", "🔐  Authentication")
    doctor_menu = main.add_submenu("6", "🩺  Doctor / Diagnostics")
    main.add(MenuItem(key="q", label="❌  Exit", handler=lambda: None, exit_after=True))

    # Wire submenu operations (imported from their modules)
    dataset_operations(datasets_menu)
    dashboard_operations(dashboards_menu)
    dataflow_operations(dataflows_menu)
    jobs_operations(jobs_menu)
    auth_operations(auth_menu)

    all_menus = {
        "main": main,
        "datasets": datasets_menu,
        "dashboards": dashboards_menu,
        "dataflows": dataflows_menu,
        "jobs": jobs_menu,
        "auth": auth_menu,
    }

    return main, all_menus
```

---

## Step 3.2: Create `asftool/cli/main.py`

The main entry point with the menu loop.

```python
"""Main CLI entry point — interactive menu or subcommands."""

import asyncio
import sys

import typer
from rich.console import Console
from rich.panel import Panel

from asftool.cli.menu import create_menus
from asftool.cli.session import Session
from asftool.cli.ui import console, print_error, print_header, print_info

app = typer.Typer(
    name="asftool",
    help="FOSS Analytics Tool for Salesforce TCRM",
    add_completion=False,
    no_args_is_help=False,  # We handle no-args ourselves
)

# Import subcommand groups
from asftool.cli.commands import auth, datasets, dashboards, dataflows, jobs

app.add_typer(auth.app, name="auth")
app.add_typer(datasets.app, name="datasets")
app.add_typer(dashboards.app, name="dashboards")
app.add_typer(dataflows.app, name="dataflows")
app.add_typer(jobs.app, name="jobs")


def _show_session_header(session: Session) -> None:
    """Display session status at top of menu."""
    # This runs async, so we'll do a quick check
    async def _get_status():
        try:
            status = await session.auth_service.status(session.alias)
            if status["authenticated"]:
                username = status.get("username", "unknown")
                instance = status.get("instance_url", "N/A")
                token_status = "✓ Valid" if not status["token_expired"] else "⚠ Expired"
                return f"[green]{username}[/green] • {instance} • Token: {token_status}"
            else:
                return "[yellow]Not authenticated[/yellow]"
        except Exception:
            return "[red]Status error[/red]"

    try:
        status_text = asyncio.run(_get_status())
    except RuntimeError:
        status_text = "[dim]Loading...[/dim]"

    console.print(Panel.fit(
        f"[bold]ASFTool[/bold] — FOSS Analytics Tool for TCRM\n"
        f"Session: {status_text}\n"
        f"Org alias: {session.alias}",
        title="[cyan]Session[/cyan]",
        border_style="cyan",
    ))


def _render_menu(menu: "Menu") -> None:
    """Render menu with Rich formatting."""
    print_header(menu.title)
    if menu.subtitle:
        print_info(menu.subtitle)
    console.print()

    for item in menu.items:
        if item.submenu:
            console.print(f"  [cyan]{item.key}[/cyan]  {item.label} ▸")
        else:
            console.print(f"  [cyan]{item.key}[/cyan]  {item.label}")
    console.print()


async def _run_menu_loop(main_menu: "Menu") -> None:
    """Main interactive loop."""
    session = Session()
    current_menu = main_menu

    while True:
        _show_session_header(session)
        _render_menu(current_menu)

        # Get valid keys
        valid_keys = [item.key for item in current_menu.items]
        prompt = f"Select [{'/'.join(valid_keys)}]: "

        try:
            choice = console.input(prompt).strip().lower()
        except (EOFError, KeyboardInterrupt):
            console.print("\n[yellow]Exiting...[/yellow]")
            break

        if not choice:
            continue

        # Find matching item
        item = next((i for i in current_menu.items if i.key.lower() == choice), None)
        if not item:
            console.print(f"[red]Invalid choice: {choice}[/red]")
            continue

        if item.submenu:
            current_menu = item.submenu
            continue

        if item.handler:
            try:
                await item.handler()
            except Exception as e:
                print_error(f"Error: {e}")
            console.print()  # spacing

        if item.exit_after or choice == "q":
            if current_menu.parent:
                current_menu = current_menu.parent
            else:
                break


@app.callback(invoke_without_command=True)
def main(
    ctx: typer.Context,
    alias: str = typer.Option("default", "--alias", "-a", help="Default org alias"),
) -> None:
    """ASFTool — FOSS Analytics Tool for TCRM."""
    if ctx.invoked_subcommand:
        return  # Let Typer handle subcommands

    # No subcommand → interactive menu
    main_menu, _ = create_menus()
    asyncio.run(_run_menu_loop(main_menu))


if __name__ == "__main__":
    app()
```

---

## Step 3.3: Update `asftool/cli/ui.py`

```python
"""Rich console utilities."""

from rich.console import Console
from rich.panel import Panel

console = Console()


def print_header(text: str) -> None:
    console.rule(f"[bold cyan]{text}[/bold cyan]")


def print_info(text: str) -> None:
    console.print(f"[blue]ℹ[/blue] {text}")


def print_success(text: str) -> None:
    console.print(f"[green]✓[/green] {text}")


def print_warning(text: str) -> None:
    console.print(f"[yellow]⚠[/yellow] {text}")


def print_error(text: str) -> None:
    console.print(f"[red]✗[/red] {text}")
```

---

## Step 3.4: Create Submenu Operation Modules

Create one file per domain to keep menus organized:

```
asftool/cli/menus/
├── __init__.py
├── datasets.py      # dataset_operations(menu)
├── dashboards.py    # dashboard_operations(menu)
├── dataflows.py     # dataflow_operations(menu)
├── jobs.py          # jobs_operations(menu)
├── auth.py          # auth_operations(menu)
```

Each follows the same pattern:

```python
# asftool/cli/menus/datasets.py
"""Dataset menu operations."""

from asftool.cli.menu import MenuItem

def dataset_operations(menu: "Menu") -> None:
    """Wire up dataset submenu operations."""
    menu.add(MenuItem("1", "List all datasets", handler=list_datasets))
    menu.add(MenuItem("2", "Extract dataset to CSV", handler=extract_dataset))
    menu.add(MenuItem("3", "Upload CSV to dataset", handler=upload_dataset))
    menu.add(MenuItem("4", "Delete dataset", handler=delete_dataset))
    menu.add(MenuItem("5", "Show dataset details", handler=show_dataset))
    menu.add(MenuItem("b", "Back", exit_after=True))

# Handlers defined below or imported from cli/commands/datasets.py
async def list_datasets(): ...
```

---

## Acceptance Criteria

- [ ] `asftool` (no args) launches interactive menu
- [ ] Menu shows session status (user, instance, token state)
- [ ] Navigation: number keys enter submenus, "b" goes back, "q" exits
- [ ] Each domain has its own submenu (datasets, dashboards, dataflows, jobs, auth)
- [ ] Handlers are async and delegate to `Session` + services
- [ ] Rich formatting throughout (panels, tables, colors)
- [ ] `uv run pytest tests/ -v` passes (no TUI tests)
- [ ] `uv run ruff check asftool/cli/` passes

---

## Notes

- This replaces ~3,500 lines of Textual TUI with ~200 lines of Rich + questionary-free menus
- `console.input()` is sync but works fine — handlers are async, loop runs in `asyncio.run()`
- The menu system is extensible: add new domains by creating a new menu module
- Keep handlers thin — they call `Session` → services → client, just like CLI commands