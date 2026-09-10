# Phase 5: Dashboard Operations Menu

**Goal:** Wire dashboard menu handlers to existing `DashboardService`.

---

## Prerequisites

- Phase 3-4 complete
- `DashboardService` already implemented in `core/services/dashboard_service.py`

---

## Files to Create/Modify

```
asftool/cli/menus/dashboards.py
asftool/cli/commands/dashboards.py
```

---

## Step 5.1: Update `asftool/cli/commands/dashboards.py`

```python
"""Dashboard CLI commands."""

import asyncio

import typer
from rich.table import Table

from asftool.cli.session import Session
from asftool.cli.ui import console, print_error, print_header, print_info, print_success
from asftool.core.services import DashboardService

app = typer.Typer(help="Dashboard operations")


def _run(coro):
    return asyncio.run(coro)


@app.command("list")
def list_dashboards(
    page_size: int = typer.Option(50, "--page-size"),
):
    """List all dashboards."""
    async def _list():
        session = Session()
        try:
            async with session.client_context() as client:
                service = DashboardService(client, session.settings)
                dashboards = await service.list_dashboards(page_size=page_size)

                if not dashboards:
                    print_info("No dashboards found")
                    return

                table = Table(title="Dashboards", show_header=True)
                table.add_column("ID", style="cyan")
                table.add_column("Name", style="white")
                table.add_column("App", style="blue")
                table.add_column("Datasets", justify="right", style="green")
                table.add_column("Updated", style="dim")

                for db in dashboards:
                    table.add_row(
                        db.id,
                        db.name,
                        db.app_id or "N/A",
                        str(len(db.datasets)) if db.datasets else "0",
                        db.updated_at.strftime("%Y-%m-%d") if db.updated_at else "N/A",
                    )
                console.print(table)
        except Exception as e:
            print_error(f"List failed: {e}")
            raise typer.Exit(1)
        finally:
            await session.close()

    _run(_list())


@app.command("backup")
def backup_dashboard(
    dashboard_id: str = typer.Argument(..., help="Dashboard ID"),
    output: str = typer.Option("./dashboard_backup", "--output", "-o", help="Output directory"),
):
    """Backup dashboard JSON."""
    async def _backup():
        session = Session()
        try:
            async with session.client_context() as client:
                service = DashboardService(client, session.settings)
                path = await service.backup_dashboard(dashboard_id, output)
                print_success(f"Dashboard backed up to {path}")
        except Exception as e:
            print_error(f"Backup failed: {e}")
            raise typer.Exit(1)
        finally:
            await session.close()

    _run(_backup())


@app.command("show")
def show_dashboard(
    dashboard_id: str = typer.Argument(..., help="Dashboard ID"),
):
    """Show dashboard details."""
    async def _show():
        session = Session()
        try:
            async with session.client_context() as client:
                service = DashboardService(client, session.settings)
                db = await service.get_dashboard(dashboard_id)

                print_header(f"Dashboard: {db.name}")
                print_info(f"ID: {db.id}")
                print_info(f"App: {db.app_id or 'N/A'}")
                print_info(f"Datasets: {', '.join(db.datasets) if db.datasets else 'None'}")
                print_info(f"Created: {db.created_at}")
                print_info(f"Updated: {db.updated_at}")
        except Exception as e:
            print_error(f"Show failed: {e}")
            raise typer.Exit(1)
        finally:
            await session.close()

    _run(_show())
```

---

## Step 5.2: Create `asftool/cli/menus/dashboards.py`

```python
"""Dashboard menu operations."""

from asftool.cli.menu import MenuItem


async def list_dashboards():
    from asftool.cli.commands.dashboards import list_dashboards as cmd
    await cmd()


async def backup_dashboard():
    from asftool.cli.commands.dashboards import backup_dashboard as cmd
    from asftool.cli.ui import prompt_text
    dashboard_id = prompt_text("Dashboard ID")
    if not dashboard_id:
        return
    output = prompt_text("Output directory", default="./dashboard_backup")
    await cmd(dashboard_id=dashboard_id, output=output)


async def show_dashboard():
    from asftool.cli.commands.dashboards import show_dashboard as cmd
    from asftool.cli.ui import prompt_text
    dashboard_id = prompt_text("Dashboard ID")
    if not dashboard_id:
        return
    await cmd(dashboard_id=dashboard_id)


def dashboard_operations(menu: "Menu") -> None:
    menu.add(MenuItem("1", "List all dashboards", handler=list_dashboards))
    menu.add(MenuItem("2", "Backup dashboard JSON", handler=backup_dashboard))
    menu.add(MenuItem("3", "Show dashboard details", handler=show_dashboard))
    menu.add(MenuItem("b", "Back", exit_after=True))
```

---

## Acceptance Criteria

- [ ] `asftool dashboards list` works
- [ ] `asftool dashboards backup <id> -o dir` works
- [ ] `asftool dashboards show <id>` works
- [ ] Interactive menu: "1" → list, "2" → backup, "3" → show
- [ ] Errors displayed with Rich formatting