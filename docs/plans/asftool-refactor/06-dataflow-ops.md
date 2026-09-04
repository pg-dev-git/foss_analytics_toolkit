# Phase 6: Dataflow Operations Menu

**Goal:** Wire dataflow menu handlers to existing `DataflowService`.

---

## Prerequisites

- Phase 3-5 complete
- `DataflowService` already implemented in `core/services/dataflow_service.py`

---

## Files to Create/Modify

```
asftool/cli/menus/dataflows.py
asftool/cli/commands/dataflows.py
```

---

## Step 6.1: Update `asftool/cli/commands/dataflows.py`

```python
"""Dataflow CLI commands."""

import asyncio

import typer
from rich.table import Table

from asftool.cli.session import Session
from asftool.cli.ui import console, print_error, print_header, print_info, print_success, print_warning
from asftool.core.services import DataflowService

app = typer.Typer(help="Dataflow operations")


def _run(coro):
    return asyncio.run(coro)


@app.command("list")
def list_dataflows(
    page_size: int = typer.Option(50, "--page-size"),
):
    """List all dataflows."""
    async def _list():
        session = Session()
        try:
            async with session.client_context() as client:
                service = DataflowService(client, session.settings)
                dataflows = await service.list_dataflows(page_size=page_size)

                if not dataflows:
                    print_info("No dataflows found")
                    return

                table = Table(title="Dataflows", show_header=True)
                table.add_column("ID", style="cyan")
                table.add_column("Name", style="white")
                table.add_column("Status", style="green")
                table.add_column("Type", style="blue")
                table.add_column("Updated", style="dim")

                for df in dataflows:
                    status_color = "green" if df.status == "Running" else "yellow" if df.status == "Stopped" else "red"
                    table.add_row(
                        df.id,
                        df.name,
                        f"[{status_color}]{df.status}[/{status_color}]",
                        df.dataflow_type or "N/A",
                        df.updated_at.strftime("%Y-%m-%d") if df.updated_at else "N/A",
                    )
                console.print(table)
        except Exception as e:
            print_error(f"List failed: {e}")
            raise typer.Exit(1)
        finally:
            await session.close()

    _run(_list())


@app.command("backup")
def backup_dataflow(
    dataflow_id: str = typer.Argument(..., help="Dataflow ID"),
    output: str = typer.Option("./dataflow_backup", "--output", "-o", help="Output directory"),
):
    """Backup dataflow JSON."""
    async def _backup():
        session = Session()
        try:
            async with session.client_context() as client:
                service = DataflowService(client, session.settings)
                path = await service.backup_dataflow(dataflow_id, output)
                print_success(f"Dataflow backed up to {path}")
        except Exception as e:
            print_error(f"Backup failed: {e}")
            raise typer.Exit(1)
        finally:
            await session.close()

    _run(_backup())


@app.command("start")
def start_dataflow(
    dataflow_id: str = typer.Argument(..., help="Dataflow ID"),
):
    """Start a dataflow."""
    async def _start():
        session = Session()
        try:
            async with session.client_context() as client:
                service = DataflowService(client, session.settings)
                await service.start_dataflow(dataflow_id)
                print_success(f"Dataflow {dataflow_id} started")
        except Exception as e:
            print_error(f"Start failed: {e}")
            raise typer.Exit(1)
        finally:
            await session.close()

    _run(_start())


@app.command("stop")
def stop_dataflow(
    dataflow_id: str = typer.Argument(..., help="Dataflow ID"),
):
    """Stop a dataflow."""
    async def _stop():
        session = Session()
        try:
            async with session.client_context() as client:
                service = DataflowService(client, session.settings)
                await service.stop_dataflow(dataflow_id)
                print_success(f"Dataflow {dataflow_id} stopped")
        except Exception as e:
            print_error(f"Stop failed: {e}")
            raise typer.Exit(1)
        finally:
            await session.close()

    _run(_stop())


@app.command("show")
def show_dataflow(
    dataflow_id: str = typer.Argument(..., help="Dataflow ID"),
):
    """Show dataflow details."""
    async def _show():
        session = Session()
        try:
            async with session.client_context() as client:
                service = DataflowService(client, session.settings)
                df = await service.get_dataflow(dataflow_id)

                print_header(f"Dataflow: {df.name}")
                print_info(f"ID: {df.id}")
                print_info(f"Status: {df.status}")
                print_info(f"Type: {df.dataflow_type}")
                print_info(f"Created: {df.created_at}")
                print_info(f"Updated: {df.updated_at}")
        except Exception as e:
            print_error(f"Show failed: {e}")
            raise typer.Exit(1)
        finally:
            await session.close()

    _run(_show())
```

---

## Step 6.2: Create `asftool/cli/menus/dataflows.py`

```python
"""Dataflow menu operations."""

from asftool.cli.menu import MenuItem


async def list_dataflows():
    from asftool.cli.commands.dataflows import list_dataflows as cmd
    await cmd()


async def backup_dataflow():
    from asftool.cli.commands.dataflows import backup_dataflow as cmd
    from asftool.cli.ui import prompt_text
    dataflow_id = prompt_text("Dataflow ID")
    if not dataflow_id:
        return
    output = prompt_text("Output directory", default="./dataflow_backup")
    await cmd(dataflow_id=dataflow_id, output=output)


async def start_dataflow():
    from asftool.cli.commands.dataflows import start_dataflow as cmd
    from asftool.cli.ui import prompt_text
    dataflow_id = prompt_text("Dataflow ID")
    if not dataflow_id:
        return
    await cmd(dataflow_id=dataflow_id)


async def stop_dataflow():
    from asftool.cli.commands.dataflows import stop_dataflow as cmd
    from asftool.cli.ui import prompt_text
    dataflow_id = prompt_text("Dataflow ID")
    if not dataflow_id:
        return
    await cmd(dataflow_id=dataflow_id)


async def show_dataflow():
    from asftool.cli.commands.dataflows import show_dataflow as cmd
    from asftool.cli.ui import prompt_text
    dataflow_id = prompt_text("Dataflow ID")
    if not dataflow_id:
        return
    await cmd(dataflow_id=dataflow_id)


def dataflow_operations(menu: "Menu") -> None:
    menu.add(MenuItem("1", "List all dataflows", handler=list_dataflows))
    menu.add(MenuItem("2", "Backup dataflow JSON", handler=backup_dataflow))
    menu.add(MenuItem("3", "Start dataflow", handler=start_dataflow))
    menu.add(MenuItem("4", "Stop dataflow", handler=stop_dataflow))
    menu.add(MenuItem("5", "Show dataflow details", handler=show_dataflow))
    menu.add(MenuItem("b", "Back", exit_after=True))
```

---

## Acceptance Criteria

- [ ] `asftool dataflows list` works
- [ ] `asftool dataflows backup <id> -o dir` works
- [ ] `asftool dataflows start <id>` works
- [ ] `asftool dataflows stop <id>` works
- [ ] `asftool dataflows show <id>` works
- [ ] Interactive menu: "1" → list, "2" → backup, "3" → start, "4" → stop, "5" → show