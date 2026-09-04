"""Dataflow CLI commands.

List, backup, start, stop, and inspect dataflows. Delegates to
DataflowService for all API work. The same functions are reused by the
interactive menu (asftool/cli/menus/dataflows.py).
"""

import asyncio
from pathlib import Path

import typer
from rich.table import Table

from asftool.cli.session import Session
from asftool.cli.ui import (
    console,
    print_error,
    print_header,
    print_info,
    print_success,
)
from asftool.core.exceptions import DataflowError
from asftool.core.services import DataflowService

app = typer.Typer(help="Dataflow operations")


def _run(coro):
    return asyncio.run(coro)


_STATUS_COLORS = {
    "Running": "green",
    "Success": "green",
    "Queued": "yellow",
    "Stopped": "yellow",
    "Failed": "red",
    "Cancelled": "red",
}


@app.command("list")
def list_dataflows():
    """List all dataflows."""
    session = Session()

    async def _list() -> None:
        try:
            async with session.client_context() as client:
                service = DataflowService(client, session.settings)
                dataflows = await service.list_dataflows()

                if not dataflows:
                    print_info("No dataflows found")
                    return

                table = Table(title="Dataflows", show_header=True)
                table.add_column("ID", style="cyan")
                table.add_column("Name", style="white")
                table.add_column("Status", style="bold")
                table.add_column("Updated", style="dim")

                for df in dataflows:
                    color = _STATUS_COLORS.get(df.status, "white")
                    table.add_row(
                        df.id,
                        df.name,
                        f"[{color}]{df.status}[/{color}]",
                        df.last_modified_date.strftime("%Y-%m-%d")
                        if df.last_modified_date
                        else "N/A",
                    )
                console.print(table)
                print_info(f"Total: {len(dataflows)} dataflows")
        except Exception as e:
            print_error(f"List failed: {e}")
            raise typer.Exit(1) from e
        finally:
            await session.close()

    _run(_list())


@app.command("backup")
def backup_dataflow(
    dataflow_id: str = typer.Argument(..., help="Dataflow ID"),
    output: Path = typer.Option(
        Path.cwd() / "dataflow_backup.json",
        "--output",
        "-o",
        help="Output JSON file path",
    ),
):
    """Backup dataflow JSON definition."""
    session = Session()

    async def _backup() -> None:
        try:
            async with session.client_context() as client:
                service = DataflowService(client, session.settings)
                await service.backup_dataflow(
                    dataflow_id=dataflow_id, output_path=str(output)
                )
                print_success(f"Dataflow {dataflow_id} backed up to {output}")
        except DataflowError as e:
            print_error(f"Backup failed: {e}")
            raise typer.Exit(1) from e
        except Exception as e:
            print_error(f"Backup failed: {e}")
            raise typer.Exit(1) from e
        finally:
            await session.close()

    _run(_backup())


@app.command("start")
def start_dataflow(
    dataflow_id: str = typer.Argument(..., help="Dataflow ID"),
):
    """Start a dataflow execution."""
    session = Session()

    async def _start() -> None:
        try:
            async with session.client_context() as client:
                service = DataflowService(client, session.settings)
                job = await service.start_dataflow(dataflow_id)
                print_success(
                    f"Dataflow {dataflow_id} started — job {job.id} ({job.status})"
                )
        except DataflowError as e:
            print_error(f"Start failed: {e}")
            raise typer.Exit(1) from e
        except Exception as e:
            print_error(f"Start failed: {e}")
            raise typer.Exit(1) from e
        finally:
            await session.close()

    _run(_start())


@app.command("stop")
def stop_dataflow(
    dataflow_id: str = typer.Argument(..., help="Dataflow ID"),
):
    """Stop a running dataflow."""
    session = Session()

    async def _stop() -> None:
        try:
            async with session.client_context() as client:
                service = DataflowService(client, session.settings)
                job = await service.stop_dataflow(dataflow_id)
                print_success(
                    f"Dataflow {dataflow_id} stop requested — job {job.id} ({job.status})"
                )
        except DataflowError as e:
            print_error(f"Stop failed: {e}")
            raise typer.Exit(1) from e
        except Exception as e:
            print_error(f"Stop failed: {e}")
            raise typer.Exit(1) from e
        finally:
            await session.close()

    _run(_stop())


@app.command("show")
def show_dataflow(
    dataflow_id: str = typer.Argument(..., help="Dataflow ID"),
):
    """Show dataflow details."""
    session = Session()

    async def _show() -> None:
        try:
            async with session.client_context() as client:
                service = DataflowService(client, session.settings)
                df = await service.get_dataflow(dataflow_id)

                print_header(f"Dataflow: {df.name}")
                print_info(f"ID: {df.id}")
                print_info(f"Label: {df.label}")
                print_info(f"Status: {df.status}")
                print_info(f"Created: {df.created_date}")
                print_info(f"Updated: {df.last_modified_date}")
        except Exception as e:
            print_error(f"Show failed: {e}")
            raise typer.Exit(1) from e
        finally:
            await session.close()

    _run(_show())


# Async wrappers for the menu layer -----------------------------------------


async def list_dataflows_async() -> None:
    list_dataflows()


async def backup_dataflow_async(dataflow_id: str, output: Path) -> None:
    backup_dataflow(dataflow_id=dataflow_id, output=output)


async def start_dataflow_async(dataflow_id: str) -> None:
    start_dataflow(dataflow_id=dataflow_id)


async def stop_dataflow_async(dataflow_id: str) -> None:
    stop_dataflow(dataflow_id=dataflow_id)


async def show_dataflow_async(dataflow_id: str) -> None:
    show_dataflow(dataflow_id=dataflow_id)
