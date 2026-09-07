"""Dataflow menu — wired to Phase 6 dataflow commands."""

from pathlib import Path

import typer

from asftool.cli.menu import Menu, MenuItem
from asftool.cli.ui import prompt_text


async def list_dataflows() -> None:
    from asftool.cli.commands.dataflows import list_dataflows_async

    try:
        await list_dataflows_async()
    except typer.Exit:
        pass


async def backup_dataflow() -> None:
    from asftool.cli.commands.dataflows import backup_dataflow_async
    from asftool.cli.ui import prompt_confirm
    from asftool.core.config import get_settings
    from asftool.core.storage import get_storage_manager

    dataflow_id = prompt_text("Dataflow ID")
    if not dataflow_id:
        return

    # Get dataflow name for path generation
    from asftool.cli.session import Session
    from asftool.core.services import DataflowService
    session = Session()
    try:
        async with session.client_context() as client:
            service = DataflowService(client, session.settings)
            dataflow = await service.get_dataflow(dataflow_id)
            dataflow_name = dataflow.name
    except Exception:
        dataflow_name = "dataflow"
    finally:
        await session.close()

    # Generate default path using StorageManager
    settings = get_settings()
    storage = get_storage_manager(settings)
    default_path = storage.dataflow_path(
        alias=session.alias,
        dataflow_name=dataflow_name,
        dataflow_id=dataflow_id,
    )

    # Present path to user and ask for confirmation/override
    from asftool.cli.ui import print_info
    print_info(f"Default output path: {default_path}")
    output = prompt_text("Output JSON path (press Enter to use default)", default=str(default_path))

    if not prompt_confirm(f"Save to {output}?"):
        print_info("Cancelled.")
        return

    try:
        await backup_dataflow_async(
            dataflow_id=dataflow_id, output=Path(output), alias=session.alias
        )
    except typer.Exit:
        pass


async def start_dataflow() -> None:
    from asftool.cli.commands.dataflows import start_dataflow_async

    dataflow_id = prompt_text("Dataflow ID")
    if not dataflow_id:
        return
    try:
        await start_dataflow_async(dataflow_id=dataflow_id)
    except typer.Exit:
        pass


async def stop_dataflow() -> None:
    from asftool.cli.commands.dataflows import stop_dataflow_async

    dataflow_id = prompt_text("Dataflow ID")
    if not dataflow_id:
        return
    try:
        await stop_dataflow_async(dataflow_id=dataflow_id)
    except typer.Exit:
        pass


async def show_dataflow() -> None:
    from asftool.cli.commands.dataflows import show_dataflow_async

    dataflow_id = prompt_text("Dataflow ID")
    if not dataflow_id:
        return
    try:
        await show_dataflow_async(dataflow_id=dataflow_id)
    except typer.Exit:
        pass


def dataflow_operations(menu: Menu) -> None:
    menu.add(MenuItem("1", "List all dataflows", handler=list_dataflows))
    menu.add(MenuItem("2", "Backup dataflow JSON", handler=backup_dataflow))
    menu.add(MenuItem("3", "Start dataflow", handler=start_dataflow))
    menu.add(MenuItem("4", "Stop dataflow", handler=stop_dataflow))
    menu.add(MenuItem("5", "Show dataflow details", handler=show_dataflow))
    menu.add(MenuItem("b", "Back", exit_after=True))
