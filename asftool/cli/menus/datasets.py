"""Dataset menu — wired to Phase 4 dataset commands.

Menu handlers call the corresponding async wrappers in
asftool.cli.commands.datasets. The wrappers handle the Session/client
lifecycle; the menu layer only collects user input and forwards.
"""

from pathlib import Path

import typer

from asftool.cli.menu import Menu, MenuItem
from asftool.cli.ui import print_error, print_info, prompt_text


async def list_datasets() -> None:
    from asftool.cli.commands.datasets import list_datasets_async

    try:
        await list_datasets_async()
    except typer.Exit:
        pass


async def extract_dataset() -> None:
    from asftool.cli.commands.datasets import extract_dataset_async
    from asftool.cli.ui import prompt_confirm
    from asftool.core.config import get_settings
    from asftool.core.storage import get_storage_manager

    dataset_id = prompt_text("Dataset ID")
    if not dataset_id:
        return

    # Get dataset name for path generation
    from asftool.cli.session import Session
    from asftool.core.services import DatasetService
    session = Session()
    try:
        async with session.client_context() as client:
            service = DatasetService(client, session.settings)
            dataset = await service.get_dataset(dataset_id)
            dataset_name = dataset.name
    except Exception:
        dataset_name = "dataset"
    finally:
        await session.close()

    # Generate default path using StorageManager
    settings = get_settings()
    storage = get_storage_manager(settings)
    default_path = storage.dataset_path(
        alias=session.alias,
        dataset_name=dataset_name,
        dataset_id=dataset_id,
    )

    # Present path to user and ask for confirmation/override
    print_info(f"Default output path: {default_path}")
    output = prompt_text("Output CSV path (press Enter to use default)", default=str(default_path))
    if not prompt_confirm(f"Save to {output}?"):
        print_info("Cancelled.")
        return

    try:
        await extract_dataset_async(dataset_id=dataset_id, output=Path(output), alias=session.alias)
    except typer.Exit:
        pass


async def upload_dataset() -> None:
    from asftool.cli.commands.datasets import upload_dataset_async

    dataset_id = prompt_text("Dataset ID")
    if not dataset_id:
        return
    file_path = prompt_text("CSV file path")
    if not file_path:
        return
    p = Path(file_path)
    if not p.exists():
        print_error(f"File not found: {file_path}")
        return
    try:
        await upload_dataset_async(dataset_id=dataset_id, file_path=p)
    except typer.Exit:
        pass


async def delete_dataset() -> None:
    from asftool.cli.commands.datasets import delete_dataset_async

    dataset_id = prompt_text("Dataset ID")
    if not dataset_id:
        return
    try:
        await delete_dataset_async(dataset_id=dataset_id)
    except typer.Exit:
        pass


async def show_dataset() -> None:
    from asftool.cli.commands.datasets import show_dataset_async

    dataset_id = prompt_text("Dataset ID")
    if not dataset_id:
        return
    try:
        await show_dataset_async(dataset_id=dataset_id)
    except typer.Exit:
        pass


def dataset_operations(menu: Menu) -> None:
    """Wire up the dataset submenu."""
    menu.add(MenuItem("1", "List all datasets", handler=list_datasets))
    menu.add(MenuItem("2", "Extract dataset to CSV", handler=extract_dataset))
    menu.add(MenuItem("3", "Upload CSV to dataset", handler=upload_dataset))
    menu.add(MenuItem("4", "Delete dataset", handler=delete_dataset))
    menu.add(MenuItem("5", "Show dataset details", handler=show_dataset))
    menu.add(MenuItem("b", "Back", exit_after=True))
