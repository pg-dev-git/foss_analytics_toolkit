"""Dataset menu — stub for Phase 3. Full implementation in Phase 4."""

from asftool.cli.menu import Menu, MenuItem
from asftool.cli.ui import print_warning


async def _stub(operation: str) -> None:
    print_warning(f"{operation} — coming in Phase 4 (Dataset Operations)")


async def list_datasets() -> None:
    await _stub("List datasets")


async def extract_dataset() -> None:
    await _stub("Extract dataset to CSV")


async def upload_dataset() -> None:
    await _stub("Upload CSV to dataset")


async def delete_dataset() -> None:
    await _stub("Delete dataset")


async def show_dataset() -> None:
    await _stub("Show dataset details")


def dataset_operations(menu: Menu) -> None:
    menu.add(MenuItem("1", "List all datasets", handler=list_datasets))
    menu.add(MenuItem("2", "Extract dataset to CSV", handler=extract_dataset))
    menu.add(MenuItem("3", "Upload CSV to dataset", handler=upload_dataset))
    menu.add(MenuItem("4", "Delete dataset", handler=delete_dataset))
    menu.add(MenuItem("5", "Show dataset details", handler=show_dataset))
    menu.add(MenuItem("b", "Back", exit_after=True))
