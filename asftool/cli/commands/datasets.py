"""Dataset CLI commands.

List, extract, upload, delete, and inspect datasets. Delegates to
DatasetService for all API work. The same functions are reused by the
interactive menu (asftool/cli/menus/datasets.py).
"""

import asyncio
from collections.abc import Callable
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
from asftool.core.exceptions import DatasetError, UploadError
from asftool.core.models import ExtractionJob, UploadJob
from asftool.core.services import DatasetService

app = typer.Typer(help="Dataset operations")


def _run(coro):
    """Run async coroutine in a fresh event loop."""
    return asyncio.run(coro)


def _to_async_cb(cb: Callable | None) -> Callable | None:
    """Return cb unchanged if it's already async; wrap sync cb to await it.

    DatasetService passes ExtractionProgress/UploadProgress objects to a
    sync callback. The TUI used to need an async callback. To keep both
    the CLI (sync) and future async callers happy, accept either.
    """
    if cb is None:
        return None
    if asyncio.iscoroutinefunction(cb):
        return cb

    async def _wrapper(p):  # type: ignore[no-untyped-def]
        cb(p)

    return _wrapper


@app.command("list")
def list_datasets(
    page_size: int = typer.Option(50, "--page-size", help="Page size"),
    sort: str = typer.Option("Mru", "--sort", help="Sort order: Mru, Name, CreatedDate"),
):
    """List all datasets."""
    session = Session()

    async def _list() -> None:
        try:
            async with session.client_context() as client:
                service = DatasetService(client, session.settings)
                datasets = await service.list_datasets(page_size=page_size, sort=sort)

                if not datasets:
                    print_info("No datasets found")
                    return

                table = Table(title="Datasets", show_header=True)
                table.add_column("ID", style="cyan")
                table.add_column("Name", style="white")
                table.add_column("Version", style="blue")
                table.add_column("Rows", justify="right", style="green")
                table.add_column("Updated", style="dim")

                for ds in datasets:
                    table.add_row(
                        ds.id,
                        ds.name,
                        ds.current_version_id or "N/A",
                        f"{ds.row_count:,}" if ds.row_count else "N/A",
                        ds.last_modified_date.strftime("%Y-%m-%d")
                        if ds.last_modified_date
                        else "N/A",
                    )
                console.print(table)
                print_info(f"Total: {len(datasets)} datasets")
        except Exception as e:
            print_error(f"List failed: {e}")
            raise typer.Exit(1) from e
        finally:
            await session.close()

    _run(_list())


@app.command("extract")
def extract_dataset(
    dataset_id: str = typer.Argument(..., help="Dataset ID"),
    output: Path = typer.Option(
        Path.cwd() / "extracted.csv", "--output", "-o", help="Output CSV path"
    ),
    show_progress: bool = typer.Option(
        True, "--progress/--no-progress", help="Show progress"
    ),
):
    """Extract dataset to CSV."""
    session = Session()

    async def _extract() -> None:
        try:
            async with session.client_context() as client:
                service = DatasetService(client, session.settings)

                def progress_cb(p: ExtractionJob) -> None:
                    if show_progress:
                        print_info(
                            f"  {p.processed_rows:,}/{p.total_rows:,} rows "
                            f"({p.current_chunk}/{p.total_chunks} chunks)"
                        )

                job = await service.extract_dataset(
                    dataset_id=dataset_id,
                    output_path=output,
                    progress_callback=_to_async_cb(progress_cb),
                )

                print_success(
                    f"Extracted {job.processed_rows:,} rows to {job.result_path}"
                )
        except DatasetError as e:
            print_error(f"Extract failed: {e}")
            raise typer.Exit(1) from e
        except Exception as e:
            print_error(f"Extract failed: {e}")
            raise typer.Exit(1) from e
        finally:
            await session.close()

    _run(_extract())


@app.command("upload")
def upload_dataset(
    dataset_id: str = typer.Argument(..., help="Dataset ID"),
    file: Path = typer.Argument(..., help="CSV file path"),
    name: str | None = typer.Option(
        None, "--name", "-n", help="Dataset name (default: from dataset)"
    ),
    operation: str = typer.Option(
        "Overwrite", "--operation", "-o", help="Overwrite or Append"
    ),
    show_progress: bool = typer.Option(
        True, "--progress/--no-progress", help="Show progress"
    ),
):
    """Upload CSV to dataset."""
    session = Session()

    async def _upload() -> None:
        try:
            async with session.client_context() as client:
                service = DatasetService(client, session.settings)

                def progress_cb(p: UploadJob) -> None:
                    if show_progress:
                        print_info(
                            f"  {p.uploaded_rows:,}/{p.total_rows:,} rows "
                            f"({p.current_part}/{p.total_parts} parts)"
                        )

                job = await service.upload_csv(
                    dataset_id=dataset_id,
                    file_path=file,
                    dataset_name=name,
                    operation=operation,
                    progress_callback=_to_async_cb(progress_cb),
                )

                print_success(
                    f"Uploaded {job.uploaded_rows:,} rows to {job.dataset_id}"
                )
        except (UploadError, FileNotFoundError) as e:
            print_error(f"Upload failed: {e}")
            raise typer.Exit(1) from e
        except Exception as e:
            print_error(f"Upload failed: {e}")
            raise typer.Exit(1) from e
        finally:
            await session.close()

    _run(_upload())


@app.command("delete")
def delete_dataset(
    dataset_id: str = typer.Argument(..., help="Dataset ID"),
    force: bool = typer.Option(False, "--force", "-f", help="Skip confirmation"),
):
    """Delete a dataset."""
    session = Session()

    if not force:
        confirm = typer.confirm(f"Delete dataset {dataset_id}?")
        if not confirm:
            print_info("Cancelled")
            return

    async def _delete() -> None:
        try:
            async with session.client_context() as client:
                service = DatasetService(client, session.settings)
                await service.delete_dataset(dataset_id)
                print_success(f"Dataset {dataset_id} deleted")
        except DatasetError as e:
            print_error(f"Delete failed: {e}")
            raise typer.Exit(1) from e
        except Exception as e:
            print_error(f"Delete failed: {e}")
            raise typer.Exit(1) from e
        finally:
            await session.close()

    _run(_delete())


@app.command("show")
def show_dataset(
    dataset_id: str = typer.Argument(..., help="Dataset ID"),
):
    """Show dataset details."""
    session = Session()

    async def _show() -> None:
        try:
            async with session.client_context() as client:
                service = DatasetService(client, session.settings)
                dataset = await service.get_dataset(dataset_id)

                print_header(f"Dataset: {dataset.name}")
                print_info(f"ID: {dataset.id}")
                print_info(f"Version: {dataset.current_version_id or 'N/A'}")
                if dataset.row_count is not None:
                    print_info(f"Rows: {dataset.row_count:,}")
                else:
                    print_info("Rows: N/A")
                print_info(f"Created: {dataset.created_date}")
                print_info(f"Updated: {dataset.last_modified_date}")
                print_info(f"Description: {dataset.description or 'N/A'}")
        except Exception as e:
            print_error(f"Show failed: {e}")
            raise typer.Exit(1) from e
        finally:
            await session.close()

    _run(_show())


# Reusable helper for menu handlers — they call these functions instead of
# duplicating the argument-parsing logic.
async def list_datasets_async() -> None:
    """Async wrapper used by the menu loop."""
    list_datasets()


async def extract_dataset_async(dataset_id: str, output: Path) -> None:
    """Async wrapper used by the menu loop."""
    extract_dataset(dataset_id=dataset_id, output=output, show_progress=True)


async def upload_dataset_async(dataset_id: str, file_path: Path) -> None:
    """Async wrapper used by the menu loop."""
    upload_dataset(dataset_id=dataset_id, file=file_path, show_progress=True)


async def delete_dataset_async(dataset_id: str) -> None:
    """Async wrapper used by the menu loop."""
    delete_dataset(dataset_id=dataset_id, force=True)


async def show_dataset_async(dataset_id: str) -> None:
    """Async wrapper used by the menu loop."""
    show_dataset(dataset_id=dataset_id)
