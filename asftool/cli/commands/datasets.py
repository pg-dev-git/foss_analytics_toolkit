"""Dataset CLI commands.

List, extract, upload, delete, and inspect datasets. Delegates to
DatasetService for all API work. The same async functions are reused
by the interactive menu (asftool/cli/menus/datasets.py).

Each command exposes both:
  - A Typer command (``list``, ``extract``, ...) callable from the CLI.
  - An ``*_async`` wrapper with the actual coroutine, callable from
    the interactive menu (which already runs an asyncio event loop).

The Typer command bodies are thin shims that call ``_run(_async_fn())``
so the async logic lives in exactly one place.
"""

import asyncio
from collections.abc import Callable
from pathlib import Path
from typing import Literal

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
    """Run async coroutine in a fresh event loop (used only by Typer commands)."""
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


# ---------------------------------------------------------------------------
# Async wrappers (used by the interactive menu and by the Typer commands below)
# ---------------------------------------------------------------------------


async def list_datasets_async(
    page_size: int = 50, sort: str = "Mru"
) -> None:
    """List all datasets."""
    session = Session()
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


async def extract_dataset_async(
    dataset_id: str,
    output: Path,
    show_progress: bool = True,
) -> None:
    """Extract dataset to CSV."""
    session = Session()
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


async def upload_dataset_async(
    dataset_id: str,
    file_path: Path,
    name: str | None = None,
    operation: Literal["Overwrite", "Append"] = "Overwrite",
    show_progress: bool = True,
) -> None:
    """Upload CSV to dataset."""
    session = Session()
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
                file_path=file_path,
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


async def delete_dataset_async(dataset_id: str) -> None:
    """Delete a dataset (no confirmation — menu callers handle UX)."""
    session = Session()
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


async def show_dataset_async(dataset_id: str) -> None:
    """Show dataset details."""
    session = Session()
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


# ---------------------------------------------------------------------------
# Typer commands (thin shims that call the async wrappers)
# ---------------------------------------------------------------------------


@app.command("list")
def list_datasets(
    page_size: int = typer.Option(50, "--page-size", help="Page size"),
    sort: str = typer.Option("Mru", "--sort", help="Sort order: Mru, Name, CreatedDate"),
):
    """List all datasets."""
    _run(list_datasets_async(page_size=page_size, sort=sort))


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
    _run(
        extract_dataset_async(
            dataset_id=dataset_id, output=output, show_progress=show_progress
        )
    )


@app.command("upload")
def upload_dataset(
    dataset_id: str = typer.Argument(..., help="Dataset ID"),
    file: Path = typer.Argument(..., help="CSV file path"),
    name: str | None = typer.Option(
        None, "--name", "-n", help="Dataset name (default: from dataset)"
    ),
    operation: Literal["Overwrite", "Append"] = typer.Option(
        "Overwrite", "--operation", "-o", help="Overwrite or Append"
    ),
    show_progress: bool = typer.Option(
        True, "--progress/--no-progress", help="Show progress"
    ),
):
    """Upload CSV to dataset."""
    _run(
        upload_dataset_async(
            dataset_id=dataset_id,
            file_path=file,
            name=name,
            operation=operation,
            show_progress=show_progress,
        )
    )


@app.command("delete")
def delete_dataset(
    dataset_id: str = typer.Argument(..., help="Dataset ID"),
    force: bool = typer.Option(False, "--force", "-f", help="Skip confirmation"),
):
    """Delete a dataset."""
    if not force:
        confirm = typer.confirm(f"Delete dataset {dataset_id}?")
        if not confirm:
            print_info("Cancelled")
            return
    _run(delete_dataset_async(dataset_id=dataset_id))


@app.command("show")
def show_dataset(
    dataset_id: str = typer.Argument(..., help="Dataset ID"),
):
    """Show dataset details."""
    _run(show_dataset_async(dataset_id=dataset_id))
