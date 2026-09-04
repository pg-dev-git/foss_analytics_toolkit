# Phase 4: Dataset Operations Menu

**Goal:** Wire dataset menu handlers to existing `DatasetService`.

---

## Prerequisites

- Phase 3 complete (menu loop working)
- `DatasetService` already implemented in `core/services/dataset_service.py`

---

## Files to Create/Modify

```
asftool/cli/menus/datasets.py          # Menu wiring
asftool/cli/commands/datasets.py       # CLI command handlers (reused by menu)
```

---

## Step 4.1: Update `asftool/cli/commands/datasets.py`

Replace stub with real implementation using `Session` + `DatasetService`.

```python
"""Dataset CLI commands."""

import asyncio
from pathlib import Path

import typer
from rich.table import Table

from asftool.cli.session import Session
from asftool.cli.ui import console, print_error, print_header, print_info, print_success, print_warning, prompt_text
from asftool.core.exceptions import DatasetError, UploadError
from asftool.core.models import ExtractionJob, UploadJob
from asftool.core.services import DatasetService

app = typer.Typer(help="Dataset operations")


def _run(coro):
    return asyncio.run(coro)


def _get_service() -> tuple[DatasetService, Session]:
    session = Session()
    return session, session


@app.command("list")
def list_datasets(
    page_size: int = typer.Option(50, "--page-size", help="Page size"),
    sort: str = typer.Option("Mru", "--sort", help="Sort order: Mru, Name, CreatedDate"),
):
    """List all datasets."""
    async def _list():
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
                        ds.updated_at.strftime("%Y-%m-%d") if ds.updated_at else "N/A",
                    )
                console.print(table)
                print_info(f"Total: {len(datasets)} datasets")
        except Exception as e:
            print_error(f"List failed: {e}")
            raise typer.Exit(1)
        finally:
            await session.close()

    _run(_list())


@app.command("extract")
def extract_dataset(
    dataset_id: str = typer.Argument(..., help="Dataset ID"),
    output: Path = typer.Option(Path.cwd() / "extracted.csv", "--output", "-o", help="Output CSV path"),
    progress: bool = typer.Option(True, "--progress/--no-progress", help="Show progress"),
):
    """Extract dataset to CSV."""
    async def _extract():
        session = Session()
        try:
            async with session.client_context() as client:
                service = DatasetService(client, session.settings)

                def progress_cb(p):
                    if progress:
                        print_info(f"  {p.processed_rows:,}/{p.total_rows:,} rows ({p.current_chunk}/{p.total_chunks} chunks)")

                job = await service.extract_dataset(
                    dataset_id=dataset_id,
                    output_path=output,
                    progress_callback=progress_cb,
                )

                print_success(f"Extracted {job.processed_rows:,} rows to {job.result_path}")
        except DatasetError as e:
            print_error(f"Extract failed: {e}")
            raise typer.Exit(1)
        except Exception as e:
            print_error(f"Extract failed: {e}")
            raise typer.Exit(1)
        finally:
            await session.close()

    _run(_extract())


@app.command("upload")
def upload_dataset(
    dataset_id: str = typer.Argument(..., help="Dataset ID"),
    file: Path = typer.Argument(..., help="CSV file path"),
    name: str | None = typer.Option(None, "--name", "-n", help="Dataset name (default: from dataset)"),
    operation: str = typer.Option("Overwrite", "--operation", "-o", help="Overwrite, Append, Delete"),
    progress: bool = typer.Option(True, "--progress/--no-progress", help="Show progress"),
):
    """Upload CSV to dataset."""
    async def _upload():
        session = Session()
        try:
            async with session.client_context() as client:
                service = DatasetService(client, session.settings)

                def progress_cb(p):
                    if progress:
                        print_info(f"  {p.uploaded_rows:,}/{p.total_rows:,} rows ({p.current_part}/{p.total_parts} parts)")

                job = await service.upload_csv(
                    dataset_id=dataset_id,
                    file_path=file,
                    dataset_name=name,
                    operation=operation,
                    progress_callback=progress_cb,
                )

                print_success(f"Uploaded {job.uploaded_rows:,} rows to {job.dataset_id}")
        except (UploadError, FileNotFoundError) as e:
            print_error(f"Upload failed: {e}")
            raise typer.Exit(1)
        except Exception as e:
            print_error(f"Upload failed: {e}")
            raise typer.Exit(1)
        finally:
            await session.close()

    _run(_upload())


@app.command("delete")
def delete_dataset(
    dataset_id: str = typer.Argument(..., help="Dataset ID"),
    force: bool = typer.Option(False, "--force", "-f", help="Skip confirmation"),
):
    """Delete a dataset."""
    async def _delete():
        session = Session()
        if not force:
            confirm = typer.confirm(f"Delete dataset {dataset_id}?")
            if not confirm:
                print_info("Cancelled")
                return

        try:
            async with session.client_context() as client:
                service = DatasetService(client, session.settings)
                await service.delete_dataset(dataset_id)
                print_success(f"Dataset {dataset_id} deleted")
        except DatasetError as e:
            print_error(f"Delete failed: {e}")
            raise typer.Exit(1)
        except Exception as e:
            print_error(f"Delete failed: {e}")
            raise typer.Exit(1)
        finally:
            await session.close()

    _run(_delete())


@app.command("show")
def show_dataset(
    dataset_id: str = typer.Argument(..., help="Dataset ID"),
):
    """Show dataset details."""
    async def _show():
        session = Session()
        try:
            async with session.client_context() as client:
                service = DatasetService(client, session.settings)
                dataset = await service.get_dataset(dataset_id)

                print_header(f"Dataset: {dataset.name}")
                print_info(f"ID: {dataset.id}")
                print_info(f"Version: {dataset.current_version_id}")
                print_info(f"Rows: {dataset.row_count:,}" if dataset.row_count else "Rows: N/A")
                print_info(f"Created: {dataset.created_at}")
                print_info(f"Updated: {dataset.updated_at}")
                print_info(f"Description: {dataset.description or 'N/A'}")
        except Exception as e:
            print_error(f"Show failed: {e}")
            raise typer.Exit(1)
        finally:
            await session.close()

    _run(_show())
```

---

## Step 4.2: Create `asftool/cli/menus/datasets.py`

Wire the menu handlers to reuse the CLI command logic.

```python
"""Dataset menu operations."""

from asftool.cli.menu import MenuItem


async def list_datasets():
    from asftool.cli.commands.datasets import list_datasets as cmd
    await cmd()


async def extract_dataset():
    from asftool.cli.commands.datasets import extract_dataset as cmd
    # Get dataset ID from user
    from asftool.cli.ui import console, prompt_text
    dataset_id = prompt_text("Dataset ID")
    if not dataset_id:
        return
    output = prompt_text("Output CSV path", default="extracted.csv")
    await cmd(dataset_id=dataset_id, output=output)


async def upload_dataset():
    from asftool.cli.commands.datasets import upload_dataset as cmd
    from asftool.cli.ui import prompt_text
    dataset_id = prompt_text("Dataset ID")
    if not dataset_id:
        return
    file_path = prompt_text("CSV file path")
    if not file_path:
        return
    await cmd(dataset_id=dataset_id, file=file_path)


async def delete_dataset():
    from asftool.cli.commands.datasets import delete_dataset as cmd
    from asftool.cli.ui import prompt_text
    dataset_id = prompt_text("Dataset ID")
    if not dataset_id:
        return
    await cmd(dataset_id=dataset_id, force=False)


async def show_dataset():
    from asftool.cli.commands.datasets import show_dataset as cmd
    from asftool.cli.ui import prompt_text
    dataset_id = prompt_text("Dataset ID")
    if not dataset_id:
        return
    await cmd(dataset_id=dataset_id)


def dataset_operations(menu: "Menu") -> None:
    """Wire up dataset submenu."""
    menu.add(MenuItem("1", "List all datasets", handler=list_datasets))
    menu.add(MenuItem("2", "Extract dataset to CSV", handler=extract_dataset))
    menu.add(MenuItem("3", "Upload CSV to dataset", handler=upload_dataset))
    menu.add(MenuItem("4", "Delete dataset", handler=delete_dataset))
    menu.add(MenuItem("5", "Show dataset details", handler=show_dataset))
    menu.add(MenuItem("b", "Back", exit_after=True))
```

---

## Acceptance Criteria

- [ ] `asftool datasets list` works
- [ ] `asftool datasets extract <id> -o file.csv` works
- [ ] `asftool datasets upload <id> file.csv` works
- [ ] `asftool datasets delete <id> --force` works
- [ ] `asftool datasets show <id>` works
- [ ] Interactive menu: "1" → list, "2" → extract, "3" → upload, "4" → delete, "5" → show
- [ ] Progress callback shows rows/chunks during extract/upload
- [ ] Errors displayed with Rich formatting
- [ ] `uv run pytest tests/integration/test_api_endpoints.py::test_dataset -v` passes

---

## Notes

- All handlers use `Session` → `DatasetService` → `SalesforceClient`
- Progress callbacks use the same pattern as TUI but with Rich output
- CLI commands and menu handlers share the same code (DRY)
- The parallel extraction (async SAQL + ProcessPool merge) is already in `DatasetService.extract_dataset`