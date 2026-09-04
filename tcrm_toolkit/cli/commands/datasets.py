"""Dataset CLI commands."""

import asyncio
from pathlib import Path

import typer

from tcrm_toolkit.cli.ui import (
    console,
    print_header,
    print_success,
    print_error,
    print_info,
    print_warning,
    prompt_confirm,
    prompt_text,
    prompt_select,
    create_dataset_table,
    print_dataset_details,
    print_extraction_progress,
    print_upload_progress,
    run_with_progress,
)
from tcrm_toolkit.core import get_settings, SalesforceClient, create_client
from tcrm_toolkit.core.services.dataset_service import DatasetService
from tcrm_toolkit.core.crypto import CryptoManager
from tcrm_toolkit.core.services.auth_service import AuthService

app = typer.Typer(name="datasets", help="Dataset commands")


def _get_dataset_service() -> tuple[DatasetService, SalesforceClient, AuthService]:
    """Get configured dataset service with client and auth."""
    settings = get_settings()
    auth_service = AuthService(settings, CryptoManager(settings.encryption_key))
    # We'll need to get the token from keyring
    # For now, this is a placeholder - in real usage, token would be retrieved
    raise NotImplementedError("Need to implement token retrieval from keyring")


@app.command("list")
def list_datasets(
    page_size: int = typer.Option(50, "--page-size", "-n", help="Number of datasets per page"),
    sort: str = typer.Option("Mru", "--sort", "-s", help="Sort order: Mru, Name, CreatedDate"),
) -> None:
    """List all datasets."""
    asyncio.run(_list_datasets_async(page_size, sort))


async def _list_datasets_async(page_size: int, sort: str) -> None:
    """Async list datasets implementation."""
    print_header("Datasets", "Listing all TCRM datasets")

    # This is a placeholder - needs proper auth integration
    print_warning("Authentication integration pending - use 'tcrm auth login' first")
    print_info("This command requires a valid Salesforce session")


@app.command("get")
def get_dataset(
    dataset_id: str = typer.Argument(help="Dataset ID"),
) -> None:
    """Get dataset details."""
    asyncio.run(_get_dataset_async(dataset_id))


async def _get_dataset_async(dataset_id: str) -> None:
    """Async get dataset implementation."""
    print_header("Dataset Details", f"Dataset ID: {dataset_id}")
    print_warning("Authentication integration pending")


@app.command("extract")
def extract_dataset(
    dataset_id: str = typer.Argument(help="Dataset ID"),
    output: Path = typer.Option(
        None,
        "--output",
        "-o",
        help="Output CSV file path",
    ),
) -> None:
    """Extract dataset to CSV."""
    asyncio.run(_extract_dataset_async(dataset_id, output))


async def _extract_dataset_async(dataset_id: str, output: Path | None) -> None:
    """Async extract dataset implementation."""
    if output is None:
        output = Path(f"{dataset_id}.csv")

    print_header("Extract Dataset", f"Dataset: {dataset_id} -> {output}")
    print_warning("Authentication integration pending")


@app.command("upload")
def upload_dataset(
    dataset_id: str = typer.Argument(help="Dataset ID"),
    file: Path = typer.Argument(help="CSV file to upload"),
    operation: str = typer.Option(
        "Overwrite",
        "--operation",
        help="Upload operation: Overwrite or Append",
    ),
) -> None:
    """Upload CSV to dataset."""
    asyncio.run(_upload_dataset_async(dataset_id, file, operation))


async def _upload_dataset_async(dataset_id: str, file: Path, operation: str) -> None:
    """Async upload dataset implementation."""
    print_header("Upload Dataset", f"File: {file} -> Dataset: {dataset_id}")
    print_warning("Authentication integration pending")


@app.command("delete")
def delete_dataset(
    dataset_id: str = typer.Argument(help="Dataset ID"),
    force: bool = typer.Option(False, "--force", "-f", help="Skip confirmation"),
) -> None:
    """Delete a dataset."""
    asyncio.run(_delete_dataset_async(dataset_id, force))


async def _delete_dataset_async(dataset_id: str, force: bool) -> None:
    """Async delete dataset implementation."""
    if not force:
        confirm = prompt_confirm(f"Delete dataset {dataset_id}? This cannot be undone.")
        if not confirm:
            print_info("Cancelled")
            return

    print_header("Delete Dataset", f"Dataset: {dataset_id}")
    print_warning("Authentication integration pending")


@app.command("dependencies")
def dataset_dependencies(
    dataset_id: str = typer.Argument(help="Dataset ID"),
) -> None:
    """Show dataset dependencies (downstream dataflows/dashboards)."""
    asyncio.run(_dataset_dependencies_async(dataset_id))


async def _dataset_dependencies_async(dataset_id: str) -> None:
    """Async dataset dependencies implementation."""
    print_header("Dataset Dependencies", f"Dataset: {dataset_id}")
    print_warning("Authentication integration pending")