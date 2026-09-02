"""Dataset CLI commands."""

import asyncio
from pathlib import Path

import typer

from tcrm_toolkit.cli.ui import (
    console,
    create_dataset_table,
    print_dataset_details,
    print_error,
    print_extraction_progress,
    print_header,
    print_info,
    print_success,
    print_upload_progress,
    prompt_confirm,
)
from tcrm_toolkit.core import SalesforceClient, get_settings
from tcrm_toolkit.core.auth import SFCLIAuthService
from tcrm_toolkit.core.crypto import create_crypto_manager
from tcrm_toolkit.core.services.dataset_service import DatasetService

app = typer.Typer(name="datasets", help="Dataset commands")


async def _get_client(alias: str = "default") -> SalesforceClient:
    """Get authenticated SalesforceClient."""
    settings = get_settings()
    crypto = create_crypto_manager()
    auth_service = SFCLIAuthService(settings, crypto)
    try:
        token = await auth_service.get_access_token(alias)
        instance_url = await auth_service.get_instance_url(alias)
    except Exception as e:
        print_error(f"Authentication failed: {e}. Run 'tcrm auth login' first.")
        raise typer.Exit(1)
    return SalesforceClient(access_token=token, instance_url=instance_url, settings=settings)


@app.command("list")
def list_datasets(
    page_size: int = typer.Option(50, "--page-size", "-n", help="Number of datasets per page"),
    sort: str = typer.Option("Mru", "--sort", "-s", help="Sort order: Mru, Name, CreatedDate"),
    alias: str = typer.Option("default", "--alias", "-a", help="Org alias"),
) -> None:
    """List all datasets."""
    asyncio.run(_list_datasets_async(page_size, sort, alias))


async def _list_datasets_async(page_size: int, sort: str, alias: str) -> None:
    """Async list datasets implementation."""
    client = await _get_client(alias)
    async with client:
        service = DatasetService(client, get_settings())
        datasets = await service.list_datasets(page_size=page_size, sort=sort)
        table = create_dataset_table(datasets)
        console.print(table)


@app.command("get")
def get_dataset(
    dataset_id: str = typer.Argument(help="Dataset ID"),
    alias: str = typer.Option("default", "--alias", "-a", help="Org alias"),
) -> None:
    """Get dataset details."""
    asyncio.run(_get_dataset_async(dataset_id, alias))


async def _get_dataset_async(dataset_id: str, alias: str) -> None:
    """Async get dataset implementation."""
    client = await _get_client(alias)
    async with client:
        service = DatasetService(client, get_settings())
        dataset = await service.get_dataset(dataset_id)
        print_dataset_details(dataset)


@app.command("extract")
def extract_dataset(
    dataset_id: str = typer.Argument(help="Dataset ID"),
    output: Path = typer.Option(
        None,
        "--output",
        "-o",
        help="Output CSV file path",
    ),
    alias: str = typer.Option("default", "--alias", "-a", help="Org alias"),
) -> None:
    """Extract dataset to CSV."""
    asyncio.run(_extract_dataset_async(dataset_id, output, alias))


async def _extract_dataset_async(dataset_id: str, output: Path | None, alias: str) -> None:
    """Async extract dataset implementation."""
    if output is None:
        output = Path(f"{dataset_id}.csv")

    print_header("Extract Dataset", f"Dataset: {dataset_id} -> {output}")
    client = await _get_client(alias)
    async with client:
        service = DatasetService(client, get_settings())
        await service.extract_dataset(
            dataset_id,
            output,
            progress_callback=lambda p: print_extraction_progress(p),
        )
        print_success(f"Dataset extracted successfully to {output}")


@app.command("upload")
def upload_dataset(
    dataset_id: str = typer.Argument(help="Dataset ID"),
    file: Path = typer.Argument(help="CSV file to upload"),
    operation: str = typer.Option(
        "Overwrite",
        "--operation",
        help="Upload operation: Overwrite or Append",
    ),
    alias: str = typer.Option("default", "--alias", "-a", help="Org alias"),
) -> None:
    """Upload CSV to dataset."""
    asyncio.run(_upload_dataset_async(dataset_id, file, operation, alias))


async def _upload_dataset_async(dataset_id: str, file: Path, operation: str, alias: str) -> None:
    """Async upload dataset implementation."""
    print_header("Upload Dataset", f"File: {file} -> Dataset: {dataset_id}")
    client = await _get_client(alias)
    async with client:
        service = DatasetService(client, get_settings())
        await service.upload_csv(
            dataset_id,
            file,
            operation=operation,
            progress_callback=lambda p: print_upload_progress(p),
        )
        print_success(f"CSV uploaded successfully to dataset {dataset_id}")


@app.command("delete")
def delete_dataset(
    dataset_id: str = typer.Argument(help="Dataset ID"),
    force: bool = typer.Option(False, "--force", "-f", help="Skip confirmation"),
    alias: str = typer.Option("default", "--alias", "-a", help="Org alias"),
) -> None:
    """Delete a dataset."""
    asyncio.run(_delete_dataset_async(dataset_id, force, alias))


async def _delete_dataset_async(dataset_id: str, force: bool, alias: str) -> None:
    """Async delete dataset implementation."""
    if not force:
        confirm = prompt_confirm(f"Delete dataset {dataset_id}? This cannot be undone.")
        if not confirm:
            print_info("Cancelled")
            return

    client = await _get_client(alias)
    async with client:
        service = DatasetService(client, get_settings())
        await service.delete_dataset(dataset_id)
        print_success(f"Dataset {dataset_id} deleted successfully")


@app.command("dependencies")
def dataset_dependencies(
    dataset_id: str = typer.Argument(help="Dataset ID"),
    alias: str = typer.Option("default", "--alias", "-a", help="Org alias"),
) -> None:
    """Show dataset dependencies (downstream dataflows/dashboards)."""
    asyncio.run(_dataset_dependencies_async(dataset_id, alias))


async def _dataset_dependencies_async(dataset_id: str, alias: str) -> None:
    """Async dataset dependencies implementation."""
    client = await _get_client(alias)
    async with client:
        service = DatasetService(client, get_settings())
        deps = await service.get_dataset_dependencies(dataset_id)
        print_header("Dataset Dependencies", f"Dataset: {dataset_id}")
        if not deps:
            print_info("No dependencies found")
        else:
            for dep in deps:
                print_info(f" - {dep.get('type', 'Unknown')}: {dep.get('name', dep.get('id', 'N/A'))}")

