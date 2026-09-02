"""Dataflow CLI commands."""

import asyncio
from pathlib import Path

import typer

from tcrm_toolkit.cli.ui import (
    console,
    create_dataflow_job_table,
    create_dataflow_table,
    print_dataflow_details,
    print_error,
    print_info,
    print_success,
)
from tcrm_toolkit.core import SalesforceClient, get_settings
from tcrm_toolkit.core.auth import SFCLIAuthService
from tcrm_toolkit.core.crypto import create_crypto_manager
from tcrm_toolkit.core.services.dataflow_service import DataflowService

app = typer.Typer(name="dataflows", help="Dataflow commands")


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
def list_dataflows(
    alias: str = typer.Option("default", "--alias", "-a", help="Org alias"),
) -> None:
    """List all dataflows."""
    asyncio.run(_list_dataflows_async(alias))


async def _list_dataflows_async(alias: str) -> None:
    """Async list dataflows implementation."""
    client = await _get_client(alias)
    async with client:
        service = DataflowService(client, get_settings())
        dataflows = await service.list_dataflows()
        table = create_dataflow_table(dataflows)
        console.print(table)


@app.command("get")
def get_dataflow(
    dataflow_id: str = typer.Argument(help="Dataflow ID"),
    alias: str = typer.Option("default", "--alias", "-a", help="Org alias"),
) -> None:
    """Get dataflow details."""
    asyncio.run(_get_dataflow_async(dataflow_id, alias))


async def _get_dataflow_async(dataflow_id: str, alias: str) -> None:
    """Async get dataflow implementation."""
    client = await _get_client(alias)
    async with client:
        service = DataflowService(client, get_settings())
        dataflow = await service.get_dataflow(dataflow_id)
        print_dataflow_details(dataflow)


@app.command("start")
def start_dataflow(
    dataflow_id: str = typer.Argument(help="Dataflow ID"),
    alias: str = typer.Option("default", "--alias", "-a", help="Org alias"),
) -> None:
    """Start a dataflow."""
    asyncio.run(_start_dataflow_async(dataflow_id, alias))


async def _start_dataflow_async(dataflow_id: str, alias: str) -> None:
    """Async start dataflow implementation."""
    client = await _get_client(alias)
    async with client:
        service = DataflowService(client, get_settings())
        job = await service.start_dataflow(dataflow_id)
        print_success(f"Dataflow {dataflow_id} started. Job ID: {job.id} (Status: {job.status})")


@app.command("stop")
def stop_dataflow(
    dataflow_id: str = typer.Argument(help="Dataflow ID"),
    alias: str = typer.Option("default", "--alias", "-a", help="Org alias"),
) -> None:
    """Stop a running dataflow."""
    asyncio.run(_stop_dataflow_async(dataflow_id, alias))


async def _stop_dataflow_async(dataflow_id: str, alias: str) -> None:
    """Async stop dataflow implementation."""
    client = await _get_client(alias)
    async with client:
        service = DataflowService(client, get_settings())
        job = await service.stop_dataflow(dataflow_id)
        print_success(f"Dataflow {dataflow_id} stop requested. Job ID: {job.id} (Status: {job.status})")


@app.command("jobs")
def list_dataflow_jobs(
    alias: str = typer.Option("default", "--alias", "-a", help="Org alias"),
) -> None:
    """List dataflow jobs."""
    asyncio.run(_list_dataflow_jobs_async(alias))


async def _list_dataflow_jobs_async(alias: str) -> None:
    """Async list dataflow jobs implementation."""
    client = await _get_client(alias)
    async with client:
        service = DataflowService(client, get_settings())
        jobs = await service.list_dataflow_jobs()
        table = create_dataflow_job_table(jobs)
        console.print(table)


@app.command("wait")
def wait_for_job(
    job_id: str = typer.Argument(help="Dataflow job ID"),
    poll_interval: int = typer.Option(10, "--interval", "-i", help="Poll interval in seconds"),
    timeout: int = typer.Option(3600, "--timeout", "-t", help="Timeout in seconds"),
    alias: str = typer.Option("default", "--alias", "-a", help="Org alias"),
) -> None:
    """Wait for a dataflow job to complete."""
    asyncio.run(_wait_for_job_async(job_id, poll_interval, timeout, alias))


async def _wait_for_job_async(job_id: str, poll_interval: int, timeout: int, alias: str) -> None:
    """Async wait for job implementation."""
    client = await _get_client(alias)
    async with client:
        service = DataflowService(client, get_settings())
        print_info(f"Waiting for job {job_id} to complete...")
        job = await service.wait_for_dataflow_job(job_id, poll_interval=poll_interval, timeout=timeout)
        if job.status == "Success":
            print_success(f"Job {job_id} completed successfully (Status: {job.status})")
        else:
            print_error(f"Job {job_id} finished with status: {job.status}")


@app.command("backup")
def backup_dataflow(
    dataflow_id: str = typer.Argument(help="Dataflow ID"),
    output: Path = typer.Option(None, "--output", "-o", help="Output JSON file path"),
    alias: str = typer.Option("default", "--alias", "-a", help="Org alias"),
) -> None:
    """Backup dataflow definition."""
    asyncio.run(_backup_dataflow_async(dataflow_id, output, alias))


async def _backup_dataflow_async(dataflow_id: str, output: Path | None, alias: str) -> None:
    """Async backup dataflow implementation."""
    if output is None:
        output = Path(f"{dataflow_id}_backup.json")

    client = await _get_client(alias)
    async with client:
        service = DataflowService(client, get_settings())
        await service.backup_dataflow(dataflow_id, str(output))
        print_success(f"Dataflow backed up successfully to {output}")

