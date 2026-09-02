"""Data Manager Job CLI commands."""

import asyncio

import typer

from tcrm_toolkit.cli.ui import (
    console,
    create_dataflow_job_table,
    print_error,
    print_header,
    print_info,
)
from tcrm_toolkit.core import SalesforceClient, get_settings
from tcrm_toolkit.core.auth import SFCLIAuthService
from tcrm_toolkit.core.crypto import create_crypto_manager
from tcrm_toolkit.core.services.dataflow_service import DataflowService

app = typer.Typer(name="jobs", help="Data Manager job commands")


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
def list_jobs(
    alias: str = typer.Option("default", "--alias", "-a", help="Org alias"),
) -> None:
    """List Data Manager jobs."""
    asyncio.run(_list_jobs_async(alias))


async def _list_jobs_async(alias: str) -> None:
    """Async list jobs implementation."""
    client = await _get_client(alias)
    async with client:
        service = DataflowService(client, get_settings())
        jobs = await service.list_dataflow_jobs()
        table = create_dataflow_job_table(jobs)
        console.print(table)


@app.command("get")
def get_job(
    job_id: str = typer.Argument(help="Job ID"),
    alias: str = typer.Option("default", "--alias", "-a", help="Org alias"),
) -> None:
    """Get job details."""
    asyncio.run(_get_job_async(job_id, alias))


async def _get_job_async(job_id: str, alias: str) -> None:
    """Async get job implementation."""
    client = await _get_client(alias)
    async with client:
        service = DataflowService(client, get_settings())
        job = await service.get_dataflow_job_status(job_id)
        if not job:
            print_error(f"Job {job_id} not found")
            raise typer.Exit(1)
        print_header("Job Details", f"Job ID: {job_id}")
        print_info(f"Dataflow: {job.dataflow_name} ({job.dataflow_id})")
        print_info(f"Command: {job.command}")
        print_info(f"Status: {job.status}")
        if job.start_time:
            print_info(f"Started: {job.start_time}")
        if job.end_time:
            print_info(f"Ended: {job.end_time}")

