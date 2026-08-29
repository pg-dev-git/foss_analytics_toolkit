"""Dataflow CLI commands."""

import asyncio

import typer

from tcrm_toolkit.cli.ui import (
    print_header,
    print_warning,
)

app = typer.Typer(name="dataflows", help="Dataflow commands")


@app.command("list")
def list_dataflows() -> None:
    """List all dataflows."""
    asyncio.run(_list_dataflows_async())


async def _list_dataflows_async() -> None:
    """Async list dataflows implementation."""
    print_header("Dataflows", "Listing all TCRM dataflows")
    print_warning("Authentication integration pending - use 'tcrm auth login' first")


@app.command("get")
def get_dataflow(
    dataflow_id: str = typer.Argument(help="Dataflow ID"),
) -> None:
    """Get dataflow details."""
    asyncio.run(_get_dataflow_async(dataflow_id))


async def _get_dataflow_async(dataflow_id: str) -> None:
    """Async get dataflow implementation."""
    print_header("Dataflow Details", f"Dataflow ID: {dataflow_id}")
    print_warning("Authentication integration pending")


@app.command("start")
def start_dataflow(
    dataflow_id: str = typer.Argument(help="Dataflow ID"),
) -> None:
    """Start a dataflow."""
    asyncio.run(_start_dataflow_async(dataflow_id))


async def _start_dataflow_async(dataflow_id: str) -> None:
    """Async start dataflow implementation."""
    print_header("Start Dataflow", f"Dataflow: {dataflow_id}")
    print_warning("Authentication integration pending")


@app.command("stop")
def stop_dataflow(
    dataflow_id: str = typer.Argument(help="Dataflow ID"),
) -> None:
    """Stop a running dataflow."""
    asyncio.run(_stop_dataflow_async(dataflow_id))


async def _stop_dataflow_async(dataflow_id: str) -> None:
    """Async stop dataflow implementation."""
    print_header("Stop Dataflow", f"Dataflow: {dataflow_id}")
    print_warning("Authentication integration pending")


@app.command("jobs")
def list_dataflow_jobs() -> None:
    """List dataflow jobs."""
    asyncio.run(_list_dataflow_jobs_async())


async def _list_dataflow_jobs_async() -> None:
    """Async list dataflow jobs implementation."""
    print_header("Dataflow Jobs", "Listing all dataflow job executions")
    print_warning("Authentication integration pending")


@app.command("wait")
def wait_for_job(
    job_id: str = typer.Argument(help="Dataflow job ID"),
    poll_interval: int = typer.Option(10, "--interval", "-i", help="Poll interval in seconds"),
    timeout: int = typer.Option(3600, "--timeout", "-t", help="Timeout in seconds"),
) -> None:
    """Wait for a dataflow job to complete."""
    asyncio.run(_wait_for_job_async(job_id, poll_interval, timeout))


async def _wait_for_job_async(job_id: str, poll_interval: int, timeout: int) -> None:
    """Async wait for job implementation."""
    print_header("Wait for Job", f"Job: {job_id}")
    print_warning("Authentication integration pending")


@app.command("backup")
def backup_dataflow(
    dataflow_id: str = typer.Argument(help="Dataflow ID"),
    output: str = typer.Option(None, "--output", "-o", help="Output JSON file path"),
) -> None:
    """Backup dataflow definition."""
    asyncio.run(_backup_dataflow_async(dataflow_id, output))


async def _backup_dataflow_async(dataflow_id: str, output: str | None) -> None:
    """Async backup dataflow implementation."""
    print_header("Backup Dataflow", f"Dataflow: {dataflow_id}")
    print_warning("Authentication integration pending")
