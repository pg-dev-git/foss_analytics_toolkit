"""Data Manager Job CLI commands."""

import asyncio

import typer

from tcrm_toolkit.cli.ui import (
    console,
    print_header,
    print_warning,
)
from tcrm_toolkit.core import get_settings

app = typer.Typer(name="jobs", help="Data Manager job commands")


@app.command("list")
def list_jobs() -> None:
    """List Data Manager jobs."""
    asyncio.run(_list_jobs_async())


async def _list_jobs_async() -> None:
    """Async list jobs implementation."""
    print_header("Data Manager Jobs", "Listing all Data Manager jobs")
    print_warning("Authentication integration pending - use 'tcrm auth login' first")


@app.command("get")
def get_job(
    job_id: str = typer.Argument(help="Job ID"),
) -> None:
    """Get job details."""
    asyncio.run(_get_job_async(job_id))


async def _get_job_async(job_id: str) -> None:
    """Async get job implementation."""
    print_header("Job Details", f"Job ID: {job_id}")
    print_warning("Authentication integration pending")