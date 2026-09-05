"""Data Manager Jobs CLI commands.

List and inspect dataflow jobs. Delegates to DataflowService for all
API work. The same async functions are reused by the interactive menu
(asftool/cli/menus/jobs.py).

Each command exposes both:
  - A Typer command (``list``, ``show``) callable from the CLI.
  - An ``*_async`` wrapper with the actual coroutine, callable from
    the interactive menu (which already runs an asyncio event loop).

The Typer command bodies are thin shims that call ``_run(_async_fn())``
so the async logic lives in exactly one place.
"""

import asyncio

import typer
from rich.table import Table

from asftool.cli.session import Session
from asftool.cli.ui import console, print_error, print_header, print_info, print_warning
from asftool.core.exceptions import DataflowError
from asftool.core.services import DataflowService

app = typer.Typer(help="Data Manager jobs")


def _run(coro):
    """Run async coroutine in a fresh event loop (used only by Typer commands)."""
    return asyncio.run(coro)


_STATUS_COLORS = {
    "Success": "green",
    "Running": "green",
    "Queued": "yellow",
    "Cancelled": "yellow",
    "Failed": "red",
}


# ---------------------------------------------------------------------------
# Async wrappers (used by the interactive menu and by the Typer commands below)
# ---------------------------------------------------------------------------


async def list_jobs_async() -> None:
    """List all dataflow jobs."""
    session = Session()
    try:
        async with session.client_context() as client:
            service = DataflowService(client, session.settings)
            jobs = await service.list_dataflow_jobs()

            if not jobs:
                print_info("No Data Manager jobs found")
                return

            table = Table(title="Data Manager Jobs", show_header=True)
            table.add_column("ID", style="cyan")
            table.add_column("Dataflow", style="white")
            table.add_column("Command", style="blue")
            table.add_column("Status", style="bold")
            table.add_column("Started", style="dim")

            for job in jobs:
                color = _STATUS_COLORS.get(job.status, "white")
                started = (
                    job.start_time.strftime("%Y-%m-%d %H:%M")
                    if job.start_time
                    else "N/A"
                )
                table.add_row(
                    job.id,
                    job.dataflow_name,
                    job.command,
                    f"[{color}]{job.status}[/{color}]",
                    started,
                )
            console.print(table)
            print_info(f"Total: {len(jobs)} jobs")
    except Exception as e:
        print_error(f"List failed: {e}")
        raise typer.Exit(1) from e
    finally:
        await session.close()


async def show_job_async(job_id: str) -> None:
    """Show job details."""
    session = Session()
    try:
        async with session.client_context() as client:
            service = DataflowService(client, session.settings)
            job = await service.get_dataflow_job_status(job_id)
            if job is None:
                print_warning(f"Job {job_id} not found")
                raise typer.Exit(1) from None

            print_header(f"Job: {job.id}")
            print_info(f"Dataflow: {job.dataflow_name} ({job.dataflow_id})")
            print_info(f"Command: {job.command}")
            color = _STATUS_COLORS.get(job.status, "white")
            print_info(f"Status: [{color}]{job.status}[/{color}]")
            print_info(
                f"Started: {job.start_time.isoformat() if job.start_time else 'N/A'}"
            )
            print_info(
                f"Ended: {job.end_time.isoformat() if job.end_time else 'N/A'}"
            )
            if job.error_message:
                print_info(f"Error: {job.error_message}")
    except DataflowError as e:
        print_error(f"Show failed: {e}")
        raise typer.Exit(1) from e
    except Exception as e:
        print_error(f"Show failed: {e}")
        raise typer.Exit(1) from e
    finally:
        await session.close()


# ---------------------------------------------------------------------------
# Typer commands (thin shims that call the async wrappers)
# ---------------------------------------------------------------------------


@app.command("list")
def list_jobs():
    """List all dataflow jobs."""
    _run(list_jobs_async())


@app.command("show")
def show_job(
    job_id: str = typer.Argument(..., help="Job ID"),
):
    """Show job details."""
    _run(show_job_async(job_id=job_id))
