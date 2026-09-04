"""Data Manager Jobs CLI commands.

List and inspect dataflow jobs. Delegates to DataflowService for all
API work. The same functions are reused by the interactive menu
(asftool/cli/menus/jobs.py).
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
    return asyncio.run(coro)


_STATUS_COLORS = {
    "Success": "green",
    "Running": "green",
    "Queued": "yellow",
    "Cancelled": "yellow",
    "Failed": "red",
}


@app.command("list")
def list_jobs():
    """List all dataflow jobs."""
    session = Session()

    async def _list() -> None:
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

    _run(_list())


@app.command("show")
def show_job(
    job_id: str = typer.Argument(..., help="Job ID"),
):
    """Show job details."""
    session = Session()

    async def _show() -> None:
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

    _run(_show())


# Async wrappers for the menu layer -----------------------------------------


async def list_jobs_async() -> None:
    list_jobs()


async def show_job_async(job_id: str) -> None:
    show_job(job_id=job_id)
