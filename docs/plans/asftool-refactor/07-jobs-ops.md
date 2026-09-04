# Phase 7: Data Manager Jobs Menu

**Goal:** Wire jobs menu handlers to existing data manager job endpoints.

---

## Prerequisites

- Phase 3-6 complete
- Data manager job service or client methods available

---

## Files to Create/Modify

```
asftool/cli/menus/jobs.py
asftool/cli/commands/jobs.py
```

---

## Step 7.1: Create `asftool/cli/commands/jobs.py`

```python
"""Data Manager Jobs CLI commands."""

import asyncio

import typer
from rich.table import Table

from asftool.cli.session import Session
from asftool.cli.ui import console, print_error, print_header, print_info, print_success
from asftool.core.client import SalesforceClient

app = typer.Typer(help="Data Manager jobs")


def _run(coro):
    return asyncio.run(coro)


@app.command("list")
def list_jobs(
    page_size: int = typer.Option(50, "--page-size"),
):
    """List all Data Manager jobs."""
    async def _list():
        session = Session()
        try:
            async with session.client_context() as client:
                jobs = await client.list_dataflow_jobs(page_size=page_size)

                if not jobs:
                    print_info("No Data Manager jobs found")
                    return

                table = Table(title="Data Manager Jobs", show_header=True)
                table.add_column("ID", style="cyan")
                table.add_column("Name", style="white")
                table.add_column("Status", style="green")
                table.add_column("Type", style="blue")
                table.add_column("Updated", style="dim")

                for job in jobs:
                    status = job.get("status", "Unknown")
                    status_color = "green" if status == "Completed" else "yellow" if status in ("Running", "Queued") else "red"
                    table.add_row(
                        job.get("id", "N/A"),
                        job.get("name", "N/A"),
                        f"[{status_color}]{status}[/{status_color}]",
                        job.get("type", "N/A"),
                        job.get("updatedAt", "N/A")[:10] if job.get("updatedAt") else "N/A",
                    )
                console.print(table)
        except Exception as e:
            print_error(f"List failed: {e}")
            raise typer.Exit(1)
        finally:
            await session.close()

    _run(_list())


@app.command("show")
def show_job(
    job_id: str = typer.Argument(..., help="Job ID"),
):
    """Show job details."""
    async def _show():
        session = Session()
        try:
            async with session.client_context() as client:
                job = await client.get_dataflow_job(job_id)

                print_header(f"Job: {job.get('name', job_id)}")
                print_info(f"ID: {job.get('id')}")
                print_info(f"Status: {job.get('status')}")
                print_info(f"Type: {job.get('type')}")
                print_info(f"Created: {job.get('createdAt')}")
                print_info(f"Updated: {job.get('updatedAt')}")
        except Exception as e:
            print_error(f"Show failed: {e}")
            raise typer.Exit(1)
        finally:
            await session.close()

    _run(_show())
```

---

## Step 7.2: Create `asftool/cli/menus/jobs.py`

```python
"""Data Manager Jobs menu operations."""

from asftool.cli.menu import MenuItem


async def list_jobs():
    from asftool.cli.commands.jobs import list_jobs as cmd
    await cmd()


async def show_job():
    from asftool.cli.commands.jobs import show_job as cmd
    from asftool.cli.ui import prompt_text
    job_id = prompt_text("Job ID")
    if not job_id:
        return
    await cmd(job_id=job_id)


def jobs_operations(menu: "Menu") -> None:
    menu.add(MenuItem("1", "List all jobs", handler=list_jobs))
    menu.add(MenuItem("2", "Show job details", handler=show_job))
    menu.add(MenuItem("b", "Back", exit_after=True))
```

---

## Acceptance Criteria

- [ ] `asftool jobs list` works
- [ ] `asftool jobs show <id>` works
- [ ] Interactive menu: "1" → list, "2" → show
- [ ] If no Data Manager service exists, implement minimal client methods