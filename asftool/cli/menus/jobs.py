"""Data Manager Jobs menu — wired to Phase 7 job commands."""

import typer

from asftool.cli.menu import Menu, MenuItem
from asftool.cli.ui import prompt_text


async def list_jobs() -> None:
    from asftool.cli.commands.jobs import list_jobs_async

    try:
        await list_jobs_async()
    except typer.Exit:
        pass


async def show_job() -> None:
    from asftool.cli.commands.jobs import show_job_async

    job_id = prompt_text("Job ID")
    if not job_id:
        return
    try:
        await show_job_async(job_id=job_id)
    except typer.Exit:
        pass


def jobs_operations(menu: Menu) -> None:
    menu.add(MenuItem("1", "List all jobs", handler=list_jobs))
    menu.add(MenuItem("2", "Show job details", handler=show_job))
    menu.add(MenuItem("b", "Back", exit_after=True))
