"""Rich UI components for ASFTool CLI."""

from collections.abc import Callable
from typing import Any

from rich.console import Console
from rich.panel import Panel
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TaskProgressColumn,
    TextColumn,
    TimeRemainingColumn,
)
from rich.prompt import Confirm, Prompt
from rich.table import Table
from rich.text import Text

from asftool.core.models import (
    Dashboard,
    Dataflow,
    DataflowJob,
    Dataset,
    ExtractionProgress,
    UploadProgress,
)

console = Console()


def print_header(title: str, subtitle: str | None = None) -> None:
    """Print a styled header."""
    text = Text(title, style="bold cyan")
    if subtitle:
        text.append(f"\n{subtitle}", style="dim")
    console.print(Panel(text, border_style="cyan"))


def print_success(message: str) -> None:
    """Print a success message."""
    console.print(f"[green]✓[/green] {message}")


def print_error(message: str) -> None:
    """Print an error message."""
    console.print(f"[red]✗[/red] {message}")


def print_warning(message: str) -> None:
    """Print a warning message."""
    console.print(f"[yellow]⚠[/yellow] {message}")


def print_info(message: str) -> None:
    """Print an info message."""
    console.print(f"[blue]ℹ[/blue] {message}")


def create_dataset_table(datasets: list[Dataset]) -> Table:
    """Create a formatted table for datasets."""
    table = Table(title="Datasets", show_header=True, header_style="bold magenta")
    table.add_column("#", style="dim", width=4)
    table.add_column("ID", style="cyan", no_wrap=True)
    table.add_column("Name", style="green")
    table.add_column("Label", style="white")
    table.add_column("Rows", justify="right", style="yellow")
    table.add_column("Status", style="magenta")

    for i, ds in enumerate(datasets, 1):
        rows = str(ds.row_count) if ds.row_count is not None else "N/A"
        table.add_row(str(i), ds.id, ds.name, ds.label, rows, ds.status)

    return table


def create_dashboard_table(dashboards: list[Dashboard]) -> Table:
    """Create a formatted table for dashboards."""
    table = Table(title="Dashboards", show_header=True, header_style="bold magenta")
    table.add_column("#", style="dim", width=4)
    table.add_column("ID", style="cyan", no_wrap=True)
    table.add_column("Name", style="green")
    table.add_column("Label", style="white")
    table.add_column("Folder", style="yellow")

    for i, db in enumerate(dashboards, 1):
        folder = db.folder_name or "N/A"
        table.add_row(str(i), db.id, db.name, db.label, folder)

    return table


def create_dataflow_table(dataflows: list[Dataflow]) -> Table:
    """Create a formatted table for dataflows."""
    table = Table(title="Dataflows", show_header=True, header_style="bold magenta")
    table.add_column("#", style="dim", width=4)
    table.add_column("ID", style="cyan", no_wrap=True)
    table.add_column("Name", style="green")
    table.add_column("Label", style="white")
    table.add_column("Status", style="yellow")

    for i, df in enumerate(dataflows, 1):
        table.add_row(str(i), df.id, df.name, df.label, df.status)

    return table


def create_dataflow_job_table(jobs: list[DataflowJob]) -> Table:
    """Create a formatted table for dataflow jobs."""
    table = Table(title="Dataflow Jobs", show_header=True, header_style="bold magenta")
    table.add_column("#", style="dim", width=4)
    table.add_column("ID", style="cyan", no_wrap=True)
    table.add_column("Dataflow", style="green")
    table.add_column("Command", style="yellow")
    table.add_column("Status", style="magenta")
    table.add_column("Start Time", style="dim")
    table.add_column("End Time", style="dim")

    for i, job in enumerate(jobs, 1):
        start = job.start_time.strftime("%Y-%m-%d %H:%M") if job.start_time else "N/A"
        end = job.end_time.strftime("%Y-%m-%d %H:%M") if job.end_time else "N/A"
        table.add_row(str(i), job.id, job.dataflow_name, job.command, job.status, start, end)

    return table


def create_progress_bar() -> Progress:
    """Create a progress bar for long-running operations."""
    return Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        TimeRemainingColumn(),
        console=console,
    )


async def run_with_progress(
    coro,
    description: str,
    progress_callback: Callable | None = None,
) -> Any:
    """Run an async coroutine with a progress bar."""
    with create_progress_bar() as progress:
        task = progress.add_task(description, total=None)

        async def update_progress(progress_data):
            if hasattr(progress_data, 'current_chunk') and hasattr(progress_data, 'total_chunks'):
                progress.update(task, completed=progress_data.current_chunk, total=progress_data.total_chunks)
            elif hasattr(progress_data, 'current_part') and hasattr(progress_data, 'total_parts'):
                progress.update(task, completed=progress_data.current_part, total=progress_data.total_parts)

        if progress_callback:
            # Wrap the callback to update progress bar
            original_callback = progress_callback

            async def wrapped_callback(data):
                await original_callback(data)
                await update_progress(data)

            return await coro(progress_callback=wrapped_callback)
        else:
            return await coro()


def prompt_select(prompt_text: str, choices: list[str], default: str | None = None) -> str:
    """Prompt user to select from a list of choices."""
    console.print(f"\n[bold]{prompt_text}[/bold]")
    for i, choice in enumerate(choices, 1):
        console.print(f"  [cyan]{i}[/cyan]. {choice}")

    while True:
        try:
            selection = Prompt.ask(
                "Enter your choice",
                default=str(default) if default else None,
            )
            idx = int(selection) - 1
            if 0 <= idx < len(choices):
                return choices[idx]
            print_error("Invalid selection. Please try again.")
        except ValueError:
            print_error("Please enter a number.")


def prompt_confirm(prompt_text: str, default: bool = False) -> bool:
    """Prompt user for yes/no confirmation."""
    return Confirm.ask(prompt_text, default=default)


def prompt_text(prompt_text: str, default: str | None = None) -> str:
    """Prompt user for text input."""
    return Prompt.ask(prompt_text, default=default)


def prompt_password(prompt_text: str) -> str:
    """Prompt user for password input (hidden)."""
    return Prompt.ask(prompt_text, password=True)


def print_dataset_details(dataset: Dataset) -> None:
    """Print detailed dataset information."""
    table = Table(show_header=False, box=None)
    table.add_column("Field", style="cyan")
    table.add_column("Value", style="white")

    table.add_row("ID", dataset.id)
    table.add_row("Name", dataset.name)
    table.add_row("Label", dataset.label)
    table.add_row("Description", dataset.description or "N/A")
    table.add_row("Status", dataset.status)
    table.add_row("Type", dataset.type)
    table.add_row("Row Count", str(dataset.row_count) if dataset.row_count else "N/A")
    table.add_row("Created", dataset.created_date.strftime("%Y-%m-%d %H:%M"))
    table.add_row("Last Modified", dataset.last_modified_date.strftime("%Y-%m-%d %H:%M"))

    console.print(Panel(table, title=f"Dataset: {dataset.label}", border_style="green"))


def print_dashboard_details(dashboard: Dashboard) -> None:
    """Print detailed dashboard information."""
    table = Table(show_header=False, box=None)
    table.add_column("Field", style="cyan")
    table.add_column("Value", style="white")

    table.add_row("ID", dashboard.id)
    table.add_row("Name", dashboard.name)
    table.add_row("Label", dashboard.label)
    table.add_row("Description", dashboard.description or "N/A")
    table.add_row("Folder", dashboard.folder_name or "N/A")
    table.add_row("Created", dashboard.created_date.strftime("%Y-%m-%d %H:%M"))
    table.add_row("Last Modified", dashboard.last_modified_date.strftime("%Y-%m-%d %H:%M"))

    console.print(Panel(table, title=f"Dashboard: {dashboard.label}", border_style="green"))


def print_dataflow_details(dataflow: Dataflow) -> None:
    """Print detailed dataflow information."""
    table = Table(show_header=False, box=None)
    table.add_column("Field", style="cyan")
    table.add_column("Value", style="white")

    table.add_row("ID", dataflow.id)
    table.add_row("Name", dataflow.name)
    table.add_row("Label", dataflow.label)
    table.add_row("Description", dataflow.description or "N/A")
    table.add_row("Status", dataflow.status)
    table.add_row("Created", dataflow.created_date.strftime("%Y-%m-%d %H:%M"))
    table.add_row("Last Modified", dataflow.last_modified_date.strftime("%Y-%m-%d %H:%M"))

    console.print(Panel(table, title=f"Dataflow: {dataflow.label}", border_style="green"))


def print_extraction_progress(progress: ExtractionProgress) -> None:
    """Print extraction progress."""
    pct = (progress.processed_rows / progress.total_rows * 100) if progress.total_rows > 0 else 0
    console.print(
        f"[cyan]Chunk {progress.current_chunk}/{progress.total_chunks}[/cyan] | "
        f"[green]{progress.processed_rows:,}/{progress.total_rows:,} rows[/green] "
        f"([yellow]{pct:.1f}%[/yellow])"
    )


def print_upload_progress(progress: UploadProgress) -> None:
    """Print upload progress."""
    pct = (progress.uploaded_rows / progress.total_rows * 100) if progress.total_rows > 0 else 0
    console.print(
        f"[cyan]Part {progress.current_part}/{progress.total_parts}[/cyan] | "
        f"[green]{progress.uploaded_rows:,}/{progress.total_rows:,} rows[/green] "
        f"([yellow]{pct:.1f}%[/yellow])"
    )
