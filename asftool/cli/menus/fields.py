"""Field Impact Analysis interactive menu."""

import typer

from asftool.cli.menu import Menu, MenuItem
from asftool.cli.ui import print_info, prompt_text


async def analyze_full() -> None:
    """Prompt user for parameters and run full analysis."""
    from asftool.cli.commands.fields import analyze_field_async

    print_info("=== Field Impact Analysis ===")
    search_term = prompt_text("Field API name or label")
    if not search_term:
        print_info("Cancelled.")
        return

    try:
        await analyze_field_async(search_term=search_term, mode="both", fmt="table")
    except typer.Exit:
        pass


def field_operations(menu: Menu) -> None:
    """Wire up the field impact analysis submenu."""
    menu.add(MenuItem("1", "Analyze field impact (all assets)", handler=analyze_full))
    menu.add(MenuItem("b", "Back", exit_after=True))
