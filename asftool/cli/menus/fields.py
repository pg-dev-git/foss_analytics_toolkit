"""Field Impact Analysis interactive menu."""

import typer

from asftool.cli.menu import Menu, MenuItem
from asftool.cli.ui import print_info, prompt_text, prompt_confirm


async def analyze_full() -> None:
    """Prompt user for parameters and run full analysis."""
    from asftool.cli.commands.fields import analyze_field_async

    print_info("=== Field Impact Analysis ===")
    search_term = prompt_text("Field API name or label")
    if not search_term:
        print_info("Cancelled.")
        return

    from asftool.core.services import FieldImpactService
    from asftool.cli.session import Session

    session = Session()
    try:
        async with session.client_context() as client:
            service = FieldImpactService(client, session.settings)
            await analyze_field_async(
                search_term=search_term,
                mode="both",
                fmt="table",
            )
    except typer.Exit:
        pass
    finally:
        await session.close()


def field_operations(menu: Menu) -> None:
    """Wire up the field impact analysis submenu."""
    menu.add(MenuItem("1", "Analyze field impact (all assets)", handler=analyze_full))
    menu.add(MenuItem("2", "Quick scan (summary mode)", handler=analyze_full))
    menu.add(MenuItem("b", "Back", exit_after=True))
