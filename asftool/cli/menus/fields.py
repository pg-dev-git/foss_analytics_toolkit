"""Field Impact Analysis interactive menu.

Mirrors the extract_dataset menu flow: prompt for parameters, show the
default output path from StorageManager, let the user override, then
run the analysis.
"""

from pathlib import Path

import typer

from asftool.cli.menu import Menu, MenuItem
from asftool.cli.ui import print_info, prompt_confirm, prompt_text


async def analyze_full() -> None:
    """Prompt user for parameters and run full analysis."""
    from asftool.cli.commands.fields import analyze_field_async
    from asftool.cli.session import Session
    from asftool.cli.ui import print_error
    from asftool.core.config import get_settings
    from asftool.core.storage import get_storage_manager

    print_info("=== Field Impact Analysis ===")
    search_term = prompt_text("Field API name or label")
    if not search_term:
        print_info("Cancelled.")
        return

    mode = prompt_text("Match mode (exact/fuzzy/both)", default="both") or "both"
    threshold_str = prompt_text("Fuzzy threshold (0-100)", default="85") or "85"
    try:
        threshold = int(threshold_str)
    except ValueError:
        threshold = 85

    # Generate the default output path via StorageManager. The alias
    # comes from Session (default "default") without opening an API client.
    alias = Session().alias
    settings = get_settings()
    storage = get_storage_manager(settings)
    default_path = storage.field_impact_path(alias=alias, search_term=search_term)
    print_info(f"Default output path: {default_path}")
    output_str = prompt_text(
        "Output JSON path (press Enter to use default)",
        default=str(default_path),
    )
    if not prompt_confirm(f"Save report to {output_str}?"):
        print_info("Cancelled.")
        return

    try:
        await analyze_field_async(
            search_term=search_term,
            mode=mode,
            threshold=threshold,
            output=Path(output_str),
            fmt="table",
        )
    except typer.Exit:
        pass
    except Exception as e:
        print_error(f"Analysis failed: {e}")


def field_operations(menu: Menu) -> None:
    """Wire up the field impact analysis submenu."""
    menu.add(MenuItem("1", "Analyze field impact (all assets)", handler=analyze_full))
    menu.add(MenuItem("b", "Back", exit_after=True))
