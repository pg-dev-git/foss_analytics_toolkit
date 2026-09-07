"""Configuration interactive menu.

Mirrors the pattern used by other submenus: prompt for parameters,
call the async wrapper from ``cli/commands/config``.
"""

import re

from asftool.cli.menu import Menu, MenuItem
from asftool.cli.ui import (
    print_error,
    print_info,
    prompt_confirm,
    prompt_select,
    prompt_text,
)

# Last-resort fallback list when the Salesforce metadata endpoint is
# unreachable (offline, no auth, etc.). Salesforce typically supports
# ~6 versions at any time, so this stays useful even when it's a few
# releases behind.
_FALLBACK_VERSIONS = [
    "v60.0", "v61.0", "v62.0", "v63.0", "v64.0",
    "v65.0", "v66.0", "v67.0", "v68.0",
]


async def show_config() -> None:
    """Display current effective + persisted configuration."""
    from asftool.cli.commands.config import show as show_cmd
    show_cmd()


async def set_api_version_from_list() -> None:
    """Discover available API versions from the org and let the user pick one."""
    from asftool.cli.session import Session

    print_info("=== Set API Version ===")
    session = Session()
    versions: list[str] = []
    discovery_error: str | None = None
    try:
        async with session.client_context() as client:
            try:
                versions = await client.list_available_api_versions()
            except Exception as e:
                discovery_error = str(e)
                logger_msg = f"Version discovery failed: {e}"  # noqa: F841
    finally:
        await session.close()

    if not versions:
        if discovery_error:
            print_error(f"Could not reach Salesforce to list versions: {discovery_error}")
        print_info("Falling back to a hardcoded list of known stable versions.")
        versions = _FALLBACK_VERSIONS

    # Show the user a numbered list and let them pick.
    print_info("Available API versions:")
    # prompt_select takes a list of (label, value) pairs. We use the version
    # string for both to keep the menu compact.
    current = (await _get_current_api_version())
    print_info(f"Currently active: {current}")
    choice = prompt_select(
        prompt_text="Choose API version",
        choices=versions,
        default=current if current in versions else versions[-1],
    )
    if not choice:
        print_info("Cancelled.")
        return
    await _persist(choice)


async def set_api_version_manual() -> None:
    """Prompt for a version string and persist it (with format validation)."""
    print_info("=== Set API Version (manual) ===")
    print_info("Format: v<major>.<minor>, e.g. v68.0")
    raw = prompt_text("API version")
    if not raw:
        print_info("Cancelled.")
        return
    if not re.match(r"^v\d+\.\d+$", raw):
        print_error(
            f"Invalid format {raw!r}: must match v<major>.<minor> (e.g. 'v68.0')"
        )
        return
    await _persist(raw)


async def reset_config() -> None:
    """Delete the persisted config file (back to defaults)."""
    from asftool.cli.commands.config import reset as reset_cmd
    if prompt_confirm("Delete persisted config and revert to defaults?"):
        reset_cmd()


# ----------------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------------


async def _get_current_api_version() -> str:
    """Return the currently effective API version."""
    from asftool.core.config import get_settings
    return get_settings().sf_api_version


async def _persist(version: str) -> None:
    """Persist the chosen version via the existing CLI command."""
    from asftool.cli.commands.config import set_api_version
    set_api_version(version=version)


def config_operations(menu: Menu) -> None:
    """Wire up the configuration submenu."""
    menu.add(MenuItem("1", "Show current configuration", handler=show_config))
    menu.add(MenuItem("2", "Set API version (from org list)", handler=set_api_version_from_list))
    menu.add(MenuItem("3", "Set API version (manual entry)", handler=set_api_version_manual))
    menu.add(MenuItem("4", "Reset to defaults", handler=reset_config))
    menu.add(MenuItem("b", "Back", exit_after=True))
