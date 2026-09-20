"""``asftool config`` — view and edit persisted user configuration.

Stores small, user-tweakable settings at ``~/.asftool/config.json``.
Subcommands:
  - ``show``               — display all current settings
  - ``set-api-version``    — persist a preferred Salesforce API version
  - ``reset``              — delete the config file (back to defaults)
"""

import asyncio

import typer

from asftool.cli.ui import print_error, print_info, print_success
from asftool.core.config import get_settings
from asftool.core.config_store import (
    ConfigStore,
    UserConfig,
    get_user_config,
)

app = typer.Typer(help="View and edit persisted user configuration")


def _run(coro):
    return asyncio.run(coro)


@app.command("show")
def show() -> None:
    """Show all current settings (env var + persisted + default)."""
    settings = get_settings()
    user = get_user_config()
    print_info("=== Configuration ===")
    print_info(f"API version (effective):  {settings.sf_api_version}")
    print_info(f"API version (persisted):  {user.sf_api_version or '(none)'}")
    print_info(f"Default domain:           {settings.sf_default_domain}")
    print_info(f"Config dir:               {settings.config_dir}")
    print_info(f"Log file:                 {settings.log_file}")
    print_info(
        f"Persisted config file:    {ConfigStore.DEFAULT_PATH} "
        f"({'exists' if ConfigStore.DEFAULT_PATH.exists() else 'not present'})"
    )


@app.command("reset")
def reset() -> None:
    """Delete the persisted config file (revert to hardcoded defaults)."""
    store = ConfigStore()
    store.reset()
    print_success(f"Deleted {store.path} (if it existed). Settings revert to defaults.")
