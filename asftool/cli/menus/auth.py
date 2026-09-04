"""Authentication menu — wired to Phase 2 SF CLI auth commands."""

import typer

from asftool.cli.menu import Menu, MenuItem
from asftool.cli.ui import prompt_text


async def login() -> None:
    """Login via SF CLI web (default) flow."""
    from asftool.cli.commands.auth import login as cmd_login

    alias = prompt_text("Org alias", default="default")
    if not alias:
        return
    try:
        cmd_login(alias=alias)
    except typer.Exit:
        pass


async def login_device() -> None:
    """Login via SF CLI device (headless) flow."""
    from asftool.cli.commands.auth import login as cmd_login

    alias = prompt_text("Org alias", default="default")
    if not alias:
        return
    try:
        cmd_login(alias=alias, device=True)
    except typer.Exit:
        pass


async def logout() -> None:
    """Logout of an org alias."""
    from asftool.cli.commands.auth import logout as cmd_logout

    alias = prompt_text("Org alias", default="default")
    if not alias:
        return
    try:
        cmd_logout(alias=alias)
    except typer.Exit:
        pass


async def status() -> None:
    """Check auth status."""
    from asftool.cli.commands.auth import status as cmd_status

    alias = prompt_text("Org alias", default="default")
    if not alias:
        return
    try:
        cmd_status(alias=alias)
    except typer.Exit:
        pass


async def list_orgs() -> None:
    """List all authorized orgs."""
    from asftool.cli.commands.auth import list_orgs as cmd_list

    try:
        cmd_list()
    except typer.Exit:
        pass


def auth_operations(menu: Menu) -> None:
    """Wire up the authentication submenu."""
    menu.add(MenuItem("1", "Login (web — opens browser)", handler=login))
    menu.add(MenuItem("2", "Login (device — headless)", handler=login_device))
    menu.add(MenuItem("3", "Logout", handler=logout))
    menu.add(MenuItem("4", "Status", handler=status))
    menu.add(MenuItem("5", "List authorized orgs", handler=list_orgs))
    menu.add(MenuItem("b", "Back", exit_after=True))
