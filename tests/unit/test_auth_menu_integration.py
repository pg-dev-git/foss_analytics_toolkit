"""Regression test for the asyncio.run() nesting bug in the auth menu.

Bug history: Phases 4-7 (datasets, dashboards, dataflows, jobs)
correctly routed their menu handlers through `*_async` wrappers. The
Phase 2 auth menu was older and called the sync Typer command
functions directly. Those Typer commands end with
``_run(_async_fn())`` = ``asyncio.run(_async_fn())``, which raises
``RuntimeError: asyncio.run() cannot be called from a running event
loop`` when invoked from the menu's own event loop.

This test statically enforces that all auth menu handlers call the
`*_async` wrappers (not the sync Typer commands), so a future refactor
can't reintroduce the bug silently.
"""

import inspect

from asftool.cli.menus import auth as auth_menu


def test_auth_menu_handlers_are_async() -> None:
    """Every menu handler must be async — the menu loop awaits them."""
    handlers = {
        "login": auth_menu.login,
        "login_device": auth_menu.login_device,
        "logout": auth_menu.logout,
        "status": auth_menu.status,
        "list_orgs": auth_menu.list_orgs,
    }
    for name, handler in handlers.items():
        assert inspect.iscoroutinefunction(handler), (
            f"Menu handler {name!r} is not async. The menu loop awaits "
            "each handler, so they must be coroutine functions."
        )


def test_auth_menu_handlers_call_async_wrappers() -> None:
    """Each handler must reference the *_async wrapper, not the Typer command.

    Catches the exact regression: a future refactor that wires a
    menu handler back to the sync ``cmd_status`` (or similar) would
    trigger the nested-asyncio.run crash at runtime. The structural
    check ensures the wrapper name appears in the handler source.
    """
    cases: list[tuple[str, object, str]] = [
        ("login", auth_menu.login, "login_async"),
        ("login_device", auth_menu.login_device, "login_async"),
        ("logout", auth_menu.logout, "logout_async"),
        ("status", auth_menu.status, "status_async"),
        ("list_orgs", auth_menu.list_orgs, "list_orgs_async"),
    ]
    for name, handler, expected_ref in cases:
        source = inspect.getsource(handler)
        assert expected_ref in source, (
            f"Menu handler {name!r} does not reference {expected_ref!r}. "
            "It must call the async wrapper from "
            "asftool.cli.commands.auth — calling the sync Typer command "
            "would crash the menu loop with 'asyncio.run() cannot be "
            f"called from a running event loop'. Source:\n{source}"
        )
