"""Regression test for the asyncio.run() nesting bug in all submenus.

Bug history: Phases 4-7 (datasets, dashboards, dataflows, jobs) added
``*_async`` wrapper functions next to each Typer command, but the
wrappers were stubs that delegated to the sync Typer commands:

  async def list_datasets_async() -> None:
      list_datasets()  # <-- calls sync Typer command

The sync Typer command ends with ``_run(_list())`` = ``asyncio.run(...)``,
which raises ``RuntimeError: asyncio.run() cannot be called from a
running event loop`` when invoked from the menu's own event loop.

Phase 2's auth menu had the same bug; a separate fix (commit 76acfc8)
uncovered it because I happened to extract the full async body when
restructuring that file. This test covers all 5 submenus so the bug
can't come back in any of them.
"""

import inspect

import asftool.cli.menus.auth as auth_menu
import asftool.cli.menus.dashboards as dashboards_menu
import asftool.cli.menus.dataflows as dataflows_menu
import asftool.cli.menus.datasets as datasets_menu
import asftool.cli.menus.fields as fields_menu
import asftool.cli.menus.jobs as jobs_menu

# (menu_module, (handler_name, expected_async_wrapper_name))
MENU_HANDLERS: list[tuple[object, tuple[str, str]]] = [
    (auth_menu, ("login", "login_async")),
    (auth_menu, ("login_device", "login_async")),
    (auth_menu, ("logout", "logout_async")),
    (auth_menu, ("status", "status_async")),
    (auth_menu, ("list_orgs", "list_orgs_async")),
    (datasets_menu, ("list_datasets", "list_datasets_async")),
    (datasets_menu, ("extract_dataset", "extract_dataset_async")),
    (datasets_menu, ("upload_dataset", "upload_dataset_async")),
    (datasets_menu, ("delete_dataset", "delete_dataset_async")),
    (datasets_menu, ("show_dataset", "show_dataset_async")),
    (dashboards_menu, ("list_dashboards", "list_dashboards_async")),
    (dashboards_menu, ("backup_dashboard", "backup_dashboard_async")),
    (dashboards_menu, ("show_dashboard", "show_dashboard_async")),
    (dataflows_menu, ("list_dataflows", "list_dataflows_async")),
    (dataflows_menu, ("backup_dataflow", "backup_dataflow_async")),
    (dataflows_menu, ("start_dataflow", "start_dataflow_async")),
    (dataflows_menu, ("stop_dataflow", "stop_dataflow_async")),
    (dataflows_menu, ("show_dataflow", "show_dataflow_async")),
    (jobs_menu, ("list_jobs", "list_jobs_async")),
    (jobs_menu, ("show_job", "show_job_async")),
    (fields_menu, ("analyze_full", "analyze_field_async")),
]


def test_all_menu_handlers_are_async() -> None:
    """Every menu handler must be async — the menu loop awaits them."""
    for module, (handler_name, _expected_ref) in MENU_HANDLERS:
        handler = getattr(module, handler_name)
        assert inspect.iscoroutinefunction(handler), (
            f"Menu handler {module.__name__}.{handler_name} is not async. "
            "The menu loop awaits each handler, so it must be a "
            "coroutine function."
        )


def test_all_menu_handlers_call_async_wrappers() -> None:
    """Each handler must reference the *_async wrapper, not the Typer command.

    Catches the exact regression: if a handler is wired to call the
    sync Typer command (e.g. ``cmd_status``) instead of the wrapper
    (``status_async``), the menu loop will crash at runtime with
    'asyncio.run() cannot be called from a running event loop'. The
    test fails with a clear error message pointing at the offending
    handler.
    """
    for module, (handler_name, expected_ref) in MENU_HANDLERS:
        handler = getattr(module, handler_name)
        source = inspect.getsource(handler)
        assert expected_ref in source, (
            f"Menu handler {module.__name__}.{handler_name} does not "
            f"reference {expected_ref!r}. It must call the async wrapper "
            "from asftool.cli.commands.<domain> — calling the sync Typer "
            "command would crash the menu loop with 'asyncio.run() cannot "
            f"be called from a running event loop'. Source:\n{source}"
        )


def test_doctor_menu_uses_async_path() -> None:
    """Doctor menu handler must NOT call sync run_diagnostics (which uses asyncio.run).

    Regression: bug surfaced in interactive mode on 2026-07-23 — the
    menu handler was calling the sync wrapper, which itself called
    asyncio.run() and crashed the running event loop with:
      RuntimeError: asyncio.run() cannot be called from a running event loop
    The fix exposes run_diagnostics_async() for menu use; the menu
    handler must call that, not run_diagnostics().
    """
    from asftool.cli.commands.doctor import (
        run_diagnostics,
        run_diagnostics_async,
    )

    # The async function must exist.
    assert inspect.iscoroutinefunction(run_diagnostics_async), (
        "run_diagnostics_async must be a coroutine function"
    )

    # The sync wrapper must use _run (which is asyncio.run).
    src = inspect.getsource(run_diagnostics)
    assert "_run(" in src, "run_diagnostics should wrap with _run (= asyncio.run)"

    # The menu handler source must reference run_diagnostics_async, not the sync wrapper.
    import asftool.cli.menu as menu_mod
    create_src = inspect.getsource(menu_mod.create_menus)
    assert "run_diagnostics_async" in create_src, (
        "create_menus must call run_diagnostics_async (not the sync wrapper)"
    )


def test_doctor_menu_does_not_crash_inside_running_event_loop():
    """The menu's doctor handler must run cleanly inside a live event loop.

    This is the user-visible bug: clicking Doctor in interactive mode
    crashed with 'asyncio.run() cannot be called from a running event loop'.
    """
    import asyncio
    from unittest.mock import MagicMock, patch

    from asftool.cli.session import Session

    # Build a fake Session whose auth_service.sf_cli.is_available returns True.
    fake_session = MagicMock()
    fake_session.auth_service.sf_cli.is_available.return_value = True
    fake_session.auth_service.sf_cli._cli_path = "/usr/bin/sf"
    # Make session.close() an awaitable no-op.
    async def _noop_close(self):
        pass
    fake_session.close = _noop_close

    # Mock httpx so the connectivity check doesn't try the real network.
    class _MockResp:
        status_code = 200

    async def _mock_get(self, *a, **kw):
        return _MockResp()

    async def run_inside_loop():
        with patch.object(Session, "__init__", lambda self: None), \
             patch.object(Session, "close", _noop_close), \
             patch("httpx.AsyncClient.get", _mock_get), \
             patch("asftool.cli.commands.doctor._collect_checks",
                   _async_noop_collect), \
             patch("rich.console.Console.print", lambda *a, **kw: None):
            # Now call the menu's _run_doctor via the create_menus wiring.
            import asftool.cli.menu as menu_mod
            _main, _all = menu_mod.create_menus()
            doctor_item = next(
                i for i in _main.items
                if i.label and "Doctor" in i.label
            )
            assert doctor_item.handler is not None
            await doctor_item.handler()

    asyncio.run(run_inside_loop())


async def _async_noop_collect(session, verbose):
    """Return a single passing check so the doctor prints and exits."""
    return [("Test", "OK", True)]
