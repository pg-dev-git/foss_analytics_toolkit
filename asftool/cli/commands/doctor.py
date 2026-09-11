"""Doctor command — comprehensive diagnostics."""

import asyncio
import platform
import shutil
import sys

import typer
from rich.table import Table

from asftool.cli.session import Session
from asftool.cli.ui import console, print_header, print_info, print_success, print_warning
from asftool.core.auth import SFCLIAuthError
from asftool.core.config import get_settings

app = typer.Typer(help="System diagnostics", invoke_without_command=True)


def _run(coro):
    """Run async coroutine in a fresh event loop."""
    return asyncio.run(coro)


async def _collect_checks(session: Session, verbose: bool) -> list[tuple[str, str, bool]]:
    """Gather diagnostic checks. Returns (name, value, passed) tuples."""
    checks: list[tuple[str, str, bool]] = []

    # Python version
    py_version = f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
    checks.append(
        (
            "Python",
            f"{py_version} ({platform.platform()})",
            sys.version_info >= (3, 11),
        )
    )

    # uv package manager
    uv_path = shutil.which("uv")
    checks.append(("UV", uv_path or "NOT FOUND", uv_path is not None))

    # SF CLI
    sf_cli_available = session.auth_service.sf_cli.is_available()
    sf_cli_path = (
        session.auth_service.sf_cli._cli_path or "NOT FOUND"
    )
    checks.append(("SF CLI", sf_cli_path, sf_cli_available))

    # Config directory + log file
    config_dir = get_settings().config_dir
    checks.append(("Config dir", str(config_dir), config_dir.exists()))
    log_file = get_settings().log_file
    checks.append(("Log file", str(log_file), log_file.exists()))

    # Secrets present
    settings = get_settings()
    has_encryption = bool(settings.encryption_key)
    checks.append(
        ("Encryption key", "SET" if has_encryption else "MISSING", has_encryption)
    )
    has_jwt = bool(settings.jwt_secret_key)
    checks.append(
        ("JWT secret", "SET" if has_jwt else "MISSING", has_jwt)
    )

    # Keyring backend
    try:
        import keyring

        backend = keyring.get_keyring().__class__.__name__
        checks.append(("Keyring backend", backend, True))
    except Exception as e:
        checks.append(("Keyring backend", f"ERROR: {e}", False))

    # Salesforce connectivity
    try:
        import httpx

        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.get("https://login.salesforce.com")
        net_ok = resp.status_code == 200
        checks.append(
            ("Salesforce connectivity", "OK" if net_ok else "FAILED", net_ok)
        )
    except Exception as e:
        checks.append(("Salesforce connectivity", f"ERROR: {e}", False))


    # Agent readiness diagnostics
    try:
        import keyring  # already imported above, but check for accessibility
        kr = keyring.get_keyring()
        checks.append(("Agent keyring access", kr.__class__.__name__, True))
    except Exception as e:
        checks.append(("Agent keyring access", f"FAIL: {e}", False))

    # MCP server dependency check
    try:
        import mcp  # optional dependency
        checks.append(("MCP server module", "AVAILABLE", True))
    except ImportError:
        checks.append(("MCP server module", "NOT INSTALLED (optional)", True))

    # SDK import check
    try:
        from asftool.sdk.engine import ASFToolSDK
        checks.append(("SDK import", "OK", True))
    except Exception as e:
        checks.append(("SDK import", f"FAIL: {e}", False))

    # UTF-8 encoding audit
    try:
        import locale
        enc = locale.getpreferredencoding()
        utf_ok = enc.lower() in ("utf-8", "utf8", "utf-8", "utf_8")
        checks.append(("UTF-8 encoding", enc, utf_ok))
    except Exception as e:
        checks.append(("UTF-8 encoding", f"ERROR: {e}", False))

    # Auth status
    try:
        auth_status = await session.auth_service.status("default")
        if auth_status["authenticated"]:
            value = auth_status.get("username") or "Authenticated"
            auth_ok = not auth_status["token_expired"]
        else:
            value = "Not authenticated"
            auth_ok = True  # not a failure — informational
        checks.append(("Authentication", value, auth_ok))
    except SFCLIAuthError as e:
        checks.append(("Authentication", f"ERROR: {e}", False))
    except Exception as e:
        checks.append(("Authentication", f"ERROR: {e}", False))

    return checks


@app.callback()
def _doctor_default(
    ctx: typer.Context,
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose output"),
) -> None:
    """Default: run comprehensive system diagnostics."""
    if ctx.invoked_subcommand is not None:
        return  # delegate to 'config'

    if not run_diagnostics(verbose=verbose):
        raise typer.Exit(1)


def run_diagnostics(verbose: bool = False) -> bool:
    """Run the diagnostic table. Returns True if all checks pass.

    Reusable from the menu loop (asftool/cli/menu.py) and the CLI callback.
    Sync wrapper: only for direct CLI invocation (not from a running event loop).
    """
    result: bool = _run(run_diagnostics_async(verbose=verbose))  # type: ignore[no-any-return]
    return result


async def run_diagnostics_async(verbose: bool = False) -> bool:
    """Async path: safe to call from a running event loop (menu handler)."""
    session = Session()

    async def _doctor() -> bool:
        print_header("ASFTool Doctor Diagnostics")
        console.print()

        checks = await _collect_checks(session, verbose)

        table = Table(title="System Checks", show_header=True)
        table.add_column("Check", style="cyan")
        table.add_column("Value", style="white")
        table.add_column("Status", style="bold", width=10)

        all_pass = True
        for name, value, passed in checks:
            status = "[green]✓ PASS[/green]" if passed else "[red]✗ FAIL[/red]"
            if not passed:
                all_pass = False
            table.add_row(name, value, status)

        console.print(table)
        console.print()

        if all_pass:
            print_success("All checks passed!")
        else:
            print_warning("Some checks failed — see above")
            if not verbose:
                print_info("Run with --verbose for more details")
        return all_pass

    try:
        return await _doctor()
    finally:
        await session.close()


@app.command("config")
def show_config():
    """Show current configuration."""
    settings = get_settings()
    print_header("Configuration")
    print_info(f"Config dir: {settings.config_dir}")
    print_info(f"Log file: {settings.log_file}")
    print_info(f"API version: {settings.sf_api_version}")
    print_info(f"Default domain: {settings.sf_default_domain}")
    print_info(f"Encryption key: {'SET' if settings.encryption_key else 'MISSING'}")
    print_info(f"JWT secret: {'SET' if settings.jwt_secret_key else 'MISSING'}")
