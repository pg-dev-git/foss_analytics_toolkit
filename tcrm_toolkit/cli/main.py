"""Main CLI entry point for TCRM Toolkit."""

import sys
from contextlib import asynccontextmanager

import typer

from tcrm_toolkit.cli.commands.auth import app as auth_app
from tcrm_toolkit.cli.commands.dashboards import app as dashboards_app
from tcrm_toolkit.cli.commands.dataflows import app as dataflows_app
from tcrm_toolkit.cli.commands.datasets import app as datasets_app
from tcrm_toolkit.cli.commands.jobs import app as jobs_app
from tcrm_toolkit.cli.ui import (
    console,
    print_error,
    print_header,
    print_info,
    print_success,
    print_warning,
)
from tcrm_toolkit.core import get_settings
from tcrm_toolkit.core.crypto import CryptoManager
from tcrm_toolkit.core.services.auth_service import AuthService

app = typer.Typer(
    name="tcrm",
    help="Salesforce TCRM Analytics Toolkit",
    add_completion=False,
    no_args_is_help=True,
)

# Add subcommands
app.add_typer(auth_app, name="auth")
app.add_typer(datasets_app, name="datasets")
app.add_typer(dashboards_app, name="dashboards")
app.add_typer(dataflows_app, name="dataflows")
app.add_typer(jobs_app, name="jobs")


@asynccontextmanager
async def _get_authenticated_client():
    """Get an authenticated Salesforce client."""
    settings = get_settings()
    auth_service = AuthService(settings, CryptoManager(settings.encryption_key))

    # Try to get stored tokens - this is a placeholder
    # In real implementation, we'd list available users and let them choose
    print_info("Authentication required. Use 'tcrm auth login' first.")
    yield None
    await auth_service.close()


@app.command()
def interactive() -> None:
    """Launch interactive TUI mode."""
    from tcrm_toolkit.interactive import TCRMApp
    TCRMApp().run()


@app.callback(invoke_without_command=True)
def callback(
    ctx: typer.Context,
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable verbose output"),
    version: bool = typer.Option(False, "--version", help="Show version and exit"),
    interactive_flag: bool = typer.Option(False, "--interactive", "-i", help="Launch interactive TUI"),
) -> None:
    """TCRM Toolkit - Salesforce Tableau CRM Analytics Toolkit."""
    if version:
        from tcrm_toolkit import __version__
        console.print(f"tcrm-toolkit version {__version__}")
        raise typer.Exit()

    ctx.ensure_object(dict)
    ctx.obj["verbose"] = verbose

    if ctx.invoked_subcommand is None and not interactive_flag:
        if sys.stdin.isatty() and sys.stdout.isatty():
            from tcrm_toolkit.interactive import TCRMApp
            TCRMApp().run()
        else:
            ctx.invoke(app, ["--help"])


@app.command()
def init() -> None:
    """Initialize configuration file."""
    from tcrm_toolkit.core.config import generate_encryption_key, generate_jwt_secret

    print_header("TCRM Toolkit Initialization", "Generate configuration keys")

    encryption_key = generate_encryption_key()
    jwt_secret = generate_jwt_secret()

    console.print("\n[bold]Add these to your .env file:[/bold]\n")
    console.print(f"ENCRYPTION_KEY={encryption_key}")
    console.print(f"JWT_SECRET_KEY={jwt_secret}")
    console.print("\n[dim]Keep these secure! They are used to encrypt your credentials.[/dim]")


@app.command()
def config() -> None:
    """Show current configuration (without secrets)."""
    settings = get_settings()

    print_header("Configuration", "Current TCRM Toolkit settings")

    from tcrm_toolkit.cli.ui import Table
    table = Table(show_header=False, box=None)
    table.add_column("Setting", style="cyan")
    table.add_column("Value", style="white")

    table.add_row("App Name", settings.app_name)
    table.add_row("App Version", settings.app_version)
    table.add_row("Debug Mode", str(settings.debug))
    table.add_row("Log Level", settings.log_level)
    table.add_row("SF API Version", settings.sf_api_version)
    table.add_row("SF Default Domain", settings.sf_default_domain)
    table.add_row("Connected App Configured", "Yes" if settings.has_connected_app_credentials else "No")
    table.add_row("Web OAuth Configured", "Yes" if settings.has_web_oauth_credentials else "No")
    table.add_row("Device Flow Configured", "Yes" if settings.has_device_flow_credentials else "No")

    console.print(table)


@app.command()
def doctor() -> None:
    """Run diagnostics to check setup."""
    import asyncio
    import shutil
    import subprocess
    import keyring
    from pathlib import Path
    from tcrm_toolkit.interactive.safety import SafetyMonitor

    print_header("System Diagnostics", "Checking TCRM Toolkit setup and environment")

    settings = get_settings()
    checks = []

    # 1. Python version & dependencies
    import sys
    py_ver = f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
    checks.append(("Python version >= 3.11", sys.version_info >= (3, 11), f"Python {py_ver}"))

    # 2. SF CLI installation
    sf_path = shutil.which("sf") or shutil.which("sfdx")
    sf_installed = sf_path is not None
    sf_version = "Not found"
    if sf_installed:
        try:
            res = subprocess.run([sf_path, "--version"], capture_output=True, text=True, timeout=3)
            sf_version = res.stdout.strip()
        except Exception:
            sf_version = "Installed (version check failed)"
    checks.append(("Salesforce CLI (sf/sfdx)", sf_installed, sf_version))

    # 3. Keyring access
    keyring_accessible = True
    keyring_detail = "Working"
    try:
        keyring.set_password("tcrm_test", "user", "test")
        keyring.get_password("tcrm_test", "user")
        keyring.delete_password("tcrm_test", "user")
    except Exception as e:
        keyring_accessible = False
        keyring_detail = f"No backend/Headless ({e})"
    checks.append(("Keyring access", True, keyring_accessible and "Working" or f"Warning: {keyring_detail}"))

    # 4. Config & data directory permissions
    config_dir = Path.home() / ".tcrm"
    dir_writable = True
    try:
        config_dir.mkdir(parents=True, exist_ok=True)
        test_file = config_dir / ".test_write"
        test_file.write_text("test")
        test_file.unlink()
    except Exception:
        dir_writable = False
    checks.append(("~/.tcrm directory writable", dir_writable, str(config_dir)))

    # 5. Connection Safety (VPN/Proxy check)
    async def check_safety():
        monitor = SafetyMonitor(settings)
        try:
            res = await monitor.check_connection_safety(force=True)
            return res.is_safe, f"Risk: {res.risk_level.value}"
        except Exception as e:
            return False, str(e)
        finally:
            await monitor.close()

    is_safe, safety_details = asyncio.run(check_safety())
    checks.append(("Connection Safety (VPN/Proxy)", is_safe, safety_details))

    from tcrm_toolkit.cli.ui import Table
    table = Table(title="Diagnostics Summary", show_header=True)
    table.add_column("Check", style="cyan")
    table.add_column("Status", style="white")
    table.add_column("Details", style="dim")

    all_passed = True
    for check_name, passed, details in checks:
        status = "[green]✓ PASS[/green]" if passed else "[red]✗ FAIL[/red]"
        if not passed:
            all_passed = False
        table.add_row(check_name, status, details)

    console.print(table)

    if all_passed:
        print_success("All diagnostic checks passed!")
    else:
        print_warning("Some diagnostic checks failed. Review issues above.")


def main() -> None:
    """Main entry point."""
    try:
        app()
    except KeyboardInterrupt:
        console.print("\n[yellow]Interrupted by user[/yellow]")
        sys.exit(130)
    except Exception as e:
        print_error(f"Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
