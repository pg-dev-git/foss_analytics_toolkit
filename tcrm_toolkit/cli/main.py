"""Main CLI entry point for TCRM Toolkit."""

import asyncio
import sys
from contextlib import asynccontextmanager

import typer
from rich.console import Console

from tcrm_toolkit.cli.ui import console, print_header, print_error, print_info
from tcrm_toolkit.cli.commands.auth import app as auth_app
from tcrm_toolkit.cli.commands.datasets import app as datasets_app
from tcrm_toolkit.cli.commands.dashboards import app as dashboards_app
from tcrm_toolkit.cli.commands.dataflows import app as dataflows_app
from tcrm_toolkit.cli.commands.jobs import app as jobs_app
from tcrm_toolkit.core import get_settings
from tcrm_toolkit.core.config import Settings
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
    auth_service = AuthService(settings, CryptoManager())

    # Try to get stored tokens - this is a placeholder
    # In real implementation, we'd list available users and let them choose
    print_info("Authentication required. Use 'tcrm auth login' first.")
    yield None
    await auth_service.close()


@app.callback()
def callback(
    ctx: typer.Context,
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable verbose output"),
    version: bool = typer.Option(False, "--version", help="Show version and exit"),
) -> None:
    """TCRM Toolkit - Salesforce Tableau CRM Analytics Toolkit."""
    if version:
        from tcrm_toolkit import __version__
        console.print(f"tcrm-toolkit version {__version__}")
        raise typer.Exit()

    # Store verbose flag in context for subcommands
    ctx.ensure_object(dict)
    ctx.obj["verbose"] = verbose


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
    print_header("System Diagnostics", "Checking TCRM Toolkit setup")

    settings = get_settings()
    checks = []

    # Check .env file
    from pathlib import Path
    env_file = Path(".env")
    checks.append((".env file exists", env_file.exists()))

    # Check encryption key
    checks.append(("ENCRYPTION_KEY set", bool(settings.encryption_key)))
    checks.append(("JWT_SECRET_KEY set", bool(settings.jwt_secret_key)))

    # Check OAuth credentials
    checks.append(("Connected App credentials", settings.has_connected_app_credentials))
    checks.append(("Web OAuth credentials", settings.has_web_oauth_credentials))
    checks.append(("Device Flow credentials", settings.has_device_flow_credentials))

    from tcrm_toolkit.cli.ui import Table
    table = Table(title="Configuration Checks", show_header=True)
    table.add_column("Check", style="cyan")
    table.add_column("Status", style="white")

    all_passed = True
    for check, passed in checks:
        status = "[green]✓ PASS[/green]" if passed else "[red]✗ FAIL[/red]"
        if not passed:
            all_passed = False
        table.add_row(check, status)

    console.print(table)

    if all_passed:
        print_success("All checks passed!")
    else:
        print_warning("Some checks failed. Run 'tcrm init' to generate missing keys.")
        print_info("Configure OAuth credentials in .env for authentication.")


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