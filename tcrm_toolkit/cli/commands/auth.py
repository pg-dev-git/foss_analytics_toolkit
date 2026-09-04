"""Auth CLI commands."""

import asyncio
import typer

from tcrm_toolkit.cli.ui import (
    console,
    print_header,
    print_success,
    print_error,
    print_info,
    print_warning,
    prompt_confirm,
    prompt_text,
    prompt_password,
)
from tcrm_toolkit.core import get_settings
from tcrm_toolkit.core.config import Settings
from tcrm_toolkit.core.crypto import CryptoManager, create_crypto_manager
from tcrm_toolkit.core.services.auth_service import AuthService
from tcrm_toolkit.core.auth import SFCLIAuthService, SFCLIAuthError
from tcrm_toolkit.core.models import (
    ConnectedAppConfig,
    DeviceFlowConfig,
    WebOAuthConfig,
)

app = typer.Typer(name="auth", help="Authentication commands")


def _get_auth_service() -> AuthService:
    """Get configured auth service (OAuth-based)."""
    settings = get_settings()
    crypto = CryptoManager(settings.encryption_key)
    return AuthService(settings, crypto)


def _get_sf_cli_auth_service() -> SFCLIAuthService:
    """Get configured SF CLI auth service."""
    settings = get_settings()
    crypto = create_crypto_manager()
    return SFCLIAuthService(settings, crypto)


@app.command("login")
def login(
    method: str = typer.Option(
        "sfcli",
        "--method",
        "-m",
        help="Authentication method: sfcli, device, web, jwt",
    ),
    alias: str = typer.Option("default", "--alias", "-a", help="Org alias for SF CLI"),
    instance_url: str | None = typer.Option(None, "--instance-url", "-r", help="Custom instance URL"),
    username: str | None = typer.Option(None, "--username", "-u", help="Username for JWT flow"),
) -> None:
    """Authenticate with Salesforce."""
    asyncio.run(_login_async(method, alias, instance_url, username))


async def _login_async(
    method: str,
    alias: str,
    instance_url: str | None,
    username: str | None,
) -> None:
    """Async login implementation."""
    settings = get_settings()

    try:
        if method == "sfcli":
            # SF CLI-based authentication (no Connected App needed)
            auth_service = _get_sf_cli_auth_service()

            if not auth_service.sf_cli.is_available():
                print_error("SF CLI not found. Install from https://developer.salesforce.com/tools/sfdxcli")
                raise typer.Exit(1)

            print_info(f"Starting SF CLI web login (alias: {alias})...")
            print_warning("This will open a browser window for authentication")

            token = await auth_service.login(
                alias=alias,
                instance_url=instance_url,
                timeout=300,
            )

            print_success(f"Authenticated successfully via SF CLI")
            print_info(f"Instance URL: {await auth_service.get_instance_url(alias)}")
            username = await auth_service.get_username(alias)
            if username:
                print_info(f"User: {username}")

        elif method == "jwt":
            if not settings.has_connected_app_credentials:
                print_error("Connected App credentials not configured in .env")
                print_info("Set SF_CONNECTED_APP_CLIENT_ID, SF_CONNECTED_APP_CLIENT_SECRET, SF_CONNECTED_APP_USERNAME")
                raise typer.Exit(1)

            if not username:
                username = prompt_text("Enter Salesforce username")

            auth_service = _get_auth_service()
            config = ConnectedAppConfig(
                client_id=settings.sf_connected_app_client_id,
                client_secret=settings.sf_connected_app_client_secret,
                username=username,
            )

            print_info("Authenticating with JWT Bearer flow...")
            token = await auth_service.jwt_bearer_login(config)
            auth_service.store_tokens(username or token.id, token)
            print_success(f"Authenticated successfully as {token.id}")
            print_info(f"Instance URL: {token.instance_url}")

        elif method == "web":
            if not settings.has_web_oauth_credentials:
                print_error("Web OAuth credentials not configured in .env")
                print_info("Set SF_WEB_OAUTH_CLIENT_ID, SF_WEB_OAUTH_CLIENT_SECRET")
                raise typer.Exit(1)

            auth_service = _get_auth_service()
            config = WebOAuthConfig(
                client_id=settings.sf_web_oauth_client_id,
                client_secret=settings.sf_web_oauth_client_secret,
                redirect_uri=settings.sf_web_oauth_redirect_uri,
            )

            print_info("Starting Web PKCE flow...")
            print_warning("This will open a browser window for authentication")
            token = await auth_service.run_web_pkce_flow(config)
            auth_service.store_tokens(username or token.id, token)
            print_success(f"Authenticated successfully as {token.id}")
            print_info(f"Instance URL: {token.instance_url}")

        elif method == "device":
            if not settings.has_device_flow_credentials:
                print_error("Device Flow credentials not configured in .env")
                print_info("Set SF_DEVICE_FLOW_CLIENT_ID")
                raise typer.Exit(1)

            auth_service = _get_auth_service()
            config = DeviceFlowConfig(
                client_id=settings.sf_device_flow_client_id,
            )

            print_info("Starting Device Authorization Flow...")
            token = await auth_service.run_device_flow(config)
            auth_service.store_tokens(username or token.id, token)
            print_success(f"Authenticated successfully as {token.id}")
            print_info(f"Instance URL: {token.instance_url}")

        else:
            print_error(f"Unknown method: {method}. Use: sfcli, device, web, jwt")
            raise typer.Exit(1)

    except SFCLIAuthError as e:
        print_error(f"SF CLI authentication failed: {e}")
        raise typer.Exit(1)
    except Exception as e:
        print_error(f"Authentication failed: {e}")
        raise typer.Exit(1)


@app.command("logout")
def logout(
    alias: str = typer.Argument("default", help="Org alias to logout"),
) -> None:
    """Remove stored authentication for an org alias."""
    asyncio.run(_logout_async(alias))


async def _logout_async(alias: str) -> None:
    """Async logout implementation."""
    auth_service = _get_sf_cli_auth_service()

    try:
        if await auth_service.logout(alias):
            print_success(f"Logged out alias '{alias}'")
        else:
            print_warning(f"No stored credentials found for alias '{alias}'")
    except Exception as e:
        print_error(f"Logout failed: {e}")
        raise typer.Exit(1)


@app.command("status")
def status(
    alias: str = typer.Option("default", "--alias", "-a", help="Org alias to check"),
) -> None:
    """Check authentication status."""
    asyncio.run(_status_async(alias))


async def _status_async(alias: str) -> None:
    """Async status implementation."""
    auth_service = _get_sf_cli_auth_service()

    try:
        status_info = await auth_service.status(alias)

        if status_info["authenticated"]:
            print_success(f"Authenticated: {status_info['alias']}")
            if status_info.get("username"):
                print_info(f"User: {status_info['username']}")
            print_info(f"Instance: {status_info['instance_url']}")
            if status_info["token_expired"]:
                print_warning("Token expired (will auto-refresh on next use)")
            else:
                print_info("Token valid")
            if status_info.get("expires_at"):
                print_info(f"Expires: {status_info['expires_at']}")
        else:
            print_warning(status_info["message"])

        if not status_info.get("sf_cli_available", True):
            print_warning("SF CLI not available. Install from https://developer.salesforce.com/tools/sfdxcli")

    except Exception as e:
        print_error(f"Status check failed: {e}")
        raise typer.Exit(1)


@app.command("list-orgs")
def list_orgs() -> None:
    """List all authorized orgs from SF CLI."""
    asyncio.run(_list_orgs_async())


async def _list_orgs_async() -> None:
    """Async list orgs implementation."""
    auth_service = _get_sf_cli_auth_service()

    try:
        orgs = await auth_service.list_orgs()

        if not orgs:
            print_info("No authorized orgs found")
            return

        print_header("Authorized Orgs")
        for org in orgs:
            alias = org.get("alias", "N/A")
            username = org.get("username", "N/A")
            instance = org.get("instanceUrl", "N/A")
            connected = "✓" if org.get("connectedStatus") == "Connected" else "✗"
            print_info(f"  {connected} {alias} ({username}) - {instance}")

    except Exception as e:
        print_error(f"Failed to list orgs: {e}")
        raise typer.Exit(1)