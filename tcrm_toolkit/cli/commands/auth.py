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
from tcrm_toolkit.core.crypto import CryptoManager
from tcrm_toolkit.core.services.auth_service import AuthService
from tcrm_toolkit.core.models import (
    ConnectedAppConfig,
    DeviceFlowConfig,
    WebOAuthConfig,
)

app = typer.Typer(name="auth", help="Authentication commands")


def _get_auth_service() -> AuthService:
    """Get configured auth service."""
    settings = get_settings()
    crypto = CryptoManager(settings.encryption_key)
    return AuthService(settings, crypto)


@app.command("login")
def login(
    method: str = typer.Option(
        "device",
        "--method",
        "-m",
        help="Authentication method: device, web, jwt",
    ),
    username: str | None = typer.Option(None, "--username", "-u", help="Username for JWT flow"),
) -> None:
    """Authenticate with Salesforce."""
    asyncio.run(_login_async(method, username))


async def _login_async(method: str, username: str | None) -> None:
    """Async login implementation."""
    settings = get_settings()
    auth_service = _get_auth_service()

    try:
        if method == "jwt":
            if not settings.has_connected_app_credentials:
                print_error("Connected App credentials not configured in .env")
                print_info("Set SF_CONNECTED_APP_CLIENT_ID, SF_CONNECTED_APP_CLIENT_SECRET, SF_CONNECTED_APP_USERNAME")
                raise typer.Exit(1)

            if not username:
                username = prompt_text("Enter Salesforce username")

            config = ConnectedAppConfig(
                client_id=settings.sf_connected_app_client_id,
                client_secret=settings.sf_connected_app_client_secret,
                username=username,
            )

            print_info("Authenticating with JWT Bearer flow...")
            token = await auth_service.jwt_bearer_login(config)

        elif method == "web":
            if not settings.has_web_oauth_credentials:
                print_error("Web OAuth credentials not configured in .env")
                print_info("Set SF_WEB_OAUTH_CLIENT_ID, SF_WEB_OAUTH_CLIENT_SECRET")
                raise typer.Exit(1)

            config = WebOAuthConfig(
                client_id=settings.sf_web_oauth_client_id,
                client_secret=settings.sf_web_oauth_client_secret,
                redirect_uri=settings.sf_web_oauth_redirect_uri,
            )

            print_info("Starting Web PKCE flow...")
            print_warning("This will open a browser window for authentication")
            token = await auth_service.run_web_pkce_flow(config)

        elif method == "device":
            if not settings.has_device_flow_credentials:
                print_error("Device Flow credentials not configured in .env")
                print_info("Set SF_DEVICE_FLOW_CLIENT_ID")
                raise typer.Exit(1)

            config = DeviceFlowConfig(
                client_id=settings.sf_device_flow_client_id,
            )

            print_info("Starting Device Authorization Flow...")
            token = await auth_service.run_device_flow(config)

        else:
            print_error(f"Unknown method: {method}")
            raise typer.Exit(1)

        # Store tokens
        auth_service.store_tokens(username or token.id, token)
        print_success(f"Authenticated successfully as {token.id}")
        print_info(f"Instance URL: {token.instance_url}")

    except Exception as e:
        print_error(f"Authentication failed: {e}")
        raise typer.Exit(1)
    finally:
        await auth_service.close()


@app.command("logout")
def logout(
    username: str = typer.Argument(help="Username to logout"),
) -> None:
    """Remove stored credentials for a user."""
    auth_service = _get_auth_service()

    if auth_service.delete_tokens(username):
        print_success(f"Logged out {username}")
    else:
        print_warning(f"No stored credentials found for {username}")


@app.command("status")
def status(
    username: str | None = typer.Option(None, "--username", "-u", help="Username to check"),
) -> None:
    """Check authentication status."""
    auth_service = _get_auth_service()

    if username:
        token = auth_service.retrieve_tokens(username)
        if token:
            print_success(f"Authenticated as {username}")
            print_info(f"Instance: {token.instance_url}")
        else:
            print_warning(f"No stored credentials for {username}")
    else:
        print_info("Use --username to check specific user")


@app.command("refresh")
def refresh(
    username: str = typer.Argument(help="Username to refresh token for"),
) -> None:
    """Manually refresh access token."""
    asyncio.run(_refresh_async(username))


async def _refresh_async(username: str) -> None:
    """Async refresh implementation."""
    settings = get_settings()
    auth_service = _get_auth_service()

    try:
        # Determine config type from stored credentials
        # For simplicity, try Connected App first
        if settings.has_connected_app_credentials:
            config = ConnectedAppConfig(
                client_id=settings.sf_connected_app_client_id,
                client_secret=settings.sf_connected_app_client_secret,
                username=username,
            )
        elif settings.has_web_oauth_credentials:
            config = WebOAuthConfig(
                client_id=settings.sf_web_oauth_client_id,
                client_secret=settings.sf_web_oauth_client_secret,
                redirect_uri=settings.sf_web_oauth_redirect_uri,
            )
        elif settings.has_device_flow_credentials:
            config = DeviceFlowConfig(
                client_id=settings.sf_device_flow_client_id,
            )
        else:
            print_error("No OAuth credentials configured")
            raise typer.Exit(1)

        token = await auth_service.ensure_valid_token(username, config)
        print_success(f"Token refreshed for {username}")
        print_info(f"Instance: {token.instance_url}")

    except Exception as e:
        print_error(f"Token refresh failed: {e}")
        raise typer.Exit(1)
    finally:
        await auth_service.close()