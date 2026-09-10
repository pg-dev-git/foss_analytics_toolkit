# Phase 2: Auth CLI Commands (SF CLI Only)

**Goal:** Clean, production-grade CLI commands for SF CLI authentication. No more dual auth systems.

---

## Prerequisites

- Phase 0 complete (old auth_service.py deleted)
- Phase 1 complete (package renamed)

---

## Files to Create

```
asftool/cli/commands/auth.py          # Typer auth commands
asftool/cli/session.py                 # Session helper (wraps auth + services)
```

---

## Step 2.1: Create `asftool/cli/session.py`

This is the thin bridge between CLI and core services. Handles session lifecycle.

```python
"""Session helper for CLI commands."""

from contextlib import asynccontextmanager
from pathlib import Path

import structlog

from asftool.core.auth import SFCLIAuthError, SFCLIAuthService
from asftool.core.client import SalesforceClient
from asftool.core.config import Settings, get_settings
from asftool.core.crypto import CryptoManager, create_crypto_manager

logger = structlog.get_logger(__name__)


class Session:
    """Manages authenticated session for a single CLI command."""

    def __init__(
        self,
        alias: str = "default",
        settings: Settings | None = None,
        crypto: CryptoManager | None = None,
    ):
        self.alias = alias
        self.settings = settings or get_settings()
        self.crypto = crypto or create_crypto_manager()
        self._auth_service: SFCLIAuthService | None = None
        self._client: SalesforceClient | None = None

    @property
    def auth_service(self) -> SFCLIAuthService:
        """Lazy-init auth service."""
        if self._auth_service is None:
            self._auth_service = SFCLIAuthService(
                settings=self.settings,
                crypto_manager=self.crypto,
            )
        return self._auth_service

    async def get_client(self) -> SalesforceClient:
        """Get authenticated SalesforceClient."""
        if self._client is not None:
            return self._client

        # Get valid token (auto-refresh if expired)
        token = await self.auth_service.get_access_token(
            alias=self.alias,
            auto_refresh=True,
        )
        instance_url = await self.auth_service.get_instance_url(alias=self.alias)

        self._client = SalesforceClient(
            access_token=token,
            instance_url=instance_url,
            settings=self.settings,
        )
        return self._client

    async def close(self) -> None:
        """Cleanup."""
        if self._client:
            await self._client.close()
            self._client = None
        if self._auth_service:
            await self._auth_service.close()
            self._auth_service = None

    @asynccontextmanager
    async def client_context(self):
        """Async context manager for client."""
        client = await self.get_client()
        try:
            yield client
        finally:
            await self.close()
```

---

## Step 2.2: Create `asftool/cli/commands/auth.py`

Clean Typer commands using ONLY `SFCLIAuthService`.

```python
"""Authentication commands — SF CLI only."""

import asyncio

import typer
from rich.console import Console
from rich.table import Table

from asftool.cli.session import Session
from asftool.cli.ui import print_error, print_header, print_info, print_success, print_warning
from asftool.core.auth import SFCLIAuthError

app = typer.Typer(help="Authentication commands (SF CLI)")
console = Console()


def _run(coro):
    """Run async coroutine."""
    return asyncio.run(coro)


@app.command("login")
def login(
    alias: str = typer.Option("default", "--alias", "-a", help="Org alias"),
    instance_url: str | None = typer.Option(None, "--instance-url", "-r", help="Custom instance URL"),
    device: bool = typer.Option(False, "--device", "-d", help="Use device code flow (headless)"),
    timeout: int = typer.Option(300, "--timeout", "-t", help="Login timeout in seconds"),
):
    """Authenticate via SF CLI web/device login."""
    async def _login():
        session = Session(alias=alias)
        try:
            if not session.auth_service.sf_cli.is_available():
                print_error("SF CLI not found")
                print_info("Install from: https://developer.salesforce.com/tools/sfdxcli")
                raise typer.Exit(1)

            if device:
                print_info(f"Starting SF CLI device login (alias: {alias})...")
                token = await session.auth_service.login_device(
                    alias=alias, instance_url=instance_url, timeout=timeout,
                )
            else:
                print_info(f"Starting SF CLI web login (alias: {alias})...")
                print_warning("A browser window will open for authentication")
                token = await session.auth_service.login(
                    alias=alias, instance_url=instance_url, timeout=timeout,
                )

            print_success("Authenticated successfully")
            instance = await session.auth_service.get_instance_url(alias=alias)
            username = await session.auth_service.get_username(alias=alias)
            print_info(f"Instance: {instance}")
            if username:
                print_info(f"User: {username}")
        except SFCLIAuthError as e:
            print_error(f"Login failed: {e}")
            raise typer.Exit(1)
        finally:
            await session.close()

    _run(_login())


@app.command("logout")
def logout(
    alias: str = typer.Argument("default", help="Org alias to logout"),
):
    """Remove stored authentication for an org."""
    async def _logout():
        session = Session(alias=alias)
        try:
            if await session.auth_service.logout(alias=alias):
                print_success(f"Logged out alias '{alias}'")
            else:
                print_warning(f"No stored credentials for alias '{alias}'")
        except SFCLIAuthError as e:
            print_error(f"Logout failed: {e}")
            raise typer.Exit(1)
        finally:
            await session.close()

    _run(_logout())


@app.command("status")
def status(
    alias: str = typer.Option("default", "--alias", "-a", help="Org alias to check"),
):
    """Check authentication status."""
    async def _status():
        session = Session(alias=alias)
        try:
            status_info = await session.auth_service.status(alias=alias)
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
                print_warning("SF CLI not available")
        except Exception as e:
            print_error(f"Status check failed: {e}")
            raise typer.Exit(1)
        finally:
            await session.close()

    _run(_status())


@app.command("list-orgs")
def list_orgs():
    """List all authorized orgs."""
    async def _list():
        session = Session()
        try:
            orgs = await session.auth_service.list_orgs()
            if not orgs:
                print_info("No authorized orgs found")
                return

            table = Table(title="Authorized Orgs", show_header=True)
            table.add_column("✓", style="green", width=3)
            table.add_column("Alias", style="cyan")
            table.add_column("Username", style="white")
            table.add_column("Instance", style="blue")

            for org in orgs:
                connected = "✓" if org.get("connectedStatus") == "Connected" else "✗"
                table.add_row(
                    connected,
                    org.get("alias", "N/A"),
                    org.get("username", "N/A"),
                    org.get("instanceUrl", "N/A"),
                )
            console.print(table)
        except Exception as e:
            print_error(f"Failed to list orgs: {e}")
            raise typer.Exit(1)
        finally:
            await session.close()

    _run(_list())
```

---

## Step 2.3: Verify Auth Commands Work

```bash
# Test help
asftool auth --help
asftool auth login --help

# Test list-orgs (should work without auth)
asftool auth list-orgs

# Test status (will show "not authenticated" if no login yet)
asftool auth status
```

---

## Acceptance Criteria

- [ ] `asftool/cli/session.py` created with `Session` class
- [ ] `asftool/cli/commands/auth.py` has 4 commands: login, logout, status, list-orgs
- [ ] No imports of `auth_service` (pure Python OAuth)
- [ ] All commands use only `SFCLIAuthService`
- [ ] Login supports both web (default) and device (`--device`) flows
- [ ] `asftool auth list-orgs` works without authentication
- [ ] `asftool auth status` shows current session
- [ ] `uv run pytest tests/unit/test_auth_schemas.py -v` passes
- [ ] `uv run ruff check asftool/cli/commands/auth.py` passes

---

## Notes

- The `SFCLIAuthService` already does the heavy lifting (SF CLI subprocess calls, token storage)
- The CLI commands are thin wrappers — no business logic
- Device flow is for headless/SSH environments (no browser)
- Web flow is the default (opens browser via `sf org login web`)
- Both delegate to `sf org display --json` to get the actual token (legacy approach, proven)