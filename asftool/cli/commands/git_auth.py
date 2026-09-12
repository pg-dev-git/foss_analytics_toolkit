"""Git authentication CLI commands.

Commands for managing multi-provider Git credentials:
- asftool git-auth login
- asftool git-auth list
- asftool git-auth remove
- asftool git-auth validate
"""

import asyncio
import getpass
import json
import os
from typing import Optional

import typer
from rich.console import Console
from rich.panel import Panel
from rich.prompt import Confirm, Prompt
from rich.table import Table

from asftool.cli.session import Session
from asftool.cli.ui import print_error, print_info, print_success, print_warning
from asftool.core.auth.git_auth import (
    GitAuthService,
    GitAuthError,
    GitCredentialsNotFound,
    get_git_auth_service,
)
from asftool.core.models.git_auth import (
    GitAuthType,
    GitProvider,
    GitCredentials,
    SSHKeyFormat,
)

app = typer.Typer(help="Git authentication management (multi-provider)")
console = Console()


def _run(coro):
    """Run async coroutine in a fresh event loop (used only by Typer commands)."""
    return asyncio.run(coro)


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------


def _prompt_provider() -> GitProvider:
    """Prompt user to select Git provider."""
    providers = [
        ("1", GitProvider.GITHUB, "GitHub (github.com)"),
        ("2", GitProvider.GITLAB, "GitLab (gitlab.com or self-hosted)"),
        ("3", GitProvider.BITBUCKET, "BitBucket (bitbucket.org)"),
        ("4", GitProvider.AZURE_DEVOPS, "Azure DevOps (dev.azure.com)"),
    ]

    console.print("\n[bold]Select Git Provider:[/bold]")
    for key, provider, desc in providers:
        console.print(f"  {key}. {provider.value.title()} - {desc}")

    choice = Prompt.ask("Choice", choices=[p[0] for p in providers], default="1")
    return next(p[1] for p in providers if p[0] == choice)


def _prompt_auth_type(provider: GitProvider) -> GitAuthType:
    """Prompt user to select auth type based on provider."""
    auth_types = [
        ("1", GitAuthType.PAT, "Personal Access Token (fine-grained recommended)"),
        ("2", GitAuthType.SSH_KEY, "SSH Private Key"),
    ]

    # Provider-specific options
    if provider == GitProvider.GITLAB:
        auth_types.append(
            ("3", GitAuthType.PROJECT_TOKEN, "GitLab Project Access Token")
        )
    elif provider == GitProvider.AZURE_DEVOPS:
        auth_types.append(
            ("3", GitAuthType.PROJECT_TOKEN, "Azure DevOps Project PAT")
        )

    console.print("\n[bold]Select Authentication Method:[/bold]")
    for key, auth_type, desc in auth_types:
        console.print(f"  {key}. {auth_type.value.replace('_', ' ').title()} - {desc}")

    choice = Prompt.ask("Choice", choices=[a[0] for a in auth_types], default="1")
    return next(a[1] for a in auth_types if a[0] == choice)


def _prompt_host(provider: GitProvider) -> str:
    """Prompt for host URL with provider defaults."""
    defaults = {
        GitProvider.GITHUB: "github.com",
        GitProvider.GITLAB: "gitlab.com",
        GitProvider.BITBUCKET: "bitbucket.org",
        GitProvider.AZURE_DEVOPS: "dev.azure.com",
    }
    default = defaults.get(provider, "")
    return Prompt.ask("Git host URL", default=default)


def _read_token(prompt: str) -> str:
    """Read token securely (hidden input)."""
    return getpass.getpass(prompt)


def _read_multiline(prompt: str) -> str:
    """Read multi-line input (for SSH keys)."""
    console.print(f"[dim]{prompt}[/dim]")
    console.print("[dim]Enter content, end with Ctrl+D (Unix) or Ctrl+Z (Windows):[/dim]")
    lines = []
    try:
        while True:
            line = input()
            lines.append(line)
    except EOFError:
        pass
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Async wrappers (used by interactive menu and Typer commands)
# ---------------------------------------------------------------------------


async def login_async(
    alias: str,
    provider: Optional[GitProvider] = None,
    host: Optional[str] = None,
    auth_type: Optional[GitAuthType] = None,
    organization: Optional[str] = None,
    project: Optional[str] = None,
    username: Optional[str] = None,
    token: Optional[str] = None,
    ssh_key: Optional[str] = None,
    ssh_key_format: SSHKeyFormat = SSHKeyFormat.OPENSSH,
    ssh_passphrase: Optional[str] = None,
    description: Optional[str] = None,
    scopes: Optional[list[str]] = None,
    non_interactive: bool = False,
) -> None:
    """Store Git credentials interactively or via parameters."""
    service = get_git_auth_service()

    # Interactive mode - prompt for missing values
    if not non_interactive:
        if provider is None:
            provider = _prompt_provider()

        if host is None:
            host = _prompt_host(provider)

        if auth_type is None:
            auth_type = _prompt_auth_type(provider)

        if organization is None and provider in [
            GitProvider.GITHUB,
            GitProvider.GITLAB,
            GitProvider.BITBUCKET,
            GitProvider.AZURE_DEVOPS,
        ]:
            organization = Prompt.ask("Organization/namespace")

        if (
            provider == GitProvider.AZURE_DEVOPS
            or (provider == GitProvider.GITLAB and auth_type == GitAuthType.PROJECT_TOKEN)
        ) and project is None:
            project = Prompt.ask("Project name")

        if auth_type == GitAuthType.SSH_KEY:
            if ssh_key is None:
                ssh_key = _read_multiline("Paste SSH private key:")
            if ssh_key_format == SSHKeyFormat.OPENSSH:
                ssh_key_format = Prompt.ask(
                    "SSH key format",
                    choices=[f.value for f in SSHKeyFormat],
                    default=SSHKeyFormat.OPENSSH.value,
                )
            if ssh_passphrase is None and Confirm.ask("SSH key has passphrase?"):
                ssh_passphrase = _read_token("SSH passphrase: ")
        else:
            if username is None:
                username = Prompt.ask("Username (token owner)", default="git")
            if token is None:
                token = _read_token("Personal Access Token: ")

        if description is None:
            description = Prompt.ask("Description (optional)", default="")

        if scopes is None:
            scopes_input = Prompt.ask("Token scopes (comma-separated, optional)", default="")
            scopes = [s.strip() for s in scopes_input.split(",") if s.strip()] if scopes_input else []

    # Validate required fields
    if not alias:
        print_error("Alias is required")
        raise typer.Exit(1)

    if not provider:
        print_error("Provider is required")
        raise typer.Exit(1)

    if not host:
        print_error("Host is required")
        raise typer.Exit(1)

    # Build credentials
    creds_data = {
        "provider": provider,
        "host": host,
        "organization": organization,
        "project": project,
        "auth_type": auth_type,
        "alias": alias,
        "description": description or None,
        "scopes": scopes or [],
    }

    if auth_type == GitAuthType.SSH_KEY:
        if not ssh_key:
            print_error("SSH private key is required for SSH auth")
            raise typer.Exit(1)
        creds_data["ssh_private_key"] = ssh_key
        creds_data["ssh_key_format"] = ssh_key_format
        creds_data["ssh_passphrase"] = ssh_passphrase
        creds_data["username"] = username or "git"
    else:
        if not token:
            # Check env fallback
            env_token = os.environ.get("ASFTOOL_GIT_TOKEN")
            if env_token:
                token = env_token
                print_info("Using token from ASFTOOL_GIT_TOKEN environment variable")
            else:
                print_error("Token is required for PAT auth")
                raise typer.Exit(1)
        creds_data["token"] = token
        creds_data["username"] = username

    try:
        credentials = GitCredentials(**creds_data)

        # Validate
        is_valid, errors = service.validate_credentials(credentials)
        if not is_valid:
            print_error("Validation failed:")
            for err in errors:
                print_error(f"  - {err}")
            raise typer.Exit(1)

        # Store
        service.store_credentials_with_index(credentials)
        print_success(f"Credentials stored for alias '{alias}'")
        print_info(f"Provider: {provider.value}")
        print_info(f"Host: {host}")
        if organization:
            print_info(f"Organization: {organization}")
        if project:
            print_info(f"Project: {project}")
        print_info(f"Auth type: {auth_type.value}")

    except GitAuthError as e:
        print_error(f"Failed to store credentials: {e}")
        raise typer.Exit(1) from e


async def list_async(details: bool = False) -> None:
    """List all stored Git credentials."""
    service = get_git_auth_service()

    try:
        summaries = service.list_credentials()
    except Exception as e:
        print_error(f"Failed to list credentials: {e}")
        raise typer.Exit(1) from e

    if not summaries:
        print_info("No Git credentials stored")
        return

    if details:
        # Detailed view with panels
        for summary in summaries:
            panel = Panel(
                f"[bold]Provider:[/bold] {summary.provider.value}\n"
                f"[bold]Host:[/bold] {summary.host}\n"
                f"[bold]Organization:[/bold] {summary.organization or 'N/A'}\n"
                f"[bold]Project:[/bold] {summary.project or 'N/A'}\n"
                f"[bold]Auth Type:[/bold] {summary.auth_type.value}\n"
                f"[bold]Has Token:[/bold] {'Yes' if summary.has_token else 'No'}\n"
                f"[bold]Has SSH Key:[/bold] {'Yes' if summary.has_ssh_key else 'No'}\n"
                f"[bold]Scopes:[/bold] {', '.join(summary.scopes) if summary.scopes else 'None'}\n"
                f"[bold]Description:[/bold] {summary.description or 'None'}",
                title=f"[cyan]{summary.alias}[/cyan]",
                border_style="blue",
            )
            console.print(panel)
    else:
        # Table view
        table = Table(title="Stored Git Credentials", show_header=True)
        table.add_column("Alias", style="cyan")
        table.add_column("Provider", style="green")
        table.add_column("Host", style="blue")
        table.add_column("Org/Project", style="yellow")
        table.add_column("Auth", style="magenta")
        table.add_column("Token", style="white")
        table.add_column("SSH Key", style="white")

        for summary in summaries:
            org_proj = summary.organization or ""
            if summary.project:
                org_proj += f"/{summary.project}"

            table.add_row(
                summary.alias,
                summary.provider.value,
                summary.host,
                org_proj or "N/A",
                summary.auth_type.value,
                "✓" if summary.has_token else "✗",
                "✓" if summary.has_ssh_key else "✗",
            )
        console.print(table)


async def remove_async(alias: str, force: bool = False) -> None:
    """Remove stored Git credentials."""
    service = get_git_auth_service()

    # Check if exists
    try:
        service.retrieve_credentials(alias)
    except GitCredentialsNotFound:
        print_error(f"Credentials not found for alias: {alias}")
        raise typer.Exit(1)

    if not force:
        if not Confirm.ask(f"Remove credentials for '{alias}'?"):
            print_info("Cancelled")
            return

    try:
        service.delete_credentials_with_index(alias)
        print_success(f"Removed credentials for alias '{alias}'")
    except GitAuthError as e:
        print_error(f"Failed to remove credentials: {e}")
        raise typer.Exit(1) from e


async def validate_async(alias: str) -> None:
    """Validate stored Git credentials."""
    service = get_git_auth_service()

    try:
        credentials = service.retrieve_credentials(alias)
    except GitCredentialsNotFound:
        print_error(f"Credentials not found for alias: {alias}")
        raise typer.Exit(1)

    is_valid, errors = service.validate_credentials(credentials)

    if is_valid:
        print_success(f"Credentials '{alias}' are valid")
        print_info(f"Provider: {credentials.provider.value}")
        print_info(f"Host: {credentials.host}")
        print_info(f"Auth type: {credentials.auth_type.value}")

        # Show connection config (masked)
        config = credentials.to_connection_config()
        print_info("Connection config (secrets masked):")
        for key, value in config.items():
            if key in ("token", "ssh_key", "ssh_passphrase"):
                if value:
                    masked = value[:4] + "****" + value[-4:] if len(value) > 8 else "****"
                    console.print(f"  {key}: {masked}")
                else:
                    console.print(f"  {key}: [dim]not set[/dim]")
            else:
                console.print(f"  {key}: {value}")
    else:
        print_error(f"Credentials '{alias}' are invalid:")
        for err in errors:
            print_error(f"  - {err}")
        raise typer.Exit(1)


async def show_config_async(alias: str) -> None:
    """Show connection config for a credential alias."""
    service = get_git_auth_service()

    try:
        credentials = service.retrieve_credentials(alias)
    except GitCredentialsNotFound:
        print_error(f"Credentials not found for alias: {alias}")
        raise typer.Exit(1)

    config = credentials.to_connection_config()

    # Mask secrets for display
    display_config = {}
    for key, value in config.items():
        if key in ("token", "ssh_key", "ssh_passphrase") and value:
            display_config[key] = value[:4] + "****" + value[-4:] if len(value) > 8 else "****"
        else:
            display_config[key] = value

    console.print_json(json.dumps(display_config, indent=2))


# ---------------------------------------------------------------------------
# Typer commands (thin shims)
# ---------------------------------------------------------------------------


@app.command("login")
def login(
    alias: str = typer.Argument(..., help="Credential alias"),
    provider: Optional[GitProvider] = typer.Option(
        None, "--provider", "-p", help="Git provider", case_sensitive=False
    ),
    host: Optional[str] = typer.Option(None, "--host", "-h", help="Git host URL"),
    auth_type: Optional[GitAuthType] = typer.Option(
        None, "--auth-type", "-a", help="Auth type", case_sensitive=False
    ),
    organization: Optional[str] = typer.Option(
        None, "--org", "-o", help="Organization/namespace"
    ),
    project: Optional[str] = typer.Option(None, "--project", "-P", help="Project name"),
    username: Optional[str] = typer.Option(
        None, "--username", "-u", help="Username (for PAT)"
    ),
    token: Optional[str] = typer.Option(
        None, "--token", "-t", help="Personal Access Token (will prompt if omitted)"
    ),
    ssh_key: Optional[str] = typer.Option(
        None, "--ssh-key", help="SSH private key (path or content)"
    ),
    ssh_key_format: SSHKeyFormat = typer.Option(
        SSHKeyFormat.OPENSSH, "--ssh-format", help="SSH key format"
    ),
    ssh_passphrase: Optional[str] = typer.Option(
        None, "--ssh-passphrase", help="SSH key passphrase"
    ),
    description: Optional[str] = typer.Option(
        None, "--description", "-d", help="Description"
    ),
    scopes: Optional[str] = typer.Option(
        None, "--scopes", "-s", help="Comma-separated token scopes"
    ),
    non_interactive: bool = typer.Option(
        False, "--non-interactive", help="Fail if required params missing"
    ),
):
    """Store Git credentials for a provider."""
    _run(
        login_async(
            alias=alias,
            provider=provider,
            host=host,
            auth_type=auth_type,
            organization=organization,
            project=project,
            username=username,
            token=token,
            ssh_key=ssh_key,
            ssh_key_format=ssh_key_format,
            ssh_passphrase=ssh_passphrase,
            description=description,
            scopes=scopes.split(",") if scopes else None,
            non_interactive=non_interactive,
        )
    )


@app.command("list")
def list_creds(
    details: bool = typer.Option(
        False, "--details", "-d", help="Show detailed view"
    ),
):
    """List all stored Git credentials."""
    _run(list_async(details=details))


@app.command("remove")
def remove(
    alias: str = typer.Argument(..., help="Credential alias to remove"),
    force: bool = typer.Option(
        False, "--force", "-f", help="Skip confirmation"
    ),
):
    """Remove stored Git credentials."""
    _run(remove_async(alias=alias, force=force))


@app.command("validate")
def validate(
    alias: str = typer.Argument(..., help="Credential alias to validate"),
):
    """Validate stored Git credentials."""
    _run(validate_async(alias=alias))


@app.command("show-config")
def show_config(
    alias: str = typer.Argument(..., help="Credential alias"),
):
    """Show connection config for Git client libraries."""
    _run(show_config_async(alias=alias))


@app.command("test-connection")
def test_connection(
    alias: str = typer.Argument(..., help="Credential alias to test"),
):
    """Test connection to Git host using stored credentials."""
    _run(test_connection_async(alias=alias))


async def test_connection_async(alias: str) -> None:
    """Test Git connection using stored credentials."""
    service = get_git_auth_service()

    try:
        credentials = service.retrieve_credentials(alias)
    except GitCredentialsNotFound:
        print_error(f"Credentials not found for alias: {alias}")
        raise typer.Exit(1)

    print_info(f"Testing connection to {credentials.host}...")

    try:
        # Try to use dulwich or GitPython to test
        import subprocess

        config = credentials.to_connection_config()

        if credentials.auth_type == GitAuthType.SSH_KEY:
            # Test SSH connection
            ssh_key = config.get("ssh_key")
            if not ssh_key:
                print_error("No SSH key configured")
                raise typer.Exit(1)

            # Write key to temp file and test
            import tempfile

            with tempfile.NamedTemporaryFile(mode="w", suffix=".key", delete=False) as f:
                f.write(ssh_key)
                key_path = f.name

            try:
                os.chmod(key_path, 0o600)
                host = credentials.host
                result = subprocess.run(
                    ["ssh", "-i", key_path, "-o", "StrictHostKeyChecking=no", "-o", "BatchMode=yes", f"git@{host}"],
                    capture_output=True,
                    text=True,
                    timeout=10,
                )
                if result.returncode == 0 or "successfully authenticated" in result.stderr.lower():
                    print_success("SSH connection successful!")
                else:
                    print_warning(f"SSH test returned: {result.stderr[:200]}")
                    # Some hosts return non-zero even on success
                    if "permission denied" not in result.stderr.lower():
                        print_success("SSH connection likely works (non-zero exit but no permission denied)")
            finally:
                os.unlink(key_path)
        else:
            # Test HTTPS with token
            token = config.get("token")
            username = config.get("username", "git")
            if not token:
                print_error("No token configured")
                raise typer.Exit(1)

            # Try a simple git ls-remote
            clone_url = credentials.to_connection_config().get("clone_url_https", "")
            if not clone_url:
                # Build URL
                if credentials.provider == GitProvider.AZURE_DEVOPS:
                    clone_url = credentials.get_effective_token()
                clone_url = f"https://{username}:{token}@{credentials.host}/{credentials.organization}/{credentials.repository}.git"

            result = subprocess.run(
                ["git", "ls-remote", clone_url],
                capture_output=True,
                text=True,
                timeout=15,
            )
            if result.returncode == 0:
                print_success("HTTPS connection successful!")
                # Show first few refs
                lines = result.stdout.strip().split("\n")[:5]
                for line in lines:
                    console.print(f"  {line}")
                if len(result.stdout.strip().split("\n")) > 5:
                    console.print(f"  ... and {len(result.stdout.strip().split('\n')) - 5} more refs")
            else:
                print_error(f"Connection failed: {result.stderr[:500]}")
                raise typer.Exit(1)

    except subprocess.TimeoutExpired:
        print_error("Connection test timed out")
        raise typer.Exit(1)
    except FileNotFoundError:
        print_error("Git CLI not found. Install git to test connections.")
        raise typer.Exit(1)
    except Exception as e:
        print_error(f"Connection test failed: {e}")
        raise typer.Exit(1)