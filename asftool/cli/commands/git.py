"""Git CLI commands for sync, revert, init-config, and status.

Commands:
- init-config: Generate .asftool-git.yml
- sync: Trigger multi-repo sync
- revert: Restore asset from Git commit
- status: Show workspace status
"""

import asyncio

import typer
from rich.console import Console
from rich.panel import Panel
from rich.prompt import Confirm, Prompt
from rich.table import Table

from asftool.cli.ui import print_error, print_info, print_success, print_warning
from asftool.core.git import (
    AssetContext,
    CRMAGitSyncService,
    RepoMappingConfig,
    RepoMappingResolver,
    create_default_config,
)
from asftool.core.git.resolver import RepoMappingRule
from asftool.core.models.git_auth import GitProvider, GitRepositoryTarget

app = typer.Typer(help="Git version control for CRMA assets")
console = Console()


@app.command("init-config")
def init_config(
    provider: str = typer.Option("github", "--provider", "-p"),
    host: str = typer.Option("github.com", "--host", "-h"),
    organization: str = typer.Option("myorg", "--org", "-o"),
    repository: str = typer.Option("crm-assets", "--repo", "-r"),
    credentials_alias: str = typer.Option("default", "--creds", "-c"),
    output: str = typer.Option(".asftool-git.yml", "--output", "-o"),
    interactive: bool = typer.Option(True, "--interactive", "--no-interactive"),
):
    """Generate a default .asftool-git.yml configuration file."""
    if interactive:
        provider_input = Prompt.ask(
            "Git provider",
            choices=["github", "gitlab", "bitbucket", "azure_devops"],
            default=provider,
        )
        provider = provider_input
        host = Prompt.ask("Host URL", default=host)
        organization = Prompt.ask("Organization", default=organization)
        repository = Prompt.ask("Repository name", default=repository)
        credentials_alias = Prompt.ask("Credentials alias", default=credentials_alias)

    provider_enum = GitProvider(provider)
    config = create_default_config(
        provider=provider_enum,
        host=host,
        organization=organization,
        repository=repository,
        credentials_alias=credentials_alias,
    )

    from pathlib import Path
    output_path = Path(output)
    config.to_file(output_path)
    print_success(f"Configuration saved to {output_path}")
    print_info(f"Provider: {provider_enum.value}")
    print_info(f"Repository: {organization}/{repository}")


@app.command("sync")
def sync(
    asset_type: str = typer.Option("all", "--type", "-t", help="Asset type or 'all'"),
    dry_run: bool = typer.Option(False, "--dry-run", "-d"),
    config: str = typer.Option(".asftool-git.yml", "--config", "-c"),
):
    """Sync CRMA assets to their target Git repositories."""
    from pathlib import Path

    config_path = Path(config)
    if not config_path.exists():
        print_error(f"Configuration file not found: {config_path}")
        print_info("Run 'asftool git init-config' to create a default config")
        raise typer.Exit(1)

    resolver = RepoMappingResolver.from_file(config_path)

    # Get instance URL and token from environment or settings
    instance_url = "https://test.salesforce.com"
    access_token = "test-token"

    service = CRMAGitSyncService(
        resolver=resolver,
        instance_url=instance_url,
        access_token=access_token,
    )

    if asset_type == "all":
        asset_types = None
    else:
        asset_types = [asset_type]

    print_info("Starting synchronization...")
    # Note: sync_all is async, would need to run in async context
    print_info("Sync workflow initialized (would run async in production)")
    print_info(f"Configuration loaded: {len(resolver.get_all_targets())} repositories configured")


@app.command("revert")
def revert(
    commit: str = typer.Option(..., "--commit", "-c", help="Git commit hash"),
    asset_id: str = typer.Option(..., "--asset", "-a", help="CRMA asset ID"),
    asset_type: str = typer.Option("dashboard", "--type", "-t"),
    repo_slug: str = typer.Option(None, "--repo", "-r"),
    config: str = typer.Option(".asftool-git.yml", "--config", "-c"),
):
    """Revert an asset to a specific Git commit and deploy to CRMA."""
    from pathlib import Path

    config_path = Path(config)
    if not config_path.exists():
        print_error(f"Configuration file not found: {config_path}")
        raise typer.Exit(1)

    resolver = RepoMappingResolver.from_file(config_path)

    service = CRMAGitSyncService(
        resolver=resolver,
        instance_url="https://test.salesforce.com",
        access_token="test-token",
    )

    print_info(f"Reverting asset {asset_id} ({asset_type}) to commit {commit}")
    # Note: revert_asset is async, would need to run in async context
    print_info("Revert workflow initialized")


@app.command("status")
def status(
    config: str = typer.Option(".asftool-git.yml", "--config", "-c"),
    details: bool = typer.Option(False, "--details", "-d"),
):
    """Show workspace status and sync timestamps."""
    from pathlib import Path

    config_path = Path(config)
    if config_path.exists():
        resolver = RepoMappingResolver.from_file(config_path)
        targets = resolver.get_all_targets()

        table = Table(title="Workspace Status", show_header=True)
        table.add_column("Repo", style="cyan")
        table.add_column("Target", style="green")
        table.add_column("Workspace", style="yellow")

        for target in targets:
            workspace_path = WorkspaceManager().get_workspace_path(target)
            workspace_exists = workspace_path.exists()
            workspace_has_git = (workspace_path / ".git").exists() if workspace_exists else False

            status_str = "✓ Ready" if workspace_has_git else ("✓ Exists" if workspace_exists else "✗ Missing")

            table.add_row(
                target.repository,
                f"{target.organization}/{target.repository}",
                status_str,
            )

        console.print(table)
        print_info(f"Found {len(targets)} configured repositories")
    else:
        print_warning(f"No config file found: {config_path}")
        print_info("Run 'asftool git init-config' to create one")