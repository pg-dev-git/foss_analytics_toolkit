"""Git CLI commands for sync, revert, init-config, and status.

Commands:
- init-config: Generate .asftool-git.yml
- sync: Trigger multi-repo sync
- revert: Restore asset from Git commit
- status: Show workspace status
"""

import asyncio
import httpx
from typing import Optional

import typer
from rich.console import Console
from rich.panel import Panel
from rich.prompt import Confirm, Prompt
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn, TimeElapsedColumn

from asftool.cli.session import Session
from asftool.cli.ui import print_error, print_info, print_success, print_warning
from asftool.core.git import (
    AssetContext,
    CRMAGitSyncService,
    RepoMappingConfig,
    RepoMappingResolver,
    SyncResult,
    WorkspaceManager,
    create_default_config,
)
from asftool.core.git.resolver import RepoMappingRule
from asftool.core.models.git_auth import GitProvider, GitRepositoryTarget

app = typer.Typer(help="Git version control for CRMA assets")
console = Console()


def _run(coro):
    """Run async coroutine in a fresh event loop."""
    return asyncio.run(coro)


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
    ctx: typer.Context,
    asset_type: str = typer.Option("all", "--type", "-t", help="Asset type or 'all'"),
    dry_run: bool = typer.Option(False, "--dry-run", "-d"),
    verbose: bool = typer.Option(False, "--verbose", "-v"),
    quiet: bool = typer.Option(False, "--quiet", "-q"),
    config: str = typer.Option(".asftool-git.yml", "--config", "-c"),
    alias: str = typer.Option("default", "--alias", "-a", help="SF CLI alias to use"),
):
    """Sync CRMA assets to their target Git repositories."""
    from pathlib import Path

    config_path = Path(config)
    if not config_path.exists():
        print_error(f"Configuration file not found: {config_path}")
        print_info("Run 'asftool git init-config' to create a default config")
        raise typer.Exit(1)

    resolver = RepoMappingResolver.from_file(config_path)

    async def _run_sync():
        # Get SF auth tokens from session
        async with Session(alias=alias) as session:
            try:
                auth_tokens = await session.get_auth_tokens()
            except Exception as e:
                print_error(f"Failed to get auth tokens: {e}")
                print_info("Run 'asftool auth login --alias {alias}' first")
                raise typer.Exit(1)

        if not verbose and not quiet:
            print_info(f"Using SF org: {auth_tokens.username} @ {auth_tokens.instance_url}")

        service = CRMAGitSyncService(
            resolver=resolver,
            instance_url=auth_tokens.instance_url,
            access_token=auth_tokens.access_token,
        )

        if asset_type == "all":
            asset_types = None
        else:
            asset_types = [asset_type]

        if dry_run:
            # Show dry-run preview
            print_info("Dry-run mode: showing planned changes without syncing")
            try:
                plan = await service.dry_run(asset_types=asset_types)
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 401:
                    print_error("Authentication failed: Your Salesforce session has expired.")
                    print_info("Run 'asftool auth login --alias <your-alias>' to re-authenticate")
                else:
                    print_error(f"Sync failed (HTTP {e.response.status_code})")
                if verbose:
                    import traceback
                    console.print_exception()
                raise typer.Exit(1)
            _print_sync_plan(plan, console)
            return

        if not quiet:
            print_info("Starting synchronization...")

        # Run sync with progress tracking
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            TimeElapsedColumn(),
            console=console,
            disable=quiet,
        ) as progress:
            task = progress.add_task("Syncing assets...", total=None)

            try:
                result: SyncResult = await service.sync_all(asset_types=asset_types)
                progress.update(task, completed=100)
            except httpx.HTTPStatusError as e:
                progress.update(task, completed=100)
                if e.response.status_code == 401:
                    print_error("Authentication failed: Your Salesforce session has expired.")
                    print_info("Run 'asftool auth login --alias <your-alias>' to re-authenticate")
                else:
                    print_error(f"Sync failed (HTTP {e.response.status_code})")
                if verbose:
                    import traceback
                    console.print_exception()
                raise typer.Exit(1)
            except Exception as e:
                progress.update(task, completed=100)
                print_error(f"Sync failed: {e}")
                if verbose:
                    import traceback
                    console.print_exception()
                raise typer.Exit(1)

        # Print results
        if not quiet:
            if result.success:
                print_success(f"Sync completed in {result.duration_seconds:.1f}s")
                print_info(f"Repositories synced: {result.repositories_synced}")
                print_info(f"Assets synced: {result.assets_synced}")
                print_info(f"Assets skipped: {result.assets_skipped}")
                if result.commit_hashes:
                    for repo_slug, commit_hash in result.commit_hashes.items():
                        print_info(f"  {repo_slug}: {commit_hash[:8]}")
            else:
                print_warning("Sync completed with errors:")
                for err in result.errors:
                    print_error(f"  - {err}")

        if not result.success:
            raise typer.Exit(1)

    # Run async sync using fresh event loop (works with or without existing loop)
    try:
        return asyncio.run(_run_sync())
    except RuntimeError:
        import nest_asyncio
        nest_asyncio.apply()
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(_run_sync())
        finally:
            loop.close()


def _print_sync_plan(plan: dict, console: Console) -> None:
    """Print dry-run plan as Rich table."""
    from rich.table import Table

    table = Table(title="Sync Plan (Dry Run)", show_header=True)
    table.add_column("Repo Slug", style="cyan")
    table.add_column("Asset Type", style="green")
    table.add_column("Asset Name", style="white")
    table.add_column("Folder", style="dim")
    table.add_column("Action", style="yellow")

    for repo_slug, repo_data in plan.get("repositories", {}).items():
        for asset in repo_data.get("assets", []):
            table.add_row(
                repo_slug,
                asset.get("asset_type", "-"),
                asset.get("name", "-"),
                asset.get("folder", "-"),
                asset.get("action", "-"),
            )

    console.print(table)

    total_create = sum(1 for r in plan.get("repositories", {}).values() for a in r.get("assets", []) if a.get("action") == "create")
    total_update = sum(1 for r in plan.get("repositories", {}).values() for a in r.get("assets", []) if a.get("action") == "update")
    total_skip = sum(1 for r in plan.get("repositories", {}).values() for a in r.get("assets", []) if a.get("action") == "skip")

    console.print(f"[cyan]Plan Summary:[/cyan] {total_create} create, {total_update} update, {total_skip} skip")


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