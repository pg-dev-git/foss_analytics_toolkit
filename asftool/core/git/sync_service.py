"""Core Git Sync & Revert Service for CRM Analytics assets.

Orchestrates the complete workflow:
- Sync: Pull from CRMA REST API -> Route -> Normalize -> Commit -> Push
- Revert: Checkout commit -> Validate -> Deploy via PUT /wave/<assetType>/<id>/bundle
"""

import json
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

import httpx
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn

from asftool.core.auth.git_auth import get_git_auth_service
from asftool.core.git.resolver import AssetContext, RepoMappingResolver
from asftool.core.git.normalizer import CRMANormalizer
from asftool.core.git.engine import GitEngine
from asftool.core.git.workspace import WorkspaceManager, get_workspace_manager
from asftool.core.models.git_auth import GitRepositoryTarget


@dataclass
class SyncResult:
    """Result of a sync operation."""
    success: bool
    repositories_synced: int = 0
    assets_synced: int = 0
    assets_skipped: int = 0
    errors: list[str] = field(default_factory=list)
    commit_hashes: dict[str, str] = field(default_factory=dict)  # repo_slug -> commit_hash
    duration_seconds: float = 0.0


@dataclass
class RevertResult:
    """Result of a revert operation."""
    success: bool
    asset_id: str = ""
    asset_type: str = ""
    commit_hash: str = ""
    error: Optional[str] = None


class CRMAGitSyncService:
    """Core service for syncing CRMA assets to Git and reverting from Git commits."""

    # CRMA asset types and their REST API endpoints
    ASSET_TYPES = {
        "dashboard": "/services/data/v60.0/wave/dashboards",
        "recipe": "/services/data/v60.0/wave/recipes",
        "dataflow": "/services/data/v60.0/wave/dataflows",
        "lens": "/services/data/v60.0/wave/lenses",
        "dataset": "/services/data/v60.0/wave/datasets",
        "xmd": "/services/data/v60.0/wave/xmds",
    }

    # Bundle endpoint pattern
    BUNDLE_ENDPOINT_PATTERN = "/services/data/v60.0/wave/{asset_type}/{asset_id}/bundle"

    def __init__(
        self,
        resolver: RepoMappingResolver,
        normalizer: Optional[CRMANormalizer] = None,
        workspace_manager: Optional[WorkspaceManager] = None,
        instance_url: Optional[str] = None,
        access_token: Optional[str] = None,
        auth_alias: str = "default",
        progress: Optional[Progress] = None,
    ):
        """Initialize sync service.

        Args:
            resolver: RepoMappingResolver for asset routing
            normalizer: CRMANormalizer for JSON normalization
            workspace_manager: WorkspaceManager for local workspaces
            instance_url: Salesforce instance URL
            access_token: Salesforce access token
            auth_alias: SF CLI auth alias to use
            progress: Optional Rich Progress for CLI feedback
        """
        self.resolver = resolver
        self.normalizer = normalizer or CRMANormalizer()
        self.workspace_manager = workspace_manager or get_workspace_manager()
        self.instance_url = instance_url
        self.access_token = access_token
        self.auth_alias = auth_alias
        self.progress = progress

        # Git auth service for credentials
        self.git_auth_service = get_git_auth_service()

    def _get_sf_client(self) -> httpx.AsyncClient:
        """Create authenticated HTTP client for Salesforce REST API."""
        # This would integrate with existing SF auth system
        # For now, return a client that needs proper auth
        headers = {
            "Authorization": f"Bearer {self.access_token}" if self.access_token else "",
            "Content-Type": "application/json",
        }
        return httpx.AsyncClient(
            base_url=self.instance_url,
            headers=headers,
            timeout=60.0,
        )

    async def sync_all(
        self,
        asset_types: Optional[list[str]] = None,
        dry_run: bool = False,
    ) -> SyncResult:
        """Sync all CRMA assets to their target Git repositories.

        Args:
            asset_types: Optional list of asset types to sync (default: all)
            dry_run: If True, simulate without pushing to Git

        Returns:
            SyncResult with operation details
        """
        start_time = time.time()
        errors = []
        total_assets = 0
        total_synced = 0
        total_skipped = 0
        commit_hashes = {}

        types_to_sync = asset_types or list(self.ASSET_TYPES.keys())

        async with self._get_sf_client() as client:
            for asset_type in types_to_sync:
                try:
                    result = await self._sync_asset_type(
                        client,
                        asset_type,
                        dry_run,
                    )
                    total_assets += result["total"]
                    total_synced += result["synced"]
                    total_skipped += result["skipped"]
                    if result["commit_hash"]:
                        commit_hashes[result["repo_slug"]] = result["commit_hash"]
                    errors.extend(result["errors"])
                except Exception as e:
                    errors.append(f"Failed to sync {asset_type}: {e}")

        duration = time.time() - start_time

        return SyncResult(
            success=len(errors) == 0,
            repositories_synced=len(commit_hashes),
            assets_synced=total_synced,
            assets_skipped=total_skipped,
            errors=errors,
            commit_hashes=commit_hashes,
            duration_seconds=duration,
        )

    async def dry_run(
        self,
        asset_types: Optional[list[str]] = None,
    ) -> dict:
        """Generate a dry-run plan showing what would be synced without making changes.

        Args:
            asset_types: Optional list of asset types to sync (default: all)

        Returns:
            Dictionary with plan details for each repository and asset
        """
        types_to_sync = asset_types or list(self.ASSET_TYPES.keys())
        plan = {"repositories": {}}

        async with self._get_sf_client() as client:
            for asset_type in types_to_sync:
                endpoint = self.ASSET_TYPES.get(asset_type)
                if not endpoint:
                    continue

                assets = await self._fetch_all_assets(client, endpoint, asset_type)

                for asset in assets:
                    context = self._asset_to_context(asset, asset_type)
                    target = self.resolver.resolve_repository(context)
                    repo_slug = self.workspace_manager.get_workspace_slug(target)

                    if repo_slug not in plan["repositories"]:
                        plan["repositories"][repo_slug] = {
                            "target": target,
                            "assets": [],
                        }

                    # Check if asset exists in workspace and compare content
                    workspace_path = self.workspace_manager.ensure_workspace(target)
                    file_name = f"{context.name}.json"
                    if context.folder:
                        folder = self._sanitize_path(context.folder)
                        file_path = Path(target.path_prefix) / folder / asset_type / file_name
                    else:
                        file_path = Path(target.path_prefix) / asset_type / file_name

                    full_path = workspace_path / file_path
                    action = "create"
                    if full_path.exists():
                        normalized = self.normalizer.normalize(asset, asset_type)
                        with open(full_path, "r", encoding="utf-8") as f:
                            existing = f.read()
                        if existing == normalized.normalized_json:
                            action = "skip"
                        else:
                            action = "update"

                    plan["repositories"][repo_slug]["assets"].append({
                        "asset_type": asset_type,
                        "name": context.name,
                        "id": context.asset_id,
                        "folder": context.folder,
                        "action": action,
                    })

        return plan

    async def _sync_asset_type(
        self,
        client: httpx.AsyncClient,
        asset_type: str,
        dry_run: bool,
    ) -> dict[str, Any]:
        """Sync all assets of a specific type."""
        endpoint = self.ASSET_TYPES.get(asset_type)
        if not endpoint:
            return {"total": 0, "synced": 0, "skipped": 0, "errors": [f"Unknown asset type: {asset_type}"], "commit_hash": None, "repo_slug": None}

        # Fetch all assets of this type
        assets = await self._fetch_all_assets(client, endpoint, asset_type)

        # Group by target repository
        assets_by_repo: dict[str, list[dict]] = {}

        for asset in assets:
            try:
                context = self._asset_to_context(asset, asset_type)
                target = self.resolver.resolve_repository(context)
                repo_slug = self.workspace_manager.get_workspace_slug(target)

                if repo_slug not in assets_by_repo:
                    assets_by_repo[repo_slug] = {"target": target, "assets": []}

                assets_by_repo[repo_slug]["assets"].append((context, asset))
            except Exception as e:
                return {"total": len(assets), "synced": 0, "skipped": len(assets), "errors": [f"Routing failed for {asset.get('developerName', 'unknown')}: {e}"], "commit_hash": None, "repo_slug": None}

        # Process each repository
        total_synced = 0
        total_skipped = 0
        all_errors = []
        final_commit_hash = None

        for repo_slug, repo_data in assets_by_repo.items():
            target = repo_data["target"]
            repo_assets = repo_data["assets"]

            try:
                result = await self._sync_repository(
                    target,
                    repo_assets,
                    asset_type,
                    dry_run,
                )
                total_synced += result["synced"]
                total_skipped += result["skipped"]
                all_errors.extend(result["errors"])
                if result["commit_hash"]:
                    final_commit_hash = result["commit_hash"]
            except Exception as e:
                all_errors.append(f"Failed to sync repo {repo_slug}: {e}")

        return {
            "total": len(assets),
            "synced": total_synced,
            "skipped": total_skipped,
            "errors": all_errors,
            "commit_hash": final_commit_hash,
            "repo_slug": list(assets_by_repo.keys())[0] if assets_by_repo else None,
        }

    async def _fetch_all_assets(
        self,
        client: httpx.AsyncClient,
        endpoint: str,
        asset_type: str,
    ) -> list[dict[str, Any]]:
        """Fetch all assets of a type from CRMA REST API with pagination."""
        assets = []
        next_url = endpoint

        while next_url:
            response = await client.get(next_url)
            response.raise_for_status()
            data = response.json()

            if "records" in data:
                assets.extend(data["records"])

            next_url = data.get("nextPageUrl")

        return assets

    def _asset_to_context(self, asset: dict[str, Any], asset_type: str) -> AssetContext:
        """Convert CRMA asset to AssetContext for routing."""
        return AssetContext(
            asset_id=asset.get("id", ""),
            asset_type=asset_type,
            name=asset.get("developerName", asset.get("label", "unknown")),
            folder=asset.get("folderName"),
            app=asset.get("applicationId"),
        )

    async def _sync_repository(
        self,
        target: GitRepositoryTarget,
        assets: list[tuple[AssetContext, dict]],
        asset_type: str,
        dry_run: bool,
    ) -> dict[str, Any]:
        """Sync assets to a specific repository."""
        errors = []
        synced = 0
        skipped = 0
        commit_hash = None

        # Ensure workspace exists
        workspace_path = self.workspace_manager.ensure_workspace(target)

        # Get credentials for this repo
        credentials = self.git_auth_service.retrieve_credentials(target.credentials_alias)

        # Create Git engine
        with GitEngine(workspace_path, credentials) as engine:
            # Pull latest changes
            pull_result = engine.pull()
            if not pull_result["success"]:
                errors.append(f"Failed to pull latest: {pull_result.get('error')}")

            # Process each asset
            files_changed = []

            for context, asset in assets:
                try:
                    # Normalize asset JSON
                    result = self.normalizer.normalize(asset, asset_type)
                    normalized_json = result.normalized_json

                    # Determine file path in repo
                    file_name = f"{context.name}.json"
                    if context.folder:
                        # Sanitize folder name for filesystem
                        folder = self._sanitize_path(context.folder)
                        file_path = Path(target.path_prefix) / folder / asset_type / file_name
                    else:
                        file_path = Path(target.path_prefix) / asset_type / file_name

                    full_path = workspace_path / file_path
                    full_path.parent.mkdir(parents=True, exist_ok=True)

                    # Check if file exists and content changed
                    existing_content = ""
                    if full_path.exists():
                        with open(full_path, "r", encoding="utf-8") as f:
                            existing_content = f.read()

                    if existing_content == normalized_json:
                        skipped += 1
                        continue

                    # Write normalized file
                    with open(full_path, "w", encoding="utf-8") as f:
                        f.write(normalized_json)

                    files_changed.append(str(file_path))
                    synced += 1

                except Exception as e:
                    errors.append(f"Failed to process asset {context.name}: {e}")

            # Commit and push if there are changes
            if files_changed and not dry_run:
                # Stage files
                stage_result = engine.stage_files(files_changed)
                if not stage_result["success"]:
                    errors.append(f"Failed to stage files: {stage_result.get('error')}")
                else:
                    # Create commit
                    commit_msg = self._generate_commit_message(asset_type, synced, skipped)
                    commit_hash = engine.commit(commit_msg)

                    if commit_hash:
                        # Push to remote
                        push_result = engine.push(branch=target.branch)
                        if not push_result["success"]:
                            errors.append(f"Failed to push: {push_result.get('error')}")
                    else:
                        errors.append("Failed to create commit")

        return {
            "synced": synced,
            "skipped": skipped,
            "errors": errors,
            "commit_hash": commit_hash,
        }

    def _generate_commit_message(self, asset_type: str, synced: int, skipped: int) -> str:
        """Generate descriptive commit message."""
        timestamp = datetime.utcnow().isoformat()
        return f"Sync {asset_type}: {synced} updated, {skipped} unchanged ({timestamp})"

    def _sanitize_path(self, path: str) -> str:
        """Sanitize path for filesystem."""
        import re
        # Replace invalid characters
        path = re.sub(r'[<>:"/\\|?*]', "_", path)
        # Remove leading/trailing dots and spaces
        path = path.strip(". ")
        return path

    async def revert_asset(
        self,
        asset_id: str,
        asset_type: str,
        commit_hash: str,
        repo_slug: Optional[str] = None,
    ) -> RevertResult:
        """Revert an asset to a specific Git commit and deploy to CRMA.

        Args:
            asset_id: CRMA asset ID
            asset_type: Asset type (dashboard, recipe, etc.)
            commit_hash: Git commit hash to revert to
            repo_slug: Optional workspace slug (will resolve if not provided)

        Returns:
            RevertResult with operation details
        """
        try:
            # Find the repository containing this asset
            if repo_slug:
                # Use specific workspace
                workspaces = self.workspace_manager.list_workspaces()
                workspace_info = next((w for w in workspaces if w.repo_slug == repo_slug), None)
                if not workspace_info:
                    return RevertResult(
                        success=False,
                        asset_id=asset_id,
                        asset_type=asset_type,
                        commit_hash=commit_hash,
                        error=f"Workspace not found: {repo_slug}",
                    )
                target = workspace_info.target
            else:
                # Resolve from asset context - need to fetch asset first to get context
                # For now, this requires the repo_slug
                return RevertResult(
                    success=False,
                    asset_id=asset_id,
                    asset_type=asset_type,
                    commit_hash=commit_hash,
                    error="repo_slug is required for revert",
                )

            workspace_path = self.workspace_manager.get_workspace_path(target)
            credentials = self.git_auth_service.retrieve_credentials(target.credentials_alias)

            with GitEngine(workspace_path, credentials) as engine:
                # Determine file path
                # We need to know the folder - try to find file at commit
                files = engine.list_files_at_commit(commit_hash)
                asset_files = [f for f in files if f.endswith(f"{asset_id}.json") or asset_id in f]

                if not asset_files:
                    # Try with developer name pattern
                    asset_files = [f for f in files if asset_type in f and f.endswith(".json")]

                if not asset_files:
                    return RevertResult(
                        success=False,
                        asset_id=asset_id,
                        asset_type=asset_type,
                        commit_hash=commit_hash,
                        error=f"Asset file not found at commit {commit_hash}",
                    )

                # Read the file at the commit
                file_content = None
                for f in asset_files:
                    content = engine.read_file_at_commit(f, commit_hash)
                    if content:
                        file_content = content
                        break

                if not file_content:
                    return RevertResult(
                        success=False,
                        asset_id=asset_id,
                        asset_type=asset_type,
                        commit_hash=commit_hash,
                        error="Could not read asset content from commit",
                    )

                # Parse and validate JSON
                asset_json = json.loads(file_content.decode("utf-8"))

                # Validate against expected schema (basic validation)
                if not self._validate_asset_bundle(asset_json, asset_type):
                    return RevertResult(
                        success=False,
                        asset_id=asset_id,
                        asset_type=asset_type,
                        commit_hash=commit_hash,
                        error="Asset bundle validation failed",
                    )

                # Deploy to CRMA via bundle endpoint
                success = await self._deploy_bundle(asset_id, asset_type, asset_json)

                return RevertResult(
                    success=success,
                    asset_id=asset_id,
                    asset_type=asset_type,
                    commit_hash=commit_hash,
                    error=None if success else "Bundle deployment failed",
                )

        except Exception as e:
            return RevertResult(
                success=False,
                asset_id=asset_id,
                asset_type=asset_type,
                commit_hash=commit_hash,
                error=str(e),
            )

    def _validate_asset_bundle(self, asset_json: dict[str, Any], asset_type: str) -> bool:
        """Validate asset bundle against expected structure."""
        # Basic validation - ensure required fields exist
        if "developerName" not in asset_json and "label" not in asset_json:
            return False
        return True

    async def _deploy_bundle(
        self,
        asset_id: str,
        asset_type: str,
        asset_json: dict[str, Any],
    ) -> bool:
        """Deploy asset bundle to CRMA via PUT /wave/<assetType>/<id>/bundle."""
        try:
            endpoint = self.BUNDLE_ENDPOINT_PATTERN.format(
                asset_type=asset_type,
                asset_id=asset_id,
            )

            async with self._get_sf_client() as client:
                response = await client.put(
                    endpoint,
                    json=asset_json,
                )
                response.raise_for_status()
                return True
        except Exception as e:
            return False


def create_sync_service(
    resolver: RepoMappingResolver,
    instance_url: str,
    access_token: str,
    auth_alias: str = "default",
    progress: Optional[Progress] = None,
) -> CRMAGitSyncService:
    """Factory function to create sync service."""
    return CRMAGitSyncService(
        resolver=resolver,
        instance_url=instance_url,
        access_token=access_token,
        auth_alias=auth_alias,
        progress=progress,
    )