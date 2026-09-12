"""Workspace Manager for isolated Git repository workspaces.

Manages local workspace directories under ~/.asftool/workspaces/<repo_slug>/
for each target Git repository to avoid conflicts and enable parallel operations.
"""

import os
import shutil
from pathlib import Path
from typing import Optional

from pydantic import BaseModel, Field

from asftool.core.models.git_auth import GitRepositoryTarget, GitProvider


class WorkspaceInfo(BaseModel):
    """Information about a workspace."""

    model_config = {"extra": "forbid"}

    repo_slug: str
    path: Path
    target: GitRepositoryTarget
    exists: bool = False
    is_clean: bool = True
    current_branch: Optional[str] = None
    last_sync: Optional[str] = None  # ISO timestamp


class WorkspaceManager:
    """Manages isolated workspace directories for Git repositories.

    Each GitRepositoryTarget gets its own workspace directory under
    ~/.asftool/workspaces/<slug>/ where <slug> is derived from the
    repository identifier to ensure uniqueness.
    """

    def __init__(self, base_path: Optional[Path] = None):
        """Initialize workspace manager.

        Args:
            base_path: Base directory for workspaces. Defaults to ~/.asftool/workspaces/
        """
        if base_path is None:
            base_path = Path.home() / ".asftool" / "workspaces"

        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)

    def _slugify(self, text: str) -> str:
        """Create a filesystem-safe slug from text."""
        import re
        # Replace non-alphanumeric with hyphen, collapse multiple hyphens
        slug = re.sub(r"[^a-zA-Z0-9]+", "-", text)
        slug = re.sub(r"-+", "-", slug)
        return slug.strip("-").lower()

    def get_workspace_slug(self, target: GitRepositoryTarget) -> str:
        """Generate unique workspace slug for a target repository."""
        # Create slug from provider:host:org:repo:project
        parts = [
            target.provider.value,
            target.host.replace(".", "-"),
            target.organization,
            target.repository,
        ]
        if target.project:
            parts.append(target.project)

        slug = self._slugify("-".join(parts))

        # Limit length
        if len(slug) > 100:
            slug = slug[:97] + "..."

        return slug

    def get_workspace_path(self, target: GitRepositoryTarget) -> Path:
        """Get the workspace path for a target repository."""
        slug = self.get_workspace_slug(target)
        return self.base_path / slug

    def workspace_exists(self, target: GitRepositoryTarget) -> bool:
        """Check if workspace already exists and is a valid Git repo."""
        path = self.get_workspace_path(target)
        return (path / ".git").exists()

    def get_workspace_info(self, target: GitRepositoryTarget) -> WorkspaceInfo:
        """Get information about a workspace."""
        path = self.get_workspace_path(target)
        slug = self.get_workspace_slug(target)

        info = WorkspaceInfo(
            repo_slug=slug,
            path=path,
            target=target,
            exists=self.workspace_exists(target),
        )

        if info.exists:
            # Try to get current branch and clean status
            try:
                import subprocess

                result = subprocess.run(
                    ["git", "-C", str(path), "status", "--porcelain"],
                    capture_output=True,
                    text=True,
                    timeout=5,
                )
                info.is_clean = result.returncode == 0 and result.stdout.strip() == ""

                result = subprocess.run(
                    ["git", "-C", str(path), "branch", "--show-current"],
                    capture_output=True,
                    text=True,
                    timeout=5,
                )
                if result.returncode == 0:
                    info.current_branch = result.stdout.strip() or None
            except Exception:
                pass

        return info

    def list_workspaces(self) -> list[WorkspaceInfo]:
        """List all workspaces under the base directory."""
        workspaces = []

        if not self.base_path.exists():
            return workspaces

        for item in self.base_path.iterdir():
            if item.is_dir() and (item / ".git").exists():
                # Try to infer target from directory structure
                # This is a best-effort reconstruction
                workspaces.append(WorkspaceInfo(
                    repo_slug=item.name,
                    path=item,
                    target=GitRepositoryTarget(
                        provider=GitProvider.GITHUB,  # placeholder
                        host="unknown",
                        organization="unknown",
                        repository=item.name,
                        credentials_alias="unknown",
                    ),
                    exists=True,
                ))

        return workspaces

    def ensure_workspace(
        self,
        target: GitRepositoryTarget,
        clone_url: Optional[str] = None,
        branch: Optional[str] = None,
    ) -> Path:
        """Ensure workspace exists, cloning if necessary.

        Args:
            target: Target repository configuration
            clone_url: Optional explicit clone URL (uses target's default if not provided)
            branch: Optional branch to checkout (uses target's default if not provided)

        Returns:
            Path to the workspace directory
        """
        path = self.get_workspace_path(target)

        if self.workspace_exists(target):
            # Workspace exists, just ensure it's on the right branch
            self._checkout_branch(path, branch or target.branch)
            return path

        # Clone the repository
        url = clone_url or target.clone_url_https
        self._clone_repository(url, path, branch or target.branch)

        return path

    def _clone_repository(
        self,
        url: str,
        path: Path,
        branch: str,
    ) -> None:
        """Clone a repository using Git CLI."""
        import subprocess

        path.parent.mkdir(parents=True, exist_ok=True)

        # Clone with specific branch
        result = subprocess.run(
            ["git", "clone", "--branch", branch, "--single-branch", url, str(path)],
            capture_output=True,
            text=True,
            timeout=60,
        )

        if result.returncode != 0:
            # Try without branch specification (for empty repos)
            result = subprocess.run(
                ["git", "clone", url, str(path)],
                capture_output=True,
                text=True,
                timeout=60,
            )

            if result.returncode != 0:
                raise RuntimeError(f"Failed to clone repository: {result.stderr}")

            # Checkout branch after clone
            self._checkout_branch(path, branch)

    def _checkout_branch(self, path: Path, branch: str) -> None:
        """Checkout a branch in the workspace."""
        import subprocess

        result = subprocess.run(
            ["git", "-C", str(path), "checkout", branch],
            capture_output=True,
            text=True,
            timeout=10,
        )

        if result.returncode != 0:
            # Try creating the branch
            result = subprocess.run(
                ["git", "-C", str(path), "checkout", "-b", branch],
                capture_output=True,
                text=True,
                timeout=10,
            )

            if result.returncode != 0:
                raise RuntimeError(f"Failed to checkout branch '{branch}': {result.stderr}")

    def fetch_latest(self, target: GitRepositoryTarget) -> bool:
        """Fetch latest changes from remote.

        Returns:
            True if fetch succeeded, False otherwise
        """
        path = self.get_workspace_path(target)

        if not self.workspace_exists(target):
            return False

        import subprocess

        result = subprocess.run(
            ["git", "-C", str(path), "fetch", "origin"],
            capture_output=True,
            text=True,
            timeout=30,
        )

        return result.returncode == 0

    def pull_latest(self, target: GitRepositoryTarget) -> bool:
        """Pull latest changes from remote (fetch + merge).

        Returns:
            True if pull succeeded, False otherwise
        """
        path = self.get_workspace_path(target)

        if not self.workspace_exists(target):
            return False

        import subprocess

        result = subprocess.run(
            ["git", "-C", str(path), "pull", "origin"],
            capture_output=True,
            text=True,
            timeout=30,
        )

        return result.returncode == 0

    def delete_workspace(self, target: GitRepositoryTarget) -> bool:
        """Delete a workspace directory.

        Returns:
            True if deleted, False if didn't exist
        """
        path = self.get_workspace_path(target)

        if not path.exists():
            return False

        try:
            shutil.rmtree(path)
            return True
        except Exception:
            return False

    def clean_workspace(self, target: GitRepositoryTarget) -> bool:
        """Clean workspace (reset to HEAD, remove untracked files)."""
        path = self.get_workspace_path(target)

        if not self.workspace_exists(target):
            return False

        import subprocess

        # Reset to HEAD
        result = subprocess.run(
            ["git", "-C", str(path), "reset", "--hard", "HEAD"],
            capture_output=True,
            text=True,
            timeout=10,
        )

        if result.returncode != 0:
            return False

        # Clean untracked files
        result = subprocess.run(
            ["git", "-C", str(path), "clean", "-fd"],
            capture_output=True,
            text=True,
            timeout=10,
        )

        return result.returncode == 0


def get_workspace_manager(base_path: Optional[Path] = None) -> WorkspaceManager:
    """Factory function to get WorkspaceManager instance."""
    return WorkspaceManager(base_path)