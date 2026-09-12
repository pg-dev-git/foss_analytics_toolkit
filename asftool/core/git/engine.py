"""Pure-Python Git Engine using dulwich for headless Git operations.

Provides programmatic Git operations without spawning interactive shell prompts.
All operations use dulwich (pure Python) for maximum portability.
"""

import os
import stat
import tempfile
from pathlib import Path
from typing import Any, Optional

import dulwich.config
import dulwich.errors
import dulwich.index
import dulwich.object_store
import dulwich.pack
import dulwich.porcelain
import dulwich.repo
from dulwich.objects import Blob, Commit, Tree
from dulwich.refs import RefsContainer

from asftool.core.models.git_auth import GitCredentials, GitRepositoryTarget


class GitEngineError(Exception):
    """Base exception for GitEngine errors."""

    pass


class GitEngine:
    """Pure-Python Git engine using dulwich for headless operations.

    Supports:
    - clone/fetch from remote
    - stage/commit/push changes
    - checkout commits
    - read files at specific commits
    - get commit history
    """

    def __init__(
        self,
        workspace_path: Path,
        credentials: Optional[GitCredentials] = None,
    ):
        """Initialize Git engine for a workspace.

        Args:
            workspace_path: Path to the Git repository workspace
            credentials: Optional credentials for authenticated operations
        """
        self.workspace_path = Path(workspace_path)
        self.credentials = credentials
        self._repo: Optional[dulwich.repo.Repo] = None

    @property
    def repo(self) -> dulwich.repo.Repo:
        """Get or open the dulwich repository."""
        if self._repo is None:
            if not self.workspace_path.exists():
                raise GitEngineError(f"Workspace does not exist: {self.workspace_path}")

            try:
                self._repo = dulwich.repo.Repo(str(self.workspace_path))
            except dulwich.errors.NotGitRepository:
                raise GitEngineError(f"Not a Git repository: {self.workspace_path}")

        return self._repo

    def close(self) -> None:
        """Close the repository."""
        if self._repo is not None:
            self._repo.close()
            self._repo = None

    def __enter__(self) -> "GitEngine":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    # -------------------------------------------------------------------------
    # Repository initialization
    # -------------------------------------------------------------------------

    @classmethod
    def init_repository(
        cls,
        path: Path,
        initial_branch: str = "main",
    ) -> "GitEngine":
        """Initialize a new Git repository.

        Args:
            path: Path where to create the repository
            initial_branch: Name of the initial branch

        Returns:
            GitEngine instance for the new repository
        """
        path = Path(path)
        path.mkdir(parents=True, exist_ok=True)

        repo = dulwich.repo.Repo.init(str(path), mkdir=False)

        # Set initial branch name
        with repo:
            # Create initial empty commit to establish branch
            import time

            author = b"ASFTool <asftool@salesforce.com>"
            timestamp = int(time.time())
            timezone = 0

            # Create empty tree
            tree = Tree()
            tree_id = repo.object_store.add_object(tree)

            # Create an empty file to commit
            readme_path = path / "README.md"
            readme_path.write_text("# ASFTool Repository\n\nInitialized by ASFTool.")

            # Stage and commit using porcelain
            dulwich.porcelain.add(repo, ["README.md"])
            dulwich.porcelain.commit(
                repo,
                b"Initial commit by ASFTool",
                author=author,
                committer=author,
            )

            # Rename branch to initial_branch
            if initial_branch != "master":
                master_sha = repo.refs[b"refs/heads/master"]
                repo.refs.set_symbolic_ref(b"HEAD", b"refs/heads/" + initial_branch.encode())
                repo.refs[b"refs/heads/" + initial_branch.encode()] = master_sha
                # Remove master branch
                if b"refs/heads/master" in repo.refs:
                    del repo.refs[b"refs/heads/master"]

        return cls(path)

    @classmethod
    def clone_repository(
        cls,
        url: str,
        path: Path,
        branch: str = "main",
        credentials: Optional[GitCredentials] = None,
        depth: Optional[int] = None,
    ) -> "GitEngine":
        """Clone a remote repository.

        Args:
            url: Repository URL (HTTPS or SSH)
            path: Local path to clone into
            branch: Branch to checkout
            credentials: Optional authentication credentials
            depth: Optional shallow clone depth

        Returns:
            GitEngine instance for the cloned repository
        """
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)

        # Prepare auth for dulwich
        auth = None
        if credentials:
            auth = cls._create_dulwich_auth(credentials, url)

        # Clone using dulwich
        try:
            if depth:
                # Shallow clone not directly supported in dulwich, use full clone
                pass

            dulwich.porcelain.clone(
                url,
                str(path),
                branch=branch,
                checkout=True,
                auth=auth,
            )
        except Exception as e:
            raise GitEngineError(f"Failed to clone repository: {e}")

        engine = cls(path, credentials)

        # Verify branch was checked out
        try:
            with engine.repo:
                head_ref = engine.repo.refs[b"HEAD"]
                if not head_ref.decode().endswith(f"refs/heads/{branch}"):
                    # Try to checkout the branch
                    engine.checkout_branch(branch)
        except Exception:
            pass

        return engine

    @staticmethod
    def _create_dulwich_auth(credentials: GitCredentials, url: str) -> Optional[Any]:
        """Create dulwich authentication handler."""
        from dulwich.client import get_transport_and_path
        from dulwich.client import HttpGitClient

        # For HTTPS with token
        if credentials.auth_type.value in ("pat", "project_token"):
            token = credentials.get_effective_token()
            username = credentials.get_effective_username() or "git"

            if token:
                # Create a custom client with auth
                def auth_handler(host, path):
                    return (username, token)

                return auth_handler

        # For SSH, dulwich uses SSH agent by default
        # For SSH with explicit key, we'd need custom transport
        # This is a simplified version - full SSH key support requires more setup

        return None

    # -------------------------------------------------------------------------
    # Remote operations
    # -------------------------------------------------------------------------

    def fetch(self, remote: str = "origin") -> dict[str, Any]:
        """Fetch from remote.

        Args:
            remote: Remote name

        Returns:
            Dict with fetch results
        """
        try:
            result = dulwich.porcelain.fetch(
                self.repo,
                remote,
                auth=self._get_auth_for_remote(remote),
            )
            return {"success": True, "result": result}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def pull(self, remote: str = "origin", branch: str = "main") -> dict[str, Any]:
        """Pull from remote (fetch + merge).

        Args:
            remote: Remote name
            branch: Branch to pull

        Returns:
            Dict with pull results
        """
        try:
            result = dulwich.porcelain.pull(
                self.repo,
                remote,
                branch=branch,
                auth=self._get_auth_for_remote(remote),
            )
            return {"success": True, "result": result}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def push(
        self,
        remote: str = "origin",
        branch: str = "main",
        force: bool = False,
    ) -> dict[str, Any]:
        """Push to remote.

        Args:
            remote: Remote name
            branch: Branch to push
            force: Force push

        Returns:
            Dict with push results
        """
        try:
            refspecs = [f"refs/heads/{branch}:refs/heads/{branch}"]
            result = dulwich.porcelain.push(
                self.repo,
                remote,
                refspecs=refspecs,
                force=force,
                auth=self._get_auth_for_remote(remote),
            )
            return {"success": True, "result": result}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def _get_auth_for_remote(self, remote: str) -> Optional[Any]:
        """Get auth handler for a remote."""
        if not self.credentials:
            return None

        # Get remote URL
        try:
            config = self.repo.get_config()
            url = config.get((b'remote "' + remote.encode() + b'"', b"url"))
            if url:
                return self._create_dulwich_auth(self.credentials, url.decode())
        except Exception:
            pass

        return None

    # -------------------------------------------------------------------------
    # Branch operations
    # -------------------------------------------------------------------------

    def checkout_branch(self, branch: str, create: bool = False) -> bool:
        """Checkout a branch.

        Args:
            branch: Branch name
            create: Create branch if it doesn't exist

        Returns:
            True on success
        """
        try:
            ref_name = f"refs/heads/{branch}"

            if create:
                # Create branch from current HEAD
                head_commit = self.repo.head()
                self.repo.refs[ref_name.encode()] = head_commit

            # Checkout
            self.repo.refs[b"HEAD"] = ref_name.encode()

            # Reset working directory
            self._reset_working_tree()

            return True
        except Exception as e:
            raise GitEngineError(f"Failed to checkout branch '{branch}': {e}")

    def create_branch(self, branch: str, start_point: Optional[str] = None) -> bool:
        """Create a new branch.

        Args:
            branch: New branch name
            start_point: Commit hash or ref to start from (default: HEAD)

        Returns:
            True on success
        """
        try:
            ref_name = f"refs/heads/{branch}"

            if start_point:
                commit_id = self.repo.refs[start_point.encode()]
            else:
                commit_id = self.repo.head()

            self.repo.refs[ref_name.encode()] = commit_id
            return True
        except Exception as e:
            raise GitEngineError(f"Failed to create branch '{branch}': {e}")

    def get_current_branch(self) -> Optional[str]:
        """Get current branch name."""
        try:
            head = self.repo.refs[b"HEAD"]
            # HEAD could be a symbolic reference (like b'refs/heads/main')
            # or a direct commit hash
            if head.startswith(b"ref: "):
                ref = head[5:].decode()
                if ref.startswith("refs/heads/"):
                    return ref[11:]
            # If HEAD points directly to a commit hash, get branch from refs
            # Find a branch that points to this commit
            head_commit = self.repo.head()
            for ref in self.repo.refs.as_dict().keys():
                ref_str = ref.decode()
                if ref_str.startswith("refs/heads/"):
                    branch_name = ref_str[11:]
                    if self.repo.refs[ref] == head_commit:
                        return branch_name
            return None
        except Exception:
            return None

    def list_branches(self) -> list[str]:
        """List all local branches."""
        branches = []
        for ref in self.repo.refs.as_dict().keys():
            ref_str = ref.decode()
            if ref_str.startswith("refs/heads/"):
                branches.append(ref_str[11:])
        return branches

    # -------------------------------------------------------------------------
    # Working tree operations
    # -------------------------------------------------------------------------

    def stage_files(self, paths: list[str]) -> dict[str, Any]:
        """Stage files for commit.

        Args:
            paths: List of file paths relative to repo root

        Returns:
            Dict with staging results
        """
        try:
            # Use dulwich porcelain add which handles index properly
            import dulwich.porcelain

            # Convert paths to strings (not bytes) for porcelain
            str_paths = [str(p) for p in paths]
            dulwich.porcelain.add(self.repo, str_paths)

            return {"success": True, "staged": paths}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def stage_all_changes(self) -> dict[str, Any]:
        """Stage all changes (including new files)."""
        try:
            # Get status to find all changed files
            status = self.get_status()

            all_paths = []
            all_paths.extend(status.get("staged", []))
            all_paths.extend(status.get("unstaged", []))
            all_paths.extend(status.get("untracked", []))

            return self.stage_files(all_paths)
        except Exception as e:
            return {"success": False, "error": str(e)}

    def get_status(self) -> dict[str, list[str]]:
        """Get repository status."""
        try:
            status = dulwich.porcelain.status(self.repo)

            return {
                "staged": [s.decode() for s in status.staged],
                "unstaged": [s.decode() for s in status.unstaged],
                "untracked": [s.decode() for s in status.untracked],
            }
        except Exception as e:
            raise GitEngineError(f"Failed to get status: {e}")

    def _reset_working_tree(self) -> None:
        """Reset working tree to match HEAD."""
        try:
            head_commit = self.repo.head()
            tree = self.repo[head_commit].tree
            self._checkout_tree(tree)
        except Exception as e:
            raise GitEngineError(f"Failed to reset working tree: {e}")

    def _checkout_tree(self, tree_id: bytes) -> None:
        """Checkout a tree to working directory."""
        tree = self.repo[tree_id]

        for name, mode, obj_id in tree.iteritems():
            path = self.workspace_path / name.decode()
            obj = self.repo[obj_id]

            if stat.S_ISDIR(mode):
                path.mkdir(parents=True, exist_ok=True)
                self._checkout_tree(obj_id)
            else:
                path.parent.mkdir(parents=True, exist_ok=True)
                with open(path, "wb") as f:
                    if isinstance(obj, Blob):
                        f.write(obj.data)
                    else:
                        # For other object types, serialize
                        f.write(obj.data)

    # -------------------------------------------------------------------------
    # Commit operations
    # -------------------------------------------------------------------------

    def commit(
        self,
        message: str,
        author: Optional[str] = None,
        author_email: Optional[str] = None,
    ) -> Optional[str]:
        """Create a commit from staged changes.

        Args:
            message: Commit message
            author: Author name (default: ASFTool)
            author_email: Author email (default: asftool@salesforce.com)

        Returns:
            Commit hash if successful, None otherwise
        """
        try:
            import dulwich.porcelain

            author_str = f"{author or 'ASFTool'} <{author_email or 'asftool@salesforce.com'}>"

            # Use dulwich porcelain commit
            commit_id = dulwich.porcelain.commit(
                self.repo,
                message.encode(),
                author=author_str.encode(),
                committer=author_str.encode(),
            )

            return commit_id.decode()
        except Exception as e:
            raise GitEngineError(f"Failed to create commit: {e}")

    def get_commit_history(
        self,
        branch: str = "main",
        max_count: int = 50,
    ) -> list[dict[str, Any]]:
        """Get commit history for a branch.

        Args:
            branch: Branch name
            max_count: Maximum number of commits to return

        Returns:
            List of commit info dicts
        """
        try:
            ref_name = f"refs/heads/{branch}"
            commit_id = self.repo.refs[ref_name.encode()]

            commits = []
            walker = self.repo.get_walker(include=[commit_id])

            for entry in walker:
                if len(commits) >= max_count:
                    break

                commit = entry.commit
                commits.append({
                    "hash": commit.id.decode(),
                    "short_hash": commit.id.decode()[:8],
                    "message": commit.message.decode().strip(),
                    "author": commit.author.decode(),
                    "author_time": commit.author_time,
                    "committer": commit.committer.decode(),
                    "committer_time": commit.commit_time,
                    "parents": [p.decode() for p in commit.parents],
                })

            return commits
        except Exception as e:
            raise GitEngineError(f"Failed to get commit history: {e}")

    # -------------------------------------------------------------------------
    # File operations at specific commits
    # -------------------------------------------------------------------------

    def read_file_at_commit(
        self,
        file_path: str,
        commit_hash: str,
    ) -> Optional[bytes]:
        """Read a file at a specific commit.

        Args:
            file_path: Path to file relative to repo root
            commit_hash: Commit hash

        Returns:
            File content as bytes, or None if not found
        """
        try:
            commit = self.repo[commit_hash.encode()]
            tree = self.repo[commit.tree]

            # Navigate to file
            path_parts = file_path.split("/")
            current_tree = tree

            for part in path_parts[:-1]:
                if part not in [name.decode() for name, _, _ in current_tree.iteritems()]:
                    return None
                mode, obj_id = current_tree[part.encode()]
                current_tree = self.repo[obj_id]

            # Get file
            file_name = path_parts[-1].encode()
            if file_name not in current_tree:
                return None

            mode, obj_id = current_tree[file_name]
            obj = self.repo[obj_id]

            if isinstance(obj, Blob):
                return obj.data

            return None
        except Exception:
            return None

    def file_exists_at_commit(self, file_path: str, commit_hash: str) -> bool:
        """Check if file exists at a specific commit."""
        return self.read_file_at_commit(file_path, commit_hash) is not None

    def list_files_at_commit(
        self,
        commit_hash: str,
        path_prefix: str = "",
    ) -> list[str]:
        """List all files at a specific commit.

        Args:
            commit_hash: Commit hash
            path_prefix: Optional path prefix to filter

        Returns:
            List of file paths
        """
        try:
            commit = self.repo[commit_hash.encode()]
            tree = self.repo[commit.tree]

            files = []

            def walk_tree(tree_obj: Tree, prefix: str = "") -> None:
                for name, mode, obj_id in tree_obj.iteritems():
                    name_str = name.decode()
                    full_path = prefix + name_str

                    if stat.S_ISDIR(mode):
                        walk_tree(self.repo[obj_id], full_path + "/")
                    else:
                        if not path_prefix or full_path.startswith(path_prefix):
                            files.append(full_path)

            walk_tree(tree)
            return files
        except Exception as e:
            raise GitEngineError(f"Failed to list files at commit: {e}")

    # -------------------------------------------------------------------------
    # Utility methods
    # -------------------------------------------------------------------------

    def get_head_commit(self) -> Optional[str]:
        """Get current HEAD commit hash."""
        try:
            return self.repo.head().decode()
        except Exception:
            return None

    def get_diff(
        self,
        commit_hash: str,
        file_path: Optional[str] = None,
    ) -> str:
        """Get diff between commit and its parent.

        Args:
            commit_hash: Commit hash
            file_path: Optional file to limit diff to

        Returns:
            Diff as string
        """
        try:
            commit = self.repo[commit_hash.encode()]

            if not commit.parents:
                return ""

            parent = self.repo[commit.parents[0]]

            # Get diff
            changes = dulwich.diff_tree.tree_changes(
                self.repo.object_store,
                parent.tree,
                commit.tree,
            )

            diff_lines = []
            for old_path, new_path, old_mode, new_mode, old_sha, new_sha in changes:
                if file_path and new_path.decode() != file_path:
                    continue

                if old_sha and new_sha:
                    old_blob = self.repo[old_sha]
                    new_blob = self.repo[new_sha]
                    diff = dulwich.patch.diff_blobs(
                        old_blob.data,
                        new_blob.data,
                        old_path.decode(),
                        new_path.decode(),
                    )
                    diff_lines.append(diff.decode())
                elif new_sha:
                    new_blob = self.repo[new_sha]
                    diff_lines.append(f"+++ {new_path.decode()}\n{new_blob.data.decode()}")
                elif old_sha:
                    old_blob = self.repo[old_sha]
                    diff_lines.append(f"--- {old_path.decode()}\n{old_blob.data.decode()}")

            return "\n".join(diff_lines)
        except Exception as e:
            raise GitEngineError(f"Failed to get diff: {e}")


def create_git_engine(
    workspace_path: Path,
    credentials: Optional[GitCredentials] = None,
) -> GitEngine:
    """Factory function to create GitEngine instance."""
    return GitEngine(workspace_path, credentials)