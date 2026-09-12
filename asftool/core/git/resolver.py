"""Multi-repository mapping resolver for CRMA assets.

Parses .asftool-git.yml configuration and routes CRMA assets
(dashboards, recipes, dataflows, lenses, datasets) to target Git repositories
based on folder/app names, asset types, or wildcard rules.
"""

import os
import re
from pathlib import Path
from typing import Any, Optional

import yaml
from pydantic import BaseModel, Field, field_validator

from asftool.core.models.git_auth import GitProvider, GitRepositoryTarget


class RepoMappingRule(BaseModel):
    """Single mapping rule for routing assets to repositories.

    Rules are evaluated in order; first match wins.
    Supports matching by:
    - CRMA folder name (exact or glob pattern)
    - CRMA app name (exact or glob pattern)
    - Asset type (dashboard, recipe, dataflow, lens, dataset, xmd)
    - Asset name (exact or glob pattern)
    - Wildcard (*) for catch-all
    """

    model_config = {"extra": "forbid"}

    # Match criteria (all must match for rule to apply)
    folder: Optional[str] = Field(
        None,
        description="CRMA folder name pattern (supports * and ? glob)",
    )
    app: Optional[str] = Field(
        None,
        description="CRMA app name pattern (supports * and ? glob)",
    )
    asset_type: Optional[str] = Field(
        None,
        description="Asset type pattern: dashboard, recipe, dataflow, lens, dataset, xmd",
    )
    name: Optional[str] = Field(
        None,
        description="Asset name pattern (supports * and ? glob)",
    )

    # Target repository
    target: GitRepositoryTarget = Field(
        ...,
        description="Target Git repository configuration",
    )

    # Optional: override path prefix within repo
    path_prefix: Optional[str] = Field(
        None,
        description="Override target.path_prefix for this rule",
    )

    @field_validator("asset_type")
    @classmethod
    def validate_asset_type(cls, v: Optional[str]) -> Optional[str]:
        if v is not None:
            valid_types = {"dashboard", "recipe", "dataflow", "lens", "dataset", "xmd"}
            if v not in valid_types and "*" not in v and "?" not in v:
                raise ValueError(
                    f"asset_type must be one of {valid_types} or glob pattern"
                )
        return v

    def matches(
        self,
        folder: Optional[str],
        app: Optional[str],
        asset_type: str,
        name: str,
    ) -> bool:
        """Check if this rule matches the given asset context."""
        if self.folder and not self._glob_match(self.folder, folder or ""):
            return False
        if self.app and not self._glob_match(self.app, app or ""):
            return False
        if self.asset_type and not self._glob_match(self.asset_type, asset_type):
            return False
        if self.name and not self._glob_match(self.name, name):
            return False
        return True

    @staticmethod
    def _glob_match(pattern: str, text: str) -> bool:
        """Match glob pattern (supports * and ?) against text."""
        # Convert glob to regex
        regex = re.escape(pattern)
        regex = regex.replace(r"\*", ".*").replace(r"\?", ".")
        return re.fullmatch(regex, text, re.IGNORECASE) is not None


class RepoMappingConfig(BaseModel):
    """Complete .asftool-git.yml configuration."""

    model_config = {"extra": "forbid"}

    version: str = Field(default="1", description="Config schema version")
    default_target: Optional[GitRepositoryTarget] = Field(
        None,
        description="Default target for unmatched assets",
    )
    rules: list[RepoMappingRule] = Field(
        default_factory=list,
        description="Ordered list of mapping rules (first match wins)",
    )

    @classmethod
    def from_file(cls, path: Path) -> "RepoMappingConfig":
        """Load configuration from YAML file."""
        if not path.exists():
            raise FileNotFoundError(f"Config file not found: {path}")

        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}

        return cls(**data)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "RepoMappingConfig":
        """Create configuration from dictionary."""
        return cls(**data)

    def to_file(self, path: Path) -> None:
        """Write configuration to YAML file."""
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            yaml.safe_dump(
                self.model_dump(mode="json", exclude_none=True),
                f,
                sort_keys=False,
                allow_unicode=True,
            )


class AssetContext(BaseModel):
    """Context for an asset being routed."""

    model_config = {"extra": "forbid"}

    asset_id: str
    asset_type: str  # dashboard, recipe, dataflow, lens, dataset, xmd
    name: str
    folder: Optional[str] = None
    app: Optional[str] = None
    project: Optional[str] = None  # For future use


class RepoMappingResolver:
    """Resolves CRMA assets to target Git repositories.

    Loads .asftool-git.yml and provides deterministic routing based on
    folder/app names, asset types, and wildcard rules.
    """

    DEFAULT_CONFIG_NAME = ".asftool-git.yml"

    def __init__(self, config: Optional[RepoMappingConfig] = None):
        """Initialize with optional pre-loaded config."""
        self._config = config
        self._config_path: Optional[Path] = None

    @classmethod
    def from_file(cls, path: Path) -> "RepoMappingResolver":
        """Create resolver from config file."""
        config = RepoMappingConfig.from_file(path)
        resolver = cls(config)
        resolver._config_path = path
        return resolver

    @classmethod
    def from_default_location(cls, search_path: Optional[Path] = None) -> "RepoMappingResolver":
        """Create resolver by finding config in standard locations.

        Search order:
        1. Explicit search_path if provided
        2. Current working directory
        3. User home directory (~/.asftool-git.yml)
        4. XDG config directory (~/.config/asftool/.asftool-git.yml)
        """
        search_paths = []

        if search_path:
            search_paths.append(search_path)

        # CWD
        search_paths.append(Path.cwd() / cls.DEFAULT_CONFIG_NAME)

        # Home directory
        search_paths.append(Path.home() / cls.DEFAULT_CONFIG_NAME)

        # XDG config
        xdg_config = os.environ.get("XDG_CONFIG_HOME", Path.home() / ".config")
        search_paths.append(Path(xdg_config) / "asftool" / cls.DEFAULT_CONFIG_NAME)

        for path in search_paths:
            if path.exists():
                return cls.from_file(path)

        # Return empty resolver (will use defaults)
        return cls()

    @property
    def config(self) -> Optional[RepoMappingConfig]:
        """Get current configuration."""
        return self._config

    @property
    def config_path(self) -> Optional[Path]:
        """Get path to loaded config file."""
        return self._config_path

    def resolve_repository(self, asset: AssetContext) -> GitRepositoryTarget:
        """Resolve target repository for an asset.

        Args:
            asset: AssetContext with folder, app, asset_type, name

        Returns:
            GitRepositoryTarget for the asset

        Raises:
            ValueError: If no matching rule and no default target
        """
        if not self._config:
            raise ValueError("No configuration loaded")

        # Check rules in order
        for rule in self._config.rules:
            if rule.matches(asset.folder, asset.app, asset.asset_type, asset.name):
                target = rule.target.model_copy()
                if rule.path_prefix is not None:
                    target.path_prefix = rule.path_prefix
                return target

        # Fall back to default target
        if self._config.default_target:
            return self._config.default_target

        raise ValueError(
            f"No mapping rule matches asset: folder={asset.folder}, "
            f"app={asset.app}, type={asset.asset_type}, name={asset.name}. "
            f"Add a rule or set default_target in {self._config_path or 'config'}"
        )

    def get_all_targets(self) -> list[GitRepositoryTarget]:
        """Get all unique target repositories referenced in config."""
        if not self._config:
            return []

        targets = []
        seen = set()

        if self._config.default_target:
            key = self._target_key(self._config.default_target)
            if key not in seen:
                seen.add(key)
                targets.append(self._config.default_target)

        for rule in self._config.rules:
            key = self._target_key(rule.target)
            if key not in seen:
                seen.add(key)
                targets.append(rule.target)

        return targets

    @staticmethod
    def _target_key(target: GitRepositoryTarget) -> str:
        """Generate unique key for a target."""
        return f"{target.provider.value}:{target.host}:{target.organization}:{target.repository}:{target.project or ''}"

    def validate_config(self) -> list[str]:
        """Validate configuration for common issues.

        Returns:
            List of warning/error messages (empty if valid)
        """
        issues = []

        if not self._config:
            issues.append("No configuration loaded")
            return issues

        # Check for duplicate targets with different credentials
        targets_by_key = {}
        for rule in self._config.rules:
            key = self._target_key(rule.target)
            if key in targets_by_key:
                if targets_by_key[key].credentials_alias != rule.target.credentials_alias:
                    issues.append(
                        f"Warning: Target {key} uses different credentials aliases: "
                        f"'{targets_by_key[key].credentials_alias}' vs "
                        f"'{rule.target.credentials_alias}'"
                    )
            else:
                targets_by_key[key] = rule.target

        if self._config.default_target:
            key = self._target_key(self._config.default_target)
            if key in targets_by_key:
                if targets_by_key[key].credentials_alias != self._config.default_target.credentials_alias:
                    issues.append(
                        f"Warning: Default target uses different credentials alias than rule target: "
                        f"'{targets_by_key[key].credentials_alias}' vs "
                        f"'{self._config.default_target.credentials_alias}'"
                    )

        # Check for unreachable rules (shadowed by earlier rules)
        # This is a heuristic - we can't know all possible asset contexts
        for i, rule in enumerate(self._config.rules):
            if i > 0:
                # Check if this rule is completely shadowed by previous rules
                # We can't fully determine this without knowing all possible assets
                pass

        return issues


def create_default_config(
    provider: GitProvider,
    host: str,
    organization: str,
    repository: str,
    credentials_alias: str,
    project: Optional[str] = None,
    branch: str = "main",
    path_prefix: str = "",
) -> RepoMappingConfig:
    """Create a default single-repo configuration."""
    target = GitRepositoryTarget(
        provider=provider,
        host=host,
        organization=organization,
        repository=repository,
        project=project,
        branch=branch,
        credentials_alias=credentials_alias,
        path_prefix=path_prefix,
    )

    return RepoMappingConfig(
        version="1",
        default_target=target,
        rules=[],
    )


def create_multi_repo_config(
    rules: list[RepoMappingRule],
    default_target: Optional[GitRepositoryTarget] = None,
) -> RepoMappingConfig:
    """Create a multi-repo configuration with explicit rules."""
    return RepoMappingConfig(
        version="1",
        default_target=default_target,
        rules=rules,
    )