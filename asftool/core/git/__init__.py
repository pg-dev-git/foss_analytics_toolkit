"""Git synchronization core module for ASFTool.

This module contains all Git-related business logic, completely
decoupled from CLI presentation layer.

Components:
- resolver: Multi-repo mapping configuration and asset routing
- normalizer: CRMA JSON normalization for clean diffs
- workspace: Isolated workspace directory management
- engine: Pure-Python Git operations (clone, fetch, commit, push)
- sync: Core sync and revert service layer
"""

from asftool.core.git.normalizer import CRMANormalizer, NormalizerConfig, create_normalizer
from asftool.core.git.resolver import (
    AssetContext,
    RepoMappingConfig,
    RepoMappingResolver,
    RepoMappingRule,
    create_default_config,
    create_multi_repo_config,
)

__all__ = [
    # Resolver
    "RepoMappingResolver",
    "RepoMappingConfig",
    "RepoMappingRule",
    "AssetContext",
    "create_default_config",
    "create_multi_repo_config",
    # Normalizer
    "CRMANormalizer",
    "NormalizerConfig",
    "create_normalizer",
]