"""Pydantic models for Field Impact Analysis Crawler."""

from datetime import datetime
from enum import StrEnum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


class MatchType(StrEnum):
    """Type of field match."""
    EXACT = "exact"
    FUZZY = "fuzzy"


class AssetType(StrEnum):
    """Type of ASFT asset."""
    DATASET = "dataset"
    DASHBOARD = "dashboard"
    DATAFLOW = "dataflow"
    REPLICATED_DATASET = "replicated_dataset"


class MatchMode(StrEnum):
    """Matching mode for field search."""
    EXACT = "exact"
    FUZZY = "fuzzy"
    BOTH = "both"


# =============================================================================
# Asset Dependency Graph Models
# =============================================================================

class AssetDependency(BaseModel):
    """Node in the asset dependency graph."""
    id: str
    name: str
    type: AssetType
    label: str | None = None
    parent_id: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)

    model_config = ConfigDict(extra="allow", use_enum_values=True)


class AssetDependencyGraph(BaseModel):
    """Dependency graph structure with traversal methods."""
    nodes: dict[str, AssetDependency] = Field(default_factory=dict)
    edges: dict[str, list[str]] = Field(default_factory=dict)  # parent_id -> [child_ids]

    model_config = ConfigDict(extra="allow")

    def add_node(self, node: AssetDependency) -> None:
        """Add a node to the graph."""
        self.nodes[node.id] = node

    def add_edge(self, parent_id: str, child_id: str) -> None:
        """Add a directed edge parent -> child."""
        if parent_id not in self.edges:
            self.edges[parent_id] = []
        if child_id not in self.edges[parent_id]:
            self.edges[parent_id].append(child_id)

    def get_children(self, node_id: str) -> list[AssetDependency]:
        """Get direct children of a node."""
        child_ids = self.edges.get(node_id, [])
        return [self.nodes[cid] for cid in child_ids if cid in self.nodes]

    def get_parents(self, node_id: str) -> list[AssetDependency]:
        """Get direct parents of a node (reverse lookup)."""
        parents = []
        for pid, children in self.edges.items():
            if node_id in children and pid in self.nodes:
                parents.append(self.nodes[pid])
        return parents

    def get_all_downstream(self, node_id: str) -> list[AssetDependency]:
        """Get all downstream nodes (recursive)."""
        visited = set()
        result = []

        def dfs(nid: str) -> None:
            if nid in visited:
                return
            visited.add(nid)
            for child in self.get_children(nid):
                result.append(child)
                dfs(child.id)

        dfs(node_id)
        return result

    def get_all_upstream(self, node_id: str) -> list[AssetDependency]:
        """Get all upstream nodes (recursive)."""
        visited = set()
        result = []

        def dfs(nid: str) -> None:
            if nid in visited:
                return
            visited.add(nid)
            for parent in self.get_parents(nid):
                result.append(parent)
                dfs(parent.id)

        dfs(node_id)
        return result

    def filter_by_type(self, asset_type: AssetType) -> list[AssetDependency]:
        """Filter nodes by asset type."""
        return [n for n in self.nodes.values() if n.type == asset_type]


# =============================================================================
# Dataset Field Analysis Models
# =============================================================================

class DatasetFieldReference(BaseModel):
    """Field reference found in a dataset."""
    field_api_name: str
    field_label: str | None = None
    field_type: str | None = None
    dataset_id: str
    dataset_name: str
    version_id: str | None = None
    match_type: MatchType
    match_score: int = Field(ge=0, le=100)
    source: Literal["measure", "dimension", "date"] = "dimension"

    model_config = ConfigDict(extra="allow", use_enum_values=True)


class DatasetFieldAnalysisResult(BaseModel):
    """Aggregated field analysis result for a dataset."""
    dataset_id: str
    dataset_name: str
    dataset_label: str | None = None
    version_id: str | None = None
    matches: list[DatasetFieldReference] = Field(default_factory=list)
    total_fields_scanned: int = 0
    scan_duration_ms: int = 0

    model_config = ConfigDict(extra="allow")

    @property
    def match_count(self) -> int:
        return len(self.matches)

    @property
    def exact_match_count(self) -> int:
        return sum(1 for m in self.matches if m.match_type == MatchType.EXACT)

    @property
    def fuzzy_match_count(self) -> int:
        return sum(1 for m in self.matches if m.match_type == MatchType.FUZZY)

    @property
    def display_name(self) -> str:
        """Return a user-friendly display name with cascading fallback."""
        return (
            self.dataset_label
            or self.dataset_name
            or self.dataset_id
            or "Unknown Dataset"
        )


# =============================================================================
# Dashboard Field Analysis Models
# =============================================================================

class DashboardFieldReference(BaseModel):
    """Field reference found in a dashboard widget/step."""
    widget_id: str
    widget_type: str | None = None
    step_id: str | None = None
    field_path: str  # e.g., "step.query.measures[0].field"
    field_api_name: str | None = None
    field_label: str | None = None
    dashboard_id: str
    dashboard_name: str
    match_type: MatchType
    match_score: int = Field(ge=0, le=100)

    model_config = ConfigDict(extra="allow", use_enum_values=True)


class DashboardFieldAnalysisResult(BaseModel):
    """Aggregated field analysis result for a dashboard."""
    dashboard_id: str
    dashboard_name: str
    dashboard_label: str | None = None
    matches: list[DashboardFieldReference] = Field(default_factory=list)
    total_widgets_scanned: int = 0
    total_steps_scanned: int = 0
    scan_duration_ms: int = 0

    model_config = ConfigDict(extra="allow")

    @property
    def match_count(self) -> int:
        return len(self.matches)

    @property
    def exact_match_count(self) -> int:
        return sum(1 for m in self.matches if m.match_type == MatchType.EXACT)

    @property
    def fuzzy_match_count(self) -> int:
        return sum(1 for m in self.matches if m.match_type == MatchType.FUZZY)

    @property
    def display_name(self) -> str:
        """Return a user-friendly display name with cascading fallback."""
        return (
            self.dashboard_label
            or self.dashboard_name
            or self.dashboard_id
            or "Unknown Dashboard"
        )


# =============================================================================
# Dataflow Field Analysis Models
# =============================================================================

class DataflowFieldReference(BaseModel):
    """Field reference found in a dataflow node/transform."""
    node_id: str
    node_type: str | None = None  # e.g., "sfdcDigest", "transform", "append"
    field_name: str
    field_context: str | None = None  # e.g., "sourceField", "targetField", "expression"
    dataflow_id: str
    dataflow_name: str
    match_type: MatchType
    match_score: int = Field(ge=0, le=100)

    model_config = ConfigDict(extra="allow", use_enum_values=True)


class DataflowFieldAnalysisResult(BaseModel):
    """Aggregated field analysis result for a dataflow."""
    dataflow_id: str
    dataflow_name: str
    dataflow_label: str | None = None
    matches: list[DataflowFieldReference] = Field(default_factory=list)
    total_nodes_scanned: int = 0
    scan_duration_ms: int = 0

    model_config = ConfigDict(extra="allow")

    @property
    def match_count(self) -> int:
        return len(self.matches)

    @property
    def exact_match_count(self) -> int:
        return sum(1 for m in self.matches if m.match_type == MatchType.EXACT)

    @property
    def fuzzy_match_count(self) -> int:
        return sum(1 for m in self.matches if m.match_type == MatchType.FUZZY)

    @property
    def display_name(self) -> str:
        """Return a user-friendly display name with cascading fallback."""
        return (
            self.dataflow_label
            or self.dataflow_name
            or self.dataflow_id
            or "Unknown Dataflow"
        )


# =============================================================================
# Replicated Dataset Field Analysis Models
# =============================================================================

class ReplicatedDatasetField(BaseModel):
    """Field in a replicated dataset (connected object)."""
    field_api_name: str
    field_label: str | None = None
    field_type: str | None = None
    is_nillable: bool | None = None
    is_unique: bool | None = None
    replicated_dataset_id: str
    object_name: str
    match_type: MatchType
    match_score: int = Field(ge=0, le=100)

    model_config = ConfigDict(extra="allow", use_enum_values=True)


class ReplicatedDatasetFieldAnalysisResult(BaseModel):
    """Aggregated field analysis result for a replicated dataset."""
    replicated_dataset_id: str
    object_name: str
    object_label: str | None = None
    matches: list[ReplicatedDatasetField] = Field(default_factory=list)
    total_fields_scanned: int = 0
    scan_duration_ms: int = 0

    model_config = ConfigDict(extra="allow")

    @property
    def match_count(self) -> int:
        return len(self.matches)

    @property
    def exact_match_count(self) -> int:
        return sum(1 for m in self.matches if m.match_type == MatchType.EXACT)

    @property
    def fuzzy_match_count(self) -> int:
        return sum(1 for m in self.matches if m.match_type == MatchType.FUZZY)

    @property
    def display_name(self) -> str:
        """Return a user-friendly display name with cascading fallback."""
        return (
            self.object_label
            or self.object_name
            or self.replicated_dataset_id
            or "Unknown Replicated Dataset"
        )


# =============================================================================
# Final Impact Report Models
# =============================================================================

class FieldImpactScope(BaseModel):
    """Input parameters defining the analysis scope."""
    search_term: str
    match_mode: MatchMode = MatchMode.BOTH
    fuzzy_threshold: int = Field(default=85, ge=0, le=100)
    include_datasets: bool = True
    include_dashboards: bool = True
    include_dataflows: bool = True
    include_replicated: bool = True
    application_id: str | None = None  # Limit to app's dependency graph
    asset_ids: dict[AssetType, list[str]] | None = None  # Explicit asset ID filters

    model_config = ConfigDict(extra="allow", use_enum_values=True)


class FieldImpactSummary(BaseModel):
    """High-level summary of impact analysis."""
    total_assets_scanned: int = 0
    total_matches: int = 0
    exact_matches: int = 0
    fuzzy_matches: int = 0
    by_asset_type: dict[AssetType, int] = Field(default_factory=dict)
    datasets_scanned: int = 0
    dashboards_scanned: int = 0
    dataflows_scanned: int = 0
    replicated_datasets_scanned: int = 0

    model_config = ConfigDict(extra="allow", use_enum_values=True)


class FieldImpactDetail(BaseModel):
    """Detailed findings grouped by asset type."""
    datasets: list[DatasetFieldAnalysisResult] = Field(default_factory=list)
    dashboards: list[DashboardFieldAnalysisResult] = Field(default_factory=list)
    dataflows: list[DataflowFieldAnalysisResult] = Field(default_factory=list)
    replicated_datasets: list[ReplicatedDatasetFieldAnalysisResult] = Field(default_factory=list)

    model_config = ConfigDict(extra="allow")


class FieldImpactReport(BaseModel):
    """Complete field impact analysis report."""
    scope: FieldImpactScope
    summary: FieldImpactSummary
    details: FieldImpactDetail
    generated_at: datetime = Field(default_factory=datetime.utcnow)
    execution_time_ms: int = 0
    errors: list[str] = Field(default_factory=list)  # Non-fatal errors during scan

    model_config = ConfigDict(extra="allow", use_enum_values=True)

    def to_summary_dict(self) -> dict[str, Any]:
        """Return a condensed dict for quick display."""
        match_mode = self.scope.match_mode
        if isinstance(match_mode, MatchMode):
            match_mode_str = match_mode.value
        else:
            match_mode_str = str(match_mode)

        by_asset_type = {}
        for k, v in self.summary.by_asset_type.items():
            key = k.value if isinstance(k, AssetType) else str(k)
            by_asset_type[key] = v

        return {
            "search_term": self.scope.search_term,
            "match_mode": match_mode_str,
            "total_assets_scanned": self.summary.total_assets_scanned,
            "total_matches": self.summary.total_matches,
            "exact_matches": self.summary.exact_matches,
            "fuzzy_matches": self.summary.fuzzy_matches,
            "by_asset_type": by_asset_type,
            "execution_time_ms": self.execution_time_ms,
            "generated_at": self.generated_at.isoformat(),
            "error_count": len(self.errors),
        }
