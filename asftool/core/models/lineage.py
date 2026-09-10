"""Pydantic graph models for visual data lineage and dependency mapping."""

from __future__ import annotations

from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class AssetType(StrEnum):
    DATASET = "dataset"
    RECIPE = "recipe"
    DATAFLOW = "dataflow"
    DASHBOARD = "dashboard"
    LENS = "lens"


class LineageNode(BaseModel):
    """A single asset node in the lineage graph."""

    id: str
    name: str
    asset_type: AssetType
    label: str | None = None
    url: str | None = None  # Salesforce deep-link
    metadata: dict[str, Any] = Field(default_factory=dict)

    model_config = ConfigDict(extra="allow", use_enum_values=True)


class LineageEdge(BaseModel):
    """Directed dependency edge between two assets."""

    source: str
    target: str
    relation: str = "depends_on"  # e.g., dataset_dependency, field_match
    label: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)

    model_config = ConfigDict(extra="allow")


class LineageGraph(BaseModel):
    """Complete lineage graph with nodes and edges."""

    nodes: list[LineageNode] = Field(default_factory=list)
    edges: list[LineageEdge] = Field(default_factory=list)
    root_asset_id: str | None = None

    model_config = ConfigDict(extra="allow")

    def add_node(self, node: LineageNode) -> None:
        if not any(n.id == node.id for n in self.nodes):
            self.nodes.append(node)

    def add_edge(self, edge: LineageEdge) -> None:
        self.edges.append(edge)

    def get_downstream(self, asset_id: str) -> list[LineageNode]:
        visited: set[str] = set()
        result: list[LineageNode] = []

        def dfs(nid: str) -> None:
            if nid in visited:
                return
            visited.add(nid)
            for edge in self.edges:
                if edge.source == nid:
                    for n in self.nodes:
                        if n.id == edge.target and n.id != nid:
                            result.append(n)
                            dfs(n.id)

        dfs(asset_id)
        # Deduplicate while preserving order
        seen: set[str] = set()
        unique: list[LineageNode] = []
        for n in result:
            if n.id not in seen:
                seen.add(n.id)
                unique.append(n)
        return unique

    def get_upstream(self, asset_id: str) -> list[LineageNode]:
        visited: set[str] = set()
        result: list[LineageNode] = []

        def dfs(nid: str) -> None:
            if nid in visited:
                return
            visited.add(nid)
            for edge in self.edges:
                if edge.target == nid:
                    for n in self.nodes:
                        if n.id == edge.source and n.id != nid:
                            result.append(n)
                            dfs(n.id)

        dfs(asset_id)
        seen: set[str] = set()
        unique: list[LineageNode] = []
        for n in result:
            if n.id not in seen:
                seen.add(n.id)
                unique.append(n)
        return unique
