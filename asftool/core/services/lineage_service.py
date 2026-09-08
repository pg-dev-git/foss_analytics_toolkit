"""Lineage service for dependency extraction, graph construction, and rendering."""

from __future__ import annotations

import json
from typing import Any

import graphviz

from asftool.core.models.lineage import (
    AssetType,
    LineageEdge,
    LineageGraph,
    LineageNode,
)


class LineageService:
    """Service that constructs LineageGraph objects and renders them."""

    def __init__(self, client: Any) -> None:
        self.client = client

    async def build_graph(self, root_asset_id: str) -> LineageGraph:
        """Recursively traverse TCRM dependency API and build a LineageGraph."""
        graph = LineageGraph(root_asset_id=root_asset_id)
        visited: set[str] = set()

        async def traverse(asset_id: str, parent_path: list[str]) -> None:
            if asset_id in visited:
                return
            visited.add(asset_id)

            try:
                data = await self.client.get_dependencies(asset_id)
            except Exception:
                # If dependency API fails, treat as leaf
                return

            # Extract nodes from response
            for item in data.get("dependencies", []) if isinstance(data, dict) else []:
                node_id = item.get("id", item.get("assetId"))
                name = item.get("name", item.get("label", node_id))
                asset_type_str = item.get("assetType", item.get("type", "dataset"))
                try:
                    atype = AssetType(asset_type_str)
                except ValueError:
                    atype = AssetType.DATASET
                node = LineageNode(
                    id=str(node_id),
                    name=str(name),
                    asset_type=atype,
                    url=item.get("url") or item.get("deepLink"),
                    metadata=item.get("metadata", {}),
                )
                graph.add_node(node)
                graph.add_edge(
                    LineageEdge(
                        source=str(asset_id),
                        target=str(node_id),
                        relation=item.get("relation", "depends_on"),
                        label=item.get("label"),
                        metadata=item.get("edgeMetadata", {}),
                    )
                )
                await traverse(str(node_id), parent_path + [str(asset_id)])

        await traverse(root_asset_id, [])
        return graph

    def render_svg(self, graph: LineageGraph, output_path: str) -> str:
        """Generate a clean SVG vector file using Graphviz."""
        dot = graphviz.Digraph(format="svg", engine="dot")
        dot.attr(rankdir="TB", bgcolor="white", fontname="Helvetica")
        dot.attr("node", fontname="Helvetica", fontsize="10")

        # Node styling by asset type
        shapes_colors = {
            AssetType.DATASET: ("cylinder", "#e1f5fe"),
            AssetType.RECIPE: ("ellipse", "#fff3e0"),
            AssetType.DATAFLOW: ("box3d", "#e8f5e9"),
            AssetType.DASHBOARD: ("rect", "#ede7f6"),
            AssetType.LENS: ("diamond", "#fce4ec"),
        }

        for node in graph.nodes:
            shape, fill = shapes_colors.get(node.asset_type, ("ellipse", "#f5f5f5"))
            dot.node(
                node.id,
                label=f"{node.name}\n({node.asset_type.value})",
                shape=shape,
                style="filled",
                fillcolor=fill,
                color="#333333",
                url=node.url or "",
            )

        for edge in graph.edges:
            dot.edge(
                edge.source,
                edge.target,
                label=edge.label or edge.relation,
            )

        dot.render(output_path, cleanup=False)
        # graphviz returns filename with .svg appended; return actual path
        return output_path if output_path.endswith(".svg") else f"{output_path}.svg"

    def render_mermaid(self, graph: LineageGraph) -> str:
        """Produce raw Mermaid flowchart string (.mmd)."""
        lines = ["flowchart TD"]
        for node in graph.nodes:
            safe_id = node.id.replace("-", "_").replace("/", "_")
            lines.append(f"    {safe_id}[\"{node.name}\\n({node.asset_type.value})\"]")
        for edge in graph.edges:
            src = edge.source.replace("-", "_").replace("/", "_")
            tgt = edge.target.replace("-", "_").replace("/", "_")
            label = (edge.label or edge.relation).replace('"', '\\"')
            lines.append(f"    {src} -->|\"{label}\"| {tgt}")
        return "\n".join(lines)

    def to_node_edge_json(self, graph: LineageGraph) -> dict[str, Any]:
        """Standardized frontend graph payload."""
        return {
            "nodes": [
                {
                    "id": n.id,
                    "label": n.name,
                    "asset_type": n.asset_type.value,
                    "url": n.url,
                    "metadata": n.metadata,
                }
                for n in graph.nodes
            ],
            "edges": [
                {
                    "source": e.source,
                    "target": e.target,
                    "relation": e.relation,
                    "label": e.label,
                    "metadata": e.metadata,
                }
                for e in graph.edges
            ],
            "root_asset_id": graph.root_asset_id,
        }
