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

    def build_graph_from_impact(self, impact_json_path: str) -> LineageGraph:
        """Build a LineageGraph from a FieldImpactReport JSON file.

        This connects the lineage module to the Field Impact Analysis output:
        nodes are the source datasets (where a field originates) and consuming
        assets (dashboards, dataflows); edges show field-match relationships.
        """
        from pathlib import Path

        path = Path(impact_json_path)
        if not path.exists():
            raise FileNotFoundError(f"Impact file not found: {impact_json_path}")

        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            raise ValueError(f"Invalid JSON in impact file: {exc}") from exc

        scope = data.get("scope", {})
        search_term = scope.get("search_term", "unknown")
        graph = LineageGraph(root_asset_id=search_term)

        details = data.get("details", {})
        if not details:
            return graph

        # Dataset results
        dataset_results = details.get("datasets", [])
        for result in dataset_results:
            ds_id = result.get("dataset_id")
            ds_name = result.get("dataset_name") or result.get("name", "Unknown")
            if ds_id:
                node = LineageNode(
                    id=str(ds_id),
                    name=str(ds_name),
                    asset_type=AssetType.DATASET,
                )
                if not any(n.id == node.id for n in graph.nodes):
                    graph.add_node(node)
            for match in result.get("matches", []):
                match_target = match.get("dataset_id") or ds_id
                if match_target:
                    graph.add_edge(
                        LineageEdge(
                            source=str(match_target) if match_target else "unknown",
                            target=str(ds_id) if ds_id else str(match_target),
                            relation=f"field_{match.get('match_type', 'unknown')}",
                            label=f"Field: {match.get('field_api_name', 'N/A')}",
                            metadata={
                                "match_type": match.get("match_type"),
                                "match_score": match.get("match_score"),
                            },
                        )
                    )

        # Dashboard results (consumers)
        for result in details.get("dashboards", []):
            db_id = result.get("dashboard_id")
            db_name = result.get("dashboard_name") or result.get("name", "Dashboard")
            if db_id:
                node = LineageNode(
                    id=str(db_id),
                    name=str(db_name),
                    asset_type=AssetType.DASHBOARD,
                )
                if not any(n.id == node.id for n in graph.nodes):
                    graph.add_node(node)
            for match in result.get("matches", []):
                ds_ref = match.get("dataset_id") or match.get("dataset_name")
                if ds_ref and db_id:
                    if not any(n.id == str(ds_ref) for n in graph.nodes):
                        source_node = LineageNode(
                            id=str(ds_ref),
                            name=str(match.get("dataset_name", ds_ref)),
                            asset_type=AssetType.DATASET,
                        )
                        graph.add_node(source_node)
                    graph.add_edge(
                        LineageEdge(
                            source=str(ds_ref),
                            target=str(db_id),
                            relation="field_usage",
                            label=f"Field: {match.get('field_api_name', 'N/A')}",
                            metadata={
                                "widget_type": match.get("widget_type"),
                                "match_type": match.get("match_type"),
                            },
                        )
                    )

        # Dataflow results
        for result in details.get("dataflows", []):
            df_id = result.get("dataflow_id")
            df_name = result.get("dataflow_name") or result.get("name", "Dataflow")
            if df_id:
                node = LineageNode(
                    id=str(df_id),
                    name=str(df_name),
                    asset_type=AssetType.DATAFLOW,
                )
                if not any(n.id == node.id for n in graph.nodes):
                    graph.add_node(node)
            for match in result.get("matches", []):
                source_ref = match.get("dataset_id") or match.get("node_id")
                if source_ref and df_id:
                    if not any(n.id == str(source_ref) for n in graph.nodes):
                        source_name = str(match.get("dataset_name", match.get("field_name", source_ref)))
                        source_node = LineageNode(
                            id=str(source_ref),
                            name=source_name,
                            asset_type=AssetType.RECIPE,
                        )
                        graph.add_node(source_node)
                    graph.add_edge(
                        LineageEdge(
                            source=str(source_ref),
                            target=str(df_id),
                            relation="recipe_dependency",
                            label=f"Field: {match.get('field_name', 'N/A')}",
                            metadata={
                                "node_type": match.get("node_type"),
                                "match_type": match.get("match_type"),
                                "field_context": match.get("field_context"),
                            },
                        )
                    )

        return graph

    def render_svg(self, graph: LineageGraph, output_path: str) -> str:
        """Generate a clean SVG vector file using Graphviz with improved layout."""
        dot = graphviz.Digraph(format="svg", engine="dot")
        
        # Graph-level attributes for better layout
        dot.attr(
            rankdir="TB",
            bgcolor="white",
            fontname="Helvetica",
            fontsize="14",
            # Better spacing between ranks
            ranksep="1.5",
            nodesep="0.8",
            # Allow edges to cross fewer nodes
            splines="ortho",
            # Prevent overlapping
            overlap="scale",
            # Margin around the graph
            margin="0.5",
            # Pad to prevent clipping
            pad="0.5",
        )
        
        # Node attributes
        dot.attr("node", 
            fontname="Helvetica",
            fontsize="11",
            fontcolor="#333333",
            margin="0.15,0.08",
            penwidth="1.2",
        )
        
        # Edge attributes
        dot.attr("edge",
            fontname="Helvetica",
            fontsize="9",
            fontcolor="#555555",
            color="#666666",
            penwidth="1.0",
            arrowsize="0.8",
        )

        # Node styling by asset type
        shapes_colors = {
            AssetType.DATASET: ("cylinder", "#e1f5fe", "#01579b"),
            AssetType.RECIPE: ("ellipse", "#fff3e0", "#e65100"),
            AssetType.DATAFLOW: ("box3d", "#e8f5e9", "#1b5e20"),
            AssetType.DASHBOARD: ("rect", "#ede7f6", "#4a148c"),
            AssetType.LENS: ("diamond", "#fce4ec", "#880e4f"),
        }

        # Group nodes by type for subgraph clustering
        nodes_by_type: dict[AssetType, list[LineageNode]] = {}
        for node in graph.nodes:
            nodes_by_type.setdefault(node.asset_type, []).append(node)

        # Create subgraphs (clusters) for each asset type
        for asset_type, nodes in nodes_by_type.items():
            shape, fill, border = shapes_colors.get(asset_type, ("ellipse", "#f5f5f5", "#333333"))
            cluster_name = f"cluster_{asset_type.value if hasattr(asset_type, 'value') else asset_type}"
            
            with dot.subgraph(name=cluster_name) as c:
                c.attr(
                    label=(asset_type.value if hasattr(asset_type, 'value') else asset_type).title(),
                    style="dashed,rounded",
                    color=border,
                    fontname="Helvetica-Bold",
                    fontsize="12",
                    fontcolor=border,
                    bgcolor="#fafafa",
                    margin="10",
                    penwidth="1.5",
                )
                
                for node in nodes:
                    c.node(
                        node.id,
                        label=f"{node.name}\n({asset_type.value if hasattr(asset_type, 'value') else asset_type})",
                        shape=shape,
                        style="filled,rounded",
                        fillcolor=fill,
                        color=border,
                        fontname="Helvetica",
                        fontsize="10",
                        penwidth="1.5",
                        tooltip=node.url or "",
                    )

        # Add edges with better formatting
        for edge in graph.edges:
            # Truncate long labels for readability
            label = edge.label or edge.relation
            if len(label) > 35:
                label = label[:32] + "..."
            
            dot.edge(
                edge.source,
                edge.target,
                label=label,
                fontname="Helvetica",
                fontsize="9",
                fontcolor="#555555",
                color="#777777",
                penwidth="1.0",
                arrowsize="0.7",
                # Add some spacing around edge labels
                labeldistance="2.5",
                labelangle="-25",
            )

        dot.render(output_path, cleanup=False)
        # graphviz returns filename with .svg appended; return actual path
        return output_path if output_path.endswith(".svg") else f"{output_path}.svg"

    def render_mermaid(self, graph: LineageGraph) -> str:
        """Produce raw Mermaid flowchart string (.mmd)."""
        lines = ["flowchart TD"]
        for node in graph.nodes:
            safe_id = node.id.replace("-", "_").replace("/", "_")
            at_type_str = node.asset_type.value if hasattr(node.asset_type, "value") else node.asset_type
            lines.append(f"    {safe_id}[\"{node.name}\n({at_type_str})\"]")
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
                    "asset_type": n.asset_type.value if hasattr(n.asset_type, "value") else n.asset_type,
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
