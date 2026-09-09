"""Lineage service for dependency extraction, graph construction, and rendering."""

from __future__ import annotations

import json
from typing import Any

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
                        source_name = str(
                            match.get("dataset_name", match.get("field_name", source_ref))
                        )
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

    def render_mermaid(self, graph: LineageGraph) -> str:
        """Produce a clean, readable Mermaid flowchart string (.mmd).

        Follows Mermaid best practices:
        - Subgraphs group nodes by asset type (datasets, dashboards, dataflows).
        - ``classDef`` color-coding per asset type.
        - Human-friendly node IDs derived from asset names (not long SF ids).
        - Self-referencing field edges are skipped to reduce visual clutter
          (the field matches are still visible in the JSON export).
        """
        import re

        lines = [
            "%%{init: {'theme':'base', 'themeVariables': {",
            "  'primaryColor': '#e1f5fe', 'primaryTextColor': '#01579b',",
            "  'primaryBorderColor': '#01579b', 'lineColor': '#6b7280',",
            "  'secondaryColor': '#e8f5e9', 'tertiaryColor': '#fff3e0'",
            "}}}%%",
            "flowchart TD",
        ]

        # Prefix + classDef style per asset type
        type_map = {
            AssetType.DATASET: ("ds", "datasetStyle"),
            AssetType.RECIPE: ("rcp", "recipeStyle"),
            AssetType.DATAFLOW: ("df", "dataflowStyle"),
            AssetType.DASHBOARD: ("db", "dashboardStyle"),
            AssetType.LENS: ("lens", "lensStyle"),
        }

        def _safe_name(name: str) -> str:
            safe = re.sub(r"[^A-Za-z0-9_]", "_", str(name))
            safe = re.sub(r"_+", "_", safe).strip("_")
            return safe[:40] or "asset"

        # Assign stable human-readable ids from asset names.
        id_map: dict[str, str] = {}
        used: set[str] = set()
        labels: dict[str, str] = {}
        node_style: dict[str, str] = {}

        for node in graph.nodes:
            prefix, style = type_map.get(node.asset_type, ("as", "datasetStyle"))
            base = _safe_name(node.name)
            candidate = f"{prefix}_{base}"
            i = 2
            while candidate in used:
                candidate = f"{prefix}_{base}_{i}"
                i += 1
            used.add(candidate)
            id_map[node.id] = candidate
            labels[candidate] = node.name
            node_style[candidate] = style

        # Group node ids by asset type for subgraphs.
        groups: dict[str, list[str]] = {}
        for node in graph.nodes:
            key = (
                node.asset_type.value if hasattr(node.asset_type, "value") else str(node.asset_type)
            )
            groups.setdefault(key, []).append(id_map[node.id])

        # class definitions (styled per skill guidance: color only, no emoji)
        lines.append("    classDef datasetStyle fill:#e1f5fe,stroke:#01579b,color:#01579b")
        lines.append("    classDef dashboardStyle fill:#ede7f6,stroke:#4a148c,color:#4a148c")
        lines.append("    classDef dataflowStyle fill:#e8f5e9,stroke:#1b5e20,color:#1b5e20")
        lines.append("    classDef recipeStyle fill:#fff3e0,stroke:#e65100,color:#e65100")
        lines.append("    classDef lensStyle fill:#fce4ec,stroke:#880e4f,color:#880e4f")

        # Emit one subgraph per asset type (stable order).
        for tkey in (t.value for t in AssetType):
            ids = groups.get(tkey, [])
            if not ids:
                continue
            lines.append(f"    subgraph {tkey.upper()}[{tkey.title()}]")
            for nid in ids:
                lines.append(f'        {nid}["{labels[nid]}"]')
            lines.append("    end")

        # Edges between distinct assets; skip self-references (node->itself).
        for edge in graph.edges:
            src = id_map.get(edge.source)
            tgt = id_map.get(edge.target)
            if not src or not tgt or src == tgt:
                continue
            label = edge.label or edge.relation or ""
            if label.startswith("Field: "):
                label = label[len("Field: ") :]
            label = label.replace('"', '\\"')
            lines.append(f'    {src} -->|"{label}"| {tgt}')

        # Apply style classes.
        for nid, style in node_style.items():
            lines.append(f"    class {nid} {style}")

        return "\n".join(lines)

    def to_node_edge_json(self, graph: LineageGraph) -> dict[str, Any]:
        """Standardized frontend graph payload."""
        return {
            "nodes": [
                {
                    "id": n.id,
                    "label": n.name,
                    "asset_type": n.asset_type.value
                    if hasattr(n.asset_type, "value")
                    else n.asset_type,
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
