"""Build and traverse the asset dependency graph from Analytics REST API applications."""

import structlog

from asftool.core.client import SalesforceClient
from asftool.core.models import AssetDependency, AssetDependencyGraph, AssetType

logger = structlog.get_logger(__name__)


# Map Analytics REST API dependency type strings to our AssetType enum.
_ASFT_TYPE_MAP: dict[str, AssetType] = {
    "dataset": AssetType.DATASET,
    "xdataset": AssetType.DATASET,
    "dashboard": AssetType.DASHBOARD,
    "lens": AssetType.DASHBOARD,  # lenses are dashboards in the Analytics REST API
    "dataflow": AssetType.DATAFLOW,
    "replicateddataset": AssetType.REPLICATED_DATASET,
    "replicated_dataset": AssetType.REPLICATED_DATASET,
}


def _to_asset_type(t: str) -> AssetType | None:
    """Map an Analytics REST API dependency type string to AssetType, or None if unknown."""
    if not t:
        return None
    return _ASFT_TYPE_MAP.get(t.lower().replace(" ", ""))


def _build_node_from_dependency(raw: dict) -> AssetDependency | None:
    """Convert a raw Analytics REST API dependency dict into an AssetDependency node."""
    asset_type = _to_asset_type(raw.get("type", ""))
    if asset_type is None:
        return None
    return AssetDependency(
        id=str(raw.get("id", "")),
        name=raw.get("name", ""),
        type=asset_type,
        label=raw.get("label"),
        metadata={k: v for k, v in raw.items() if k not in {"id", "name", "type", "label"}},
    )


class AssetGraphBuilder:
    """Builds an AssetDependencyGraph from an Analytics REST API application."""

    MAX_DEPTH = 5  # Guard against pathological cycles in upstream recursion

    def __init__(self, client: SalesforceClient):
        self.client = client

    async def build_from_application(self, application_id: str) -> AssetDependencyGraph:
        """Fetch the dependency tree for an application and build a graph.

        The Analytics REST API returns dependencies recursively (each node has its own
        `dependencies` array). We walk that tree up to MAX_DEPTH.
        """
        graph = AssetDependencyGraph()
        # Seed the root node.
        root = AssetDependency(
            id=application_id,
            name=f"app:{application_id}",
            type=AssetType.DASHBOARD,  # Applications are dashboard-like containers
            label=f"Application {application_id}",
        )
        graph.add_node(root)

        await self._walk_dependencies(application_id, graph, depth=0)
        return graph

    async def _walk_dependencies(
        self, asset_id: str, graph: AssetDependencyGraph, depth: int
    ) -> None:
        """Recursively fetch dependencies for a node and add to the graph."""
        if depth > self.MAX_DEPTH:
            logger.warning("graph_max_depth_reached", asset_id=asset_id, depth=depth)
            return

        try:
            raw = await self.client.get_application_dependencies(asset_id)
        except Exception as e:
            logger.warning("graph_dependency_fetch_failed", asset_id=asset_id, error=str(e))
            return

        deps = raw.get("dependencies", []) or []
        for dep in deps:
            node = _build_node_from_dependency(dep)
            if node is None:
                continue
            graph.add_node(node)
            graph.add_edge(asset_id, node.id)
            # Recurse into this dependency.
            await self._walk_dependencies(node.id, graph, depth + 1)

    async def build_from_dataset(self, dataset_id: str) -> AssetDependencyGraph:
        """Build a graph rooted at a single dataset (downstream consumers)."""
        graph = AssetDependencyGraph()
        root = AssetDependency(
            id=dataset_id,
            name=f"ds:{dataset_id}",
            type=AssetType.DATASET,
            label=f"Dataset {dataset_id}",
        )
        graph.add_node(root)

        try:
            raw = await self.client.get_dataset_dependencies(dataset_id)
        except Exception as e:
            logger.warning(
                "graph_dataset_deps_failed", dataset_id=dataset_id, error=str(e)
            )
            return graph

        deps = raw.get("dependencies", []) or []
        for dep in deps:
            node = _build_node_from_dependency(dep)
            if node is None:
                continue
            graph.add_node(node)
            graph.add_edge(dataset_id, node.id)

        return graph
