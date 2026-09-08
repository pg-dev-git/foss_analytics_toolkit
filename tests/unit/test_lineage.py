"""Unit tests for lineage models, SVG/Mermaid/JSON rendering."""

import json
from pathlib import Path

import pytest

from asftool.core.models.lineage import AssetType, LineageEdge, LineageGraph, LineageNode
from asftool.core.services.lineage_service import LineageService


class TestLineageModels:
    def test_lineage_node_creation(self):
        node = LineageNode(
            id="ds_01",
            name="Sales Dataset",
            asset_type=AssetType.DATASET,
            url="https://example.com",
        )
        assert node.id == "ds_01"
        assert node.asset_type == AssetType.DATASET

    def test_lineage_edge_default_relation(self):
        edge = LineageEdge(source="ds_01", target="dash_01")
        assert edge.relation == "depends_on"

    def test_lineage_graph_downstream(self):
        graph = LineageGraph()
        graph.add_node(LineageNode(id="a", name="A", asset_type=AssetType.DATASET))
        graph.add_node(LineageNode(id="b", name="B", asset_type=AssetType.RECIPE))
        graph.add_edge(LineageEdge(source="a", target="b"))
        downstream = graph.get_downstream("a")
        assert len(downstream) == 1
        assert downstream[0].id == "b"

    def test_lineage_graph_upstream(self):
        graph = LineageGraph()
        graph.add_node(LineageNode(id="a", name="A", asset_type=AssetType.DATASET))
        graph.add_node(LineageNode(id="b", name="B", asset_type=AssetType.RECIPE))
        graph.add_edge(LineageEdge(source="a", target="b"))
        upstream = graph.get_upstream("b")
        assert len(upstream) == 1
        assert upstream[0].id == "a"


class TestLineageRendering:
    def test_render_mermaid_includes_nodes_and_edges(self):
        service = LineageService(None)
        graph = LineageGraph()
        graph.add_node(LineageNode(id="ds_01", name="DS", asset_type=AssetType.DATASET))
        graph.add_node(LineageNode(id="db_01", name="DB", asset_type=AssetType.DASHBOARD))
        graph.add_edge(LineageEdge(source="ds_01", target="db_01"))
        mmd = service.render_mermaid(graph)
        assert "flowchart TD" in mmd
        assert "ds_01" in mmd
        assert "db_01" in mmd

    def test_to_node_edge_json_schema(self):
        service = LineageService(None)
        graph = LineageGraph()
        graph.add_node(LineageNode(id="n1", name="N1", asset_type=AssetType.LENS))
        payload = service.to_node_edge_json(graph)
        assert "nodes" in payload
        assert "edges" in payload
        assert payload["nodes"][0]["id"] == "n1"
