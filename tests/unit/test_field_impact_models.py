"""Unit tests for Field Impact Analysis models."""

import pytest
from pydantic import ValidationError

from asftool.core.models import (
    AssetDependency,
    AssetDependencyGraph,
    AssetType,
    DashboardFieldReference,
    DataflowFieldReference,
    DatasetFieldAnalysisResult,
    DatasetFieldReference,
    FieldImpactDetail,
    FieldImpactReport,
    FieldImpactScope,
    FieldImpactSummary,
    MatchMode,
    MatchType,
    ReplicatedDatasetField,
)


class TestAssetDependency:
    """Tests for AssetDependency model."""

    def test_create_minimal(self):
        dep = AssetDependency(id="1", name="TestDataset", type=AssetType.DATASET)
        assert dep.id == "1"
        assert dep.name == "TestDataset"
        assert dep.type == AssetType.DATASET
        assert dep.label is None
        assert dep.parent_id is None
        assert dep.metadata == {}

    def test_create_full(self):
        dep = AssetDependency(
            id="2",
            name="TestDashboard",
            type=AssetType.DASHBOARD,
            label="Test Dashboard",
            parent_id="1",
            metadata={"description": "A test dashboard"},
        )
        assert dep.label == "Test Dashboard"
        assert dep.parent_id == "1"
        assert dep.metadata == {"description": "A test dashboard"}

    def test_invalid_type(self):
        with pytest.raises(ValidationError):
            AssetDependency(id="1", name="Test", type="invalid_type")


class TestAssetDependencyGraph:
    """Tests for AssetDependencyGraph model."""

    def test_add_node_and_edge(self):
        graph = AssetDependencyGraph()
        node1 = AssetDependency(id="1", name="App", type=AssetType.DASHBOARD)
        node2 = AssetDependency(id="2", name="Dataset", type=AssetType.DATASET, parent_id="1")

        graph.add_node(node1)
        graph.add_node(node2)
        graph.add_edge("1", "2")

        assert "1" in graph.nodes
        assert "2" in graph.nodes
        assert graph.edges["1"] == ["2"]

    def test_get_children(self):
        graph = AssetDependencyGraph()
        node1 = AssetDependency(id="1", name="App", type=AssetType.DASHBOARD)
        node2 = AssetDependency(id="2", name="Dataset", type=AssetType.DATASET)
        node3 = AssetDependency(id="3", name="Dataflow", type=AssetType.DATAFLOW)

        graph.add_node(node1)
        graph.add_node(node2)
        graph.add_node(node3)
        graph.add_edge("1", "2")
        graph.add_edge("1", "3")

        children = graph.get_children("1")
        assert len(children) == 2
        assert {c.id for c in children} == {"2", "3"}

    def test_get_parents(self):
        graph = AssetDependencyGraph()
        node1 = AssetDependency(id="1", name="App", type=AssetType.DASHBOARD)
        node2 = AssetDependency(id="2", name="Dataset", type=AssetType.DATASET)

        graph.add_node(node1)
        graph.add_node(node2)
        graph.add_edge("1", "2")

        parents = graph.get_parents("2")
        assert len(parents) == 1
        assert parents[0].id == "1"

    def test_get_all_downstream(self):
        graph = AssetDependencyGraph()
        node1 = AssetDependency(id="1", name="App", type=AssetType.DASHBOARD)
        node2 = AssetDependency(id="2", name="Dataset", type=AssetType.DATASET)
        node3 = AssetDependency(id="3", name="Dataflow", type=AssetType.DATAFLOW)
        node4 = AssetDependency(id="4", name="Dashboard", type=AssetType.DASHBOARD)

        graph.add_node(node1)
        graph.add_node(node2)
        graph.add_node(node3)
        graph.add_node(node4)
        graph.add_edge("1", "2")
        graph.add_edge("2", "3")
        graph.add_edge("3", "4")

        downstream = graph.get_all_downstream("1")
        assert len(downstream) == 3
        assert [n.id for n in downstream] == ["2", "3", "4"]

    def test_get_all_upstream(self):
        graph = AssetDependencyGraph()
        node1 = AssetDependency(id="1", name="App", type=AssetType.DASHBOARD)
        node2 = AssetDependency(id="2", name="Dataset", type=AssetType.DATASET)
        node3 = AssetDependency(id="3", name="Dataflow", type=AssetType.DATAFLOW)

        graph.add_node(node1)
        graph.add_node(node2)
        graph.add_node(node3)
        graph.add_edge("1", "2")
        graph.add_edge("2", "3")

        upstream = graph.get_all_upstream("3")
        assert len(upstream) == 2
        assert [n.id for n in upstream] == ["2", "1"]

    def test_filter_by_type(self):
        graph = AssetDependencyGraph()
        graph.add_node(AssetDependency(id="1", name="App", type=AssetType.DASHBOARD))
        graph.add_node(AssetDependency(id="2", name="Dataset", type=AssetType.DATASET))
        graph.add_node(AssetDependency(id="3", name="Dataflow", type=AssetType.DATAFLOW))
        graph.add_node(AssetDependency(id="4", name="Dashboard2", type=AssetType.DASHBOARD))

        dashboards = graph.filter_by_type(AssetType.DASHBOARD)
        assert len(dashboards) == 2
        assert all(d.type == AssetType.DASHBOARD for d in dashboards)

        datasets = graph.filter_by_type(AssetType.DATASET)
        assert len(datasets) == 1


class TestDatasetFieldReference:
    """Tests for DatasetFieldReference model."""

    def test_create_exact_match(self):
        ref = DatasetFieldReference(
            field_api_name="Amount",
            field_label="Amount",
            field_type="Numeric",
            dataset_id="ds1",
            dataset_name="SalesData",
            version_id="v1",
            match_type=MatchType.EXACT,
            match_score=100,
            source="measure",
        )
        assert ref.match_type == MatchType.EXACT
        assert ref.match_score == 100
        assert ref.source == "measure"

    def test_create_fuzzy_match(self):
        ref = DatasetFieldReference(
            field_api_name="Account_Name",
            field_label="Account Name",
            field_type="Text",
            dataset_id="ds1",
            dataset_name="SalesData",
            match_type=MatchType.FUZZY,
            match_score=85,
            source="dimension",
        )
        assert ref.match_type == MatchType.FUZZY
        assert ref.match_score == 85

    def test_score_bounds(self):
        with pytest.raises(ValidationError):
            DatasetFieldReference(
                field_api_name="Test",
                dataset_id="ds1",
                dataset_name="Test",
                match_type=MatchType.EXACT,
                match_score=101,  # Invalid > 100
            )

        with pytest.raises(ValidationError):
            DatasetFieldReference(
                field_api_name="Test",
                dataset_id="ds1",
                dataset_name="Test",
                match_type=MatchType.EXACT,
                match_score=-1,  # Invalid < 0
            )


class TestDatasetFieldAnalysisResult:
    """Tests for DatasetFieldAnalysisResult model."""

    def test_properties(self):
        result = DatasetFieldAnalysisResult(
            dataset_id="ds1",
            dataset_name="SalesData",
            matches=[
                DatasetFieldReference(
                    field_api_name="Amount",
                    dataset_id="ds1",
                    dataset_name="SalesData",
                    match_type=MatchType.EXACT,
                    match_score=100,
                ),
                DatasetFieldReference(
                    field_api_name="Account_Name",
                    dataset_id="ds1",
                    dataset_name="SalesData",
                    match_type=MatchType.FUZZY,
                    match_score=85,
                ),
            ],
        )
        assert result.match_count == 2
        assert result.exact_match_count == 1
        assert result.fuzzy_match_count == 1


class TestDashboardFieldReference:
    """Tests for DashboardFieldReference model."""

    def test_create(self):
        ref = DashboardFieldReference(
            widget_id="w1",
            widget_type="chart",
            step_id="s1",
            field_path="step.query.measures[0].field",
            field_api_name="Amount",
            field_label="Amount",
            dashboard_id="db1",
            dashboard_name="Sales Dashboard",
            match_type=MatchType.EXACT,
            match_score=100,
        )
        assert ref.widget_id == "w1"
        assert ref.field_path == "step.query.measures[0].field"
        assert ref.match_type == MatchType.EXACT


class TestDataflowFieldReference:
    """Tests for DataflowFieldReference model."""

    def test_create(self):
        ref = DataflowFieldReference(
            node_id="n1",
            node_type="sfdcDigest",
            field_name="AccountId",
            field_context="sourceField",
            dataflow_id="df1",
            dataflow_name="ETL Pipeline",
            match_type=MatchType.FUZZY,
            match_score=90,
        )
        assert ref.node_type == "sfdcDigest"
        assert ref.field_context == "sourceField"
        assert ref.match_score == 90


class TestReplicatedDatasetField:
    """Tests for ReplicatedDatasetField model."""

    def test_create(self):
        field = ReplicatedDatasetField(
            field_api_name="AccountId",
            field_label="Account ID",
            field_type="reference",
            is_nillable=True,
            is_unique=False,
            replicated_dataset_id="rd1",
            object_name="Account",
            match_type=MatchType.EXACT,
            match_score=100,
        )
        assert field.object_name == "Account"
        assert field.is_nillable is True
        assert field.match_type == MatchType.EXACT


class TestFieldImpactScope:
    """Tests for FieldImpactScope model."""

    def test_defaults(self):
        scope = FieldImpactScope(search_term="Amount")
        assert scope.search_term == "Amount"
        assert scope.match_mode == MatchMode.BOTH
        assert scope.fuzzy_threshold == 85
        assert scope.include_datasets is True
        assert scope.include_dashboards is True
        assert scope.include_dataflows is True
        assert scope.include_replicated is True
        assert scope.application_id is None

    def test_custom_values(self):
        scope = FieldImpactScope(
            search_term="Account",
            match_mode=MatchMode.FUZZY,
            fuzzy_threshold=90,
            include_dataflows=False,
            application_id="app123",
        )
        assert scope.match_mode == MatchMode.FUZZY
        assert scope.fuzzy_threshold == 90
        assert scope.include_dataflows is False
        assert scope.application_id == "app123"

    def test_threshold_bounds(self):
        with pytest.raises(ValidationError):
            FieldImpactScope(search_term="Test", fuzzy_threshold=101)

        with pytest.raises(ValidationError):
            FieldImpactScope(search_term="Test", fuzzy_threshold=-1)


class TestFieldImpactSummary:
    """Tests for FieldImpactSummary model."""

    def test_defaults(self):
        summary = FieldImpactSummary()
        assert summary.total_assets_scanned == 0
        assert summary.total_matches == 0
        assert summary.by_asset_type == {}

    def test_with_data(self):
        summary = FieldImpactSummary(
            total_assets_scanned=10,
            total_matches=5,
            exact_matches=3,
            fuzzy_matches=2,
            by_asset_type={
                AssetType.DATASET: 2,
                AssetType.DASHBOARD: 3,
            },
            datasets_scanned=4,
            dashboards_scanned=3,
            dataflows_scanned=2,
            replicated_datasets_scanned=1,
        )
        assert summary.total_assets_scanned == 10
        assert summary.by_asset_type[AssetType.DATASET] == 2


class TestFieldImpactReport:
    """Tests for FieldImpactReport model."""

    def test_create_full_report(self):
        scope = FieldImpactScope(search_term="Amount")
        summary = FieldImpactSummary(total_assets_scanned=5, total_matches=2)
        details = FieldImpactDetail()
        report = FieldImpactReport(
            scope=scope,
            summary=summary,
            details=details,
            execution_time_ms=1500,
        )
        assert report.scope.search_term == "Amount"
        assert report.execution_time_ms == 1500
        assert report.generated_at is not None

    def test_to_summary_dict(self):
        scope = FieldImpactScope(search_term="Amount", match_mode=MatchMode.BOTH)
        summary = FieldImpactSummary(
            total_assets_scanned=10,
            total_matches=5,
            exact_matches=3,
            fuzzy_matches=2,
            by_asset_type={AssetType.DATASET: 3, AssetType.DASHBOARD: 2},
        )
        details = FieldImpactDetail()
        report = FieldImpactReport(
            scope=scope,
            summary=summary,
            details=details,
            execution_time_ms=1500,
            errors=["Warning: dataset ds1 timed out"],
        )
        summary_dict = report.to_summary_dict()

        assert summary_dict["search_term"] == "Amount"
        assert summary_dict["match_mode"] == "both"
        assert summary_dict["total_assets_scanned"] == 10
        assert summary_dict["total_matches"] == 5
        assert summary_dict["exact_matches"] == 3
        assert summary_dict["fuzzy_matches"] == 2
        assert summary_dict["by_asset_type"]["dataset"] == 3
        assert summary_dict["by_asset_type"]["dashboard"] == 2
        assert summary_dict["error_count"] == 1


class TestEnums:
    """Tests for enum values."""

    def test_match_type_values(self):
        assert MatchType.EXACT.value == "exact"
        assert MatchType.FUZZY.value == "fuzzy"

    def test_asset_type_values(self):
        assert AssetType.DATASET.value == "dataset"
        assert AssetType.DASHBOARD.value == "dashboard"
        assert AssetType.DATAFLOW.value == "dataflow"
        assert AssetType.REPLICATED_DATASET.value == "replicated_dataset"

    def test_match_mode_values(self):
        assert MatchMode.EXACT.value == "exact"
        assert MatchMode.FUZZY.value == "fuzzy"
        assert MatchMode.BOTH.value == "both"


class TestModelSerialization:
    """Tests for model serialization round-trip."""

    def test_dataset_field_reference_roundtrip(self):
        original = DatasetFieldReference(
            field_api_name="Amount",
            dataset_id="ds1",
            dataset_name="SalesData",
            match_type=MatchType.EXACT,
            match_score=100,
        )
        json_str = original.model_dump_json()
        restored = DatasetFieldReference.model_validate_json(json_str)
        assert restored.field_api_name == original.field_api_name
        assert restored.match_type == original.match_type

    def test_asset_dependency_graph_roundtrip(self):
        graph = AssetDependencyGraph()
        graph.add_node(AssetDependency(id="1", name="A", type=AssetType.DATASET))
        graph.add_node(AssetDependency(id="2", name="B", type=AssetType.DASHBOARD))
        graph.add_edge("1", "2")

        json_str = graph.model_dump_json()
        restored = AssetDependencyGraph.model_validate_json(json_str)
        assert "1" in restored.nodes
        assert "2" in restored.nodes
        assert restored.edges == {"1": ["2"]}

    def test_field_impact_report_roundtrip(self):
        scope = FieldImpactScope(search_term="Test")
        summary = FieldImpactSummary()
        details = FieldImpactDetail()
        report = FieldImpactReport(scope=scope, summary=summary, details=details)

        json_str = report.model_dump_json()
        restored = FieldImpactReport.model_validate_json(json_str)
        assert restored.scope.search_term == "Test"
        assert restored.summary.total_assets_scanned == 0
