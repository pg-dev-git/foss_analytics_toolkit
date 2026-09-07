"""Field Impact Analysis Service - crawls TCRM assets to find where a field is used."""

import asyncio
import time
from collections.abc import Awaitable, Callable
from typing import Any

import structlog

from asftool.core.client import SalesforceClient
from asftool.core.config import Settings, get_settings
from asftool.core.exceptions import (
    SalesforceAPIError,
    SalesforceNotFoundError,
)
from asftool.core.models import (
    AssetDependencyGraph,
    AssetType,
    DashboardFieldAnalysisResult,
    DashboardFieldReference,
    DataflowFieldAnalysisResult,
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
    ReplicatedDatasetFieldAnalysisResult,
)
from asftool.core.services.asset_graph import AssetGraphBuilder
from asftool.core.services.field_matcher import FieldMatcher, extract_field_strings

logger = structlog.get_logger(__name__)

ProgressCallback = Callable[[str, int, int], Awaitable[None]]
"""Callback signature: (stage_name, current, total)."""


class FieldImpactService:
    """Orchestrates field impact analysis across TCRM assets.

    Scans datasets (via XMD), dashboards (via widget/step JSON), dataflows
    (via recipe JSON), and replicated datasets (via /fields endpoint) for
    any usage of a target field name or label.
    """

    def __init__(
        self,
        client: SalesforceClient,
        settings: Settings | None = None,
        max_concurrent: int = 10,
    ):
        self.client = client
        self.settings = settings or get_settings()
        self.matcher = FieldMatcher(
            threshold=self.settings.field_impact_default_fuzzy_threshold
        )
        self.max_concurrent = max_concurrent
        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._graph_builder = AssetGraphBuilder(client)

    # =========================================================================
    # Main entry point
    # =========================================================================

    async def analyze_field_impact(
        self,
        search_term: str,
        match_mode: MatchMode = MatchMode.BOTH,
        fuzzy_threshold: int | None = None,
        include_datasets: bool = True,
        include_dashboards: bool = True,
        include_dataflows: bool = True,
        include_replicated: bool = True,
        application_id: str | None = None,
        progress_callback: ProgressCallback | None = None,
    ) -> FieldImpactReport:
        """Run the full field impact analysis.

        Args:
            search_term: Field API name or label to search for.
            match_mode: EXACT, FUZZY, or BOTH.
            fuzzy_threshold: Override the configured default threshold.
            include_*: Asset-type toggles.
            application_id: If provided, limit the scan to assets in this
                application's dependency graph.
            progress_callback: Optional async callback for progress.
        """
        if fuzzy_threshold is not None:
            self.matcher.threshold = max(0, min(100, fuzzy_threshold))

        scope = FieldImpactScope(
            search_term=search_term,
            match_mode=match_mode,
            fuzzy_threshold=self.matcher.threshold,
            include_datasets=include_datasets,
            include_dashboards=include_dashboards,
            include_dataflows=include_dataflows,
            include_replicated=include_replicated,
            application_id=application_id,
        )

        start = time.monotonic()
        details = FieldImpactDetail()
        errors: list[str] = []
        graph: AssetDependencyGraph | None = None

        if application_id:
            try:
                if progress_callback:
                    await progress_callback("building_graph", 0, 1)
                graph = await self._graph_builder.build_from_application(application_id)
                if progress_callback:
                    await progress_callback("building_graph", 1, 1)
            except Exception as e:
                errors.append(f"Failed to build application graph: {e}")
                logger.warning("graph_build_failed", application_id=application_id, error=str(e))

        stages: list[tuple[str, bool, AssetType | None]] = [
            ("datasets", include_datasets, AssetType.DATASET),
            ("dashboards", include_dashboards, AssetType.DASHBOARD),
            ("dataflows", include_dataflows, AssetType.DATAFLOW),
            ("replicated_datasets", include_replicated, AssetType.REPLICATED_DATASET),
        ]
        total_stages = sum(1 for _, enabled, _ in stages if enabled)
        stage_idx = 0

        if include_datasets:
            stage_idx += 1
            if progress_callback:
                await progress_callback("datasets", stage_idx - 1, total_stages)
            ds_results, ds_errors = await self._scan_datasets_safe(
                search_term, match_mode, graph
            )
            details.datasets = ds_results
            errors.extend(ds_errors)
            if progress_callback:
                await progress_callback("datasets_complete", len(ds_results), total_stages)

        if include_dashboards:
            stage_idx += 1
            if progress_callback:
                await progress_callback("dashboards", stage_idx - 1, total_stages)
            db_results, db_errors = await self._scan_dashboards_safe(
                search_term, match_mode, graph
            )
            details.dashboards = db_results
            errors.extend(db_errors)
            if progress_callback:
                await progress_callback("dashboards_complete", len(db_results), total_stages)

        if include_dataflows:
            stage_idx += 1
            if progress_callback:
                await progress_callback("dataflows", stage_idx - 1, total_stages)
            df_results, df_errors = await self._scan_dataflows_safe(
                search_term, match_mode, graph
            )
            details.dataflows = df_results
            errors.extend(df_errors)
            if progress_callback:
                await progress_callback("dataflows_complete", len(df_results), total_stages)

        if include_replicated:
            stage_idx += 1
            if progress_callback:
                await progress_callback("replicated_datasets", stage_idx - 1, total_stages)
            rd_results, rd_errors = await self._scan_replicated_datasets_safe(
                search_term, match_mode, graph
            )
            details.replicated_datasets = rd_results
            errors.extend(rd_errors)
            if progress_callback:
                await progress_callback(
                    "replicated_datasets_complete", len(rd_results), total_stages
                )

        summary = self._build_summary(details)
        elapsed_ms = int((time.monotonic() - start) * 1000)

        return FieldImpactReport(
            scope=scope,
            summary=summary,
            details=details,
            execution_time_ms=elapsed_ms,
            errors=errors,
        )

    # =========================================================================
    # Safe scan wrappers (one per asset type)
    # =========================================================================

    async def _scan_datasets_safe(
        self,
        search_term: str,
        mode: MatchMode,
        graph: AssetDependencyGraph | None,
    ) -> tuple[list[DatasetFieldAnalysisResult], list[str]]:
        try:
            results = await self._scan_datasets(search_term, mode, graph)
            return results, []
        except Exception as e:
            logger.error("scan_datasets_failed", error=str(e))
            return [], [f"Datasets scan failed: {e}"]

    async def _scan_dashboards_safe(
        self,
        search_term: str,
        mode: MatchMode,
        graph: AssetDependencyGraph | None,
    ) -> tuple[list[DashboardFieldAnalysisResult], list[str]]:
        try:
            results = await self._scan_dashboards(search_term, mode, graph)
            return results, []
        except Exception as e:
            logger.error("scan_dashboards_failed", error=str(e))
            return [], [f"Dashboards scan failed: {e}"]

    async def _scan_dataflows_safe(
        self,
        search_term: str,
        mode: MatchMode,
        graph: AssetDependencyGraph | None,
    ) -> tuple[list[DataflowFieldAnalysisResult], list[str]]:
        try:
            results = await self._scan_dataflows(search_term, mode, graph)
            return results, []
        except Exception as e:
            logger.error("scan_dataflows_failed", error=str(e))
            return [], [f"Dataflows scan failed: {e}"]

    async def _scan_replicated_datasets_safe(
        self,
        search_term: str,
        mode: MatchMode,
        graph: AssetDependencyGraph | None,
    ) -> tuple[list[ReplicatedDatasetFieldAnalysisResult], list[str]]:
        try:
            results = await self._scan_replicated_datasets(search_term, mode, graph)
            return results, []
        except Exception as e:
            logger.error("scan_replicated_failed", error=str(e))
            return [], [f"Replicated datasets scan failed: {e}"]

    # =========================================================================
    # Dataset scanning
    # =========================================================================

    async def _scan_datasets(
        self,
        search_term: str,
        mode: MatchMode,
        graph: AssetDependencyGraph | None,
    ) -> list[DatasetFieldAnalysisResult]:
        """Scan all datasets (or graph-filtered) for field matches via XMD."""
        # List datasets (with pagination).
        datasets = await self._list_all_datasets()
        if graph is not None:
            graph_dataset_ids = {
                n.id for n in graph.filter_by_type(AssetType.DATASET)
            }
            if graph_dataset_ids:
                datasets = [d for d in datasets if d.get("id") in graph_dataset_ids]

        if not datasets:
            return []

        tasks = [self._scan_one_dataset(d, search_term, mode) for d in datasets]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        return [r for r in results if isinstance(r, DatasetFieldAnalysisResult)]

    async def _list_all_datasets(self) -> list[dict[str, Any]]:
        """List all datasets, handling pagination."""
        all_ds: list[dict] = []
        page_token: str | None = None
        while True:
            resp = await self.client.list_datasets(page_token=page_token)
            all_ds.extend(resp.get("datasets", []))
            next_url = resp.get("nextPageUrl")
            if not next_url:
                break
            page_token = self._extract_page_token(next_url)
            if not page_token:
                break
        return all_ds

    async def _scan_one_dataset(
        self, dataset: dict, search_term: str, mode: MatchMode
    ) -> DatasetFieldAnalysisResult:
        dataset_id = dataset.get("id", "")
        dataset_name = dataset.get("name", "")
        version_id = dataset.get("currentVersionId")

        start = time.monotonic()
        result = DatasetFieldAnalysisResult(
            dataset_id=dataset_id,
            dataset_name=dataset_name,
            dataset_label=dataset.get("label"),
            version_id=version_id,
        )

        if not version_id:
            return result

        try:
            xmd_raw = await self.client.get_dataset_xmd(dataset_id, version_id)
        except (SalesforceNotFoundError, SalesforceAPIError) as e:
            logger.debug("xmd_fetch_failed", dataset_id=dataset_id, error=str(e))
            return result

        # Extract field candidates from all XMD sections.
        candidates: list[tuple[str, str, str, str]] = []  # (api, label, type, source)
        for measure in xmd_raw.get("measures", []):
            f = measure.get("field", "")
            if f and not f.endswith("_epoch"):
                candidates.append((f, measure.get("label", ""), measure.get("type", ""), "measure"))
        for dim in xmd_raw.get("dimensions", []):
            f = dim.get("field", "")
            if f and not any(f.endswith(s) for s in (
                "_Second", "_Minute", "_Hour", "_Day", "_Week",
                "_Month", "_Quarter", "_Year", "_epoch",
            )):
                candidates.append((f, dim.get("label", ""), dim.get("type", ""), "dimension"))
        for dt in xmd_raw.get("dates", []):
            f = dt.get("field", "")
            if f:
                candidates.append((f, dt.get("label", ""), dt.get("type", ""), "date"))

        result.total_fields_scanned = len(candidates)
        for api, label, ftype, source in candidates:
            mtype, score, _ = self.matcher.match_field(search_term, api, label, mode)
            if mtype is None:
                continue
            result.matches.append(
                DatasetFieldReference(
                    field_api_name=api,
                    field_label=label or None,
                    field_type=ftype or None,
                    dataset_id=dataset_id,
                    dataset_name=dataset_name,
                    version_id=version_id,
                    match_type=mtype,
                    match_score=score,
                    source=source,  # type: ignore[arg-type]
                )
            )

        result.scan_duration_ms = int((time.monotonic() - start) * 1000)
        return result

    # =========================================================================
    # Dashboard scanning
    # =========================================================================

    async def _scan_dashboards(
        self,
        search_term: str,
        mode: MatchMode,
        graph: AssetDependencyGraph | None,
    ) -> list[DashboardFieldAnalysisResult]:
        """Scan dashboards for field references in widget/step JSON."""
        all_db: list[dict] = []
        page_token: str | None = None
        while True:
            resp = await self.client.list_dashboards(page_token=page_token)
            all_db.extend(resp.get("dashboards", []))
            next_url = resp.get("nextPageUrl")
            if not next_url:
                break
            page_token = self._extract_page_token(next_url)
            if not page_token:
                break

        if graph is not None:
            graph_db_ids = {n.id for n in graph.filter_by_type(AssetType.DASHBOARD)}
            if graph_db_ids:
                all_db = [d for d in all_db if d.get("id") in graph_db_ids]

        if not all_db:
            return []

        tasks = [self._scan_one_dashboard(d, search_term, mode) for d in all_db]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        return [r for r in results if isinstance(r, DashboardFieldAnalysisResult)]

    async def _scan_one_dashboard(
        self, dashboard: dict, search_term: str, mode: MatchMode
    ) -> DashboardFieldAnalysisResult:
        dashboard_id = dashboard.get("id", "")
        dashboard_name = dashboard.get("name", "")

        start = time.monotonic()
        result = DashboardFieldAnalysisResult(
            dashboard_id=dashboard_id,
            dashboard_name=dashboard_name,
            dashboard_label=dashboard.get("label"),
        )

        try:
            full = await self.client.get_dashboard_full(dashboard_id)
        except (SalesforceNotFoundError, SalesforceAPIError) as e:
            logger.debug("dashboard_fetch_failed", dashboard_id=dashboard_id, error=str(e))
            return result

        # Walk the dashboard JSON looking for field-name-like strings.
        widgets = self._walk_for_widgets(full)
        result.total_widgets_scanned = len(widgets)
        result.total_steps_scanned = sum(len(w.get("steps", [])) for w in widgets)

        for widget in widgets:
            widget_id = widget.get("id", "")
            widget_type = widget.get("type", "")
            for step in widget.get("steps", []):
                step_id = step.get("id", "")
                step_label = step.get("label", "")
                # Examine the step's query/body for field references.
                step_json = {"query": step.get("query"), "body": step.get("body"), "label": step_label}
                strings = extract_field_strings(step_json)
                for path, value in strings:
                    mtype, score, _ = self.matcher.match_field(
                        search_term, value, None, mode
                    )
                    if mtype is None:
                        continue
                    result.matches.append(
                        DashboardFieldReference(
                            widget_id=widget_id,
                            widget_type=widget_type,
                            step_id=step_id,
                            field_path=f"{widget_id}.{step_id}.{path}",
                            field_api_name=value,
                            field_label=None,
                            dashboard_id=dashboard_id,
                            dashboard_name=dashboard_name,
                            match_type=mtype,
                            match_score=score,
                        )
                    )

        result.scan_duration_ms = int((time.monotonic() - start) * 1000)
        return result

    def _walk_for_widgets(self, dashboard_json: Any) -> list[dict]:
        """Extract widget-like dicts from a dashboard JSON.

        The TCRM dashboard schema nests widgets under state.widgets or directly
        under widgets. We accept both shapes.
        """
        candidates: list[dict] = []
        if not isinstance(dashboard_json, dict):
            return candidates
        for path in ("widgets", "state.widgets", "lens.widgets"):
            node = dashboard_json
            for key in path.split("."):
                if isinstance(node, dict):
                    node = node.get(key, {})
                else:
                    node = {}
            if isinstance(node, list):
                candidates.extend([w for w in node if isinstance(w, dict)])
        return candidates

    # =========================================================================
    # Dataflow scanning
    # =========================================================================

    async def _scan_dataflows(
        self,
        search_term: str,
        mode: MatchMode,
        graph: AssetDependencyGraph | None,
    ) -> list[DataflowFieldAnalysisResult]:
        """Scan dataflow recipes for field references in nodes/transforms."""
        try:
            resp = await self.client.list_dataflows()
        except Exception as e:
            logger.warning("list_dataflows_failed", error=str(e))
            return []

        all_df = resp.get("dataflows", []) or []
        if graph is not None:
            graph_df_ids = {n.id for n in graph.filter_by_type(AssetType.DATAFLOW)}
            if graph_df_ids:
                all_df = [d for d in all_df if d.get("id") in graph_df_ids]

        if not all_df:
            return []

        tasks = [self._scan_one_dataflow(d, search_term, mode) for d in all_df]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        return [r for r in results if isinstance(r, DataflowFieldAnalysisResult)]

    async def _scan_one_dataflow(
        self, dataflow: dict, search_term: str, mode: MatchMode
    ) -> DataflowFieldAnalysisResult:
        dataflow_id = dataflow.get("id", "")
        dataflow_name = dataflow.get("name", "")

        start = time.monotonic()
        result = DataflowFieldAnalysisResult(
            dataflow_id=dataflow_id,
            dataflow_name=dataflow_name,
            dataflow_label=dataflow.get("label"),
        )

        try:
            definition = await self.client.get_dataflow_definition(dataflow_id)
        except (SalesforceNotFoundError, SalesforceAPIError) as e:
            logger.debug("dataflow_def_failed", dataflow_id=dataflow_id, error=str(e))
            return result

        nodes = self._extract_dataflow_nodes(definition)
        result.total_nodes_scanned = len(nodes)

        for node in nodes:
            node_id = node.get("id", "")
            node_type = node.get("type", node.get("action", ""))
            # Pull field-name-like strings from the node.
            strings = extract_field_strings(node)
            for path, value in strings:
                mtype, score, _ = self.matcher.match_field(
                    search_term, value, None, mode
                )
                if mtype is None:
                    continue
                result.matches.append(
                    DataflowFieldReference(
                        node_id=node_id,
                        node_type=node_type,
                        field_name=value,
                        field_context=path,
                        dataflow_id=dataflow_id,
                        dataflow_name=dataflow_name,
                        match_type=mtype,
                        match_score=score,
                    )
                )

        result.scan_duration_ms = int((time.monotonic() - start) * 1000)
        return result

    def _extract_dataflow_nodes(self, definition: Any) -> list[dict]:
        """Extract node-like dicts from a dataflow recipe.

        TCRM recipes can nest nodes under definition.nodes, transformations,
        or as a top-level list. We grab all of them.
        """
        candidates: list[dict] = []
        if not isinstance(definition, dict):
            return candidates
        for path in ("nodes", "definition.nodes", "transformations", "actions"):
            node = definition
            for key in path.split("."):
                if isinstance(node, dict):
                    node = node.get(key, {})
                else:
                    node = {}
            if isinstance(node, list):
                candidates.extend([n for n in node if isinstance(n, dict)])
            elif isinstance(node, dict):
                # Some TCRM recipes nest under "nodes" with subkeys.
                for v in node.values():
                    if isinstance(v, dict):
                        candidates.append(v)
        return candidates

    # =========================================================================
    # Replicated dataset scanning
    # =========================================================================

    async def _scan_replicated_datasets(
        self,
        search_term: str,
        mode: MatchMode,
        graph: AssetDependencyGraph | None,
    ) -> list[ReplicatedDatasetFieldAnalysisResult]:
        """Scan replicated datasets (connected objects) for field matches."""
        try:
            resp = await self.client.list_replicated_datasets()
        except Exception as e:
            logger.warning("list_replicated_failed", error=str(e))
            return []

        all_rd = resp.get("replicatedDatasets", []) or []
        if graph is not None:
            graph_rd_ids = {
                n.id for n in graph.filter_by_type(AssetType.REPLICATED_DATASET)
            }
            if graph_rd_ids:
                all_rd = [d for d in all_rd if d.get("id") in graph_rd_ids]

        if not all_rd:
            return []

        tasks = [self._scan_one_replicated(d, search_term, mode) for d in all_rd]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        return [r for r in results if isinstance(r, ReplicatedDatasetFieldAnalysisResult)]

    async def _scan_one_replicated(
        self, dataset: dict, search_term: str, mode: MatchMode
    ) -> ReplicatedDatasetFieldAnalysisResult:
        rd_id = dataset.get("id", "")
        object_name = dataset.get("name", "")

        start = time.monotonic()
        result = ReplicatedDatasetFieldAnalysisResult(
            replicated_dataset_id=rd_id,
            object_name=object_name,
            object_label=dataset.get("label"),
        )

        try:
            fields_resp = await self.client.get_replicated_dataset_fields(rd_id)
        except SalesforceNotFoundError:
            # Replicated datasets may disappear - skip silently.
            return result
        except SalesforceAPIError as e:
            logger.debug("replicated_fields_failed", rd_id=rd_id, error=str(e))
            return result

        fields = fields_resp.get("fields", []) or []
        result.total_fields_scanned = len(fields)

        for f in fields:
            api = f.get("field", "") or f.get("name", "")
            label = f.get("label", "")
            if not api:
                continue
            mtype, score, _ = self.matcher.match_field(search_term, api, label, mode)
            if mtype is None:
                continue
            result.matches.append(
                ReplicatedDatasetField(
                    field_api_name=api,
                    field_label=label or None,
                    field_type=f.get("type"),
                    is_nillable=f.get("isNillable"),
                    is_unique=f.get("isUnique"),
                    replicated_dataset_id=rd_id,
                    object_name=object_name,
                    match_type=mtype,
                    match_score=score,
                )
            )

        result.scan_duration_ms = int((time.monotonic() - start) * 1000)
        return result

    # =========================================================================
    # Summary & helpers
    # =========================================================================

    def _build_summary(self, details: FieldImpactDetail) -> FieldImpactSummary:
        ds_with_matches = [d for d in details.datasets if d.match_count > 0]
        db_with_matches = [d for d in details.dashboards if d.match_count > 0]
        df_with_matches = [d for d in details.dataflows if d.match_count > 0]
        rd_with_matches = [d for d in details.replicated_datasets if d.match_count > 0]

        all_matches = (
            [m for d in ds_with_matches for m in d.matches]
            + [m for d in db_with_matches for m in d.matches]
            + [m for d in df_with_matches for m in d.matches]
            + [m for d in rd_with_matches for m in d.matches]
        )
        exact = sum(1 for m in all_matches if m.match_type == MatchType.EXACT)
        fuzzy = sum(1 for m in all_matches if m.match_type == MatchType.FUZZY)

        by_type = {
            AssetType.DATASET: len(ds_with_matches),
            AssetType.DASHBOARD: len(db_with_matches),
            AssetType.DATAFLOW: len(df_with_matches),
            AssetType.REPLICATED_DATASET: len(rd_with_matches),
        }

        return FieldImpactSummary(
            total_assets_scanned=(
                len(details.datasets)
                + len(details.dashboards)
                + len(details.dataflows)
                + len(details.replicated_datasets)
            ),
            total_matches=len(all_matches),
            exact_matches=exact,
            fuzzy_matches=fuzzy,
            by_asset_type=by_type,
            datasets_scanned=len(details.datasets),
            dashboards_scanned=len(details.dashboards),
            dataflows_scanned=len(details.dataflows),
            replicated_datasets_scanned=len(details.replicated_datasets),
        )

    @staticmethod
    def _extract_page_token(url: str) -> str | None:
        """Extract pageToken query param from a next-page URL."""
        if not url:
            return None
        if "pageToken=" not in url:
            return None
        try:
            after = url.split("pageToken=", 1)[1]
            return after.split("&", 1)[0]
        except Exception:
            return None
