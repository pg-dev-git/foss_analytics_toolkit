# Field Impact Analysis Crawler — Phased Execution Plan

**Branch**: `feature/asftool-refactor`  
**Target**: Build a "Field Impact Analysis Crawler" feature for ASFTool that accepts a field API name or label and traverses Salesforce CRM Analytics assets to locate where the field is used (datasets, dashboards, dataflows, replicated datasets), employing both exact and fuzzy matching.

---

## Phase Completion Log

| Phase | Status | Date | Commit | Tests | Notes |
|-------|--------|------|--------|-------|-------|
| **1: Foundation & Pydantic Models** | ✅ Complete | 2026-07-23 | 5aa98c2 | 29/29 | Models for all 4 asset types, dependency graph, impact report |
| **2: Core Async API Methods** | ✅ Complete | 2026-07-23 | TBD | 21/21 | 6 new client methods, URL construction, error handling |
| **3: Crawler Engine & Fuzzy Matching** | ⏳ Pending | — | — | — | — |
| **4: CLI Presentation Layer** | ⏳ Pending | — | — | — | — |

### Phase 1 Details
- **Files added**: `asftool/core/models/field_impact.py`, `tests/unit/test_field_impact_models.py`
- **Files modified**: `pyproject.toml` (added `thefuzz[speedup]>=0.22.0`), `asftool/core/models/__init__.py` (export new models + `__all__`)
- **Models created**: `MatchType`, `AssetType`, `MatchMode` (StrEnums); `AssetDependency`, `AssetDependencyGraph`; `DatasetFieldReference`, `DatasetFieldAnalysisResult`; `DashboardFieldReference`, `DashboardFieldAnalysisResult`; `DataflowFieldReference`, `DataflowFieldAnalysisResult`; `ReplicatedDatasetField`, `ReplicatedDatasetFieldAnalysisResult`; `FieldImpactScope`, `FieldImpactSummary`, `FieldImpactDetail`, `FieldImpactReport`
- **Graph methods**: `add_node`, `add_edge`, `get_children`, `get_parents`, `get_all_downstream`, `get_all_upstream`, `filter_by_type`
- **Validation**: score bounds (0-100) enforced via Pydantic `Field(ge=0, le=100)`

### Phase 2 Details
- **Files modified**: `asftool/core/client.py` (added 6 new methods + `get_dataflow_definition`)
- **Files added**: `tests/unit/test_client_field_impact.py` (21 tests)
- **New client methods**:
  - `list_applications()` → `GET /wave/applications`
  - `get_application_dependencies(app_id)` → `GET /wave/applications/{id}/dependencies`
  - `get_dashboard_full(dashboard_id)` → `GET /wave/dashboards/{id}` (full JSON for widget scanning)
  - `get_dataflow_definition(dataflow_id)` → `GET /wave/dataflows/{id}` (full recipe JSON)
  - `list_replicated_datasets()` → `GET /wave/replicatedDatasets`
  - `get_replicated_dataset_fields(rep_id)` → `GET /wave/replicatedDatasets/{id}/fields`
- **Resilience**: All methods inherit retry logic via `self._request` → `self.retry_client` (exponential jitter, 3 attempts, transient errors only)
- **Test coverage**: success paths, empty results, 404/403/500 errors, URL construction, retry client configured

---

## Architecture Alignment

This plan strictly follows the ASFTool architectural constraints:
- **Async-First**: All API interactions use `async/await` and `httpx.AsyncClient` (no threading)
- **Pydantic Models**: All data validation/serialization via Pydantic v2 models in `asftool/core/models/`
- **Resilience**: `@retry` via `tenacity`, structured logging via `structlog`
- **Layered Dependency**: `cli/` → `core/tasks/` → `core/services/` → `core/`
- **Menu-Driven CLI**: Rich + questionary "always running OS" style (no TUI framework)

---

## Required REST API Endpoints to Integrate

| Endpoint | Purpose | Current Status |
|----------|---------|----------------|
| `/wave/dependencies/{applicationId}` | Asset dependencies for application graph | ❌ Not implemented |
| `/wave/datasets/{id}/versions/{versionId}/xmds/main` | Dataset XMD (field identifiers/labels) | ✅ Implemented in client |
| `/wave/dashboards/{id}` | Dashboard JSON metadata (scan for field refs) | ✅ Implemented in client |
| `/wave/replicatedDatasets/{replicatedDatasetId}/fields` | Replicated dataset fields tracing | ❌ Not implemented |
| `/wave/dashboards/{id}/datasets` | Dashboard → Dataset linkage | ✅ Implemented in client |
| `/wave/dataflows` | Dataflow listing (for recipe field scanning) | ✅ Implemented in client |

---

## Phase 1: Foundation & Pydantic Data Models

**Objective**: Define all Pydantic schemas for endpoint responses and the final impact report.

### Sub-tasks

#### 1.1 Add `thefuzz` dependency
```toml
# pyproject.toml - add to dependencies
"thefuzz[speedup]>=0.22.0",  # For fuzzy string matching (RapidFuzz backend)
```

#### 1.2 Create Field Impact Models (`asftool/core/models/field_impact.py` — NEW FILE)

**Models for Asset Dependencies:**
- `AssetDependency` — Node in dependency graph (id, name, type, label, parent_id)
- `AssetDependencyGraph` — Graph structure with nodes/edges, traversal methods

**Models for Dataset Field Analysis:**
- `DatasetFieldReference` — Field found in dataset (field_api_name, label, type, dataset_id, dataset_name, version_id, match_type: "exact"|"fuzzy", match_score)
- `DatasetFieldAnalysisResult` — Aggregated result per dataset

**Models for Dashboard Field Analysis:**
- `DashboardFieldReference` — Field reference in dashboard (widget_id, widget_type, field_path, dashboard_id, dashboard_name, match_type, match_score)
- `DashboardFieldAnalysisResult` — Aggregated result per dashboard

**Models for Dataflow Field Analysis:**
- `DataflowFieldReference` — Field reference in dataflow (node_id, node_type, field_name, dataflow_id, dataflow_name, match_type, match_score)
- `DataflowFieldAnalysisResult` — Aggregated result per dataflow

**Models for Replicated Dataset Field Analysis:**
- `ReplicatedDatasetField` — Field in replicated dataset (field_api_name, label, type, replicated_dataset_id, object_name, match_type, match_score)
- `ReplicatedDatasetFieldAnalysisResult` — Aggregated result per replicated dataset

**Models for Final Impact Report:**
- `FieldImpactScope` — Input parameters (search_term, match_mode: "exact"|"fuzzy", fuzzy_threshold, include_datasets, include_dashboards, include_dataflows, include_replicated)
- `FieldImpactSummary` — High-level counts (total_assets_scanned, total_matches, exact_matches, fuzzy_matches, by_asset_type)
- `FieldImpactDetail` — Per-asset detailed findings
- `FieldImpactReport` — Complete report (scope, summary, details, generated_at, execution_time_ms)

**Enums:**
- `MatchType` = Literal["exact", "fuzzy"]
- `AssetType` = Literal["dataset", "dashboard", "dataflow", "replicated_dataset"]
- `MatchMode` = Literal["exact", "fuzzy", "both"]

#### 1.3 Export new models from `asftool/core/models/__init__.py`

#### 1.4 Unit Tests for Models (`tests/unit/test_field_impact_models.py` — NEW FILE)
- Test model validation with sample API responses
- Test enum constraints
- Test computed properties
- Test serialization/deserialization round-trip

---

## Phase 2: Core Async API Methods

**Objective**: Extend `SalesforceClient` with new async methods for all required endpoints, wrapped with `tenacity` retry logic.

### Sub-tasks

#### 2.1 Add Application Dependencies Method (`asftool/core/client.py`)
```python
async def get_application_dependencies(self, application_id: str) -> Any:
    """Get asset dependencies for an application (builds the dependency graph)."""
    response = await self.get(f"{self.wave_base_url}/applications/{application_id}/dependencies")
    return response.json()
```
- Add retry for transient failures (same policy as existing methods)
- Structured logging for request/response

#### 2.2 Add Dashboard Full Metadata Method (`asftool/core/client.py`)
```python
async def get_dashboard_full(self, dashboard_id: str) -> Any:
    """Get full dashboard JSON including widgets, steps, and field references."""
    response = await self.get(f"{self.wave_base_url}/dashboards/{dashboard_id}")
    return response.json()
```
- Reuses existing `get_dashboard` but ensures full JSON returned

#### 2.3 Add Replicated Dataset Fields Method (`asftool/core/client.py`)
```python
async def get_replicated_dataset_fields(self, replicated_dataset_id: str) -> Any:
    """Get fields for a replicated dataset (connected object fields)."""
    response = await self.get(f"{self.wave_base_url}/replicatedDatasets/{replicated_dataset_id}/fields")
    return response.json()
```
- Handle 404 gracefully (replicated datasets may not exist)

#### 2.4 Add Dataflow Definition Method (`asftool/core/client.py`)
```python
async def get_dataflow_definition(self, dataflow_id: str) -> Any:
    """Get full dataflow definition (JSON recipe) for field scanning."""
    response = await self.get(f"{self.wave_base_url}/dataflows/{dataflow_id}")
    return response.json()
```

#### 2.5 Add List Replicated Datasets Method (`asftool/core/client.py`)
```python
async def list_replicated_datasets(self) -> Any:
    """List all replicated datasets."""
    response = await self.get(f"{self.wave_base_url}/replicatedDatasets")
    return response.json()
```

#### 2.6 Add List Applications Method (`asftool/core/client.py`)
```python
async def list_applications(self) -> Any:
    """List all Analytics applications."""
    response = await self.get(f"{self.wave_base_url}/applications")
    return response.json()
```

#### 2.7 Unit Tests for New Client Methods (`tests/unit/test_client_field_impact.py` — NEW FILE)
- Mock HTTP responses for each new endpoint
- Test retry behavior on 429/5xx
- Test error handling for 404/403
- Verify URL construction and headers

---

## Phase 3: Crawler Engine & Fuzzy Matching

**Objective**: Build the core graph traversal logic and text comparison using `difflib` and `thefuzz`.

### Sub-tasks

#### 3.1 Create Field Matcher Utility (`asftool/core/services/field_matcher.py` — NEW FILE)

**Exact Matching:**
```python
def exact_match(search_term: str, target: str, case_sensitive: bool = False) -> bool
```

**Fuzzy Matching (using thefuzz/RapidFuzz):**
```python
def fuzzy_match(
    search_term: str, 
    target: str, 
    threshold: int = 85,  # 0-100
    scorer: str = "ratio"  # ratio, partial_ratio, token_sort_ratio, token_set_ratio
) -> tuple[bool, int]:  # (matched, score)
```

**Multi-field Matcher:**
```python
def match_field(
    search_term: str,
    field_api_name: str,
    field_label: str | None = None,
    threshold: int = 85,
    mode: MatchMode = "both",
) -> tuple[MatchType | None, int]:
    """Returns (match_type, score) or (None, 0) if no match."""
```

#### 3.2 Create Asset Graph Builder (`asftool/core/services/asset_graph.py` — NEW FILE)

**Graph Traversal:**
```python
class AssetGraph:
    def __init__(self):
        self.nodes: dict[str, AssetDependency] = {}
        self.edges: dict[str, list[str]] = {}  # parent_id -> [child_ids]

    async def build_from_application(self, client: SalesforceClient, application_id: str) -> "AssetGraph":
        """Fetch and build full dependency graph for an application."""

    def find_upstream(self, node_id: str) -> list[AssetDependency]:
        """Find all upstream dependencies."""

    def find_downstream(self, node_id: str) -> list[AssetDependency]:
        """Find all downstream dependents."""

    def get_all_assets_by_type(self, asset_type: AssetType) -> list[AssetDependency]:
        """Filter nodes by type."""
```

#### 3.3 Create Field Impact Service (`asftool/core/services/field_impact_service.py` — NEW FILE)

**Main Entry Point:**
```python
class FieldImpactService:
    def __init__(self, client: SalesforceClient, settings: Settings | None = None):
        self.client = client
        self.settings = settings or get_settings()
        self.matcher = FieldMatcher()
        self.graph_builder = AssetGraph()

    async def analyze_field_impact(
        self,
        search_term: str,
        match_mode: MatchMode = "both",
        fuzzy_threshold: int = 85,
        include_datasets: bool = True,
        include_dashboards: bool = True,
        include_dataflows: bool = True,
        include_replicated: bool = True,
        application_id: str | None = None,  # If provided, limit to app's dependency graph
        progress_callback: Callable | None = None,
    ) -> FieldImpactReport:
        """Main analysis entry point."""
```

**Dataset Scanning:**
```python
async def _scan_datasets(self, search_term: str, match_mode: MatchMode, threshold: int) -> list[DatasetFieldAnalysisResult]:
    """Scan all datasets (or filtered by graph) for field matches via XMD."""
    # 1. List datasets (or get from graph)
    # 2. For each dataset, get XMD
    # 3. Extract fields from XMD (measures, dimensions, dates)
    # 4. Match against search_term (api_name and label)
    # 5. Return structured results
```

**Dashboard Scanning:**
```python
async def _scan_dashboards(self, search_term: str, match_mode: MatchMode, threshold: int) -> list[DashboardFieldAnalysisResult]:
    """Scan dashboards for field references in widgets/steps."""
    # 1. List dashboards
    # 2. For each dashboard, get full JSON
    # 3. Traverse widgets/steps to find field references
    # 4. Match against search_term
    # 5. Return structured results with widget context
```

**Dataflow Scanning:**
```python
async def _scan_dataflows(self, search_term: str, match_mode: MatchMode, threshold: int) -> list[DataflowFieldAnalysisResult]:
    """Scan dataflow recipes for field references."""
    # 1. List dataflows
    # 2. For each, get full definition (JSON)
    # 3. Traverse nodes (transforms, loads, etc.) for field names
    # 4. Match against search_term
    # 5. Return structured results
```

**Replicated Dataset Scanning:**
```python
async def _scan_replicated_datasets(self, search_term: str, match_mode: MatchMode, threshold: int) -> list[ReplicatedDatasetFieldAnalysisResult]:
    """Scan replicated datasets (connected objects) for field matches."""
    # 1. List replicated datasets
    # 2. For each, get fields via /replicatedDatasets/{id}/fields
    # 3. Match against search_term
    # 4. Return structured results
```

**Graph-Limited Scanning:**
```python
async def _scan_with_graph_limit(self, search_term: str, application_id: str, ...) -> FieldImpactReport:
    """If application_id provided, build graph and only scan assets in graph."""
    graph = await self.graph_builder.build_from_application(self.client, application_id)
    # Filter asset lists by graph nodes
    # Proceed with scanning
```

#### 3.4 Parallel Execution (Optional - using existing TaskRunner)
- Use `core/tasks/runner.py` `TaskRunner` for parallel dataset/dashboard scanning
- Semaphore for rate limiting (max 10 concurrent)

#### 3.5 Unit Tests for Crawler Engine (`tests/unit/test_field_impact_service.py` — NEW FILE)
- Test exact matching (case sensitive/insensitive)
- Test fuzzy matching with various scorers and thresholds
- Test field matcher with api_name + label
- Test graph builder with mocked dependency responses
- Test service integration with mocked client (mock all scan methods)
- Test empty results, partial matches, threshold boundaries

---

## Phase 4: CLI Presentation Layer

**Objective**: Expose the crawler via Typer CLI commands and Rich interactive menu.

### Sub-tasks

#### 4.1 Add Field Matcher Dependency
```bash
uv add thefuzz[speedup]
```

#### 4.2 Create CLI Commands (`asftool/cli/commands/fields.py` — NEW FILE)

```python
"""Field Impact Analysis CLI commands."""

app = typer.Typer(help="Field impact analysis across TCRM assets")

@app.command("analyze")
def analyze_field(
    search_term: str = typer.Argument(..., help="Field API name or label to search for"),
    mode: MatchMode = typer.Option("both", "--mode", "-m", help="Match mode: exact, fuzzy, both"),
    threshold: int = typer.Option(85, "--threshold", "-t", help="Fuzzy match threshold (0-100)"),
    application: str | None = typer.Option(None, "--app", "-a", help="Limit to application dependency graph"),
    output: Path | None = typer.Option(None, "--output", "-o", help="Output report path (JSON)"),
    format: Literal["table", "json", "summary"] = typer.Option("table", "--format", "-f", help="Output format"),
    include_datasets: bool = typer.Option(True, "--datasets/--no-datasets"),
    include_dashboards: bool = typer.Option(True, "--dashboards/--no-dashboards"),
    include_dataflows: bool = typer.Option(True, "--dataflows/--no-dataflows"),
    include_replicated: bool = typer.Option(True, "--replicated/--no-replicated"),
):
    """Analyze field impact across all TCRM assets."""
    _run(analyze_field_async(...))

@app.command("scan-dataset")
def scan_dataset(
    dataset_id: str = typer.Argument(..., help="Dataset ID"),
    search_term: str = typer.Argument(..., help="Field API name or label"),
    mode: MatchMode = typer.Option("both", "--mode", "-m"),
    threshold: int = typer.Option(85, "--threshold", "-t"),
):
    """Scan a single dataset for field matches."""
    _run(scan_dataset_async(...))
```

#### 4.3 Create Async Wrappers (following existing pattern)
```python
async def analyze_field_async(...):
    session = Session()
    try:
        async with session.client_context() as client:
            service = FieldImpactService(client, session.settings)
            report = await service.analyze_field_impact(...)
            # Format and output based on format option
    finally:
        await session.close()
```

#### 4.4 Create Menu (`asftool/cli/menus/fields.py` — NEW FILE)

```python
def field_operations(menu: Menu) -> None:
    menu.add(MenuItem("1", "Analyze field impact (all assets)", handler=analyze_field))
    menu.add(MenuItem("2", "Scan single dataset", handler=scan_dataset))
    menu.add(MenuItem("3", "Scan single dashboard", handler=scan_dashboard))
    menu.add(MenuItem("4", "Scan single dataflow", handler=scan_dataflow))
    menu.add(MenuItem("5", "Scan replicated datasets", handler=scan_replicated))
    menu.add(MenuItem("b", "Back", exit_after=True))
```

#### 4.5 Wire into Main Menu (`asftool/cli/menu.py`)
```python
# In create_menus():
fields_menu = main.add_submenu("7", "🔍  Field Impact Analysis")
# ...
from asftool.cli.menus.fields import field_operations
field_operations(fields_menu)

all_menus["fields"] = fields_menu
```

#### 4.6 Register CLI Command Group (`asftool/cli/main.py`)
```python
from asftool.cli.commands.fields import app as fields_app
app.add_typer(fields_app, name="fields")
```

#### 4.7 Integration Tests (`tests/integration/test_field_impact_cli.py` — NEW FILE)
- Test CLI command parsing
- Test menu handler invocation
- Test output formatting (table, JSON, summary)
- Test progress callback integration

---

## Cross-Cutting Concerns (Apply to All Phases)

### Logging
- Use `structlog.get_logger(__name__)` in all new modules
- Log key operations: scan start/end, matches found, errors
- Include structured context: `asset_type`, `asset_id`, `match_type`, `score`

### Error Handling
- Define `FieldImpactError` in `asftool/core/exceptions.py`
- Wrap all external API calls in try/except
- Return partial results on individual asset failures (don't fail entire scan)

### Configuration
- Add settings in `asftool/core/config.py`:
  ```python
  field_impact_default_fuzzy_threshold: int = 85
  field_impact_max_concurrent_scans: int = 10
  field_impact_default_match_mode: str = "both"
  ```

### Type Hints
- Full type hints on all public functions
- Use `TypedDict` for complex dict structures from API
- Avoid `Any` where possible

---

## Acceptance Criteria

### Phase 1 Complete When:
- [x] `thefuzz` added to pyproject.toml
- [x] All Pydantic models defined and exported
- [x] Models validate against sample API responses
- [x] Unit tests pass (`uv run pytest tests/unit/test_field_impact_models.py -v`) — **29 tests passing**
- [x] `uv run mypy asftool` clean — **no issues**
- [x] `uv run ruff check` clean on changed files — **all passed**

### Phase 2 Complete When:
- [x] All 6 new client methods implemented with retry
- [x] Client methods handle errors gracefully
- [x] Unit tests pass for new client methods — **21/21 tests passing**
- [x] `uv run mypy asftool` clean — **no issues**

### Phase 3 Complete When:
- [ ] FieldMatcher handles exact + fuzzy with configurable threshold
- [ ] AssetGraph builds and traverses correctly
- [ ] FieldImpactService orchestrates all 4 scan types
- [ ] Parallel execution works (if implemented)
- [ ] Unit tests pass for matcher, graph, service
- [ ] Integration tests pass with mocked client

### Phase 4 Complete When:
- [ ] `asftool fields analyze --help` works
- [ ] `asftool fields scan-dataset --help` works
- [ ] Interactive menu shows "Field Impact Analysis" option
- [ ] Menu handlers invoke async wrappers correctly
- [ ] Output formats (table, JSON, summary) render correctly
- [ ] All tests pass: `uv run pytest -v`
- [ ] Lint clean: `uv run ruff check .`
- [ ] Type check clean: `uv run mypy asftool`

---

## Implementation Notes

### Reusing Existing Patterns
- Follow `DatasetService` pattern for service structure
- Follow `cli/commands/datasets.py` pattern for CLI commands
- Follow `cli/menus/datasets.py` pattern for menus
- Use `Session` + `client_context()` for auth/client lifecycle

### Fuzzy Matching Strategy
- Primary: `thefuzz.fuzz.token_set_ratio` (handles word order, partial matches)
- Fallback: `difflib.SequenceMatcher` for zero-dependency option
- Threshold: 85 default (configurable), scores 0-100
- Match both API name AND label; return best score

### Graph Traversal Strategy
- Start from application → get dependencies (downstream: datasets, dashboards, dataflows)
- Recursively fetch dependencies for each asset
- Build bidirectional graph for upstream/downstream queries
- Cache graph during single analysis run

### Performance Considerations
- Parallelize independent asset scans (datasets, dashboards, dataflows)
- Rate limit: semaphore(10) + httpx connection pool(10)
- Skip assets without field metadata (e.g., empty datasets)
- Progress callback for long-running scans

### Future Enhancements (Not in Scope)
- Export to CSV/Excel
- Visual graph output (GraphViz/D3)
- Scheduled impact analysis
- Webhook notifications on schema changes
- Field lineage tracking across dataflow transforms

---

## File Summary

### New Files
| Phase | File | Purpose |
|-------|------|---------|
| 1 | `asftool/core/models/field_impact.py` | Pydantic models for field impact |
| 1 | `tests/unit/test_field_impact_models.py` | Model unit tests |
| 2 | `tests/unit/test_client_field_impact.py` | Client method tests |
| 3 | `asftool/core/services/field_matcher.py` | Fuzzy/exact matching logic |
| 3 | `asftool/core/services/asset_graph.py` | Dependency graph builder |
| 3 | `asftool/core/services/field_impact_service.py` | Main crawler service |
| 3 | `tests/unit/test_field_impact_service.py` | Service unit tests |
| 4 | `asftool/cli/commands/fields.py` | Typer CLI commands |
| 4 | `asftool/cli/menus/fields.py` | Interactive menu |
| 4 | `tests/integration/test_field_impact_cli.py` | CLI integration tests |

### Modified Files
| Phase | File | Changes |
|-------|------|---------|
| 1 | `pyproject.toml` | Add `thefuzz` dependency |
| 1 | `asftool/core/models/__init__.py` | Export new models |
| 2 | `asftool/core/client.py` | Add 6 new API methods |
| 2 | `asftool/core/config.py` | Add field impact settings |
| 2 | `asftool/core/exceptions.py` | Add `FieldImpactError` |
| 4 | `asftool/cli/main.py` | Register fields command group |
| 4 | `asftool/cli/menu.py` | Wire fields menu |

---

## Next Steps

**Awaiting approval to begin Phase 1 implementation.**

Once approved, I will:
1. Add `thefuzz` dependency
2. Create `field_impact.py` models file
3. Export models from `__init__.py`
4. Write model unit tests
5. Run validation checks