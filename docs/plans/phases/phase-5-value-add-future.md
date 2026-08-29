# Phase 5: Value-Add Features (Future)

**Document**: `docs/plans/phases/phase-5-value-add-future.md`  
**Status**: Planned for future implementation (Post-MVP)  
**Branch**: N/A (features to be implemented after Phase 4)  
**Depends on**: Phase 4 complete

---

## 🎯 Objective

Document high-value features that extend beyond the MVP Interactive TUI. These features are **parked** for future implementation based on user feedback and prioritization. They represent the "power user" capabilities that make this toolkit indispensable for Salesforce Analytics administrators and developers.

---

## 📋 Value-Add Features Overview

| Feature | Priority | Description | Estimated Effort |
|---------|----------|-------------|------------------|
| **Bulk Backup/Restore** | High | One-click backup/restore of all dashboards, datasets, dataflows | 3-5 days |
| **Dataset Diff/Compare** | High | Compare two datasets or two versions (schema + data) | 5-7 days |
| **Dashboard Versioning** | Medium | Git-like history for dashboards with visual diff | 7-10 days |
| **Data Lineage Graph** | Medium | Visual graph: Dataflow → Dataset → Dashboard → Lens | 10-14 days |
| **Scheduled Operations** | Medium | Cron-style: "Extract dataset X daily at 2am" | 5-7 days |
| **Multi-Org Dashboard Sync** | High | Promote dashboards Sandbox → Prod with ID remapping | 7-10 days |
| **Export Formats** | Medium | CSV, JSON, Parquet, Avro, Excel, SQL INSERTs | 3-5 days |
| **Smart Alerts** | Medium | "Alert me when dataflow fails", "API usage > 80%" | 5-7 days |
| **Operation Scripts** | Low | Record UI actions → replay as YAML/JSON script | 5-7 days |
| **Plugin System** | Low | Custom Python plugins via entry points | 10-14 days |

---

## 🔧 Detailed Feature Specifications

### 1. Bulk Backup/Restore

**Description**: Backup or restore all analytics metadata with a single command, preserving folder structure and dependencies.

**Components**:
- **Backup All**: 
  - Dashboards → JSON files in folder structure
  - Datasets → CSV + metadata JSON
  - Dataflows → JSON definitions
  - Optional: Compress to ZIP/GZIP
- **Restore All**:
  - Restore from backup directory
  - Handle ID remapping (optional)
  - Preserve folder structure
  - Dependency-aware restore order

**Implementation**:
```python
# tcrm_toolkit/interactive/operations/bulk_ops.py
class BulkOperationsManager:
    async def backup_all(
        self,
        output_dir: Path,
        include_datasets: bool = True,
        include_dashboards: bool = True,
        include_dataflows: bool = True,
        compress: bool = False,
    ) -> BulkBackupResult:
        # Implementation using existing services with progress tracking
    
    async def restore_all(
        self,
        backup_dir: Path,
        remap_ids: bool = False,
        dry_run: bool = False,
    ) -> BulkRestoreResult:
        # Implementation with dependency resolution
```

**Value**: Reduces hours of manual work to minutes. Essential for migration, disaster recovery, and version control.

---

### 2. Dataset Diff/Compare

**Description**: Compare two datasets or two versions of the same dataset, showing schema and data differences.

**Components**:
- **Schema Diff**: 
  - Field additions/removals
  - Type changes
  - Label/description changes
  - Nullability changes
- **Data Diff** (sample-based for large datasets):
  - Row count difference
  - Value distribution comparison
  - Sample row differences (first/last 1000 rows)
  - Statistical comparison (min/max/avg/stddev)

**Implementation**:
```python
# tcrm_toolkit/interactive/operations/dataset_diff.py
class DatasetDiffEngine:
    async def compare_datasets(
        self,
        dataset_id_a: str,
        dataset_id_b: str,
        version_a: str | None = None,
        version_b: str | None = None,
        sample_size: int = 10000,
    ) -> DatasetDiffResult:
        # Compare XMD/schema
        # Sample data for comparison if needed
        # Generate human-readable and machine-readable diff
```

**Value**: Essential for development, testing, and change management. Answers "What changed between these datasets?"

---

### 3. Dashboard Versioning

**Description**: Git-like version control for dashboards with branching, merging, and visual diff.

**Components**:
- **Version Storage**: 
  - Store dashboard JSON in local Git repo or database
  - Metadata: timestamp, user, description, tags
- **Visual Diff**:
  - Side-by-side JSON comparison
  - Widget-level diff (added/removed/modified)
  - Dataset usage changes
- **Branching**:
  - Create branches for experimentation
  - Merge changes between branches
  - Conflict resolution

**Implementation**:
```python
# tcrm_toolkit/interactive/operations/dashboard_version.py
class DashboardVersionControl:
    async def create_version(
        self,
        dashboard_id: str,
        description: str = "",
        tags: list[str] = [],
    ) -> DashboardVersion:
        # Store current dashboard state
    
    async def diff_versions(
        self,
        version_a: str,
        version_b: str,
    ) -> DashboardDiff:
        # Generate visual diff
    
    async def checkout_version(
        self,
        dashboard_id: str,
        version_id: str,
    ) -> None:
        # Restore dashboard to specific version
```

**Value**: Enables safe experimentation, rollback, and collaboration on dashboard development.

---

### 4. Data Lineage Graph

**Description**: Interactive visualization of data flow from source to consumption.

**Components**:
- **Graph Nodes**:
  - Dataflows (processing steps)
  - Datasets (storage)
  - Dashboards (consumption)
  - Lenses/Charts (visualization)
  - External Sources (if available)
- **Graph Edges**:
  - Dataflow → Dataset (output)
  - Dataset → Dataflow (input)
  - Dataset → Dashboard (source)
  - Dataset → Lens (source)
- **Interactivity**:
  - Zoom/pan/navigate
  - Node details on hover/click
  - Filter by type/status
  - Export as image/JSON

**Implementation**:
```python
# tcrm_toolkit/interactive/operations/lineage.py
class LineageEngine:
    async def build_lineage_graph(
        self,
        root_ids: list[str] | None = None,
        include_external: bool = False,
    ) -> LineageGraph:
        # Traverse dependencies outward and inward
        # Build nodes and edges
    
    async def get_upstream_lineage(
        self,
        item_id: str,
        item_type: Literal["dataset", "dashboard", "lens"],
        max_depth: int = 10,
    ) -> LineageSubgraph:
        # Find all sources that feed into this item
    
    async def get_downstream_lineage(
        self,
        item_id: str,
        item_type: Literal["dataset", "dashboard", "lens"],
        max_depth: int = 10,
    ) -> LineageSubgraph:
        # Find all items that consume this item
```

**Value**: Critical for impact analysis ("What happens if I change this field?") and debugging data issues.

---

### 5. Scheduled Operations

**Description**: Cron-style scheduling for recurring operations.

**Components**:
- **Components**:
- **Schedule Definition**:
  - Cron expressions (standard format)
  - Timezone support
  - Retry policies
  - Notification on completion/failure
- **Supported Operations**:
  - Extract dataset → CSV/JSON/Parquet
  - Upload CSV → Dataset
  - Backup dashboard/dataset/dataflow
  - Run dataflow
  - Custom scripts
- **Execution Engine**:
  - Background scheduler (APScheduler or custom)
  - Persistent job store (SQLite)
  - Concurrent execution limits
  - Logging and audit trail

**Implementation**:
```python
# tcrm_toolkit/interactive/operations/scheduler.py
class OperationScheduler:
    async def schedule_operation(
        self,
        cron_expression: str,
        operation: str,
        parameters: dict,
        timezone: str = "UTC",
    ) -> ScheduledOperation:
        # Create scheduled job
    
    async def run_scheduled(self, scheduled_op: ScheduledOperation) -> OperationResult:
        # Execute the operation
    
    async def list_scheduled(self) -> list[ScheduledOperation]:
        # Get all scheduled operations
```

**Value**: Automates repetitive tasks like daily extracts, weekly backups, monthly reports.

---

### 6. Multi-Org Dashboard Sync

**Description**: Promote dashboards between orgs (e.g., Sandbox → Production) with intelligent ID remapping.

**Components**:
- **Dependency Analysis**:
  - Identify all datasets used by dashboard
  - Map source org IDs → target org IDs
  - Handle folder structure differences
- **ID Remapping**:
  - Dataset IDs in dashboard JSON
  - Dataset references in widgets/queries
  - Folder IDs
- **Conflict Resolution**:
  - Handle existing dashboard with same name
  - Option to overwrite, rename, or skip
  - Backup before overwrite

**Implementation**:
```python
# tcrm_toolkit/interactive/operations/org_sync.py
class OrgSyncManager:
    async def sync_dashboard(
        self,
        source_dashboard_id: str,
        target_org_alias: str,
        target_folder: str | None = None,
        conflict_resolution: Literal["skip", "rename", "overwrite"] = "skip",
    ) -> SyncResult:
        # Analyze dependencies
        # Remap IDs
        # Create in target org
    
    async def sync_bulk(
        self,
        source_org_alias: str,
        target_org_alias: str,
        pattern: str | None = None,
        include_dependencies: bool = True,
    ) -> BulkSyncResult:
        # Sync multiple dashboards
```

**Value**: Eliminates manual recreation of dashboards when promoting changes between environments.

---

### 7. Export Formats

**Description**: Support multiple export formats beyond CSV for different use cases.

**Components**:
- **Export Formats**:
  - CSV (current)
  - JSON (pretty and compact)
  - Parquet (columnar, efficient for analytics)
  - Avro (schema-based, good for streaming)
  - Excel (.xlsx) - for business users
  - SQL INSERT statements - for database loading
- **Format Selection**:
  - Per-operation basis
  - Default configurable
  - Automatic based on file extension
- **Metadata Preservation**:
  - Schema information in export (where format supports)
  - Data types and labels

**Implementation**:
```python
# tcrm_toolkit/interactive/operations/export.py
class ExportEngine:
    async def export_dataset(
        self,
        dataset_id: str,
        output_path: Path,
        format: Literal["csv", "json", "parquet", "avro", "excel", "sql"],
        options: dict = {},
    ) -> ExportResult:
        # Extract dataset then convert to target format
    
    async def export_dashboard(
        self,
        dashboard_id: str,
        output_path: Path,
        format: Literal["json", "yaml"],
    ) -> ExportResult:
        # Export dashboard definition
```

**Value**: Enables integration with other systems (data warehouses, BI tools, databases).

---

### 8. Smart Alerts

**Description**: Proactive monitoring and alerting for critical conditions.

**Components**:
- **Alert Types**:
  - Dataflow failure
  - Dataset row count anomaly (sudden drop/spike)
  - API usage threshold (e.g., >80% of daily limit)
  - Dashboard load time degradation
  - Failed login attempts
- **Notification Channels**:
  - Email (SMTP)
  - Slack webhook
  - Microsoft Teams
  - PagerDuty
  - Webhook (custom)
- **Alert Management**:
  - Deduplication (avoid alert storms)
  - Escalation policies
  - Silence/maintenance windows
  - Alert history and analytics

**Implementation**:
```python
# tcrm_toolkit/interactive/operations/alerts.py
class AlertManager:
    async def check_alerts(self) -> list[Alert]:
        # Evaluate all alert conditions
    
    async def send_alert(
        self,
        alert: Alert,
        channels: list[NotificationChannel],
    ) -> None:
        # Send via configured channels
    
    async def schedule_checks(
        self,
        interval: int = 300,  # 5 minutes
    ) -> None:
        # Background alert checking
```

**Value**: Prevents surprises by notifying teams of issues before they impact business.

---

### 9. Operation Scripts

**Description**: Record and replay sequences of operations for automation and sharing.

**Components**:
- **Recording**:
  - Capture user actions in TUI
  - Generate YAML/JSON script
  - Include parameters and timing
- **Playback**:
  - Execute scripted operations
  - Variable substitution
  - Conditional logic (if/else)
  - Looping constructs
- **Script Library**:
  - Community-shared scripts
  - Version control integration
  - Script validation and testing

**Implementation**:
```python
# tcrm_toolkit/interactive/operations/scripting.py
class OperationScriptEngine:
    async def record_session(
        self,
        start_action: str,
        end_action: str,
    ) -> OperationScript:
        # Record user interactions
    
    async def play_script(
        self,
        script: OperationScript,
        variables: dict = {},
    ) -> ScriptExecutionResult:
        # Execute recorded operations
    
    async def validate_script(
        self,
        script: OperationScript,
    ) -> list[ValidationError]:
        # Check script for errors
```

**Value**: Enables sharing of complex procedures and reduces training time.

---

### 10. Plugin System

**Description**: Extensible architecture for custom operations and integrations.

**Components**:
- **Plugin Interface**:
  - Standard base class for plugins
  - Hook points (pre/post operation, menu items, etc.)
  - Access to services and session
- **Discovery**:
  - Entry points via `importlib.metadata`
  - Local plugin directory
  - Explicit plugin loading
- **Sandboxing**:
  - Restricted access to dangerous operations
  - Permission system (read-only, read-write, admin)
  - Isolation from core functionality
- **Marketplace**:
  - Official plugin repository
  - Installation/update mechanism
  - Rating and review system

**Implementation**:
```python
# tcrm_toolkit/interactive/plugins/base.py
class PluginBase:
    """Base class for all TUI plugins."""
    
    name: str
    version: str
    description: str
    author: str
    
    async def initialize(self, session: SessionManager) -> None:
        """Called when plugin is loaded."""
    
    async def cleanup(self) -> None:
        """Called when plugin is unloaded."""
    
    def get_menu_items(self) -> list[MenuItem]:
        """Return menu items to add to TUI."""
    
    def get_operations(self) -> dict[str, Callable]:
        """Return operations to add to command palette."""
```

**Value**: Enables community contributions and customization for specific org needs.

---

## 📈 Implementation Roadmap (Suggested)

### Release 1.1 (1-2 months post-MVP)
- Bulk Backup/Restore
- Dataset Diff/Compare
- Export Formats (JSON, Parquet, Excel)

### Release 1.2 (3-4 months post-MVP)
- Dashboard Versioning
- Multi-Org Dashboard Sync
- Scheduled Operations (basic)

### Release 1.3 (5-6 months post-MVP)
- Data Lineage Graph
- Smart Alerts
- Export Formats (Avro, SQL)

### Release 1.4 (7-8 months post-MVP)
- Operation Scripts
- Plugin System (basic)
- Enhanced Scheduled Operations (timezone, retry)

### Release 1.5 (9-10 months post-MVP)
- Advanced Plugin System (hooks, sandboxing)
- Alert Notification Channels (Slack, Email, etc.)
- Performance optimizations and scalability

---

## 🔗 Dependencies and Integration

### New Dependencies
| Feature | New Dependencies |
|---------|------------------|
| Bulk Backup/Restore | `zipfile`, `gzip` (stdlib), `boto3` (optional for S3) |
| Dataset Diff/Compare | `deepdiff`, `pandas-profiling` (optional) |
| Dashboard Versioning | `GitPython` or `dulwich` |
| Data Lineage Graph | `graphviz`, `pygraphviz` (optional), `networkx` |
| Scheduled Operations | `APScheduler` |
| Export Formats | `openpyxl`, `fastparquet`, `fastavro`, `tabulate` |
| Smart Alerts | `python-slugify`, `jinja2` (for templates) |
| Operation Scripts | `PyYAML`, `jsonschema` |
| Plugin System | `importlib-metadata` (Python <3.8) |

### Integration Points
All features integrate through:
- **SessionManager** - for authenticated access
- **TaskRunner** - for background execution
- **NotificationManager** - for alerts and progress
- **ConfigManager** - for feature-specific settings
- **Command Palette** - for discoverability
- **Context Menus** - for object-specific actions

---

## 📝 Architecture Decisions (Log in `architecture-decisions.md`)

- [ ] Decision: Bulk operations use streaming to avoid memory issues
- [ ] Decision: Dataset diff uses sampling for >1M row datasets
- [ ] Decision: Dashboard versioning uses local Git repo by default
- [ ] Decision: Lineage graph caches results for 5 minutes
- [ ] Decision: Scheduled operations use persistent SQLite job store
- [ ] Decision: Export format selection based on file extension
- [ ] Decision: Smart alerts run in background with 5-minute interval
- [ ] Decision: Operation scripts use YAML for human-readability
- [ ] Decision: Plugin system uses importlib.metadata for discovery

---

*End of Phase 5 Document*