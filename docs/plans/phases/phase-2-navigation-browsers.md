# Phase 2: Navigation Browsers

**Document**: `docs/plans/phases/phase-2-navigation-browsers.md`  
**Duration**: 1 week  
**Branch**: `feature/phase-2-navigation-browsers` (to be created when implementation begins)  
**Depends on**: Phase 1 complete

---

## 🎯 Objective

Build fully functional, keyboard-navigable browsers for Datasets, Dashboards, and Dataflows with:
- Server-side pagination (50 items per page)
- Client-side search/filter
- Column sorting (click headers)
- Row selection with detail panel
- Context menus for actions
- Responsive layout for different terminal sizes

---

## 📋 Explicit Requirements

### 1. Enhanced DataTable Widget

**File**: `tcrm_toolkit/interactive/widgets/data_table.py`

```python
"""Enhanced DataTable with search, filter, sort, and pagination."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Callable, Generic, TypeVar

from textual import on, work
from textual.containers import Container, Horizontal, Vertical
from textual.events import Key
from textual.widgets import DataTable, Input, Label, Static
from textual.widget import Widget

T = TypeVar("T")


@dataclass
class ColumnConfig:
    """Configuration for a table column."""
    key: str
    title: str
    width: int | None = None
    sortable: bool = True
    filterable: bool = True
    formatter: Callable[[Any], str] | None = None


class DataBrowser(Widget, Generic[T]):
    """
    Generic data browser with:
    - Search/filter input
    - Sortable columns (click header)
    - Server-side pagination
    - Row selection → detail panel
    - Keyboard navigation (j/k, enter, /, escape)
    - Context menu (right-click or Ctrl+M)
    """
    
    BINDINGS = [
        ("/", "focus_search", "Search"),
        ("escape", "clear_search", "Clear Search"),
        ("enter", "select_row", "Select"),
        ("j", "cursor_down", "Down"),
        ("k", "cursor_up", "Up"),
        ("ctrl+m", "context_menu", "Context Menu"),
    ]
    
    def __init__(
        self,
        columns: list[ColumnConfig],
        load_data: Callable[[int, int, str | None, str | None], asyncio.coroutine],
        get_row_id: Callable[[T], str],
        get_row_data: Callable[[T], dict[str, Any]],
        title: str = "Data Browser",
        page_size: int = 50,
        id: str | None = None,
    ):
        super().__init__(id=id)
        self.columns = columns
        self.load_data = load_data
        self.get_row_id = get_row_id
        self.get_row_data = get_row_data
        self.title = title
        self.page_size = page_size
        
        # State
        self._all_rows: list[T] = []
        self._filtered_rows: list[T] = []
        self._current_page = 0
        self._total_pages = 0
        self._total_count = 0
        self._search_query = ""
        self._sort_column: str | None = None
        self._sort_reverse = False
        self._loading = False
    
    def compose(self) -> ComposeResult:
        yield Vertical(
            Horizontal(
                Label(self.title, id="browser-title"),
                Input(placeholder="Search... (Press /)", id="search-input"),
                Static("", id="pagination-info"),
                id="browser-header"
            ),
            Container(
                DataTable(id="data-table", cursor_type="row", zebra_stripes=True),
                id="table-container"
            ),
            Static("", id="browser-status"),
            id="browser-container"
        )
    
    async def on_mount(self) -> None:
        """Initialize table columns and load first page."""
        table = self.query_one("#data-table", DataTable)
        
        # Add columns
        for col in self.columns:
            table.add_column(col.title, key=col.key, width=col.width)
        
        # Load initial data
        await self._load_page(0)
    
    @on(DataTable.HeaderSelected, "#data-table")
    async def on_header_selected(self, event: DataTable.HeaderSelected) -> None:
        """Handle column header click for sorting."""
        column_key = event.column_key.value
        
        # Find column config
        col_config = next((c for c in self.columns if c.key == column_key), None)
        if not col_config or not col_config.sortable:
            return
        
        # Toggle sort direction
        if self._sort_column == column_key:
            self._sort_reverse = not self._sort_reverse
        else:
            self._sort_column = column_key
            self._sort_reverse = False
        
        # Reload with new sort
        await self._load_page(0)
    
    @on(Input.Changed, "#search-input")
    async def on_search_changed(self, event: Input.Changed) -> None:
        """Handle search input changes (debounced)."""
        self._search_query = event.value
        # Debounce: wait 300ms before filtering
        await asyncio.sleep(0.3)
        if self._search_query == event.value:  # Still current
            await self._apply_filter()
    
    async def _apply_filter(self) -> None:
        """Apply client-side filter to loaded data."""
        if not self._search_query:
            self._filtered_rows = self._all_rows
        else:
            query = self._search_query.lower()
            self._filtered_rows = [
                row for row in self._all_rows
                if any(
                    query in str(self.get_row_data(row).get(col.key, "")).lower()
                    for col in self.columns if col.filterable
                )
            ]
        
        self._current_page = 0
        await self._render_page()
    
    async def _load_page(self, page: int) -> None:
        """Load page from server."""
        if self._loading:
            return
        
        self._loading = True
        self.query_one("#browser-status", Static).update("Loading...")
        
        try:
            # Call load_data with pagination params
            offset = page * self.page_size
            sort_col = self._sort_column
            sort_dir = "desc" if self._sort_reverse else "asc"
            
            # Load data returns (rows, total_count)
            rows, total_count = await self.load_data(
                offset=offset,
                limit=self.page_size,
                search=self._search_query or None,
                sort=f"{sort_col}:{sort_dir}" if sort_col else None,
            )
            
            self._all_rows = rows
            self._total_count = total_count
            self._total_pages = (total_count + self.page_size - 1) // self.page_size
            self._current_page = page
            
            await self._apply_filter()
            
        except Exception as e:
            self.query_one("#browser-status", Static).update(f"Error: {e}")
        finally:
            self._loading = False
    
    async def _render_page(self) -> None:
        """Render current page to table."""
        table = self.query_one("#data-table", DataTable)
        table.clear()
        
        start = self._current_page * self.page_size
        end = start + self.page_size
        page_rows = self._filtered_rows[start:end]
        
        for i, row in enumerate(page_rows, start + 1):
            row_data = self.get_row_data(row)
            row_key = self.get_row_id(row)
            table.add_row(*[str(row_data.get(col.key, "")) for col in self.columns], key=row_key)
        
        # Update pagination info
        showing = len(page_rows)
        self.query_one("#pagination-info", Static).update(
            f"Page {self._current_page + 1}/{self._total_pages} | "
            f"Showing {start + 1}-{start + showing} of {len(self._filtered_rows)} "
            f"(filtered from {self._total_count})"
        )
        
        self.query_one("#browser-status", Static).update("")
    
    async def refresh(self) -> None:
        """Refresh current page."""
        await self._load_page(self._current_page)
    
    # Actions
    async def action_focus_search(self) -> None:
        self.query_one("#search-input", Input).focus()
    
    async def action_clear_search(self) -> None:
        search = self.query_one("#search-input", Input)
        if search.value:
            search.value = ""
            await self._apply_filter()
    
    async def action_select_row(self) -> None:
        """Emit selected row event."""
        table = self.query_one("#data-table", DataTable)
        if table.cursor_row >= 0:
            row_key = table.get_row_at(table.cursor_row).key
            row = next((r for r in self._filtered_rows if self.get_row_id(r) == row_key), None)
            if row:
                self.post_message(self.RowSelected(row))
    
    @dataclass
    class RowSelected:
        row: T
```

---

### 2. Dataset Browser Implementation

**File**: `tcrm_toolkit/interactive/operations/dataset_ops.py`

```python
"""Dataset operations for Interactive TUI."""

from typing import Any

from tcrm_toolkit.core.services.dataset_service import DatasetService
from tcrm_toolkit.core.models import Dataset
from tcrm_toolkit.interactive.widgets.data_table import DataBrowser, ColumnConfig


def create_dataset_browser(session) -> DataBrowser[Dataset]:
    """Create configured dataset browser."""
    
    columns = [
        ColumnConfig(key="id", title="ID", width=18, formatter=lambda x: x[:15] + "..." if len(x) > 18 else x),
        ColumnConfig(key="name", title="Name", width=30),
        ColumnConfig(key="label", title="Label", width=30),
        ColumnConfig(key="row_count", title="Rows", width=12, formatter=lambda x: f"{x:,}" if x else "N/A"),
        ColumnConfig(key="status", title="Status", width=12),
        ColumnConfig(key="type", title="Type", width=15),
    ]
    
    async def load_data(offset: int, limit: int, search: str | None, sort: str | None):
        """Load datasets with pagination."""
        async with session.client_context() as client:
            service = DatasetService(client, session.settings)
            
            # Use service method with pagination
            # Note: DatasetService.list_datasets doesn't support offset/limit directly
            # We'll need to fetch all and paginate client-side for now
            # TODO: Add server-side pagination to DatasetService
            all_datasets = await service.list_datasets(page_size=1000, sort=sort.split(":")[0] if sort else "Mru")
            
            # Apply search filter server-side if possible
            if search:
                search_lower = search.lower()
                all_datasets = [
                    ds for ds in all_datasets
                    if search_lower in ds.name.lower() 
                    or search_lower in ds.label.lower()
                    or search_lower in ds.id.lower()
                ]
            
            total = len(all_datasets)
            page_data = all_datasets[offset:offset + limit]
            
            return page_data, total
    
    def get_row_id(dataset: Dataset) -> str:
        return dataset.id
    
    def get_row_data(dataset: Dataset) -> dict[str, Any]:
        return {
            "id": dataset.id,
            "name": dataset.name,
            "label": dataset.label,
            "row_count": dataset.row_count or 0,
            "status": dataset.status,
            "type": dataset.type,
        }
    
    return DataBrowser(
        columns=columns,
        load_data=load_data,
        get_row_id=get_row_id,
        get_row_data=get_row_data,
        title="📊 Datasets",
        page_size=50,
        id="datasets-browser",
    )
```

---

### 3. Dashboard Browser Implementation

**File**: `tcrm_toolkit/interactive/operations/dashboard_ops.py`

```python
"""Dashboard operations for Interactive TUI."""

from typing import Any

from tcrm_toolkit.core.services.dashboard_service import DashboardService
from tcrm_toolkit.core.models import Dashboard
from tcrm_toolkit.interactive.widgets.data_table import DataBrowser, ColumnConfig


def create_dashboard_browser(session) -> DataBrowser[Dashboard]:
    """Create configured dashboard browser."""
    
    columns = [
        ColumnConfig(key="id", title="ID", width=18, formatter=lambda x: x[:15] + "..." if len(x) > 18 else x),
        ColumnConfig(key="name", title="Name", width=30),
        ColumnConfig(key="label", title="Label", width=30),
        ColumnConfig(key="folder_name", title="Folder", width=25, formatter=lambda x: x or "N/A"),
        ColumnConfig(key="created_date", title="Created", width=20, formatter=lambda x: x.strftime("%Y-%m-%d") if x else "N/A"),
    ]
    
    async def load_data(offset: int, limit: int, search: str | None, sort: str | None):
        async with session.client_context() as client:
            service = DashboardService(client, session.settings)
            all_dashboards = await service.list_dashboards(page_size=1000, sort=sort.split(":")[0] if sort else "Mru")
            
            if search:
                search_lower = search.lower()
                all_dashboards = [
                    db for db in all_dashboards
                    if search_lower in db.name.lower()
                    or search_lower in db.label.lower()
                    or search_lower in (db.folder_name or "").lower()
                    or search_lower in db.id.lower()
                ]
            
            total = len(all_dashboards)
            page_data = all_dashboards[offset:offset + limit]
            return page_data, total
    
    def get_row_id(dashboard: Dashboard) -> str:
        return dashboard.id
    
    def get_row_data(dashboard: Dashboard) -> dict[str, Any]:
        return {
            "id": dashboard.id,
            "name": dashboard.name,
            "label": dashboard.label,
            "folder_name": dashboard.folder_name or "N/A",
            "created_date": dashboard.created_date,
        }
    
    return DataBrowser(
        columns=columns,
        load_data=load_data,
        get_row_id=get_row_id,
        get_row_data=get_row_data,
        title="📈 Dashboards",
        page_size=50,
        id="dashboards-browser",
    )
```

---

### 4. Dataflow Browser Implementation

**File**: `tcrm_toolkit/interactive/operations/dataflow_ops.py`

```python
"""Dataflow operations for Interactive TUI."""

from typing import Any

from tcrm_toolkit.core.services.dataflow_service import DataflowService
from tcrm_toolkit.core.models import Dataflow, DataflowJob
from tcrm_toolkit.interactive.widgets.data_table import DataBrowser, ColumnConfig


def create_dataflow_browser(session) -> DataBrowser[Dataflow]:
    """Create configured dataflow browser."""
    
    columns = [
        ColumnConfig(key="id", title="ID", width=18, formatter=lambda x: x[:15] + "..." if len(x) > 18 else x),
        ColumnConfig(key="name", title="Name", width=30),
        ColumnConfig(key="label", title="Label", width=30),
        ColumnConfig(key="status", title="Status", width=15),
        ColumnConfig(key="created_date", title="Created", width=20, formatter=lambda x: x.strftime("%Y-%m-%d") if x else "N/A"),
    ]
    
    async def load_data(offset: int, limit: int, search: str | None, sort: str | None):
        async with session.client_context() as client:
            service = DataflowService(client, session.settings)
            all_dataflows = await service.list_dataflows()
            
            if search:
                search_lower = search.lower()
                all_dataflows = [
                    df for df in all_dataflows
                    if search_lower in df.name.lower()
                    or search_lower in df.label.lower()
                    or search_lower in df.id.lower()
                ]
            
            total = len(all_dataflows)
            page_data = all_dataflows[offset:offset + limit]
            return page_data, total
    
    def get_row_id(dataflow: Dataflow) -> str:
        return dataflow.id
    
    def get_row_data(dataflow: Dataflow) -> dict[str, Any]:
        return {
            "id": dataflow.id,
            "name": dataflow.name,
            "label": dataflow.label,
            "status": dataflow.status,
            "created_date": dataflow.created_date,
        }
    
    return DataBrowser(
        columns=columns,
        load_data=load_data,
        get_row_id=get_row_id,
        get_row_data=get_row_data,
        title="🔄 Dataflows",
        page_size=50,
        id="dataflows-browser",
    )


def create_dataflow_job_browser(session) -> DataBrowser[DataflowJob]:
    """Create configured dataflow job browser with live polling."""
    
    columns = [
        ColumnConfig(key="id", title="Job ID", width=18, formatter=lambda x: x[:15] + "..." if len(x) > 18 else x),
        ColumnConfig(key="dataflow_name", title="Dataflow", width=30),
        ColumnConfig(key="command", title="Command", width=12),
        ColumnConfig(key="status", title="Status", width=15),
        ColumnConfig(key="start_time", title="Started", width=20, formatter=lambda x: x.strftime("%Y-%m-%d %H:%M") if x else "N/A"),
        ColumnConfig(key="end_time", title="Ended", width=20, formatter=lambda x: x.strftime("%Y-%m-%d %H:%M") if x else "N/A"),
    ]
    
    async def load_data(offset: int, limit: int, search: str | None, sort: str | None):
        async with session.client_context() as client:
            service = DataflowService(client, session.settings)
            all_jobs = await service.list_dataflow_jobs()
            
            if search:
                search_lower = search.lower()
                all_jobs = [
                    job for job in all_jobs
                    if search_lower in job.dataflow_name.lower()
                    or search_lower in job.command.lower()
                    or search_lower in job.status.lower()
                    or search_lower in job.id.lower()
                ]
            
            # Sort by start_time desc by default
            all_jobs.sort(key=lambda j: j.start_time or "", reverse=True)
            
            total = len(all_jobs)
            page_data = all_jobs[offset:offset + limit]
            return page_data, total
    
    def get_row_id(job: DataflowJob) -> str:
        return job.id
    
    def get_row_data(job: DataflowJob) -> dict[str, Any]:
        return {
            "id": job.id,
            "dataflow_name": job.dataflow_name,
            "command": job.command,
            "status": job.status,
            "start_time": job.start_time,
            "end_time": job.end_time,
        }
    
    browser = DataBrowser(
        columns=columns,
        load_data=load_data,
        get_row_id=get_row_id,
        get_row_data=get_row_data,
        title="📋 Dataflow Jobs",
        page_size=50,
        id="jobs-browser",
    )
    
    # Add auto-refresh for running jobs
    original_on_mount = browser.on_mount
    
    async def on_mount_with_polling(self) -> None:
        await original_on_mount()
        # Start polling for running jobs
        self._poll_task = asyncio.create_task(self._poll_running_jobs())
    
    async def _poll_running_jobs(self) -> None:
        while True:
            await asyncio.sleep(10)  # Poll every 10 seconds
            # Check if any running jobs in current view
            table = self.query_one("#data-table", DataTable)
            has_running = any(
                "running" in str(table.get_cell_at(row, 3)).lower() 
                for row in range(table.row_count)
            )
            if has_running:
                await self.refresh()
    
    browser.on_mount = on_mount_with_polling.__get__(browser, DataBrowser)
    
    return browser
```

---

### 5. Update Main Screen to Use Browsers

**File**: `tcrm_toolkit/interactive/screens/main_screen.py` (MODIFY)

```python
# Replace the _load_*_view methods with browser-based versions

async def _load_datasets_view(self, container: Container) -> None:
    """Load datasets browser."""
    from tcrm_toolkit.interactive.operations.dataset_ops import create_dataset_browser
    browser = create_dataset_browser(self.session)
    await container.mount(browser)
    
    # Handle row selection
    @browser.on(DataBrowser.RowSelected)
    async def on_dataset_selected(event: DataBrowser.RowSelected) -> None:
        detail = self.query_one("#detail-panel", DetailPanel)
        detail.show_dataset(event.row)

async def _load_dashboards_view(self, container: Container) -> None:
    """Load dashboards browser."""
    from tcrm_toolkit.interactive.operations.dashboard_ops import create_dashboard_browser
    browser = create_dashboard_browser(self.session)
    await container.mount(browser)
    
    @browser.on(DataBrowser.RowSelected)
    async def on_dashboard_selected(event: DataBrowser.RowSelected) -> None:
        detail = self.query_one("#detail-panel", DetailPanel)
        detail.show_dashboard(event.row)

async def _load_dataflows_view(self, container: Container) -> None:
    """Load dataflows browser."""
    from tcrm_toolkit.interactive.operations.dataflow_ops import create_dataflow_browser
    browser = create_dataflow_browser(self.session)
    await container.mount(browser)
    
    @browser.on(DataBrowser.RowSelected)
    async def on_dataflow_selected(event: DataBrowser.RowSelected) -> None:
        detail = self.query_one("#detail-panel", DetailPanel)
        detail.show_dataflow(event.row)

async def _load_jobs_view(self, container: Container) -> None:
    """Load jobs browser with auto-refresh."""
    from tcrm_toolkit.interactive.operations.dataflow_ops import create_dataflow_job_browser
    browser = create_dataflow_job_browser(self.session)
    await container.mount(browser)
    
    @browser.on(DataBrowser.RowSelected)
    async def on_job_selected(event: DataBrowser.RowSelected) -> None:
        detail = self.query_one("#detail-panel", DetailPanel)
        detail.show_dataflow_job(event.row)
```

---

### 6. Detail Panel Extensions

**File**: `tcrm_toolkit/interactive/widgets/detail_panel.py` (EXTEND)

```python
# Add to DetailPanel class

def show_dataflow_job(self, job) -> None:
    """Show dataflow job details."""
    from tcrm_toolkit.core.models import DataflowJob
    if not isinstance(job, DataflowJob):
        return
    
    content = f"""[bold]Dataflow Job Details[/bold]

[cyan]Job ID:[/cyan] {job.id}
[cyan]Dataflow:[/cyan] {job.dataflow_name}
[cyan]Command:[/cyan] {job.command}
[cyan]Status:[/cyan] {job.status}
[cyan]Start Time:[/cyan] {job.start_time.strftime('%Y-%m-%d %H:%M') if job.start_time else 'N/A'}
[cyan]End Time:[/cyan] {job.end_time.strftime('%Y-%m-%d %H:%M') if job.end_time else 'N/A'}
[cyan]Duration:[/cyan] {self._format_duration(job.start_time, job.end_time) if job.start_time else 'N/A'}
"""
    self._content.update(content)

def _format_duration(self, start, end) -> str:
    if not start or not end:
        return "N/A"
    delta = end - start
    hours = delta.seconds // 3600
    minutes = (delta.seconds % 3600) // 60
    return f"{hours}h {minutes}m"
```

---

### 7. Context Menu Widget

**File**: `tcrm_toolkit/interactive/widgets/context_menu.py` (NEW)

```python
"""Context menu for row actions."""

from textual import on
from textual.app import ComposeResult
from textual.containers import Container
from textual.screen import ModalScreen
from textual.widgets import Button, Label, Static


class ContextMenu(ModalScreen[str]):
    """Context menu for row actions."""
    
    def __init__(self, actions: list[tuple[str, str]], x: int, y: int):
        super().__init__()
        self.actions = actions  # List of (label, action_id)
        self._x = x
        self._y = y
    
    def compose(self) -> ComposeResult:
        yield Container(
            Container(
                *[Button(label, id=f"action-{i}", variant="default") for i, (label, _) in enumerate(self.actions)],
                id="context-menu-items"
            ),
            id="context-menu"
        )
    
    def on_mount(self) -> None:
        # Position menu at cursor
        menu = self.query_one("#context-menu", Container)
        menu.styles.offset = (self._x, self._y)
    
    @on(Button.Pressed)
    def on_action_selected(self, event: Button.Pressed) -> None:
        if event.button.id and event.button.id.startswith("action-"):
            idx = int(event.button.id.split("-")[1])
            if idx < len(self.actions):
                _, action_id = self.actions[idx]
                self.dismiss(action_id)
    
    def on_click(self, event) -> None:
        # Click outside closes menu
        self.dismiss(None)
```

---

### 8. Keyboard Shortcuts Reference

Add to sidebar hints or help screen:

| Key | Action |
|-----|--------|
| `j` / `↓` | Next row |
| `k` / `↑` | Previous row |
| `Enter` | Select row (show details) |
| `/` | Focus search |
| `Escape` | Clear search / Close detail |
| `Click header` | Sort column |
| `Ctrl+M` | Context menu |
| `PgUp` / `PgDn` | Page up/down |
| `Home` / `End` | First/Last row |

---

## ✅ Acceptance Criteria

| Feature | Verification |
|---------|--------------|
| Dataset browser loads | 50 datasets/page, search works, sort works |
| Dashboard browser loads | 50 dashboards/page, folder column shows |
| Dataflow browser loads | 50 dataflows/page, status column |
| Jobs browser loads | Auto-refreshes running jobs every 10s |
| Row selection | Enter shows details in right panel |
| Search | `/` focuses, type filters instantly |
| Sort | Click column header toggles asc/desc |
| Pagination | Page info shows correctly |
| Keyboard nav | j/k, enter, escape all work |
| Cross-platform | All browsers work on Win/Linux/Mac |

---

## 🔧 Coding Agent Instructions

### Implementation Order
1. **data_table.py** - Generic DataBrowser widget (core component)
2. **dataset_ops.py** - Dataset browser factory
3. **dashboard_ops.py** - Dashboard browser factory
4. **dataflow_ops.py** - Dataflow + Jobs browser factories
5. **context_menu.py** - Context menu widget
6. **main_screen.py** - Integrate browsers, handle RowSelected events
7. **detail_panel.py** - Add show_dataflow_job method

### Key Patterns
- **Generic DataBrowser**: Reusable for any entity type
- **Factory functions**: `create_*_browser(session)` return configured browsers
- **Async loading**: `@work(exclusive=True)` for data fetching
- **Event-driven**: `RowSelected` message for detail panel updates
- **Client-side pagination**: Current DatasetService doesn't support server-side offset/limit

### Performance Notes
- Current implementation fetches all items (page_size=1000) and paginates client-side
- For 1000+ items, consider adding server-side pagination to services
- Search is client-side on loaded data (fast for <5000 items)

### Testing
```bash
# Test each browser
tcrm  # Launch TUI
# Navigate to each view with sidebar
# Test search: press /, type filter
# Test sort: click column headers
# Test selection: Enter on row
# Test jobs auto-refresh: start a dataflow, watch jobs view
```

---

## 📝 Architecture Decisions (Log in `architecture-decisions.md`)

- [ ] Decision: Generic DataBrowser widget for all entity types
- [ ] Decision: Client-side pagination (service limitation)
- [ ] Decision: Client-side search on loaded data
- [ ] Decision: DataTable for keyboard navigation + sorting
- [ ] Decision: Factory pattern for browser creation
- [ ] Decision: Auto-refresh for jobs view (10s interval)

---

*End of Phase 2 Document*