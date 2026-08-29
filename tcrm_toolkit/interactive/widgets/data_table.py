"""Enhanced DataTable with search, filter, sort, and pagination."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Callable, Generic, TypeVar

from textual import on, work
from textual.app import ComposeResult
from textual.containers import Container, Horizontal, Vertical
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
    - Server-side / client-side pagination
    - Row selection -> detail panel
    - Keyboard navigation (j/k, enter, /, escape)
    - Context menu (ctrl+m)
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
        load_data: Callable[[int, int, str | None, str | None], Any],
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
        try:
            status = self.query_one("#browser-status", Static)
            status.update("Loading...")
        except Exception:
            pass
        
        try:
            offset = page * self.page_size
            sort_col = self._sort_column
            sort_dir = "desc" if self._sort_reverse else "asc"
            
            rows, total_count = await self.load_data(
                offset=offset,
                limit=self.page_size,
                search=self._search_query or None,
                sort=f"{sort_col}:{sort_dir}" if sort_col else None,
            )
            
            self._all_rows = rows
            self._total_count = total_count
            self._total_pages = max(1, (total_count + self.page_size - 1) // self.page_size)
            self._current_page = page
            
            await self._apply_filter()
            
        except Exception as e:
            try:
                self.query_one("#browser-status", Static).update(f"Error: {e}")
            except Exception:
                pass
        finally:
            self._loading = False
    
    async def _render_page(self) -> None:
        """Render current page to table."""
        try:
            table = self.query_one("#data-table", DataTable)
            table.clear()
            
            start = self._current_page * self.page_size
            end = start + self.page_size
            page_rows = self._filtered_rows[start:end]
            
            for i, row in enumerate(page_rows, start + 1):
                row_data = self.get_row_data(row)
                row_key = self.get_row_id(row)
                
                cell_values = []
                for col in self.columns:
                    val = row_data.get(col.key, "")
                    if col.formatter:
                        val = col.formatter(val)
                    cell_values.append(str(val))
                
                table.add_row(*cell_values, key=row_key)
            
            showing = len(page_rows)
            self.query_one("#pagination-info", Static).update(
                f"Page {self._current_page + 1}/{self._total_pages} | "
                f"Showing {start + 1}-{start + showing if showing > 0 else start} of {len(self._filtered_rows)} "
                f"(filtered from {self._total_count})"
            )
            
            self.query_one("#browser-status", Static).update("")
        except Exception:
            pass
    
    async def refresh(self) -> None:
        """Refresh current page."""
        await self._load_page(self._current_page)
    
    # Actions
    async def action_focus_search(self) -> None:
        try:
            self.query_one("#search-input", Input).focus()
        except Exception:
            pass
    
    async def action_clear_search(self) -> None:
        try:
            search = self.query_one("#search-input", Input)
            if search.value:
                search.value = ""
                await self._apply_filter()
        except Exception:
            pass
    
    async def action_select_row(self) -> None:
        """Emit selected row event."""
        try:
            table = self.query_one("#data-table", DataTable)
            if table.cursor_row >= 0 and table.row_count > 0:
                row_key = table.get_row_at(table.cursor_row).key
                row = next((r for r in self._filtered_rows if self.get_row_id(r) == row_key), None)
                if row:
                    self.post_message(self.RowSelected(row))
        except Exception:
            pass

    async def action_cursor_down(self) -> None:
        try:
            table = self.query_one("#data-table", DataTable)
            table.action_cursor_down()
        except Exception:
            pass

    async def action_cursor_up(self) -> None:
        try:
            table = self.query_one("#data-table", DataTable)
            table.action_cursor_up()
        except Exception:
            pass

    async def action_context_menu(self) -> None:
        pass
    
    @dataclass
    class RowSelected:
        row: T
