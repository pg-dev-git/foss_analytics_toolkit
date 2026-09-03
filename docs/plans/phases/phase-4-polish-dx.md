# Phase 4: Polish & Developer Experience

**Document**: `docs/plans/phases/phase-4-polish-dx.md`  
**Duration**: 1 week  
**Branch**: `feature/phase-4-polish-dx` (to be created when implementation begins)  
**Depends on**: Phase 3 complete

---

## 🎯 Objective

Polish the Interactive TUI with developer-focused features:
- Themes (dark/light/customizable)
- Configuration persistence (window size, column widths, filters)
- Command palette with fuzzy search
- Comprehensive keyboard shortcuts
- Help system and tooltips
- Error handling and user-friendly messages
- `tcrm doctor` command with safety checks
- Unit/integration tests
- Documentation

---

## 📋 Explicit Requirements

### 1. Theme System

**File**: `tcrm_toolkit/interactive/styles/default.css`

```css
/* Default light theme */
Screen {
    background: $surface;
    color: $text;
}

Header {
    background: $primary;
    color: $text;
    dock: top;
    height: 3;
}

Footer {
    background: $primary-dark;
    color: $text;
    dock: bottom;
    height: 1;
}

Static#title {
    text-style: bold;
    color: $accent;
    text-align: center;
}

Static#status-bar {
    background: $surface-lighten-2;
    color: $text-muted;
    height: 1;
    dock: bottom;
    padding: 0 1;
}

Static#sidebar-title {
    text-style: bold;
    color: $primary;
    padding: 1 0;
}

ListView#nav-list {
    background: $surface-darken-1;
    width: 25;
}

ListView#nav-list > ListItem {
    padding: 1 2;
}

ListView#nav-list > ListItem.--highlight {
    background: $primary;
    color: $text;
}

ListView#nav-list > ListItem:hover {
    background: $primary-darken-2;
}

Container#sidebar {
    width: 25;
    background: $surface-darken-1;
    border-right: solid $primary-darken-3;
}

Container#content {
    width: 1fr;
}

Container#detail-panel {
    width: 30;
    border-left: solid $primary-darken-3;
    background: $surface-darken-1;
}

DataTable {
    background: $surface;
    color: $text;
}

DataTable > .datatable--header {
    background: $primary-darken-2;
    color: $text;
    text-style: bold;
}

DataTable > .datatable--cursor {
    background: $primary;
    color: $text;
}

DataTable > .datatable--row--odd {
    background: $surface-darken-1;
}

DataTable > .datatable--row--even {
    background: $surface;
}

Label#detail-title {
    text-style: bold;
    color: $primary;
    padding: 1 0;
}

Static#detail-content {
    padding: 1 2;
    height: 1fr;
    overflow: auto;
}

ProgressBar {
    color: $success;
    background: $surface-darken-1;
}

ProgressBar > .progress-bar--complete {
    color: $success;
}

ProgressBar > .progress-bar--remaining {
    color: $surface-darken-2;
}

Button {
    margin: 1 2;
    min-width: 10;
}

Button.--primary {
    background: $success;
    color: $text;
}

Button.--primary:hover {
    background: $success-darken-2;
}

Button.--warning {
    background: $warning;
    color: $text;
}

Button.--warning:hover {
    background: $warning-darken-2;
}

Button.--error {
    background: $error;
    color: $text;
}

Button.--error:hover {
    background: $error-darken-2;
}

Input {
    margin: 1 2;
    min-width: 20;
}

Input:focus {
    border: tall $primary;
}

ModalScreen {
    align: center middle;
}

#login-container, #picker-dialog, #safety-dialog, #palette-dialog {
    background: $surface;
    border: thick $primary;
    padding: 2 4;
    width: 60;
    height: auto;
}

#login-title, #picker-title, #safety-title, #palette-title {
    text-style: bold;
    color: $primary;
    text-align: center;
    margin-bottom: 1;
}

#login-info, #safety-warning, #safety-details {
    color: $text-muted;
    text-align: center;
    margin: 1 0;
}

#safety-title {
    color: $error;
    text-style: bold;
}

#safety-warning {
    color: $warning;
}

Checkbox {
    margin: 1 2;
}

#safety-buttons {
    height: 3;
    align: center middle;
}

#safety-buttons > Button {
    margin: 0 1;
    min-width: 20;
}

#status-bar {
    layout: horizontal;
    overflow: hidden;
}

#status-bar > Static {
    margin: 0 1;
}

#browser-header {
    height: 3;
    padding: 1 2;
}

#browser-title {
    text-style: bold;
    color: $primary;
}

#search-input {
    width: 30;
}

#pagination-info {
    color: $text-muted;
}

#browser-status {
    color: $warning;
    text-align: center;
}

#history-title, #progress-title {
    text-style: bold;
    color: $primary;
    padding: 1 0;
}

#history-table, #progress-table {
    background: $surface;
    color: $text;
}

#history-table > .datatable--header,
#progress-table > .datatable--header {
    background: $primary-darken-2;
    color: $text;
    text-style: bold;
}

#history-table > .datatable--cursor,
#progress-table > .datatable--cursor {
    background: $primary;
    color: $text;
}
```

**File**: `tcrm_toolkit/interactive/styles/dark.css`

```css
/* Dark theme - extends default.css */
@import "default.css";

/* Override colors for dark theme */
:root {
    --background: #0c0c0c;
    --surface: #1e1e1e;
    --surface-darken-1: #252525;
    --surface-darken-2: #2d2d2d;
    --surface-lighten-1: #262626;
    --surface-lighten-2: #323232;
    --primary: #007acc;
    --primary-darken-2: #005a9e;
    --primary-darken-3: #004780;
    --secondary: #6a9955;
    --success: #6a9955;
    --warning: #d7ba7d;
    --error: #f44747;
    --text: #d4d4d4;
    --text-muted: #858585;
    --accent: #4ec9b0;
}
```

**File**: `tcrm_toolkit/interactive/styles/light.css`

```css
/* Light theme - extends default.css */
@import "default.css";

/* Override colors for light theme */
:root {
    --background: #ffffff;
    --surface: #f8f8f8;
    --surface-darken-1: #eeeeee;
    --surface-darken-2: #e0e0e0;
    --surface-lighten-1: #ffffff;
    --surface-lighten-2: #f2f2f2;
    --primary: #0066cc;
    --primary-darken-2: #004c99;
    --primary-darken-3: #003366;
    --secondary: #006600;
    --success: #006600;
    --warning: #cc9900;
    --error: #cc0000;
    --text: #222222;
    --text-muted: #666666;
    --accent: #009900;
}
```

**File**: `tcrm_toolkit/interactive/config.py`

```python
"""TUI-specific configuration."""

from __future__ import annotations

from pathlib import Path
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class TUIConfig(BaseSettings):
    """Configuration for Interactive TUI."""
    
    model_config = SettingsConfigDict(
        env_prefix="TCRM_TUI_",
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )
    
    # Appearance
    theme: Literal["dark", "light", "auto"] = "dark"
    keybindings: Literal["vim", "standard"] = "standard"
    show_line_numbers: bool = False
    
    # Layout
    sidebar_width: int = 25
    detail_panel_width: int = 30
    status_bar_height: int = 1
    
    # Behavior
    confirm_destructive: bool = True
    auto_refresh_interval: int = 10  # seconds for job monitoring
    max_history_items: int = 100
    
    # Performance
    browser_page_size: int = 50
    search_debounce_ms: int = 300
    
    # Paths
    config_dir: Path = Field(default_factory=lambda: Path.home() / ".tcrm")
    history_file: Path = Field(default_factory=lambda: Path.home() / ".tcrm" / "history.json")
    
    def __post_init__(self):
        self.config_dir.mkdir(parents=True, exist_ok=True)
```

---

### 2. Configuration Persistence

**File**: `tcrm_toolkit/interactive/config_manager.py`

```python
"""Configuration persistence for TUI settings."""

import json
from pathlib import Path
from typing import Any

from tcrm_toolkit.interactive.config import TUIConfig


class ConfigManager:
    """Manages persistent TUI configuration."""
    
    def __init__(self, config_dir: Path):
        self.config_dir = config_dir
        self.config_file = config_dir / "config.json"
        self._config: TUIConfig | None = None
    
    def load(self) -> TUIConfig:
        """Load configuration from file."""
        if self._config is None:
            if self.config_file.exists():
                try:
                    with open(self.config_file) as f:
                        data = json.load(f)
                    self._config = TUIConfig(**data)
                except Exception:
                    self._config = TUIConfig()
            else:
                self._config = TUIConfig()
        return self._config
    
    def save(self, config: TUIConfig) -> None:
        """Save configuration to file."""
        self._config = config
        self.config_dir.mkdir(parents=True, exist_ok=True)
        with open(self.config_file, "w") as f:
            json.dump(config.model_dump(), f, indent=2)
    
    def get(self) -> TUIConfig:
        """Get current configuration."""
        if self._config is None:
            return self.load()
        return self._config
    
    def update(self, **kwargs) -> None:
        """Update configuration values."""
        config = self.get()
        for key, value in kwargs.items():
            if hasattr(config, key):
                setattr(config, key, value)
        self.save(config)
```

---

### 3. Window State Persistence

**File**: `tcrm_toolkit/interactive/window_manager.py`

```python
"""Window state persistence (size, position, splits)."""

import json
from pathlib import Path
from typing import Any

from textual.app import App
from textual.geometry import Size


class WindowManager:
    """Manages window state persistence."""
    
    def __init__(self, config_dir: Path):
        self.config_dir = config_dir
        self.state_file = config_dir / "window_state.json"
    
    def save_state(self, app: App) -> None:
        """Save current window state."""
        try:
            state = {
                "size": {
                    "width": app.size.width,
                    "height": app.size.height,
                },
                # Note: Textual doesn't expose split sizes easily
                # Would need to query specific containers
            }
            self.config_dir.mkdir(parents=True, exist_ok=True)
            with open(self.state_file, "w") as f:
                json.dump(state, f, indent=2)
        except Exception:
            pass  # Fail silently
    
    def load_state(self) -> dict[str, Any] | None:
        """Load window state from file."""
        if not self.state_file.exists():
            return None
        
        try:
            with open(self.state_file) as f:
                return json.load(f)
        except Exception:
            return None
    
    def apply_state(self, app: App) -> None:
        """Apply saved window state."""
        state = self.load_state()
        if state and "size" in state:
            size = state["size"]
            # Note: Textual apps are resized externally
            # This is mainly for reference
            pass
```

---

### 4. Column Persistence

**File**: `tcrm_toolkit/interactive/widgets/data_table.py` (EXTEND)

Add to DataBrowser class:

```python
def __init__(self, ..., config_manager: ConfigManager | None = None, browser_id: str = "default"):
    # ... existing init ...
    self.config_manager = config_manager
    self.browser_id = browser_id
    self._column_states: dict[str, dict] = {}
    
    # Load column state
    if self.config_manager:
        self._load_column_state()

def _load_column_state(self) -> None:
    """Load column widths, visibility, sort order."""
    if not self.config_manager:
        return
    
    try:
        states = self.config_manager.get().browser_column_states or {}
        self._column_states = states.get(self.browser_id, {})
        
        # Apply column widths
        for col in self.columns:
            if col.key in self._column_states:
                width = self._column_states[col.key].get("width")
                if width is not None:
                    col.width = width
    except Exception:
        pass

def _save_column_state(self) -> None:
    """Save column widths, visibility, sort order."""
    if not self.config_manager:
        return
    
    try:
        states = self.config_manager.get().browser_column_states or {}
        states[self.browser_id] = self._column_states
        
        # Update current state
        for col in self.columns:
            if col.key not in self._column_states:
                self._column_states[col.key] = {}
            self._column_states[col.key]["width"] = col.width
        
        self.config_manager.update(browser_column_states=states)
    except Exception:
        pass

# Call _save_column_state when columns change
# In on_header_selected method:
@on(DataTable.HeaderSelected, "#data-table")
async def on_header_selected(self, event: DataTable.HeaderSelected) -> None:
    # ... existing sort logic ...
    await self._load_page(0)
    self._save_column_state()  # Save after sort change

# In compose method or when table is created:
# After adding columns, apply saved widths
```

---

### 5. Help System

**File**: `tcrm_toolkit/interactive/screens/help_screen.py`

```python
"""Help screen showing keyboard shortcuts and usage."""

from textual import on
from textual.app import ComposeResult
from textual.containers import Container, Vertical, ScrollableContainer
from textual.screen import Screen
from textual.widgets import Button, Label, ListItem, ListView, Static, TabbedContent, TabPane


class HelpScreen(Screen):
    """Help screen with keyboard shortcuts and usage guide."""
    
    BINDINGS = [
        ("escape", "dismiss", "Close"),
        ("q", "dismiss", "Close"),
    ]
    
    def compose(self) -> ComposeResult:
        yield Container(
            Vertical(
                Static("⌨️  TCRM Toolkit - Keyboard Shortcuts", id="help-title"),
                TabbedContent(
                    TabPane("Navigation", id="tab-nav"),
                    TabPane("Actions", id="tab-actions"),
                    TabPane("Data Browsers", id="tab-browsers"),
                    TabPane("General", id="tab-general"),
                    id="help-tabs"
                ),
                Button("Close", id="close-btn", variant="primary"),
                id="help-container"
            ),
            id="help-dialog"
        )
    
    def on_mount(self) -> None:
        self._populate_help_tabs()
    
    def _populate_help_tabs(self) -> None:
        # Navigation tab
        nav_list = self.query_one("#tab-nav", Vertical)
        nav_list.mount(ListView(
            *[ListItem(Label(shortcut)) for shortcut in [
                "Tab / Shift+Tab - Navigate between panels",
                "Ctrl+O - Organization picker",
                "Ctrl+P - Command palette",
                "F1 - Help screen",
                "Esc - Back / Cancel / Clear search",
                "Arrow keys / Vi j/k - Navigate lists",
                "Enter - Select / Activate",
                "Space - Toggle checkbox",
                "Page Up/Down - Scroll pages",
                "Home/End - Start/End of list",
            ]]
        ))
        
        # Actions tab
        actions_list = self.query_one("#tab-actions", Vertical)
        actions_list.mount(ListView(
            *[ListItem(Label(shortcut)) for shortcut in [
                "E - Extract selected dataset",
                "U - Upload to selected dataset",
                "B - Backup selected dashboard",
                "R - Restore dashboard from backup",
                "S - Start selected dataflow",
                "T - Stop selected dataflow",
                "J - View dataflow jobs",
                "D - Delete selected item (with confirmation)",
                "Y - Show dependencies",
                "C - Copy ID to clipboard",
            ]]
        ))
        
        # Browsers tab
        browsers_list = self.query_one("#tab-browsers", Vertical)
        browsers_list.mount(ListView(
            *[ListItem(Label(shortcut)) for shortcut in [
                "/ - Focus search input",
                "Escape - Clear search",
                "Enter - Apply search (when typing)",
                "Click column header - Sort column",
                "Shift+Click - Multi-column sort",
                "Right-click / Ctrl+M - Context menu",
                "Ctrl+C - Copy selected row",
                "Ctrl+V - Paste (if applicable)",
            ]]
        ))
        
        # General tab
        general_list = self.query_one("#tab-general", Vertical)
        general_list.mount(ListView(
            *[ListItem(Label(shortcut)) for shortcut in [
                "Ctrl+Q - Quit application",
                "Ctrl+S - Save layout (experimental)",
                "Ctrl+L - Clear screen",
                "Ctrl+R - Refresh current view",
                "Ctrl+F - Fullscreen toggle (experimental)",
            ]]
        ))
    
    @on(Button.Pressed, "#close-btn")
    def on_close_pressed(self) -> None:
        self.dismiss()
    
    def action_dismiss(self) -> None:
        self.dismiss()
```

---

### 6. Error Handling & User Messages

**File**: `tcrm_toolkit/interactive/notifications.py`

```python
"""Enhanced notification system for TUI."""

from textual import work
from textual.app import App
from textual.widget import Widget

from tcrm_toolkit.core.config import Settings, get_settings


class NotificationManager:
    """Manages user notifications with levels and persistence."""
    
    def __init__(self, app: App):
        self.app = app
        self.settings = get_settings()
        self._notifications: list[dict] = []
    
    def notify(
        self,
        message: str,
        title: str = "TCRM Toolkit",
        severity: str = "information",
        timeout: float | None = None,
        sticky: bool = False,
    ) -> None:
        """
        Show notification to user.
        
        Args:
            message: Notification message
            title: Notification title
            severity: one of "information", "warning", "error", "success"
            timeout: Auto-dismiss after seconds (None for sticky)
            sticky: 0)
            sticky: Remains until dismissed
        """
        # Map severity to Textual notification types
        severity_map = {
            "information": "information",
            "warning": "warning",
            "error": "error",
            "success": "success",
        }
        
        textual_severity = severity_map.get(severity, "information")
        
        # Show notification
        self.app.notify(
            message,
            title=title,
            severity=textual_severity,
            timeout=timeout or (0 if sticky else 3),
        )
        
        # Store in history
        self._notifications.append({
            "timestamp": datetime.utcnow(),
            "title": title,
            "message": message,
            "severity": severity,
            "timeout": timeout,
            "sticky": sticky,
        })
        
        # Limit history
        if len(self._notifications) > 100:
            self._notifications = self._notifications[-100:]
    
    def info(self, message: str, **kwargs) -> None:
        self.notify(message, severity="information", **kwargs)
    
    def warning(self, message: str, **kwargs) -> None:
        self.notify(message, severity="warning", **kwargs)
    
    def error(self, message: str, **kwargs) -> None:
        self.notify(message, severity="error", **kwargs)
    
    def success(self, message: str, **kwargs) -> None:
        self.notify(message, severity="success", **kwargs)
    
    def get_history(self) -> list[dict]:
        return list(self._notifications)


# Global notification manager (set in app)
_notification_manager: NotificationManager | None = None


def init_notifications(app: App) -> NotificationManager:
    """Initialize global notification manager."""
    global _notification_manager
    _notification_manager = NotificationManager(app)
    return _notification_manager


def get_notification_manager() -> NotificationManager:
    """Get global notification manager."""
    if _notification_manager is None:
        raise RuntimeError("Notification manager not initialized")
    return _notification_manager


def notify_info(message: str, **kwargs) -> None:
    get_notification_manager().info(message, **kwargs)


def notify_warning(message: str, **kwargs) -> None:
    get_notification_manager().warning(message, **kwargs)


def notify_error(message: str, **kwargs) -> None:
    get_notification_manager().error(message, **kwargs)


def notify_success(message: str, **kwargs) -> None:
    get_notification_manager().success(message, **kwargs)
```

**Update TCRMApp to use notification manager**:

**File**: `tcrm_toolkit/interactive/app.py` (ADD)

```python
# In __init__
self.notifications = init_notifications(self)

# Replace self.notify calls with:
self.notifications.success("Authenticated successfully")
self.notifications.warning("Token expired (will auto-refresh)")
self.notifications.error("Failed to load datasets: {e}")
```

---

### 7. Doctor Command Enhancement

**File**: `tcrm_toolkit/cli/commands/doctor.py` (ENHANCE)

```python
"""Enhanced doctor command with safety checks."""

import asyncio
import platform
import sys
from pathlib import Path

import typer
from rich.table import Table
from rich.text import Text

from tcrm_toolkit.cli.ui import (
    console,
    print_header,
    print_success,
    print_error,
    print_warning,
    print_info,
)
from tcrm_toolkit.core import get_settings
from tcrm_toolkit.core.auth import SFCLIAuthService
from tcrm_toolkit.core.crypto import create_crypto_manager
from tcrm_toolkit.core.platform import get_os, is_windows, is_linux, is_macos
from tcrm_toolkit.core.sf_cli import SFCLIManager
from tcrm_toolkit.core.token_store import TokenStore
from tcrm_toolkit.interactive.safety import SafetyMonitor


@app.command()
def doctor() -> None:
    """Run comprehensive system diagnostics."""
    asyncio.run(_doctor_async())


async def _doctor_async() -> None:
    """Async doctor implementation."""
    settings = get_settings()
    crypto = create_crypto_manager()
    auth_service = SFCLIAuthService(settings, crypto)
    safety = SafetyMonitor(settings)
    sf_cli = SFCLIManager()
    token_store = TokenStore(crypto)
    
    print_header("System Diagnostics", "CRMA Toolkit Health Check")
    
    # Run all checks
    checks = await asyncio.gather(
        _check_python_version(),
        _check_dependencies(),
        _check_sf_cli(sf_cli),
        _check_auth_status(auth_service, token_store),
        _check_safety_monitor(safety),
        _check_keyring(),
        _check_directories(),
        _check_network(),
        return_exceptions=True,
    )
    
    # Process results
    passed = 0
    total = len(checks)
    
    table = Table(title="Diagnostic Results", show_header=True)
    table.add_column("Check", style="cyan")
    table.add_column("Status", style="white")
    table.add_column("Details", style="dim")
    
    check_names = [
        "Python Version",
        "Dependencies",
        "SF CLI Installation",
        "Authentication Status",
        "Connection Safety",
        "Keyring Access",
        "Directory Permissions",
        "Network Connectivity",
    ]
    
    for i, result in enumerate(checks):
        name = check_names[i]
        if isinstance(result, Exception):
            status = Text("❌ FAIL", style="red")
            details = str(result)
            print_error(f"{name}: {details}")
        else:
            status, details = result
            if "PASS" in status:
                passed += 1
                print_info(f"{name}: {details}")
            else:
                print_warning(f"{name}: {details}")
        
        table.add_row(name, status, details)
    
    console.print(table)
    
    # Summary
    if passed == total:
        print_success(f"All {total} checks passed! System is ready.")
    else:
        print_warning(f"{passed}/{total} checks passed. {total - passed} issues found.")
        print_info("Run 'tcrm interactive' to start TUI despite warnings.")
        print_info("Critical issues may prevent certain features from working.")


async def _check_python_version() -> tuple[str, str]:
    """Check Python version."""
    version = sys.version_info
    if version.major == 3 and version.minor >= 11:
        return ("[green]✓ PASS[/green]", f"Python {version.major}.{version.minor}.{version.micro}")
    return ("[red]✗ FAIL[/red]", f"Python 3.11+ required, got {version.major}.{version.minor}")


async def _check_dependencies() -> tuple[str, str]:
    """Check required dependencies."""
    required = [
        "textual",
        "rich",
        "httpx",
        "pydantic",
        "pydantic-settings",
        "keyring",
        "cryptography",
        "pandas",
        "structlog",
        "tenacity",
        "typer",
    ]
    
    missing = []
    for dep in required:
        try:
            __import__(dep)
        except ImportError:
            missing.append(dep)
    
    if not missing:
        return ("[green]✓ PASS[/green]", f"All {len(required)} dependencies installed")
    return ("[red]✗ FAIL[/red]", f"Missing: {', '.join(missing)}")


async def _check_sf_cli(sf_cli: SFCLIManager) -> tuple[str, str]:
    """Check SF CLI installation."""
    if sf_cli.is_available():
        try:
            version = await sf_cli.get_org_info()  # This will fail if not logged in, but CLI exists
            return ("[green]✓ PASS[/green]", "SF CLI installed and accessible")
        except Exception:
            return ("[green]✓ PASS[/green]", "SF CLI installed (not logged in)")
    return ("[red]✗ FAIL[/red]", "SF CLI not found. Install from https://developer.salesforce.com/tools/sfdxcli")


async def _check_auth_status(auth_service: SFCLIAuthService, token_store: TokenStore) -> tuple[str, str]:
    """Check authentication status."""
    try:
        # Check for any valid token
        orgs = await auth_service.list_orgs()
        if orgs:
            # Check if any token is valid
            for org in orgs:
                alias = org.get("alias", "default")
                try:
                    token = await token_store.load_token(alias)
                    if token and not token.is_expired():
                        return ("[green]✓ PASS[/green]", f"Valid token for {alias}")
                except Exception:
                    continue
            return ("[yellow]⚠ WARNING[/yellow]", f"{len(orgs)} orgs found, but tokens may be expired")
        return ("[yellow]⚠ WARNING[/yellow]", "No orgs authenticated. Run 'tcrm auth login'")
    except Exception as e:
        return ("[red]✗ FAIL[/red]", f"Auth check failed: {e}")


async def _check_safety_monitor(safety: SafetyMonitor) -> tuple[str, str]:
    """Check safety monitor functionality."""
    try:
        result = await safety.check_connection_safety()
        if result.is_safe:
            return ("[green]✓ PASS[/green]", "Connection safe (no VPN/Proxy detected)")
        elif result.risk_level == "warning":
            return ("[yellow]⚠ WARNING[/yellow]", f"Warning: {result.details}")
        else:
            return ("[red]✗ FAIL[/red]", f"Critical: {result.details}")
    except Exception as e:
        return ("[yellow]⚠ WARNING[/yellow]", f"Safety check error: {e}")


async def _check_keyring() -> tuple[str, str]:
    """Check keyring accessibility."""
    try:
        import keyring
        keyring.set_password("tcrm-toolkit-test", "test-key", "test-value")
        value = keyring.get_password("tcrm-toolkit-test", "test-key")
        keyring.delete_password("tcrm-toolkit-test", "test-key")
        if value == "test-value":
            return ("[green]✓ PASS[/green]", "Keyring accessible and functional")
        return ("[red]✗ FAIL[/red]", f"Keyring get/set/delete failed")
    except Exception as e:
        return ("[red]✗ FAIL[/red]", f"Keyring not accessible: {e}")


async def _check_directories() -> tuple[str, str]:
    """Check directory permissions."""
    try:
        from tcrm_toolkit.interactive.config import TUIConfig
        config = TUIConfig()
        # Check if we can write to config dir
        test_file = config.config_dir / "write_test.tmp"
        test_file.write_text("test")
        test_file.unlink()
        return ("[green]✓ PASS[/green]", f"Config directory writable: {config.config_dir}")
    except Exception as e:
        return ("[red]✗ FAIL[/red]", f"Directory access failed: {e}")


async def _check_network() -> tuple[str, str]:
    """Check network connectivity to Salesforce."""
    try:
        import httpx
        async with httpx.AsyncClient(timeout=5.0) as client:
            # Try to reach Salesforce login endpoint
            response = await client.get("https://login.salesforce.com", follow_redirects=True)
            if response.status_code < 500:
                return ("[green]✓ PASS[/green]", "Network reachable to Salesforce")
            return ("[yellow]⚠ WARNING[/yellow]", f"Network issue: HTTP {response.status_code}")
    except httpx.TimeoutException:
        return ("[yellow]⚠ WARNING[/yellow]", "Network timeout to Salesforce")
    except Exception as e:
        return ("[red]✗ FAIL[/red]", f"Network check failed: {e}")
```

---

### 8. Comprehensive Testing

**File**: `tests/unit/test_interactive.py`

```python
"""Unit tests for Interactive TUI components."""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from tcrm_toolkit.interactive.session import SessionManager, OrgSession
from tcrm_toolkit.interactive.safety import SafetyMonitor, SafetyResult, RiskLevel
from tcrm_toolkit.interactive.tasks import TaskRunner, TaskProgress, TaskStatus
from tcrm_toolkit.interactive.operations.dataset_extract import ParallelDatasetExtractor
from tcrm_toolkit.interactive.operations.dataset_upload import ParallelDatasetUploader


@pytest.fixture
def mock_session():
    """Mock session for testing."""
    session = MagicMock()
    session.client_context = AsyncMock()
    session.client_context.__aenter__ = AsyncMock(return_value=MagicMock())
    session.client_context.__aexit__ = AsyncMock(return_value=None)
    session.settings = MagicMock()
    session.settings.safety_block_on_critical = True
    session.settings.safety_check_interval = 300
    return session


@pytest.fixture
def mock_safety():
    """Mock safety monitor."""
    safety = MagicMock(spec=SafetyMonitor)
    safety.check_connection_safety = AsyncMock(return_value=SafetyResult(
        is_safe=True,
        risk_level=RiskLevel.SAFE,
        details="Safe"
    ))
    return safety


@pytest.mark.asyncio
async def test_session_manager_initialization(mock_session, mock_safety):
    """Test SessionManager initialization."""
    with patch('tcrm_toolkit.interactive.session.SFCLIAuthService'), \
         patch('tcrm_toolkit.interactive.session.SafetyMonitor', return_value=mock_safety):
        
        session_manager = SessionManager(
            settings=mock_session.settings,
            safety_monitor=mock_safety,
        )
        session_manager.auth_service = AsyncMock()
        session_manager.auth_service.list_orgs = AsyncMock(return_value=[])
        session_manager.auth_service.get_access_token = AsyncMock(side_effect=Exception("No token"))
        
        # Should not raise exception even with no token
        await session_manager.initialize()
        assert session_manager is not None


@pytest.mark.asyncio
async def test_safety_monitor_critical_blocks(mock_session):
    """Test that critical safety risks block operations."""
    safety = SafetyMonitor(mock_session.settings)
    safety.check_connection_safety = AsyncMock(return_value=SafetyResult(
        is_safe=False,
        risk_level=RiskLevel.CRITICAL,
        details="VPN detected: tun0"
    ))
    
    session = SessionManager(
        settings=mock_session.settings,
        safety_monitor=safety,
    )
    
    with pytest.raises(Exception):  # Should raise SafetyError
        await session.initialize()


@pytest.mark.asyncio
async def test_task_runner_basic():
    """Test TaskRunner basic functionality."""
    runner = TaskRunner(max_concurrent=2)
    
    async def sample_task():
        await asyncio.sleep(0.1)
        return "completed"
    
    result = await runner.run_task(
        lambda: sample_task(),
        name="Test Task",
    )
    
    assert result.status == "completed"
    assert result.result == "completed"
    
    await runner.close()


@pytest.mark.asyncio
async def test_parallel_extractor_init(mock_session):
    """Test ParallelDatasetExtractor initialization."""
    task_runner = TaskRunner()
    
    extractor = ParallelDatasetExtractor(
        session=mock_session,
        task_runner=task_runner,
    )
    
    assert extractor.session == mock_session
    assert extractor.task_runner == task_runner
    
    await task_runner.close()
```

---

### 9. Documentation

Create user-facing documentation:

**File**: `docs/user-guide.md`

```markdown
# CRM Toolkit User Guide

## Getting Started

### Installation

```bash
# Clone repository
git clone <repository-url>
cd crma-toolkit

# Install with interactive dependencies
uv sync --extra interactive --extra dev

# Or use Docker (recommended for consistency)
docker compose up -d
docker compose exec dev bash
```

### First Launch

```bash
# Launch interactive TUI
tcrm

# First run will prompt for SF CLI web authentication
# Make sure SF CLI is installed: https://developer.salesforce.com/tools/sfdxcli
```

## Basic Usage

### Navigation

- **Sidebar**: Use arrow keys or `j/k` to navigate between sections
- **Ctrl+O**: Organization picker (switch between SF CLI aliases)
- **Ctrl+P**: Command palette (fuzzy search all actions)
- **Tab/Shift+Tab**: Move between panels (sidebar, content, detail)
- **Esc**: Go back, clear search, or close detail panel

### Data Browsers

- **Datasets**: View, extract, upload, delete datasets
- **Dashboards**: View, backup, restore, delete dashboards
- **Dataflows**: View, start, stop, monitor jobs
- **Jobs**: Monitor dataflow execution with auto-refresh

### Search & Sort

- **/**: Focus search input
- **Type**: Filter results in real-time
- **Click column header**: Sort by that column
- **Shift+Click**: Multi-column sort
- **Escape**: Clear search

### Actions

Once you've selected a row (highlighted), you can:

- **Enter**: Show details in right panel
- **E**: Extract dataset to CSV
- **U**: Upload CSV to dataset
- **B**: Backup dashboard to JSON
- **R**: Restore dashboard from backup
- **S**: Start dataflow execution
- **T**: Stop running dataflow
- **Y**: Show dependencies (what uses this item)
- **D**: Delete item (with confirmation)
- **C**: Copy ID to clipboard

## Advanced Features

### Background Tasks

Long-running operations (extract, upload, backup) run in the background:

- View progress in the bottom status bar
- See active tasks in the Progress Panel (access via Command Palette)
- View completed/failed tasks in Task History (Command Palette → "View Task History")

### Connection Safety

The tool continuously monitors your connection for VPN/Proxy that could trigger Salesforce blocks:

- **🟢 Green**: Connection safe
- **🟡 Yellow**: Warning (e.g., system proxy set but IP clean)
- **🔴 Red**: Critical (VPN/Proxy/Tor detected) - blocks all Salesforce API calls

If a critical risk is detected:
1. A modal dialog appears explaining the risk
2. You must either disconnect the VPN/Proxy or acknowledge the risk
3. Salesforce will IMMEDIATELY disable users detected on VPN/Proxy

### Multi-Org Support

- Configure multiple orgs using `sf org login web --alias <name>`
- Switch between orgs instantly with Ctrl+O
- Each org maintains its own authenticated session
- Tokens are securely stored in your system keyring

### Themes

- **Dark theme**: Default (easy on the eyes for extended use)
- **Light theme**: For bright environments
- **Auto**: Follows system theme setting
- Change with: `TCRM_TUI_THEME=dark|light|auto tcrm`

## Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `TCRM_TUI_THEME` | Theme: dark, light, auto | dark |
| `TCRM_TUI_KEYBINDINGS` | Keybinding style: vim, standard | standard |
| `TCRM_TUI_AUTO_REFRESH_INTERVAL` | Job monitor poll interval (seconds) | 10 |
| `TCRM_TUI_MAX_HISTORY_ITEMS` | Max task history items | 100 |
| `TCRM_TUI_BROWSER_PAGE_SIZE` | Items per page in browsers | 50 |
| `SAFETY_CHECK_ENABLED` | Enable VPN/Proxy detection | true |
| `SAFETY_CHECK_INTERVAL` | Seconds between safety checks | 300 |
| `SAFETY_BLOCK_ON_CRITICAL` | Block API calls on critical risk | true |
| `SAFETY_ALLOWLIST_IPS` | Comma-separated IPs to skip checks | (empty) |

### Persistent Settings

Window size, column widths, filters, and last-viewed items are automatically saved to:
- `~/.tcrm/config.json` (TUI settings)
- `~/.tcrm/window_state.json` (window state)
- `~/.tcrm/history.json` (task history)

## Troubleshooting

### Common Issues

**SF CLI not found**
```
Error: SF CLI not found. Install from https://developer.salesforce.com/tools/sfdxcli
```
Solution: Install SF CLI from the Salesforce developer site.

**Keyring access denied**
```
Error: Keyring not accessible
```
Solution: On Linux, install `gnome-keyring` or `kwallet`. On Windows/macOS, keyring should work out-of-the-box.

**Connection blocked by safety monitor**
```
Error: Unsafe connection: VPN detected: tun0
```
Solution: Disconnect your VPN/Proxy and retry. If you're on a trusted network, add your IP to `SAFETY_ALLOWLIST_IPS`.

**No orgs found**
```
Warning: No orgs authenticated. Run 'tcrm auth login' first
```
Solution: Run `tcrm auth login` or launch TUI which will prompt for login.

### Diagnostics

Run the built-in diagnostic tool:
```bash
tcrm doctor
```

This checks:
- Python version and dependencies
- SF CLI installation
- Authentication status
- Connection safety (VPN/Proxy)
- Keyring access
- Directory permissions
- Network connectivity

## Keyboard Shortcuts Reference

### Global
| Shortcut | Action |
|----------|--------|
| `Ctrl+Q` | Quit application |
| `Ctrl+P` | Command palette |
| `Ctrl+O` | Organization picker |
| `Ctrl+R` | Refresh current view |
| `F1` | Help screen |
| `Escape` | Back / Cancel |

### Navigation
| Shortcut | Action |
|----------|--------|
| `Tab` / `Shift+Tab` | Move between panels |
| `Arrow Up` / `Arrow Down` / `j` / `k` | Navigate lists |
| `Page Up` / `Page Down` | Scroll pages |
| `Home` / `End` | Start/End of list |
| `Enter` | Select / Activate |

### Data Browsers
| Shortcut | Action |
|----------|--------|
| `/` | Focus search |
| `Escape` | Clear search |
| `Enter` | Apply search |
| `Click header` | Sort column |
| `Shift+Click` | Multi-column sort |
| `Right-click` / `Ctrl+M` | Context menu |
| `Ctrl+C` | Copy selected row |

### Actions (when row selected)
| Shortcut | Action |
|----------|--------|
| `E` | Extract dataset |
| `U` | Upload dataset |
| `B` | Backup dashboard |
| `R` | Restore dashboard |
| `S` | Start dataflow |
| `T` | Stop dataflow |
| `J` | View jobs |
| `Y` | Show dependencies |
| `D` | Delete item |
| `C` | Copy ID |

## Docker Usage

### Development
```bash
# Start development container
docker compose up -d dev
docker compose exec dev bash

# Inside container:
tcrm  # Launch TUI
tcrm doctor  # Run diagnostics
```

### Production
```bash
# Start production container
docker compose up -d prod

# Access logs
docker compose logs -f prod

# Execute commands
docker compose exec prod tcrm <command>
```

## Getting Help

- Check the troubleshooting section above
- Run `tcrm doctor` for system diagnostics
- View logs with `tcrm --verbose` (for CLI mode) or check Docker logs
- Visit the project repository for issues and documentation
```

---

## ✅ Acceptance Criteria

| Feature | Verification |
|---------|--------------|
| Themes work | Dark/light/themes switch correctly, colors update |
| Config persistence | Window size, column widths, filters saved/restored |
| Command palette | Ctrl+P shows fuzzy searchable actions |
| Help screen | F1 shows organized keyboard shortcuts |
| Notifications | Success/warning/error/info messages appear appropriately |
| Doctor command | Runs all checks, shows clear pass/fail |
| Unit tests | >80% coverage on new interactive components |
| Documentation | User guide covers installation, usage, troubleshooting |
| Cross-platform | All features work on Windows, Linux, macOS |

---

## 🔧 Coding Agent Instructions

### Implementation Order
1. **styles/** - Create CSS theme files
2. **config.py** - TUI configuration model
3. **config_manager.py** - Configuration persistence
4. **window_manager.py** - Window state persistence
5. **notifications.py** - Enhanced notification system
6. **help_screen.py** - Help screen with keyboard shortcuts
7. **doctor.py** - Enhanced doctor command
8. **tests/unit/test_interactive.py** - Unit tests
9. **docs/user-guide.md** - User-facing documentation
10. **Main App integration** - Wire up config, notifications, help

### Key Patterns
- **Configuration**: Use Pydantic Settings with env var support
- **Persistence**: JSON files in `~/.tcrm/` directory
- **Theming**: Textual CSS with variables, extend base/theme.css
- **Notifications**: Wrapper around `app.notify()` with history and severity levels
- **Help**: Tabbed content with categorized shortcuts
- **Doctor**: Async checks with Rich table output

### Testing
```bash
# Run interactive tests
pytest tests/unit/test_interactive.py -v

# Manual verification
tcrm  # Launch TUI
# Test: Ctrl+P -> type "extract" -> Enter
# Test: F1 -> help screen
# Test: Change theme via env var: TCRM_TUI_THEME=light tcrm
# Test: Resize terminal, restart, check size restored
# Test: Change column width in browser, restart, check width restored
```

---

## 📝 Architecture Decisions (Log in `architecture-decisions.md`)

- [ ] Decision: Pydantic Settings for TUI config with env var support
- [ ] Decision: JSON files in ~/.tcrm/ for persistence
- [ ] Decision: Textual CSS themes with base/default.css
- [ ] Decision: Notification manager with history and severity levels
- [ ] Decision: Help screen as ModalScreen with TabbedContent
- [ ] Decision: Doctor command runs all checks in parallel
- [ ] Decision: Unit tests for all new interactive components

---

*End of Phase 4 Document*