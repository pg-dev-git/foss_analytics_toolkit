# CRMA Toolkit - Interactive TUI Design Document

**Date**: 2026-07-23  
**Status**: Design Phase  
**Branch**: `feature/interactive-tui` (to be created)

---

## 1. Executive Summary

Transform the current Typer-based CLI into a **production-grade interactive TUI (Text User Interface)** that runs as a persistent "live program" — exactly like the original legacy `FOSS_Toolkit.py` but with world-class architecture, harnessing the full Salesforce Analytics REST API surface area, and delivering critical safety features (VPN/Proxy detection) that protect developers from accidental org lockouts.

**Core Philosophy**: The existing service layer (`DatasetService`, `DashboardService`, `DataflowService`, `AuthService`, `SFCLIAuthService`) is excellent — **zero changes required**. The TUI is a pure consumer layer built on top.

---

## 2. Current State Analysis

### 2.1 Legacy Code (`_legacy/FOSS_Toolkit.py`)
- Single-process menu-driven loop (lines 36-105)
- Direct function calls with `access_token`, `server_id`, `server_domain` passed everywhere
- No async, no retry logic, no structured error handling
- Multiprocessing for bulk operations (`mp.freeze_support()`)

### 2.2 Refactored Code (`tcrm_toolkit/`)
- **Typer-based CLI** with subcommands (`datasets`, `dashboards`, `dataflows`, `jobs`, `auth`)
- **Async architecture** with `httpx` + `tenacity` retry + circuit breaker
- **SF CLI auth** as default (commit 8df846f) — web login, token storage, auto-refresh
- **Service layer** with clean separation: `DatasetService`, `DashboardService`, `DataflowService`
- **Pydantic models** for all API responses
- **Keyring-encrypted token storage** via `TokenStore` + `CryptoManager`

### 2.3 Gap
The refactor lost the **"running OS" feel** — each command is a separate process invocation. Developers want a persistent interactive session where they can browse, extract, upload, monitor jobs, and switch orgs without re-authenticating.

---

## 3. Target Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         INTERACTIVE TUI LAYER                                │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐              │
│  │  SessionManager │  │  TaskRunner     │  │  SafetyMonitor  │              │
│  │  (wraps SFCLI   │  │  (asyncio.      │  │  (VPN/Proxy     │              │
│  │   AuthService)  │  │   TaskGroup)    │  │   detection)    │              │
│  └────────┬────────┘  └────────┬────────┘  └────────┬────────┘              │
│           │                    │                    │                        │
│  ┌────────┴────────┐  ┌────────┴────────┐  ┌────────┴────────┐              │
│  │  Screen/Widget  │  │  Command        │  │  Theme/Config   │              │
│  │  Components     │  │  Palette        │  │  Manager        │              │
│  │  (Textual)      │  │  (Ctrl+P)       │  │                 │              │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘              │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                      EXISTING SERVICE LAYER (UNCHANGED)                      │
│  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐ ┌──────────────────┐   │
│  │DatasetService│ │DashboardService│ │DataflowService│ │  AuthService    │   │
│  └──────────────┘ └──────────────┘ └──────────────┘ └──────────────────┘   │
│  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐ ┌──────────────────┐   │
│  │SFCLIAuthSvc  │ │  TokenStore  │ │ CryptoManager│ │  SalesforceClient│   │
│  └──────────────┘ └──────────────┘ └──────────────┘ └──────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                    SALESFORCE ANALYTICS REST API                             │
│  Datasets • Dashboards • Dataflows • Data Manager • Limits • Metadata       │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 4. Technology Stack

| Layer | Technology | Version | Rationale |
|-------|------------|---------|-----------|
| **TUI Framework** | **Textual** | ≥0.52.0 | Modern, async, CSS styling, React-like components, production-proven |
| **Rendering** | **Rich** | ≥13.7.0 | Already in use — tables, progress, panels, syntax highlighting |
| **Async Runtime** | **asyncio** | Native | Already used throughout codebase |
| **Auth** | **SFCLIAuthService** | Existing | Web login, keyring storage, auto-refresh — **mandatory default** |
| **Config** | **Pydantic Settings** | Existing | `.env` support, type-safe |
| **IP Reputation** | **ipapi.co** | HTTP API | Free tier 1,000/day, includes `security.vpn/proxy/tor` fields |

---

## 5. Core Features (MVP)

### 5.1 Session Management (`SessionManager`)
- **Multi-org support** via SF CLI aliases (already implemented in `SFCLIAuthService.list_orgs()`)
- **Auto-refresh** via existing `SFCLIAuthService.get_access_token(auto_refresh=True)`
- **Persistent session** across TUI restarts (keyring-backed)
- **Org switcher** (Ctrl+O) — instant alias switching with status
- **Safety gate** — blocks `SalesforceClient` creation if `SafetyMonitor` flags VPN/Proxy

### 5.2 Main TUI Layout

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  TCRM Toolkit  │  Org: prod (user@company.com)  🟢 Safe  │  Ctrl+P:Cmd  │
├──────────────┬──────────────────────────────────────────────────────────────┤
│  NAVIGATION  │                  MAIN CONTENT                                 │
│  ┌────────┐  │  ┌────────────────────────────────────────────────────────┐  │
│  │📊 Data │  │  │  Datasets                                    ▼ □ ×  │  │
│  │  sets  │  │  │  ┌────┬──────────┬──────────┬────────┬──────┬───────┐ │  │
│  │📈 Dash │  │  │  │ #  │ ID       │ Name     │ Label  │ Rows │ Status│ │  │
│  │  boards│  │  │  ├────┼──────────┼──────────┼────────┼──────┼───────┤ │  │
│  │🔄 Flow │  │  │  │ 1  │ 0Fb...   │ Sales    │ Sales  │ 1.2M │ Active│ │  │
│  │  s     │  │  │  │ 2  │ 0Fb...   │ Mktg     │ Mktg   │ 850K │ Active│ │  │
│  │📋 Jobs │  │  │  │ 3  │ 0Fb...   │ Ops      │ Ops    │ 42K  │ Active│ │  │
│  │🔐 Orgs │  │  │  └────┴──────────┴──────────┴────────┴──────┴───────┘ │  │
│  │⚙️  Cfg  │  │  │  [Enter:Details] [E:Extract] [U:Upload] [D:Delete]    │  │
│  └────────┘  │  └────────────────────────────────────────────────────────┘  │
│              │                                                               │
├──────────────┴──────────────────────────────────────────────────────────────┤
│  Status: Connected • API: 1,247/15,000 • Background: 2 running • 🟢 Safe   │
└─────────────────────────────────────────────────────────────────────────────┘
```

**Components**:
- **Left Sidebar**: Navigation (Datasets, Dashboards, Dataflows, Jobs, Orgs, Config)
- **Center**: Data table (sortable, filterable, paginated, keyboard navigable)
- **Right**: Detail panel (context-sensitive — schema, widgets, job status)
- **Bottom**: Status bar (org, connection safety, API usage, background tasks)

### 5.3 Command Palette (Ctrl+P)
- Fuzzy search all actions: "extract dataset", "backup dashboard", "start dataflow", "switch org"
- Recent commands history
- Keyboard-driven workflow (Vim-style bindings optional)

### 5.4 Background Task Runner (`TaskRunner`)
- **Non-blocking** long operations (extract, upload, backup, dataflow start)
- **Progress notifications** in status bar + detail panel
- **Task history panel** — view past operations, re-run, cancel, export logs
- Built on `asyncio.TaskGroup` (Python 3.11+) for structured concurrency

### 5.5 Connection Safety Monitor (`SafetyMonitor`) — **CRITICAL**

#### 5.5.1 Threat Model
Salesforce now **immediately disables users** detected on VPN/Proxy. This is a career-ending risk for developers.

#### 5.5.2 Detection Methods

| Check | Method | Frequency |
|-------|--------|-----------|
| **IP Reputation** | `GET https://ipapi.co/json/` → parse `security.vpn`, `security.proxy`, `security.tor`, `security.hosting` | Startup + every 5 min |
| **VPN Interfaces** | Scan `/sys/class/net/` (Linux) / `Get-NetAdapter` (Windows) for `tun*`, `tap*`, `wg*`, `vpn*`, `wireguard*` | Startup + every 5 min |
| **System Proxy** | Check `http_proxy`/`https_proxy` env vars + OS proxy settings (Windows: `winreg`, macOS: `networksetup`, Linux: `gsettings`) | Startup + every 5 min |
| **DNS Leak** | Resolve `whoami.akamai.net` via system DNS vs known clean DNS (1.1.1.1) — mismatch = leak | Startup only |

#### 5.5.3 Safety Result

```python
@dataclass
class SafetyResult:
    is_safe: bool
    checks: dict[str, CheckResult]
    risk_level: Literal["safe", "warning", "critical"]
    details: str
    timestamp: datetime

@dataclass
class CheckResult:
    name: str
    passed: bool
    details: str
    remediation: str | None
```

#### 5.5.4 Behavior

| Risk Level | UI Behavior | API Behavior |
|------------|-------------|--------------|
| **Safe** | 🟢 Green indicator in status bar | Normal operation |
| **Warning** (proxy env var set but IP clean) | 🟡 Yellow indicator, toast notification | Allow with warning toast |
| **Critical** (VPN/Proxy/Tor detected) | 🔴 Red indicator, **modal dialog on startup**, persistent banner | **HARD BLOCK** — `SessionManager.get_client()` raises `SafetyError` |

#### 5.5.5 Modal Dialog (Critical Risk)

```
┌────────────────────────────────────────────────────────────────┐
│  ⚠️  CONNECTION SAFETY ALERT                                    │
├────────────────────────────────────────────────────────────────┤
│  Salesforce detects VPN/Proxy connections and will IMMEDIATELY │
│  disable your user. Continuing risks permanent org lockout.    │
│                                                                 │
│  Detected:                                                      │
│  🔴 VPN interface: tun0 (WireGuard)                            │
│  🔴 IP Reputation: VPN=true, Proxy=false, Tor=false            │
│                                                                 │
│  [ Disconnect VPN & Retry ]    [ I Understand Risks - Continue ]│
│                                                                 │
│  [ ] Don't show again this session                             │
└────────────────────────────────────────────────────────────────┘
```

#### 5.5.6 Configuration

```bash
# .env
SAFETY_CHECK_ENABLED=true           # Enable/disable (default: true)
SAFETY_CHECK_INTERVAL=300           # Seconds between checks (default: 300)
SAFETY_CHECK_IP_SERVICE=ipapi.co    # ipapi.co | ipinfo.io
SAFETY_BLOCK_ON_CRITICAL=true       # Hard block vs warn only (default: true)
SAFETY_ALLOWLIST_IPS=               # Comma-separated IPs to skip checks
```

---

## 6. Data Operations (Read + Write)

### 6.1 Dataset Browser
| Action | Key | Implementation |
|--------|-----|----------------|
| List | Auto | `DatasetService.list_datasets(page_size=50, sort="Mru")` |
| Search/Filter | `/` | Client-side filter + server-side `q` param |
| Sort | Click header | Re-fetch with `sort=Name|CreatedDate|Mru` |
| Detail | `Enter` | `DatasetService.get_dataset()` + XMD preview |
| Extract | `E` | `DatasetService.extract_dataset()` → background task |
| Upload | `U` | `DatasetService.upload_csv()` → background task |
| Delete | `D` | `DatasetService.delete_dataset()` + confirmation |
| Dependencies | `Y` | `DatasetService.get_dataset_dependencies()` |

### 6.2 Dashboard Browser
| Action | Key | Implementation |
|--------|-----|----------------|
| List | Auto | `DashboardService.list_dashboards()` |
| Detail | `Enter` | `DashboardService.get_dashboard()` + datasets |
| Backup | `B` | `DashboardService.backup_dashboard()` → JSON file |
| Restore | `R` | `DashboardService.restore_dashboard()` (new name) |
| Delete | `D` | `DashboardService.delete_dashboard()` + confirmation |

### 6.3 Dataflow Browser
| Action | Key | Implementation |
|--------|-----|----------------|
| List | Auto | `DataflowService.list_dataflows()` |
| Start | `S` | `DataflowService.start_dataflow()` → job monitor |
| Stop | `T` | `DataflowService.stop_dataflow()` |
| Jobs | `J` | `DataflowService.list_dataflow_jobs()` + live polling |
| Backup | `B` | `DataflowService.backup_dataflow()` → JSON file |

### 6.4 Job Monitor (Dataflow Jobs)
- Live polling (configurable 5-30s) via `DataflowService.wait_for_dataflow_job()`
- Status: Queued → Running → Success/Failed/Cancelled
- Auto-refresh in background task
- Notification on completion

---

## 7. Value-Add Features (Parked — Phase 5+)

*Documented for future implementation. Not in MVP scope.*

| Feature | Description | CRMA Gap |
|---------|-------------|----------|
| **Bulk Backup/Restore** | One-click backup ALL dashboards, datasets, dataflows to Git/zip/S3 | Manual, one-by-one only |
| **Dataset Diff/Compare** | Compare two dataset versions or two datasets (schema + data) | No native diff |
| **Dashboard Versioning** | Git-like history for dashboards with visual diff | No version control |
| **Data Lineage Graph** | Visual graph: Dataflow → Dataset → Dashboard → Lens | Hidden in UI |
| **Scheduled Operations** | Cron-style: "Extract dataset X daily at 2am" | No scheduling |
| **Multi-Org Dashboard Sync** | Promote dashboards Sandbox → Prod with ID remapping | Manual rebuild |
| **Export Formats** | CSV, JSON, Parquet, Avro, Excel, SQL INSERTs | CSV only |
| **Smart Alerts** | "Alert me when dataflow fails", "API usage > 80%" | Basic email only |
| **Operation Scripts** | Record UI actions → replay as YAML/JSON script | No automation |
| **Plugin System** | Custom Python plugins via entry points | Not extensible |

---

## 8. Implementation Phases

### Phase 1: Foundation (Week 1-2)
- [ ] Add `textual` + `ipapi.co` client to `pyproject.toml` `[project.optional-dependencies] interactive`
- [ ] Create `tcrm_toolkit/interactive/` module structure
- [ ] Implement `SessionManager` wrapping `SFCLIAuthService`
- [ ] Implement `SafetyMonitor` with all 4 detection methods
- [ ] Build main TUI `App` class with layout (sidebar, content, detail, status)
- [ ] Login screen → SF CLI web flow (first run)
- [ ] Org switcher (Ctrl+O) with `SFCLIAuthService.list_orgs()`
- [ ] Status bar with safety indicator (🟢/🟡/🔴)

### Phase 2: Core Navigation & Read Operations (Week 2-3)
- [ ] Dataset browser (list, search, filter, sort, paginate, detail)
- [ ] Dashboard browser (list, detail, datasets used)
- [ ] Dataflow browser (list, status, job monitor with live polling)
- [ ] Keyboard shortcuts (Vim + standard: `j/k`, `Enter`, `/`, `Esc`, `q`)
- [ ] Column configuration persistence (width, visibility, sort)

### Phase 3: Write Operations + Background Tasks (Week 3-4)
- [ ] `TaskRunner` with `asyncio.TaskGroup` + progress callbacks
- [ ] Dataset Extract → CSV (progress, chunked, background)
- [ ] Dataset Upload → CSV (progress, chunked, background)
- [ ] Dashboard Backup → JSON file (background)
- [ ] Dataflow Start/Stop → with job tracking
- [ ] Command Palette (Ctrl+P) with fuzzy search
- [ ] Task history panel (view, re-run, cancel, export log)

### Phase 4: Polish & Developer Experience (Week 4-5)
- [ ] Themes (dark/light/default) via Textual CSS
- [ ] Configuration persistence (window size, column prefs, theme)
- [ ] Comprehensive error handling + user-friendly messages
- [ ] `tcrm doctor` includes safety check + auth status + API limits
- [ ] Unit tests for `SafetyMonitor`, `SessionManager`, `TaskRunner`
- [ ] Integration tests for TUI screens (Textual pilot)
- [ ] Documentation: `docs/user-guide.md`, `docs/architecture.md`

### Phase 5+: Value-Add Features (Future)
*See Section 7. Prioritization TBD based on user feedback.*

---

## 9. Code Structure (New Files Only)

```
tcrm_toolkit/
├── interactive/
│   ├── __init__.py
│   ├── app.py                 # Main Textual App entry point
│   ├── session.py             # SessionManager (wraps SFCLIAuthService)
│   ├── safety.py              # SafetyMonitor (VPN/Proxy detection)
│   ├── tasks.py               # TaskRunner (background operations)
│   ├── config.py              # TUI-specific config (theme, keys, layout)
│   ├── screens/
│   │   ├── __init__.py
│   │   ├── main_screen.py     # Main layout with sidebar/content/detail
│   │   ├── login_screen.py    # First-run auth flow
│   │   ├── org_picker.py      # Ctrl+O org switcher
│   │   ├── safety_modal.py    # Critical risk modal dialog
│   ├── widgets/
│   │   ├── __init__.py
│   │   ├── data_table.py      # Sortable, filterable, paginated table
│   │   ├── detail_panel.py    # Context-sensitive detail view
│   │   ├── progress_panel.py  # Background task progress
│   │   ├── command_palette.py # Ctrl+P fuzzy search
│   │   ├── status_bar.py      # Bottom status (org, safety, API, tasks)
│   │   ├── task_history.py    # Past operations panel
│   ├── operations/
│   │   ├── __init__.py
│   │   ├── dataset_ops.py     # Extract, upload, delete, dependencies
│   │   ├── dashboard_ops.py   # Backup, restore, delete
│   │   ├── dataflow_ops.py    # Start, stop, job monitor
│   ├── styles/
│   │   ├── default.css        # Default theme
│   │   ├── dark.css           # Dark theme
│   │   ├── light.css          # Light theme
└── cli/
    └── main.py                # Add `interactive` command / default behavior
```

**Existing files unchanged** — only new additions in `interactive/`.

---

## 10. Entry Points

```bash
# Interactive TUI (NEW DEFAULT)
tcrm                              # Launches TUI, auto-auth via SF CLI if needed

# Classic CLI (PRESERVED for scripts/CI)
tcrm datasets list
tcrm dashboards backup <id>
tcrm dataflows start <id>
tcrm auth login --method sfcli    # Still works

# Explicit flags
tcrm --interactive                # Force TUI mode
tcrm --no-interactive             # Force CLI mode (for scripts)
tcrm doctor                       # Includes safety check
```

---

## 11. Dependencies

```toml
# pyproject.toml
[project.optional-dependencies]
interactive = [
    "textual>=0.52.0",
    "textual-dev>=0.1.0",      # Dev tools (CSS inspector, etc.)
    "httpx>=0.27.0",           # For ipapi.co calls (already in deps)
]

# Existing deps (unchanged):
# httpx, tenacity, pydantic, pydantic-settings, rich, typer, structlog,
# pandas, keyring, cryptography, authlib, python-jose, etc.
```

---

## 12. Configuration

```bash
# .env additions for TUI
TCRM_TUI_THEME=dark                    # dark | light | auto
TCRM_TUI_KEYBINDINGS=vim               # vim | standard
TCRM_TUI_REFRESH_INTERVAL=10           # Job monitor poll interval (seconds)
TCRM_TUI_MAX_HISTORY=100               # Task history entries to keep

# Safety Monitor (Section 5.5.6)
SAFETY_CHECK_ENABLED=true
SAFETY_CHECK_INTERVAL=300
SAFETY_CHECK_IP_SERVICE=ipapi.co
SAFETY_BLOCK_ON_CRITICAL=true
SAFETY_ALLOWLIST_IPS=
```

---

## 13. Testing Strategy

| Layer | Approach |
|-------|----------|
| **SafetyMonitor** | Unit tests with mocked `httpx` responses (VPN/proxy/clean IPs) |
| **SessionManager** | Unit tests with mocked `SFCLIAuthService` |
| **TaskRunner** | Unit tests with mocked coroutines + `asyncio.TaskGroup` |
| **TUI Screens** | Textual `pilot` testing (headless) for key flows |
| **Integration** | `tcrm doctor` + `tcrm --interactive --help` smoke tests |

---

## 14. Risks & Mitigations

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| Textual API changes | Low | Medium | Pin version, test on upgrade |
| ipapi.co rate limits | Medium | Low | Cache results 5 min, fallback to ipinfo.io |
| SF CLI not installed | High | High | Clear error message with install URL |
| VPN false positive | Low | High | Allowlist IPs, "I understand risks" override |
| Keyring unavailable (headless) | Medium | Medium | Fallback to encrypted file storage |

---

## 15. Success Criteria (MVP)

1. **Launch**: `tcrm` starts TUI in <2s (cold) / <500ms (warm)
2. **Auth**: First run → SF CLI web login → persistent session
3. **Safety**: VPN detection works on Linux/macOS/Windows, blocks API calls
4. **Navigation**: Browse 1000+ datasets/dashboards/dataflows smoothly
5. **Operations**: Extract 1M+ row dataset with progress, background, cancel
6. **Multi-org**: Switch orgs in <1s via Ctrl+O
7. **Reliability**: No crashes in 8-hour dev session

---

## 16. Open Decisions

| Decision | Options | Recommendation |
|----------|---------|----------------|
| **Block vs Warn** | Hard block / Soft warn | **Hard block** (career risk) |
| **Check Interval** | 60s / 300s / 600s | **300s** (balance safety/performance) |
| **IP Service** | ipapi.co / ipinfo.io | **ipapi.co** (simpler, includes hosting) |
| **Offline Behavior** | Allow / Block | **Allow with warning** (don't block dev work offline) |
| **Default Theme** | Dark / Light / Auto | **Dark** (developer preference) |

---

## 17. Next Steps

1. ✅ Design doc complete
2. ⏳ Create feature branch: `git checkout -b feature/interactive-tui`
3. ⏳ Add `interactive` optional dependencies to `pyproject.toml`
4. ⏳ Implement Phase 1 (Foundation)
5. ⏳ Iterate with user feedback

---

## Appendix A: Legacy Feature Parity Check

| Legacy Feature (`_legacy/`) | Current CLI | TUI MVP |
|----------------------------|-------------|---------|
| List datasets | ✅ `tcrm datasets list` | ✅ Browser |
| List dashboards | ✅ `tcrm dashboards list` | ✅ Browser |
| List dataflows | ✅ `tcrm dataflows list` | ✅ Browser |
| List Data Manager jobs | ✅ `tcrm jobs list` | ✅ Job Monitor |
| Create dataset from CSV | ✅ `tcrm datasets upload` | ✅ Upload |
| Mass backup dataflows | ❌ | 📅 Phase 5 |
| Mass backup dashboards | ❌ | 📅 Phase 5 |
| Mass backup user XMDs | ❌ | 📅 Phase 5 |
| Check TCRM limits | ✅ `tcrm limits` (TODO) | ✅ Status bar |
| Login config | ✅ `tcrm auth login` | ✅ Login screen |

---

## Appendix B: Salesforce Analytics API Endpoint Coverage

| Category | Current Client | TUI Access |
|----------|----------------|------------|
| Datasets | List, Get, Extract, Upload, Delete, XMD, Dependencies | ✅ All |
| Dashboards | List, Get, Backup, Restore, Delete, Datasets | ✅ All |
| Dataflows | List, Get, Start, Stop, Jobs, Backup | ✅ All |
| Data Manager | External Data (upload) | ✅ Upload |
| Limits | Basic | ✅ Status bar |
| Lenses | ❌ | 📅 Phase 5 |
| Apps/Folders | ❌ | 📅 Phase 5 |
| Users/Groups | ❌ | 📅 Phase 5 |
| Metadata/Lineage | ❌ | 📅 Phase 5 |

---

*End of Design Document*