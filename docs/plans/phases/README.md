# CRM Toolkit Interactive TUI - Implementation Plans

**Master Index** — Phase-by-phase implementation documents for the Interactive TUI with VPN/Proxy Safety Monitor.

---

## 📋 Phase Overview

| Phase | Document | Focus | Est. Duration | Status |
|-------|----------|-------|---------------|--------|
| **0** | [phase-0-foundation-setup.md](phase-0-foundation-setup.md) | Project setup, dependencies, Docker, cross-platform config | 2-3 days | ✅ Completed |
| **1** | [phase-1-core-infrastructure.md](phase-1-core-infrastructure.md) | SessionManager, SafetyMonitor, Textual App skeleton, SF CLI auth integration | 1 week | ✅ Completed |
| **2** | [phase-2-navigation-browsers.md](phase-2-navigation-browsers.md) | Dataset/Dashboard/Dataflow browsers, search/filter/sort, detail panels | 1 week | ✅ Completed |
| **3** | [phase-3-operations-background-tasks.md](phase-3-operations-background-tasks.md) | TaskRunner, parallel dataset extract/upload, dashboard backup, dataflow control | 1 week | ✅ Completed |
| **4** | [phase-4-polish-dx.md](phase-4-polish-dx.md) | Themes, config persistence, command palette, tests, docs, doctor command | 1 week | ⏳ Pending |
| **5** | [phase-5-value-add-future.md](phase-5-value-add-future.md) | Parked features: bulk ops, diff, lineage, scheduling, plugins | Future | ⏳ Parked |

---

## 🎯 Cross-Cutting Requirements (All Phases)

### OS-Agnostic Design
- **Native Python**: All code must run on Windows, Linux, macOS without modification
- **Path handling**: Use `pathlib.Path` exclusively, never hardcode separators
- **Process management**: Use `asyncio.subprocess` / `concurrent.futures.ProcessPoolExecutor` (not `multiprocessing` directly)
- **Terminal detection**: Handle Windows Console vs ANSI terminals (Textual handles this)
- **Keyring**: Use `keyring` backend auto-detection (works on all OSes)
- **SF CLI paths**: Already handled in `tcrm_toolkit/core/sf_cli.py` — extend as needed

### Docker Support (Optional but Recommended)
```dockerfile
# Multi-stage build for minimal production image
# Base: python:3.12-slim
# Install: SF CLI, textual, dependencies
# Entry: tcrm (interactive TUI)
```
- **Dockerfile** in repo root
- **docker-compose.yml** for dev environment
- **GitHub Actions** for multi-arch builds (amd64, arm64)

### Parallel Processing Strategy (Dataset Operations)
| Operation | Approach | Rationale |
|-----------|----------|-----------|
| **SAQL Query Execution** | `asyncio.Semaphore` + `asyncio.gather` | I/O-bound, 10-20 concurrent requests |
| **Chunk Download** | Async HTTP with controlled concurrency | Network-bound, respect API limits |
| **CSV Merging** | `ProcessPoolExecutor` (multiprocessing) | CPU-bound, pandas concat is GIL-heavy |
| **Data Processing** | `ProcessPoolExecutor` for heavy transforms | CPU-bound, bypass GIL |
| **Progress Tracking** | Shared async queue + background task | Non-blocking UI updates |

**Legacy Reference**: `_legacy/dataset_tasks/dataset_extract_MP.py` and `dataset_extract_MT.py` show the original multiprocessing approach.

---

## 📦 Documentation Structure

```
docs/plans/
├── README.md                           # This file - master index
├── phases/
│   ├── phase-0-foundation-setup.md     # Setup, deps, Docker, config
│   ├── phase-1-core-infrastructure.md  # Session, Safety, App skeleton
│   ├── phase-2-navigation-browsers.md  # Browsers, tables, detail panels
│   ├── phase-3-operations-background-tasks.md  # TaskRunner, parallel ops
│   ├── phase-4-polish-dx.md            # Themes, palette, tests, docs
│   └── phase-5-value-add-future.md     # Parked features reference
└── architecture-decisions.md           # Key decisions log (append-only)
```

---

## 🔧 Development Workflow

### For Each Phase
1. **Read** the phase document completely
2. **Create** feature branch: `git checkout -b feature/phase-X-<name>`
3. **Implement** following the explicit requirements
4. **Test** with `pytest` + manual TUI verification
5. **Document** any architecture decisions in `architecture-decisions.md`
6. **PR** with phase document as reference

### Coding Agent Instructions
Each phase document contains:
- **Explicit requirements** (what to build)
- **Code patterns** (how to build it)
- **Acceptance criteria** (how to verify)
- **File paths** (where to put code)
- **Dependencies** (what to import)
- **Cross-platform notes** (OS-specific handling)

---

## 🚀 Quick Start for Implementers

```bash
# 1. Clone and setup
git clone <repo>
cd crma

# 2. Install with interactive extras
uv sync --extra interactive --extra dev

# 3. Or use Docker (when available)
docker compose up -d dev
docker compose exec dev bash

# 4. Run TUI
tcrm                    # Interactive mode
tcrm --help             # CLI mode
tcrm doctor             # Diagnostics
```

---

## 📝 Architecture Decision Log

See [architecture-decisions.md](architecture-decisions.md) for running log of key decisions.

---

*Last updated: 2026-07-23*