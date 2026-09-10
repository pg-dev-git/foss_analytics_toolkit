# ASFTool Refactor — Atomic Implementation Plan

**Slug:** `asftool` (Analytics Salesforce FOSS Tool)  
**Display Name:** FOSS Analytics Tool for TCRM  
**Branch:** `feature/asftool-refactor` (from `main`)

---

## Philosophy (Ponytail / Software Architecture)

- **YAGNI**: No TUI framework. Rich + questionary menus = "always running OS" feel, 10x less code
- **SF CLI auth only**: Works reliably, handles web/device flow, token refresh, VPN safety
- **Thin CLI layer → Solid service layer** (already exists in `core/services/`)
- **Single auth path** → `SFCLIAuthService` → keyring storage
- **Production-grade**: structured logging, type hints, error handling, tests

---

## Phase Overview

| Phase | Document | Focus | Est. Duration |
|-------|----------|-------|---------------|
| **0** | [00-cleanup-archive.md](00-cleanup-archive.md) | Archive old branches, delete TUI, remove broken OAuth | 1 day |
| **1** | [01-core-foundation.md](01-core-foundation.md) | Rename package, update pyproject, config, logging | 1 day |
| **2** | [02-auth-cli.md](02-auth-cli.md) | SF CLI auth commands (login, logout, status, list-orgs) | 1 day |
| **3** | [03-menu-loop.md](03-menu-loop.md) | Rich + questionary "always running" menu loop | 1 day |
| **4** | [04-dataset-ops.md](04-dataset-ops.md) | Dataset menus: list, extract, upload, delete | 1 day |
| **5** | [05-dashboard-ops.md](05-dashboard-ops.md) | Dashboard menus: list, backup | 0.5 day |
| **6** | [06-dataflow-ops.md](06-dataflow-ops.md) | Dataflow menus: list, backup, start/stop | 0.5 day |
| **7** | [07-jobs-ops.md](07-jobs-ops.md) | Data Manager jobs menu | 0.5 day |
| **8** | [08-parallelism-core.md](08-parallelism-core.md) | Extract TaskRunner + parallel helpers to `core/tasks/` | 1 day |
| **9** | [09-doctor-polish.md](09-doctor-polish.md) | `asftool doctor`, cross-platform verify, README | 1 day |

**Total: ~8 days** (can parallelize some phases)

---

## Quick Start for Implementers

```bash
# 1. Create branch from main
git checkout main
git pull
git checkout -b feature/asftool-refactor

# 2. Follow phases in order
# Each phase doc is self-contained for a coding session

# 3. Install deps
uv sync --extra dev

# 4. Run tests
uv run pytest -v --tb=short

# 5. Test CLI
asftool --help
asftool auth login
asftool           # Interactive menu
```

---

## Dependency Flow

```
cli/ → core/tasks/ → core/services/ → core/auth/ + core/
                       ↓
                  core/client.py
                       ↓
                  Salesforce REST API
```

- `cli/` depends on everything (Typer commands + Rich menus)
- `core/services/` depends on `core/client.py` + `core/auth/`
- `core/client.py` depends on `core/config.py` + `core/exceptions.py`
- No circular dependencies

---

## Acceptance Criteria (All Phases)

- [ ] All 42+ existing unit tests pass
- [ ] `asftool --help` works
- [ ] `asftool auth login` works with real SF CLI
- [ ] `asftool` (no args) launches interactive menu
- [ ] No TUI dependencies in pyproject.toml
- [ ] No `core/services/auth_service.py` (pure Python OAuth removed)
- [ ] Structured logging to `~/.asftool/asftool.log` + stderr
- [ ] `asftool doctor` passes on all platforms
- [ ] README updated with new CLI usage
- [ ] `uv run ruff check .` passes
- [ ] `uv run mypy asftool` passes
