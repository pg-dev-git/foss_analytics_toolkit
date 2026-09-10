# Phase 0: Cleanup & Archive

**Goal:** Remove all TUI code, broken OAuth, and archive old branches. Start clean from `main`.

---

## Prerequisites

- On `main` branch (legacy only)
- All changes committed or stashed

---

## Step 0.1: Create Feature Branch

```bash
git checkout main
git pull
git checkout -b feature/asftool-refactor
```

---

## Step 0.2: Archive Old Branches (Optional but Recommended)

```bash
# Tag branches for historical reference before cleanup
git tag archive/feature-interactive-tui-complete feature/interactive-tui-complete
git tag archive/feature-phase-0-foundation-setup feature/phase-0-foundation-setup
git tag archive/feature-phase-2-navigation-browsers feature/phase-2-navigation-browsers
git tag archive/fix-tui-stderr-log-pollution fix/tui-stderr-log-pollution
git tag archive/refactor-greenfield-cli refactor/greenfield-cli

# Push tags
git push origin --tags

# Delete local feature branches (they're now tagged)
git branch -D feature/interactive-tui-complete
git branch -D feature/phase-0-foundation-setup
git branch -D feature/phase-2-navigation-browsers
git branch -D fix/tui-stderr-log-pollution
git branch -D refactor/greenfield-cli
```

---

## Step 0.3: Delete TUI Directory Entirely

```bash
# This removes ~3,500 lines of Textual TUI code
rm -rf tcrm_toolkit/interactive/
```

---

## Step 0.4: Delete Broken Pure Python OAuth Service

```bash
# This removes the confusing dual-auth system
rm tcrm_toolkit/core/services/auth_service.py
```

---

## Step 0.5: Update `core/services/__init__.py`

```python
# REMOVE: from .auth_service import AuthService, create_auth_service
# KEEP:
from .dataset_service import DatasetService
from .dashboard_service import DashboardService
from .dataflow_service import DataflowService

__all__ = [
    "DatasetService",
    "DashboardService",
    "DataflowService",
]
```

---

## Step 0.6: Update `core/auth/__init__.py`

```python
# REMOVE: from .sf_cli_auth import SFCLIAuthService  (if it was exporting AuthService)
# KEEP only SF CLI auth exports:
from .sf_cli_auth import SFCLIAuthError, SFCLIAuthService
from .token_store import StoredToken, TokenStore

__all__ = [
    "SFCLIAuthError",
    "SFCLIAuthService",
    "StoredToken",
    "TokenStore",
]
```

---

## Step 0.7: Remove Textual Dependencies from `pyproject.toml`

```toml
# REMOVE this entire optional-dependency section:
# interactive = [
#     "textual>=0.52.0",
#     "textual-dev>=0.1.0",
#     "httpx>=0.27.0",
# ]

# ADD questionary for menus:
[project.optional-dependencies]
dev = [
    # ... existing ...
]
# No "interactive" extra needed - questionary goes in main deps

# In main dependencies, ADD:
dependencies = [
    # ... existing ...
    "questionary>=2.0.0",
    "rich>=13.7.0",  # already there
]
```

---

## Step 0.8: Remove TUI Styles Directory

```bash
# If exists (from TUI phase)
rm -rf tcrm_toolkit/interactive/styles/
```

---

## Step 0.9: Run Tests to Verify Cleanup

```bash
uv sync --extra dev
uv run pytest -v --tb=short
# Expect: 42+ tests passing (unit tests for crypto, client, auth schemas)
# Integration tests may need adjustment if they imported AuthService
```

---

## Step 0.10: Fix Any Import Breakage in Tests

Check `tests/unit/test_interactive.py` - likely needs deletion since TUI is gone.

```bash
# If it only tested TUI widgets, delete it:
rm tests/unit/test_interactive.py
```

---

## Acceptance Criteria

- [ ] `tcrm_toolkit/interactive/` directory deleted
- [ ] `tcrm_toolkit/core/services/auth_service.py` deleted
- [ ] `core/services/__init__.py` exports only 3 services
- [ ] `core/auth/__init__.py` exports only SF CLI auth
- [ ] `pyproject.toml` has no `textual` dependencies
- [ ] `questionary` in main dependencies
- [ ] All unit tests pass (42+)
- [ ] No import errors in `tcrm_toolkit` package

---

## Notes for Implementer

- This phase is purely deletion — no new code
- Run `uv run ruff check .` after to catch any dangling imports
- The `refactor/greenfield-cli` branch had the solid foundation — we're keeping that architecture, just removing the TUI layer and broken OAuth
- Legacy `_legacy/FOSS_Toolkit.py` and `_legacy/misc_tasks/` stay as reference only