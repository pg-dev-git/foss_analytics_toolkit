# Phase 4 Prompt — Polish & Developer Experience

Copy the text below into a fresh agent session. It is self-contained.

---

You are implementing the TCRM Toolkit (a modern async Python CLI + `textual` TUI for
Salesforce Tableau CRM). Your job is to complete **Phase 4: Polish & Developer Experience**
and pick up the work exactly where the previous phase left off.

## 0. Before writing any code

1. **Read `AGENTS.md` in the repo root and follow it.** It contains the environment,
   architecture, conventions, testing, and gotchas you must respect.
2. Read the authoritative spec: `docs/plans/phases/phase-4-polish-dx.md` (especially the
   **Acceptance Criteria** table near the end and the "Implementation Order").
3. Read the previous phase-completion review: the current branch is
   `feature/interactive-tui-complete`, Phase 3 is complete. **Phase 4 is ~60% done** —
   many files already exist but the wiring is incomplete. This is your starting point.
4. Set up the environment first:
   ```bash
   uv sync --extra interactive --extra dev
   uv run pytest -v --tb=short   # baseline: 43 passing
   ```
   If tests don't pass at baseline, stop and report — do not proceed.

## 1. Context: what is already done (do NOT re-implement)

- **Themes** — `interactive/styles/{default,dark,light}.css` exist and are wired.
- **Config model** — `interactive/config.py::TUIConfig` (Pydantic-settings, `TCRM_TUI_*`
  env vars: `theme`, `sidebar_width`, `detail_panel_width`).
- **Config persistence** — `interactive/config_manager.py::ConfigManager` (JSON in
  `~/.tcrm/`, `save`/`load`/`get`/`set`).
- **Window state** — `interactive/window_manager.py::WindowManager` (JSON in `~/.tcrm/`
  for window size/preferences).
- **Notifications** — `interactive/notifications.py::NotificationManager` exists.
- **Help screen** — `interactive/screens/help_screen.py::HelpScreen` exists (tabbed,
  categorized shortcuts) and is exported from `screens/__init__.py`.
- **Task History / Progress Panel** — `interactive/widgets/{task_history,progress_panel}.py`
  are now real widgets (completed in Phase 3).

## 2. What is MISSING / INCOMPLETE (your work)

These gaps were confirmed against the live code and must be closed:

1. **F1 help binding is broken (bug).** `interactive/app.py` registers
   `Binding("f1", "help", ...)` but **no `action_help` method exists anywhere** — pressing
   F1 raises a `BindingError`. Add `action_help` (on `TCRMApp`, or dispatch to
   `MainScreen`) that launches the existing `HelpScreen` modal.
2. **Command palette is a stub.**
   `interactive/screens/main_screen.py:167` calls
   `self.notify("Command palette coming in Phase 4", ...)`. Implement a real fuzzy-search
   command palette: `Ctrl+P` should show searchable actions (e.g. "Extract dataset",
   "View Task History", "Switch org", "Help") and dispatch them. `interactive/widgets/
   command_palette.py` already exists — wire it in (or improve it). This is a key
   acceptance criterion.
3. **Column-width persistence is not implemented.** `window_manager.py` documents
   "column widths" but persists nothing related to columns, and
   `interactive/widgets/data_table.py` never saves/restores column widths. Implement
   save-on-resize / restore-on-init for `DataBrowser` columns via `WindowManager` (or
   `ConfigManager`). This is a key acceptance criterion.
4. **Config view is a TODO.**
   `interactive/screens/main_screen.py:157` mounts
   `Static("Configuration view - TODO")`. Replace it with a real configuration view
   (show current `TUIConfig`: theme, sidebar/detail widths, etc.), or, if wiring a live
   config editor is out of scope, at least render the current persisted config read-only.
5. **App integration.** Ensure the running app actually loads persisted config, applies
   the theme, and restores window/column state on startup — per the phase doc's
   "Main App integration" step.

Work through the phase doc's **Implementation Order** (themes → config → config_manager →
window_manager → notifications → help_screen → doctor → tests → docs → app integration)
and **fill the gaps above**, not re-create what already exists.

## 3. Acceptance criteria you must satisfy (from the phase doc)

| Feature | Verification |
|---------|--------------|
| Themes work | Dark/light/custom switch correctly, colors update |
| Config persistence | Window size, **column widths**, filters saved/restored |
| Command palette | **Ctrl+P shows fuzzy searchable actions** |
| Help screen | **F1 shows the organized keyboard shortcuts** |
| Notifications | Success/warning/error/info messages appear appropriately |
| Doctor command | Runs all checks, shows clear pass/fail |
| Unit tests | >80% coverage on new/changed interactive components |
| Documentation | User guide covers installation, usage, troubleshooting |
| Cross-platform | All features work on Windows, Linux, macOS |

## 4. Constraints & quality bar (from AGENTS.md)

- Python 3.11+, `pathlib.Path` everywhere, async throughout.
- Use `structlog` for logging; use `datetime.now(timezone.utc)` — **never** `datetime.utcnow()`.
- Respect the layered architecture: `cli/ → interactive/ → core/services/ → core/`.
- Persistence JSON goes in `~/.tcrm/` (see `config_manager`/`window_manager` — reuse
  them; don't fork a second persistence mechanism).
- Keep it simple. No special-case glue, no dead code. Prefer eliminating a branch over
  guarding it.
- Cross-platform: no hardcoded path separators.

## 5. Tests

- Add/extend tests under `tests/unit/test_interactive.py` for the new/changed components
  (command palette, help-screen launch, column-width save/restore, config persistence,
  notifications).
- Prefer real code paths over mocks. For the palette/help dispatch, assert the correct
  action was triggered. For persistence, write to a temp config dir and assert round-trip.
- Aim for >80% coverage on the interactive components you touch.

## 6. Documentation

- Update `docs/user-guide.md` (create if missing) with installation, usage, keyboard
  shortcuts, themes, and troubleshooting.
- Add any new architecture decisions to `docs/plans/architecture-decisions.md`.

## 7. Verify before you finish

```bash
uv run pytest -v --tb=short   # all passing (was 43 baseline; now includes new tests)
uv run ruff check .
uv run mypy tcrm_toolkit
uv run python scripts/verify-cross-platform.py
uv run tcrm --help            # CLI still loads; commands registered
```

Manual checks (report results):
- `tcrm --interactive` → press **F1** → help screen opens.
- Ctrl+**P** → type "extract" → action dispatched.
- Change a column width in a browser, restart → width restored.
- `TCRM_TUI_THEME=light tcrm --interactive` → theme applied.

If `ruff`/`mypy` report issues, fix them (lint/typecheck are non-blocking in CI, but ship
clean). Do not commit if the test suite is red.

## 8. Report back

Provide a concise summary:
- What you changed/added (file + one-line "what it does").
- Which acceptance criteria you satisfied and the manual-check evidence.
- The final test/lint/typecheck/cross-platform results.
- Any deviations from the phase doc and why.
- Suggested next steps for **Phase 5** (parked value-add features: bulk ops, diff,
  lineage, scheduling, plugins) — flag anything that Phase 4 should have prepared for.

## Non-goals

- Do not rework Phases 0–3.
- Do not implement Phase 5 (parked value-add features).
- Do not change the public CLI surface (command names/flags) unless a bug requires it.
