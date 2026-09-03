# Phase 3 Prompt — Operations & Background Tasks

Copy the text below into a fresh agent session. It is self-contained.

---

You are implementing the TCRM Toolkit (a modern async Python CLI + `textual` TUI for
Salesforce Tableau CRM). Your job is to complete **Phase 3: Operations & Background
Tasks** and pick up the work exactly where the previous review left off.

## 0. Before writing any code

1. **Read `AGENTS.md` in the repo root and follow it.** It contains the environment,
   architecture, conventions, testing, and gotchas you must respect.
2. Read the authoritative spec: `docs/plans/phases/phase-3-operations-background-tasks.md`.
   The code blocks in that document are the reference implementation you should complete.
3. Read the **phase-completion review** (the previous assistant's summary): the current
   branch is `feature/interactive-tui-complete`. Phases 0–2 are complete; **Phase 3 is
   partial** — that is your starting point. Do not touch Phase 0–2 code unless a change is
   required to complete Phase 3.
4. Set up the environment first:
   ```bash
   uv sync --extra interactive --extra dev
   uv run pytest -v --tb=short   # baseline: 42 passing
   ```
   If tests don't pass at baseline, stop and report — do not proceed.

## 1. Context: what is already done (do NOT re-implement)

- `tasks.py::TaskRunner` exists and is tested (async execution, `ProcessPoolExecutor`,
  history, cancellation). `merge_csv_chunks`, `process_csv_chunk`, `split_csv_for_parallel`
  helpers already exist.
- All four **service layers** are complete and tested:
  `core/services/{dataset,dashboard,dataflow}_service.py` and `auth_service.py`.
  Real signatures you must call:
  - `DatasetService.extract_dataset`, `extract_dataset_streaming`, `upload_csv`,
    `upload_csv_streaming`, `delete_dataset`, `get_dataset`, `get_dataset_xmd`,
    `get_row_count`, `get_dataset_dependencies`, plus private
    `_calculate_chunk_size`, `_build_saql_query`, `_extract_fields_from_xmd`.
  - `DataflowService.start_dataflow`, `stop_dataflow`, `wait_for_dataflow_job`,
    `backup_dataflow`, `list_dataflow_jobs`, `get_dataflow_job_status`.
  - `DashboardService.backup_dashboard`, `restore_dashboard`, `delete_dashboard`.
- `session.py::SessionManager` owns auth and exposes `client_context()`, `get_client()`,
  `ensure_valid_token()`. **Every** network call must go through the session — never build
  a bare `SalesforceClient`.

## 2. What is MISSING (your work)

Implement these, matching the reference implementations in the phase doc:

1. **`tcrm_toolkit/interactive/operations/dataset_extract.py`** — `ParallelDatasetExtractor`
   (parallel SAQL queries + `ProcessPoolExecutor` merge). Use the existing `tasks.py`
   helpers and `ExtractionProgress` from `core.models`.
2. **`tcrm_toolkit/interactive/operations/dataset_upload.py`** — `ParallelDatasetUploader`
   (chunk + stream uploads).
3. **`tcrm_toolkit/interactive/operations/dashboard_backup.py`** — background dashboard
   backup/restore tasks.
4. **`tcrm_toolkit/interactive/operations/dataflow_control.py`** — start/stop/jobs
   control with job-status polling.
5. **`widgets/progress_panel.py`** and **`widgets/task_history.py`** — replace the empty
   `Static` subclasses with real widgets that render progress and task history. Wire them
   into `main_screen.py` where the phase doc specifies.

## 3. Critical blocker: the CLI↔auth bridge

All CLI `datasets`, `dashboards`, `dataflows`, `jobs` commands are currently stubs that
print `"Authentication integration pending"`; `cli/commands/datasets.py` even raises
`NotImplementedError("Need to implement token retrieval from keyring")`. The service layer
is done but **unauthenticated**. Fix this so the CLI actually works end-to-end:

- Wire each command through `SessionManager` / `AuthService` / `keyring` + `token_store`
  to the real service layer. `AuthService` exposes `store_tokens`, `retrieve_tokens`,
  `delete_tokens`, `is_token_expired`, `ensure_valid_token`.
- Do **not** leave placeholders. Remove every `"Authentication integration pending"` and
  the `NotImplementedError`. The commands should behave like the read commands already do
  once authenticated.

## 4. Constraints & quality bar (from AGENTS.md)

- Python 3.11+, `pathlib.Path` everywhere, async throughout
  (`asyncio.gather`/`Semaphore` for I/O, `ProcessPoolExecutor` only for CPU-bound pandas).
- Use `structlog` for logging; use `datetime.now(timezone.utc)` — **never** `datetime.utcnow()`.
- Respect the layered architecture: `cli/ → interactive/ → core/services/ → core/`.
- Keep it simple. No special-case glue, no dead code, no stubs. Prefer eliminating a
  branch over guarding it.
- Cross-platform: no hardcoded path separators; handle Windows/Linux/macOS.

## 5. Tests

- Add tests for everything new. Prefer real code paths with the `mock_client` fixture
  (mocks the HTTP layer via `SalesforceClient._client` = `AsyncMock`); exercise real
  service/operation logic and assert on outputs/state — not mocked calls.
- Put interactive-operation tests under `tests/unit/` and any API-path tests under
  `tests/integration/`.
- Add fixtures for `ENCRYPTION_KEY`/`JWT_SECRET_KEY` if needed (see `conftest.py`).

## 6. Verify before you finish

```bash
uv run pytest -v --tb=short   # all passing (was 42 baseline; now includes new tests)
uv run ruff check .
uv run mypy tcrm_toolkit
uv run python scripts/verify-cross-platform.py
uv run tcrm --help            # CLI still loads; commands registered
```

If `ruff`/`mypy` report issues, fix them (lint/typecheck are non-blocking in CI, but
ship clean). Do not commit if the test suite is red.

## 7. Report back

Provide a concise summary:
- Which files were created/changed and what each does.
- How you wired the CLI auth bridge (which session/auth primitives you used).
- The final test/lint/typecheck/cross-platform results.
- Any deviations from the phase doc and why.
- Suggested next steps for **Phase 4** (themes/config persistence/command-palette wiring,
  including a broken F1 `action_help` binding and a stub command palette) if relevant.

## Non-goals

- Do not rework Phases 0–2.
- Do not implement Phase 5 (parked value-add features).
- Do not change the public CLI surface (command names/flags) unless a bug requires it.
