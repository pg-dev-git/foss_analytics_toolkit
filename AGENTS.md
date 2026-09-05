# AGENTS.md — ASFTool

Guidance for agents working in this repository. Read before editing.

## Project

Modern async Python CLI for Salesforce Tableau CRM (TCRM) Analytics. Rebuilds the
legacy `FOSS_Toolkit.py` (preserved under `_legacy/`) with a `typer` CLI +
`rich` interactive menus. The original "always running OS" menu approach is
preserved — no TUI framework, no Textual.

**Slug:** `asftool` (Analytics Salesforce FOSS Tool)
**Display name:** FOSS Analytics Tool for TCRM
**Current branch:** `feature/asftool-refactor` (refactor of `main`)

---

## Mandatory GitHub & Git Workflow

- **Repository**: `pg-dev-git/foss_analytics_toolkit`
- **Never work directly on `main` (or `master`).**
- **Branch Naming**: Always create a feature branch from `main`:
  - `feature/` for new features (e.g., `feature/dataset-diff`)
  - `fix/` for bug fixes (e.g., `fix/token-refresh-loop`)
  - `chore/` for maintenance/infrastructure (e.g., `chore/uv-bump`)
  - `docs/` for documentation updates (e.g., `docs/architecture-decisions`)
- **Standard GitHub Flow**:
  1. Create and switch to feature branch: `git checkout -b feature/your-feature-name`
  2. Make small, atomic commits with conventional commit messages.
  3. Push branch to remote: `git push -u origin feature/your-feature-name`
  4. Open a Pull Request against `main` using `create_pull_request` (do not merge to `main` without review).

### Conventional Commits Guidelines
Use the format `<type>(<scope>): <short description>`:
- `feat(auth): add device flow login for headless environments`
- `fix(client): resolve token refresh race condition`
- `refactor(tasks): extract TaskRunner from TUI into core`
- `docs(readme): update CLI usage examples for asftool`

---

## Environment & Commands

- Use **uv**, not pip. Run everything via `uv run ...`.
- Install deps: `uv sync --extra dev`
- Tests: `uv run pytest -v --tb=short` (expect 36+ passing)
- Lint: `uv run ruff check .`  ·  Typecheck: `uv run mypy tcrm_toolkit`
- Cross-platform check: `uv run python scripts/verify-cross-platform.py`
- CLI entrypoint: `tcrm` (Phase 1 will rename to `asftool`) (`tcrm --help`, `tcrm auth login`)
- Tests need real encryption keys — `conftest.py` provides them; copy
  `.env.example` → `.env` only if you need live settings.

---

## Architecture (layered — respect the dependency direction)

`cli/` → `core/tasks/` → `core/services/` → `core/` (auth, client, models, config, crypto)

- **cli/** — `typer` command groups (`commands/{auth,datasets,dashboards,dataflows,jobs,doctor}.py`)
  + Rich-based interactive menus (`menus/`, `main.py` menu loop). Wire user actions
  to `core/services/`. Register with `app.add_typer(sub_app, name="...")` in `cli/main.py`.
  The `Session` class in `cli/session.py` is the bridge between CLI/menu handlers
  and the auth + service layer.
- **core/tasks/** — `TaskRunner` (async + `ProcessPoolExecutor` for CPU work),
  parallel helpers (`merge_csv_chunks`, `process_csv_chunk`, `split_csv_for_parallel`).
  TUI-agnostic — used by services and CLI.
- **core/services/** — business logic: `DatasetService`, `DashboardService`,
  `DataflowService`. All async. Delegate HTTP to `SalesforceClient`.
- **core/auth/** — `sf_cli.py` (subprocess wrapper), `sf_cli_auth.py`
  (high-level `SFCLIAuthService`), `token_store.py` (encrypted keyring storage).
  **SF CLI is the only auth path** — no pure Python OAuth.
- **core/** — `client.py` (httpx + tenacity retries), `models/` (pydantic),
  `config.py` (pydantic-settings, `ASFTOOL_*` env vars after Phase 1), `crypto.py`
  (dynamic-salt encryption), `exceptions.py`, `logger.py`, `platform.py`.

### Why no TUI?

The previous attempt (`feature/interactive-tui-complete`) used Textual for a full
TUI. It accumulated ~3,500 lines across screens/widgets/operations, was fragile,
and the project became too complex. This refactor replaces it with Rich + simple
`console.input()` menus — same "always running OS" feel as the legacy
`FOSS_Toolkit.py`, but modern async + 10x less code.

### Why SF CLI only?

The legacy toolkit successfully captured tokens via `sfdx force:auth:web:login`
→ `sfdx force:org:display --json`. The modern pure-Python OAuth attempt
(`core/services/auth_service.py`) tried to run a local callback server and
prompt for the code manually — it never worked reliably. The modern SF CLI
wrapper (`core/sf_cli.py`) does exactly what the legacy did and works.

---

## Conventions

- Python 3.11+, `pathlib.Path` everywhere (no path separators hardcoded).
- Async throughout: `async/await`, `asyncio.gather`/`Semaphore` for I/O,
  `ProcessPoolExecutor` only for CPU-bound work (pandas).
- `structlog` for logging. `datetime.now(timezone.utc)` — **not** `datetime.utcnow()`
  (deprecated, flagged in CI warnings).
- **Error Logging**: Centralized structured JSON logging via `core/logger.py`
  writes concurrently to `stderr` and persistent log file
  `~/.tcrm/tcrm.log` (will become `~/.asftool/asftool.log` in Phase 1).
- Keep it simple: no special-case glue, no dead stubs. Prefer eliminating a branch
  over guarding it.
- **Package name**: `tcrm_toolkit` (Phase 1 will rename to `asftool`). Config:
  `TCRM_*` env vars (Phase 1 will become `ASFTOOL_*`). Config dir: `~/.tcrm/`
  (Phase 1 will become `~/.asftool/`).

---

## Testing

- `tests/unit/` (crypto, client retry, auth schemas) +
  `tests/integration/test_api_endpoints.py` (uses `AsyncMock` on `SalesforceClient._client`).
- Add tests for new/changed behaviour. Assert on real outputs/state, not mocked calls.
- Integration tests mock the HTTP layer via the `mock_client` fixture — exercise real
  service code, not the client.
- **Live Session Seeding & E2E Testing**:
  - To test against a live Salesforce org without interactive browser login, use `scripts/seed_session.py`:
    ```bash
    TCRM_ACCESS_TOKEN="..." TCRM_INSTANCE_URL="..." TCRM_USERNAME="..." \
    uv run python scripts/seed_session.py
    ```
  - **Token Expiry**: If a live session token expires or encounters authentication
    errors during testing, the agent must immediately notify the user to request a
    fresh token.

---

## Implementation Plan

The refactor is split into 10 atomic phases. Each phase has a dedicated document
that serves as input for a coding session to avoid context overload.

See: `docs/plans/asftool-refactor/`

| Phase | Document | Focus |
|-------|----------|-------|
| 0 | `00-cleanup-archive.md` | Archive old branches, delete TUI, remove broken OAuth |
| 1 | `01-core-foundation.md` | Rename package, update pyproject, config, logging |
| 2 | `02-auth-cli.md` | SF CLI auth commands (login, logout, status, list-orgs) |
| 3 | `03-menu-loop.md` | Rich + questionary "always running" menu loop |
| 4 | `04-dataset-ops.md` | Dataset menus: list, extract, upload, delete |
| 5 | `05-dashboard-ops.md` | Dashboard menus: list, backup |
| 6 | `06-dataflow-ops.md` | Dataflow menus: list, backup, start/stop |
| 7 | `07-jobs-ops.md` | Data Manager jobs menu |
| 8 | `08-parallelism-core.md` | Extract TaskRunner + parallel helpers to `core/tasks/` |
| 9 | `09-doctor-polish.md` | `asftool doctor`, cross-platform verify, README |

Each phase is self-contained — an agent can start from a fresh session with just
the phase document and the current branch.

---

## Gotchas

- **Phases are sequential.** Phases 0-2 are foundational — don't skip them.
  Phases 4-7 can be done in parallel once phase 3 is complete.
- **CLI write commands used to be stubs.** After phase 2, `tcrm auth login`
  is the entry point — it captures the token via SF CLI and stores in keyring.
  All subcommands (datasets, dashboards, etc.) work after their respective phases.
- **TUI is intentionally absent.** Don't add Textual or any TUI framework. The
  menu loop in `cli/main.py` is the entry point for interactive use.
- **No pure Python OAuth.** The old `core/services/auth_service.py` was deleted
  in phase 0. Only SF CLI auth is supported.
- **Config paths will change in Phase 1.** `~/.tcrm/` → `~/.asftool/`.
  `TCRM_*` env vars → `ASFTOOL_*` env vars. Migration is not provided — clean start.
- **CI on `main` does not run** (workflow targets `main`/`refactor/**`/`feature/**`;
  `main` holds only the legacy toolkit). Update CI to target the new branches
  in phase 9.

---

## Lessons Learned (read these before adding new submenus or auth code)

These are the bugs that shipped in earlier phases and were caught only
by user testing from the menu, not by unit tests. They are now fixed
and have regression tests, but the patterns below are easy to
re-introduce if you are not careful.

### 1. `*_async` wrappers must contain the async body, not delegate to sync commands

If a Typer command is `def cmd(): _run(_async_fn())` and a menu handler
calls `cmd()`, the menu loop's `asyncio.run()` collides with the
`asyncio.run()` inside `cmd()`. The error is
`RuntimeError: asyncio.run() cannot be called from a running event loop`.

The correct shape is:

```python
# Async wrapper - contains the actual coroutine body
async def cmd_async(*args, **kwargs) -> None:
    session = Session(...)
    try:
        async with session.client_context() as client:
            # ... actual work ...
    finally:
        await session.close()

# Typer command - thin shim that calls the wrapper via _run
@app.command()
def cmd(*args, **kwargs):
    _run(cmd_async(*args, **kwargs))
```

The menu handler `await cmd_async(...)` works. The CLI command works.
Both share the same code path. The regression test
`tests/unit/test_menu_submenus.py` enforces this for all 5 submenus
(auth, datasets, dashboards, dataflows, jobs).

### 2. SF CLI's `org display --json` field is `expirationDate`, not `tokenExpiration`

The legacy `sfdx` CLI used `tokenExpiration`; the current `sf` CLI uses
`expirationDate`. If the field name doesn't match, `expires_at` is
parsed as `None` and `StoredToken.is_expired()` returns whatever the
default is. See point 3 for why that default matters.

`_parse_auth_result` in `core/sf_cli.py` tries both names and
falls back gracefully. Keep the fallback list in sync with
real-world SF CLI output.

### 3. Don't return `True` for "unknown expiry" in `is_expired()`

The original code was:

```python
def is_expired(self, buffer_seconds=60):
    if not self.expires_at:
        return True   # <-- wrong
    ...
```

This made the auth flow unrecoverable: if `expires_at` was None
(because of bug #2 or any field-name change), every request was
treated as expired, which triggered a re-login via web, which got
the same broken result, forever. The user had to manually clean the
keyring.

The fix: return `False` when `expires_at` is None or unparseable.
We trust SF CLI's own refresh logic; the API call will get a real
401 if the token is actually bad, and that is a real signal we can act
on. A guess of "expired" from missing data is worse than a guess
of "fresh".

### 4. `datetime.utcnow()` is naive; comparing to aware datetimes silently raises `TypeError`

```python
expires = datetime.fromisoformat("2026-01-01T00:00:00Z")  # aware
datetime.utcnow() >= expires                                 # naive >= aware -> TypeError
```

The `except (ValueError, TypeError)` clause caught the TypeError
and returned the default value, masking the bug. The fix: use
`datetime.now(timezone.utc)` (aware) so the comparison works.

This affected `is_expired()` and only surfaced when an actually-
expired token was stored with a tz-aware `expires_at`. The bug was
found by writing the test in `tests/unit/test_token_store.py`.

### 5. Stale `tcrm` binaries on user laptops

When the user types `tcrm auth login` and it fails with
`ModuleNotFoundError: No module named 'tcrm_toolkit.cli.main'`, that is
NOT our code — it is a leftover `tcrm` shim from before the refactor
(a Python 3.13 install with the old `tcrm-toolkit` package). The fix
is environment-level: `where.exe tcrm` then delete the shim, or
`py -3.13 -m pip uninstall tcrm-toolkit`. The error message that
mentions `tcrm` from inside `asftool` is a different bug: we had
a few hardcoded user-facing strings saying "Run 'tcrm auth login'".
Fixed in commit `bdf94cb` (3 occurrences in `core/auth/sf_cli_auth.py`)
with a regression test in `tests/unit/test_auth_schemas.py` that
asserts the message contains "asftool" not "tcrm".

### 6. The menu loop has its own event loop — always test from inside it

Unit tests that call Typer commands directly work fine because there is
no outer event loop. The `asyncio.run()` nesting bug only surfaces when
the menu calls the same code. When adding or refactoring menu
handlers, add a test that exercises the menu path
(`_run_menu_loop` with a mocked `console.input`) or at minimum
the structural check in `tests/unit/test_menu_submenus.py` will
catch a regression via the `*_async` wrapper name assertion.