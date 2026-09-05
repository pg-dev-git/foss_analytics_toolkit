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

## Lessons Learned (read this section before making changes)

This section captures the most important gotchas, bugs, and
architectural patterns discovered across all 10 phases of the
refactor. Many of these were shipped and only caught by user
testing from the menu (not by unit tests). Each lesson is tied to
the commit that fixed it; follow the trail in git history if you
need full context.

### Architecture & dependencies

**1. `cli/` must never import from each other across command groups.**
Each `commands/<domain>.py` is independent. Shared logic lives in
`core/services/` and `core/auth/`. The `cli/session.py::Session`
class is the only place a domain command should reach for the
client — it owns auth + lifecycle and ensures the client is closed
even on exception. The `asftool cli main` callback is the
orchestrator that calls Typer's app or runs the menu loop. Never
short-circuit the session lifecycle for convenience.

**2. The dependency direction is strict: `cli/ → core/tasks/ →
core/services/ → core/`. Nothing below `core/` may import from
`cli/`. This is enforced by convention, not a linter rule. If you
add a new package, document its place in this chain in AGENTS.md.

**3. `Session.client_context()` is an async context manager. It
opens a `SalesforceClient` and closes it on exit, even when an
exception propagates. The session itself is closed in the
`finally` block of every `_async` function. This is the only
correct way to use the client. Don't instantiate a bare
`SalesforceClient` in a command.**

### Phase 0-1: the rename and what it broke

**4. Mechanical renames miss string content.** Phase 1 renamed
`tcrm_toolkit` → `asftool` and `tcrm` → `asftool` in code paths
(imports, pyproject, entry points) but not in user-facing strings
inside `f-strings` and exception messages. The result: 3 hardcoded
`"Run 'tcrm auth login'"` strings stayed in
`core/auth/sf_cli_auth.py`. Tests didn't catch it because no test
asserted the message text. Always grep for the old name as a string
literal — not just as an identifier — after a rename. Fixed in
commit `bdf94cb`, regression test in
`tests/unit/test_auth_schemas.py::TestAuthErrorMessages`.

**5. Phase 0 also killed the `core/services/auth_service.py`
pure-Python OAuth. Never re-introduce it.** The only auth path is
SF CLI subprocess → `org display --json` → token. The legacy
service had manual OAuth callback server code that never worked
reliably across networks. SF CLI's own refresh logic is the only
abstraction we want to depend on.

### Phase 2-7: the async/Typer pattern

**6. `*_async` wrappers must contain the actual coroutine body, not
delegate to the sync Typer command.** The correct shape is:

```python
# Async wrapper - contains the full async body
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

The anti-pattern that ships a bug:

```python
# BAD: wrapper just calls sync command
async def cmd_async(*args, **kwargs):
    cmd(*args, **kwargs)   # cmd ends with _run(_async_fn()) = asyncio.run(...)
```

When a menu handler `await cmd_async(...)` runs, it is already inside
an event loop. The inner `asyncio.run()` in `cmd` raises
`RuntimeError: asyncio.run() cannot be called from a running event
loop`, the `coroutine was never awaited` warning is logged, and the
menu dies. This bug shipped in Phases 4-7 because the wrappers
were stubs. Fixed in commits `76acfc8` (auth), `8fffaae` (datasets),
`ff96111` (dashboards), `9fbbfa2` (dataflows), `8fc8229` (jobs).
The regression test `tests/unit/test_menu_submenus.py` enforces
this for all 5 submenus by static-checking that every handler's
source contains the `*_async` wrapper name.

**7. `asyncio.run()` is one-shot per process.** Once a Typer
subcommand has finished and `asyncio.run()` returned, that event
loop is dead. If you try to `await` something that was created on
that loop (like an httpx connection) from a different loop, you
get "Event loop is closed". Solution: create the client fresh per
command inside `client_context()` (which is what `Session` does
anyway), never cache an async client across commands.

**8. The menu loop's `_run_menu_loop` runs in a single event loop.
The `await item.handler()` call is the only way to enter handler
code. A handler that does any sync I/O (including `asyncio.run()`)
will block the loop and break other concurrent features. Always
await the `*_async` wrappers — never call sync Typer commands
directly from a handler.**

### Phase 8: parallelism

**9. `asyncio.Semaphore` + `asyncio.gather` is the right pattern for
I/O-bound parallelism (e.g. concurrent SAQL queries).** Bound
concurrency to ~10 in production to respect Salesforce rate limits
(no documented limit, but empirically 10 works well).

**10. `ProcessPoolExecutor` is for CPU-bound work only** (pandas
concat, base64 encoding, CSV splitting). Picklable top-level
functions are required for worker arguments. Lambdas and bound
methods don't pickle. The pattern is `asyncio.to_thread()` to
integrate a sync CPU-bound function into async code.

**11. The `asftool/core/tasks/` module is TUI-agnostic and reusable.**
If you add a new CPU-bound operation (e.g. parquet decode, gzip
compression), put the worker function in
`asftool/core/tasks/parallel.py` and a picklable top-level, then
call it from the service via `TaskRunner.run_in_process_pool`.

### Phase 9: SF CLI auth — the most fragile integration

**12. SF CLI's `org display --json` field is `expirationDate`, not
`tokenExpiration`.** The legacy `sfdx` CLI used `tokenExpiration`;
the current `sf` CLI uses `expirationDate`. If the field name
doesn't match, `expires_at` is parsed as `None`. `_parse_auth_result`
in `core/sf_cli.py` tries both names. Keep the fallback list in
sync with real-world SF CLI output — if a future version changes
the field name again, add the new name to the `for field in
("expirationDate", "tokenExpiration"):` loop.

**13. Don't return `True` for "unknown expiry" in `is_expired()`.**
The original code was:

```python
def is_expired(self, buffer_seconds=60):
    if not self.expires_at:
        return True   # <-- wrong
    ...
```

This made the auth flow **unrecoverable**: if `expires_at` was
`None` (because of a missing/wrong field name, see #12), every
request was treated as expired, which triggered a re-login via web,
which got the same broken result, forever. The user had to manually
clean the keyring.

The fix: return `False` when `expires_at` is `None` or
unparseable. We trust SF CLI's own refresh logic; the API call
will get a real 401 if the token is actually bad, and that's a
real signal we can act on. A guess of "expired" from missing
data is worse than a guess of "fresh". Fixed in `869db7b`.

**14. `datetime.utcnow()` is naive; comparing to aware datetimes
silently raises `TypeError`.** The `except (ValueError, TypeError)`
clause in `is_expired` swallowed the error and returned the default
value, masking the bug. The fix: use
`datetime.now(timezone.utc)` when the parsed expiry has a tzinfo,
so both sides of the comparison are tz-aware.

**15. Stale `tcrm` binaries on user laptops are an environment
issue, not a code issue.** When the user types `tcrm auth login`
and it fails with `ModuleNotFoundError: No module named
'tcrm_toolkit.cli.main'`, that's a leftover `tcrm` shim from
before the refactor. The fix is environment-level: `where.exe
tcrm` then delete the shim, or
`py -3.13 -m pip uninstall tcrm-toolkit`. If the error message
mentions `tcrm` from *inside* `asftool`, that's a different bug
(hardcoded user-facing string — fixed in `bdf94cb`).

### Cross-cutting patterns

**16. `raise ... from err` in `except` blocks** (B904) and
`from None` when re-raising an unrelated error. Without this, the
original exception is lost from the traceback. The `bdf94cb` and
`76acfc8` commits cleaned up most of these.

**17. `keyring` on Windows uses the Windows Credential Manager, but
in headless containers it may not be available.** The `keyring`
package then raises `No recommended backend was available`. On
those systems, `asftool auth status` will fail. The fix is either
`uv pip install keyrings.alt` (encrypted-file backend) or a proper
GUI session. The check in `doctor` reports the keyring backend
status, so the user can diagnose from there.

**18. Tests must cover the user-visible path, not the internal one.**
The original `auth_schemas.py` tests verified the `AuthService`
class, not the `SFCLIAuthService` class, not the `cli/commands/auth.py`
Typer commands, and not the menu handlers. A bug in any of those
three layers would slip through. The same principle applies to
the menu: tests that call `cmd.login()` directly don't exercise
the menu's `asyncio.run()` nesting, so they pass while the user
sees a crash. Always add a test that drives the user-visible entry
point (`_run_menu_loop` with mocked `console.input`, or the
`typer.testing.CliRunner` for CLI commands).

### Workflow / collaboration

**19. The branch `feature/asftool-refactor` is the work-in-progress
refactor of the old `main` (which holds only the legacy toolkit).
The plan documents in `docs/plans/asftool-refactor/` are the source
of truth for what's done and what's next.** If a phase doc says
"Fix X" and the code doesn't match, the doc is right and the code
needs work — not the other way around.

**20. After every code change: run `uv run pytest`, `uv run mypy
asftool`, and `uv run ruff check .` — all three must be clean
before committing.** The plan's acceptance criteria require this
for every phase. A pre-commit hook that runs all three is worth
adding in a future phase.

**21. Stale state on user laptops is invisible to CI.** When a
user reports a bug that doesn't reproduce on the dev machine,
first suspect environment-level state: leftover binaries on PATH,
old `~/.tcrm/` configs, `~/.sfdx/`, `~/.sf/`, keyring backends.
The `asftool doctor` command is the first-line diagnostic — it
checks all of these in one table. If the user says "doctor passes
and the bug still happens", then it's a real code bug. Otherwise
it's the environment.
