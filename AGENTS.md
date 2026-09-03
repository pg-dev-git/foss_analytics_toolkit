# AGENTS.md — TCRM Toolkit

Guidance for agents working in this repository. Read before editing.

## Project

Modern async Python CLI for Salesforce Tableau CRM (TCRM). Rebuilds the legacy
`FOSS_Toolkit.py` (now preserved under `_legacy/`) with a `typer` CLI +
`textual` interactive TUI. Current work lives on `feature/interactive-tui-complete`
(the old `main` only holds the dead legacy toolkit).

---

## Mandatory GitHub & Git Workflow

- **Repository**: `pg-dev-git/foss_analytics_toolkit`
- **Never work directly on `main` (or `master`).**
- **Branch Naming**: Always create a feature branch from `main`:
  - `feature/` for new features (e.g., `feature/order-form`, `feature/product-image-upload`)
  - `fix/` for bug fixes (e.g., `fix/payment-code-generation`)
  - `chore/` for maintenance/infrastructure (e.g., `chore/docker-compose`)
  - `docs/` for documentation updates (e.g., `docs/prd-and-phases`)
- **Standard GitHub Flow**:
  1. Create and switch to feature branch: `git checkout -b feature/your-feature-name`
  2. Make small, atomic commits with conventional commit messages.
  3. Push branch to remote: `git push -u origin feature/your-feature-name`
  4. Open a Pull Request against `main` using `create_pull_request` (do not merge to `main` without review / completion of phase).

### Conventional Commits Guidelines
Use the format `<type>(<scope>): <short description>`:
- `feat(cart): add quantity increment/decrement steppers`
- `fix(checkout): resolve missing phone number validation error`
- `style(ui): update mobile touch target padding in product grid`
- `refactor(pb): extract pocketbase query helper functions`
- `docs(readme): add cloudflare tunnel setup instructions`

---

## Environment & Commands

- Use **uv**, not pip. Run everything via `uv run ...`.
- Install deps: `uv sync --extra interactive --extra dev`
- Tests: `uv run pytest -v --tb=short` (expect 42 passing)
- Lint: `uv run ruff check .`  ·  Typecheck: `uv run mypy tcrm_toolkit`
- Cross-platform check: `uv run python scripts/verify-cross-platform.py`
- CLI entrypoint: `tcrm` (`tcrm --help`, `tcrm doctor`)
- Tests need real encryption keys — `conftest.py` provides them; copy
  `.env.example` → `.env` only if you need live settings.

## Architecture (layered — respect the dependency direction)

`cli/` → `interactive/` → `core/services/` → `core/` (client, models, config, crypto, auth)

- **cli/** — `typer` command groups (`commands/{auth,dashboards,dataflows,datasets,jobs}.py`).
  Wire user actions directly to `core/services/`. Register with
  `app.add_typer(sub_app, name="...")` in `cli/main.py`.
- **interactive/** — `textual` TUI: `screens/` (Login, Main, OrgPicker, SafetyModal, Help),
  `widgets/` (DataBrowser, DetailPanel, StatusBar, CommandPalette, ...),
  `operations/` (extract/upload/backup/control), `tasks.py` (`TaskRunner` + `ProcessPoolExecutor`).
- **core/services/** — the real business logic: `dataset_service`, `dashboard_service`,
  `dataflow_service`, `auth_service`. These are async and already exercised by tests.
  `session.py::SessionManager` owns auth + `client_context()`; every network call must go
  through it, never construct a bare client.
- **core/** — `client.py` (httpx + tenacity retries), `models/` (pydantic), `config.py`
  (pydantic-settings, `TCRM_*` env vars), `crypto.py` (dynamic-salt encryption),
  `auth/{sf_cli_auth,token_store}.py`.

## Conventions

- Python 3.11+, `pathlib.Path` everywhere (no path separators hardcoded).
- Async throughout: `async/await`, `asyncio.gather`/`Semaphore` for I/O,
  `ProcessPoolExecutor` only for CPU-bound work (pandas).
- `structlog` for logging. `datetime.now(timezone.utc)` — **not** `datetime.utcnow()`
  (deprecated, flagged in CI warnings).
- Keep it simple: no special-case glue, no dead stubs. Prefer eliminating a branch over
  guarding it.

## Testing

- `tests/unit/` (crypto, client retry, auth schemas, interactive) +
  `tests/integration/test_api_endpoints.py` (uses `AsyncMock` on `SalesforceClient._client`).
- Add tests for new/changed behaviour. Assert on real outputs/state, not mocked calls.
- Integration tests mock the HTTP layer via the `mock_client` fixture — exercise real
  service code, not the client.
- **Live Session Seeding & E2E Testing**:
  - To test against a live Salesforce org without interactive browser login, use `scripts/seed_session.py`:
    ```bash
    TCRM_ACCESS_TOKEN="..." TCRM_INSTANCE_URL="..." TCRM_USERNAME="..." uv run python scripts/seed_session.py
    ```
  - **Token Expiry**: If a live session token expires or encounters authentication errors during testing, the agent must immediately notify the user to request a fresh token.

## Gotchas

- **Phases are NOT all done.** Phases 0–2 are complete. Phase 3 (operations/background
  tasks) and Phase 4 (polish/DX) are partial — see the phase-completion review.
  `TASK_LOG.md` checkboxes are stale; trust `docs/plans/phases/README.md`.
- CLI write commands are currently **stubs** (`"Authentication integration pending"`,
  `datasets.py` even raises `NotImplementedError`). The service layer is done; the
  CLI↔keyring↔service auth bridge is not.
- `widgets/progress_panel.py` and `widgets/task_history.py` are empty `Static` subclasses.
- CI on `main` does not run (workflow targets `main`/`refactor/**`/`feature/**`; `main`
  holds only the legacy toolkit).
