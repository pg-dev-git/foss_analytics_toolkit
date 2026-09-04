# TASK LOG - Greenfield Rebuild of Salesforce TCRM Toolkit

## Phase 1: Environment & Setup
- [x] 1.1 Create `_legacy/` folder and move legacy root files into it.
- [x] 1.2 Create and switch to Git branch `refactor/greenfield-cli`.
- [ ] 1.3 Scaffold `pyproject.toml` with dependencies (`typer`, `rich`, `httpx`, `pydantic`, `tenacity`, `cryptography`, `keyring`, `structlog`, `pandas`, `pytest`, `mypy`).

## Phase 2: Core Foundation & Security
- [ ] 2.1 Build `core/config.py` using `pydantic-settings`.
- [ ] 2.2 Build `core/crypto.py` with dynamic salting and `keyring` integration.
- [ ] 2.3 Build `core/client.py` using `httpx.AsyncClient` with `tenacity` retry wrappers and configurable API versions.

## Phase 3: Domain Models & Core Services
- [ ] 3.1 Implement Pydantic models in `core/models/` for Auth, Datasets, Dashboards, and Dataflows.
- [ ] 3.2 Build `core/services/auth_service.py` (Pure Python Web PKCE, Device Flow, JWT Bearer, and Auto-Refresh).
- [ ] 3.3 Build `core/services/dataset_service.py` (Async listing, CSV streaming, chunked multipart uploads).
- [ ] 3.4 Build `core/services/dashboard_service.py` (Listing, JSON backup/restore).
- [ ] 3.5 Build `core/services/dataflow_service.py` (List, start/stop dataflows, job status monitoring).

## Phase 4: CLI Presentation Layer
- [ ] 4.1 Set up `cli/main.py` entry point with `typer` and `cli/ui.py` with `rich`.
- [ ] 4.2 Implement CLI commands in `cli/commands/` mapping user actions directly to `core/services/`.

## Phase 5: Testing & Verification
- [ ] 5.1 Add unit tests for crypto, authentication schemas, and client retry logic under `tests/unit/`.
- [ ] 5.2 Add integration tests using `pytest-asyncio` and mocked API responses under `tests/integration/`.