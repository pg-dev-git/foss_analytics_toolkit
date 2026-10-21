# Recommended Development Plan — ASFTool

**Version:** 1.0
**Date:** 2026-10-21
**Source:** `CODE_REVIEW.md` (26 verified findings on `origin/master` @ `ddbb69f`)
**Format:** Agile, 7 sequential phases, atomic tasks with story points, acceptance criteria, and verification steps.

---

## 0. How to Read This Plan

### Planning principles

1. **Contract before code.** Phases 1–2 fix *what the platform actually returns*. Until the payload contracts are correct, every other fix is built on sand.
2. **Gate before release.** Phase 0 is non-negotiable and must land first. Every later phase is only as safe as the gate that verifies it.
3. **One concern per commit.** Every task below is scoped to a single logical change with an independently verifiable outcome.
4. **Live-org evidence required.** The three Critical findings were invisible to the test suite precisely because the tests used invented payloads. Every contract fix (T1.x, T2.1, T2.2) requires a real-org verification step that a human or a sandbox-org CI job performs — not just a unit test.
5. **No behaviour change without a note.** Anything that changes a user-visible contract updates the README in the *same* task, not in a later "docs cleanup" task.

### Task format

| Field | Meaning |
|---|---|
| **ID** | `T<phase>.<n>` — stable reference used in PR descriptions and commit trailers |
| **Story** | The user-facing outcome |
| **Points** | Fibonacci estimate (1 = trivial, 5 = half a day, 8 = ~1 day, 13 = ~2 days) |
| **A/C** | Acceptance criteria — all must be checkable, preferably automatically |
| **Verify** | The command or manual step that proves the task is done |
| **Finds** | The code-review finding(s) this task closes |

### Engineering standards for this sprint

- **Branch:** `feat/<phase>-<slug>` off `master`. Never commit to `master` directly.
- **Commits:** Conventional Commits, e.g. `fix(models): alias nextPageUrl for Wave list responses`. Include `Refs: T1.1` in the body.
- **PR:** Must link the task IDs it closes and include the "Verify" output in the description.
- **Definition of Done:** task A/C met · `ruff check .` clean · `mypy asftool` clean · `pytest` green · new tests added for every behaviour change · README updated if the contract changed.
- **New required CI checks (from Phase 0):** `ruff`, `mypy`, `pytest`. All three must be green to merge.

---

## 1. Release Gates

| Gate | After phase | Blocks release until |
|---|---|---|
| **G0** | Phase 0 | CI runs on every PR; `master` is green |
| **G1** | Phase 2 | `auth list-orgs`, `datasets list`, `dashboards list`, `jobs list` verified against a real sandbox org |
| **G2** | Phase 3 | `asftool` starts with no `.env`; no interactive path can hang |
| **G3** | Phase 4 | A 500-asset field scan completes without rate-limit lockout |
| **G4** | Phase 5 | Visual Lineage works end-to-end from the interactive menu |
| **G5** | Phase 6 | README matches reality; package metadata is correct |

**Release definition:** G0–G5 all green, plus a manual smoke test of the interactive menu and the six most-used CLI commands against a sandbox org.

---

## 2. Phase Overview

| # | Phase | Theme | Tasks | Points | Findings closed | Gate |
|---|---|---|---|---|---|---|
| 0 | **Stabilise the Base** | CI, red tests, pre-commit, release hygiene | 6 | 11 | L-5, L-6, part of H-1 | G0 |
| 1 | **Fix the Platform Contracts** | Pydantic aliases for real Salesforce payloads | 7 | 24 | C-2, H-4 | — |
| 2 | **Fix Auth & SF CLI** | Org discovery, env isolation, keyring, token lifecycle | 9 | 38 | C-1, H-2, M-1, M-2, M-3, M-5 | G1 |
| 3 | **Fix the Interactive Experience** | No crash on start, no hang, correct diagnostics | 7 | 24 | C-3, H-1, M-6, part of M-9 | G2 |
| 4 | **Harden the API Client** | Concurrency cap, Retry-After, error detail, retry isolation | 6 | 22 | H-3, H-5, M-7 | G3 |
| 5 | **Fix Lineage & Menus** | NameError, path mismatch, cleanup crash, dataset backups | 6 | 18 | H-6, M-8, Theme D | G4 |
| 6 | **Housekeeping & Truth** | Dead code, logging, docs, packaging, deprecations | 10 | 34 | L-1..L-4, L-7, L-8, M-4, M-9, Theme B/C | G5 |
| — | **Cross-phase** | Fixtures guard, `asftool init`, live-org suite, lesson tracking | 6 | 18 | Theme A, Theme B, X.4 | — |
| | | | **51 + 6 tasks** | **171 + 18** | **26 findings** | |

**Critical path:** T0.1 → T1.1 → T2.1 → T3.1 → T6.1 ≈ 4 weeks for one engineer at 1.0 velocity. **Total effort: 189 points (171 in phases + 18 cross-phase).**

---

## 3. Phase 0 — Stabilise the Base

**Theme:** Stop the bleeding. `master` currently has failing tests, lint errors, no CI, and a false licence classifier. Every subsequent phase depends on this gate existing.

**Epic 0: "The repo tells me when it breaks"**

| ID | Story | Points | A/C | Verify | Finds |
|---|---|---|---|---|---|
| **T0.1** | As a maintainer, I want a CI workflow so that broken code never reaches `master` again | 3 | `.github/workflows/ci.yml` exists; triggers on `push` to `master` and on all PRs; runs on Python 3.11 + 3.12; three jobs — `ruff check .`, `mypy asftool`, `pytest -q`; each job fails the build on non-zero exit; `uv` is used for dependency install | Workflow file present; the workflow runs green on `master` **after T0.2/T0.3** | L-5 |
| **T0.2** | As a maintainer, I want the two failing `config show` tests to pass so CI is green | 2 | Both `tests/integration/test_config_cli.py::TestConfigShow` tests pass; the fix is a **test-isolation** change (the tests must not depend on ambient `ENCRYPTION_KEY`/`JWT_SECRET_KEY` in the developer's shell or `.env`), **not** by deleting the assertions; a regression test asserts `config show` works with a *minimal* settings object | `pytest -q` → 239 passed | H-1 (test) |
| **T0.3** | As a maintainer, I want `ruff check .` to be clean | 1 | All 7 `F401`/`F841` findings removed; no `# noqa` added unless the import is genuinely required for a side effect (with a comment explaining why) | `ruff check .` → `All checks passed!` | L-5 |
| **T0.4** | As a developer, I want a pre-commit hook so that formatting problems are caught before commit | 2 | `.pre-commit-config.yaml` exists with `ruff`, `ruff-format`, and a `check-added-large-files` hook; `pre-commit run --all-files` passes on a clean tree | Hook run is green | L-5 |
| **T0.5** | As a publisher, I want the package metadata to state the correct licence | 1 | The `AGPLv3` classifier is replaced with `"License :: OSI Approved :: Apache Software License"`; `LICENSE` file unchanged; `pyproject.toml` `license` field is migrated to PEP 639 form (`license = "Apache-2.0"`, `license-files = ["LICENSE"]`) if the pinned `setuptools` supports it — otherwise only the classifier is fixed with a comment | `grep -i agpl pyproject.toml` → no output; `python -m build` metadata shows `License-Expression: Apache-2.0` | L-6 |
| **T0.6** | As a maintainer, I want a `CHANGELOG.md` so that users can see what changed | 2 | `CHANGELOG.md` exists with an `## [Unreleased]` section and a `Keep a Changelog`-style template; the two `pytest` failures and the 7 lint fixes are recorded under `### Fixed` / `### Changed`; the file is linked from the README | File exists and is linked | — |

**Phase 0 A/C:** `ruff check .` clean · `mypy asftool` clean · `pytest` 239/239 green · CI workflow present and green · licence classifier correct.

> **Note for T0.2:** the two failing tests fail because `Settings` requires `ENCRYPTION_KEY` and `JWT_SECRET_KEY` and the test environment has neither. The *correct* fix here is test isolation (inject a `Settings` instance / use `monkeypatch.setenv` in a fixture) **not** making the fields optional — that change belongs to T3.1 and would change the runtime contract. Do not conflate the two.

---

## 4. Phase 1 — Fix the Platform Contracts

**Theme:** The Pydantic models do not match the payloads Salesforce actually returns. This phase establishes the contract, and — critically — creates the **shared fixture module** that makes this class of bug impossible to reintroduce silently.

**Epic 1: "The models speak Salesforce"**

| ID | Story | Points | A/C | Verify | Finds |
|---|---|---|---|---|---|
| **T1.1** | As a developer, I want a checked-in module of verbatim Salesforce API payloads so that models are tested against reality, not invention | 5 | New file `tests/fixtures/wave_responses.py` contains **verbatim-shaped** payloads for at least: `GET /wave/datasets` (with `nextPageUrl`), `GET /wave/dashboards` (with `nextPageUrl`), `GET /wave/dataflows`, `GET /wave/dataflowjobs` (with `dataflowJobs`, `dataflowId`, `dataflowName`, `startTime`, `endTime`), `GET /wave/datasets/{id}/versions/{v}/xmds/main`, `GET /wave/dependencies/{id}`, `GET /wave/replicatedDatasets`; every payload uses the **camelCase key names from the Salesforce API reference**; a docstring at the top cites the API endpoint for each fixture; `tests/fixtures/__init__.py` exists | `python -c "from tests.fixtures import wave_responses"` imports cleanly | Theme A |
| **T1.2** | As a user, `asftool datasets list` must show **every** dataset, not just the first 50 | 3 | `DatasetListResponse.next_page_url` has `alias="nextPageUrl"`; `model_config` includes `populate_by_name=True`; a test feeds `wave_responses.DATASETS_PAGE_1` (which contains `nextPageUrl`) and asserts `next_page_url == "<the url>"`; a test feeds a payload with **no** `nextPageUrl` and asserts `next_page_url is None` | New test fails on the pre-fix code and passes after | C-2 |
| **T1.3** | As a user, `asftool dashboards list` must show every dashboard | 2 | `DashboardListResponse.next_page_url` has `alias="nextPageUrl"`; same two tests as T1.2 but for dashboards | New test fails pre-fix, passes post-fix | C-2 |
| **T1.4** | As a user, `asftool jobs list` must not crash against a real org | 3 | `DataflowJob` gains aliases `dataflowId`→`dataflow_id`, `dataflowName`→`dataflow_name`, `startTime`→`start_time`, `endTime`→`end_time`; `DataflowJobListResponse.dataflowjobs` gains `alias="dataflowJobs"`; `populate_by_name=True` on both; the existing fixture in `tests/integration/test_api_endpoints.py:287` is **replaced** with the real camelCase shape from T1.1; a test asserts a job parsed from the real payload has the correct `dataflow_id` and `start_time` | New test fails pre-fix, passes post-fix; `jobs list` no longer raises `ValidationError` | H-4 |
| **T1.5** | As a user, `asftool datasets list` must fetch page 2, 3, … until exhausted | 3 | `DatasetService.list_datasets` is proven to issue **N+1** requests for an N-page fixture set; the test uses a fake client that returns the fixture pages in order and asserts the page tokens sent are `[None, "<token2>", "<token3>"]` and the result contains all datasets from all pages; the same for `DashboardService.list_datasets` (may share a test helper) | Test fails pre-fix (currently stops after 1 call), passes post-fix | C-2 |
| **T1.6** | As a developer, I want one pagination helper instead of three drifting copies | 3 | New `asftool/core/pagination.py` exports `extract_page_token(url: str) -> str | None`; it URL-decodes the token (`urllib.parse.parse_qs`); the three call sites (`dataset_service.py:576`, `dashboard_service.py:147`, `field_impact_service.py:700`) are deleted and replaced with the shared helper; a unit test asserts a token containing `+` and `%` is decoded correctly (the current `field_impact_service` implementation fails this) | New test fails against the old `str.split` implementation, passes with the shared helper | Theme D |
| **T1.7** | As a maintainer, I want every model that parses a Salesforce response to have an alias-mapping test | 5 | A test module `tests/unit/test_wave_contracts.py` exists that, for **every** Pydantic model in `asftool/core/models/` that represents a Salesforce response, feeds the matching fixture from T1.1 and asserts the parsed attributes are non-empty and correctly aliased; the module is parametrised over the model→fixture mapping so adding a model without a contract test is a visible failure | `pytest tests/unit/test_wave_contracts.py` green; adding an unlisted model raises `pytest` failure listing the missing entry | Theme A, prevents recurrence |

**Phase 1 A/C:** every model parses the real payload · pagination is proven multi-page · one pagination helper · contract-test module in place.

> **Do not skip T1.1.** Without the shared fixture module, T1.2–T1.7 will be re-broken the next time someone adds a field. T1.1 is the highest-leverage task in the entire plan.

---

## 5. Phase 2 — Fix Auth & SF CLI

**Theme:** The auth layer has one Critical bug (org discovery), one env-isolation bug that breaks org resolution, and three robustness gaps that turn recoverable situations into crashes.

**Epic 2: "Auth works, and fails safely"**

| ID | Story | Points | A/C | Verify | Finds |
|---|---|---|---|---|---|
| **T2.1** | As a user, `asftool auth list-orgs` must list my authorised orgs | 5 | `SFCLIManager.list_orgs` concatenates `result.other` + `result.nonScratchOrgs` + `result.devHubs` + `result.scratchOrgs` (and `result.sandboxes` if present); the result is de-duplicated by org `id` (the live CLI returns the same org under both `other` and `nonScratchOrgs` — verified on this machine); the method raises `SFCLIError` with the raw response keys on unexpected shape instead of returning `[]`; a unit test feeds the **verbatim** `sf org list --json --all` response (captured from a real CLI) and asserts the expected org count and aliases; a **live** test asserts `asftool auth list-orgs` lists the 2 orgs present on the dev machine | Unit test passes; live run lists 2 orgs (was 0) | C-1 |
| **T2.2** | As a user, my `--api-version` flag must not break my SF CLI auth | 5 | Both `_run_command` and `_run_command_async` build an explicit `env` dict that excludes `SF_API_VERSION`; the value is passed to SF CLI only via a supported mechanism if one exists (SF CLI's own `--api-version` flag on the specific command, not a global env var); a test asserts the env passed to `subprocess.run` does not contain `SF_API_VERSION`; a live test asserts `asftool fields analyze --api-version v68.0` does not break org resolution | Unit test asserts `SF_API_VERSION not in passed_env`; live run succeeds | H-2 |
| **T2.3** | As a user on a headless server without a keyring backend, I want a clear message, not a traceback | 5 | `TokenStore.load_token` and `TokenStore.delete_token` catch `keyring.errors.NoKeyringError` and raise a dedicated `TokenStoreUnavailableError` with an actionable message (naming `keyrings.alt` / the headless install extra); `SFCLIAuthService.status()` catches it and returns `{"authenticated": False, "message": "..."}` instead of propagating; `asftool auth status` prints the message and exits 0; `delete_token` catches `PasswordDeleteError` **and** `NoKeyringError` distinctly | `asftool auth status` on a keyring-less host prints one clear line, exit 0, no traceback | M-1 |
| **T2.4** | As a user, my encryption key must work whether or not it has base64 padding | 3 | `CryptoManager.__init__` decodes with the same padding-tolerant expression as the `Settings` validator (`+ "=" * (-len(v) % 4)`); a malformed key raises `CryptoError` with a clear message instead of a bare `binascii.Error`; a unit test round-trips encrypt→decrypt with an **unpadded** 43-char key and with a padded 44-char key | Both keys round-trip; malformed key → `CryptoError` | M-2 |
| **T2.5** | As a user, an expired token must actually be detected and refreshed | 8 | `SFCLIManager._parse_auth_result` no longer pretends `expirationDate`/`tokenExpiration` exist; a new `SFCLIManager.get_token_expiry(alias)` obtains the real expiry from a source that provides it (investigate: `sf org display` has none — candidates: the `sf org list` payload, `/services/oauth2/userinfo`, or a locally-recorded "token observed at" timestamp with a bounded validity window); `StoredToken` gains `observed_at` and `is_expired()` uses `expires_at` **or** `observed_at + MAX_TOKEN_AGE`; the "Token expired (will auto-refresh on next use)" path in `auth status` becomes reachable; a unit test with a controlled clock proves a stale token triggers `refresh_token()` | Unit test: a token observed 2 h ago is `is_expired() == True`; `get_valid_token` calls refresh | M-3 |
| **T2.6** | As a user, a failed token refresh must not silently open a browser | 5 | `TokenStore.get_valid_token` raises a new `TokenRefreshError` (subclass of `OAuthError`) instead of returning `None`; `SFCLIAuthService.get_access_token` distinguishes "no token stored" (→ offer login) from "refresh failed" (→ report the refresh error, do **not** auto-launch an interactive browser login); `auto_refresh=False` never triggers a login flow | Unit test: a refresh failure does not call `login()` | M-5 |
| **T2.7** | As a user, `asftool auth status` must report the alias I asked about | 2 | `auth status --alias` is honoured end-to-end (already partially true); `asftool auth status --alias nonexistent` reports "No token for alias 'nonexistent'" rather than about `default` | Manual: `asftool auth status --alias mcp` reports on `mcp` | — |
| **T2.8** | As a user, `asftool auth list-orgs` must not fail silently when SF CLI is missing | 2 | `SFCLIAuthService.list_orgs` distinguishes "SF CLI not installed" (empty list + explicit `message`) from "parse failed" (raises); the CLI prints which one occurred; a `logger.error` includes the response **keys** to aid diagnosis | Unit test with a malformed response asserts a raised `SFCLIError` naming the unexpected keys | C-1 (robustness) |
| **T2.9** | As a maintainer, I want the SF CLI subprocess layer to be testable | 3 | `SFCLIManager` accepts an injectable "command runner" callable (default: the current `subprocess` implementation); a unit test can therefore assert the exact argv and env for every SF CLI invocation without a real CLI; the existing `tests/unit/test_sf_cli.py` is extended to cover `login_web`, `login_device`, `get_org_info`, `get_access_token`, `refresh_token`, `logout`, `list_orgs` argv shapes | New unit tests pass with no SF CLI installed | Testability for T2.1/T2.2 |

**Phase 2 A/C:** orgs listed correctly · env isolated · keyring-absent degrades safely · token expiry actually detected · no surprise browser launch.

> **T2.5 is the hardest task in the plan (8 points).** It requires research into what SF CLI actually exposes. **If no reliable expiry source exists, the honest fix is to record "token observed at" and treat the token as valid for a bounded window** — and then correct the README's auto-refresh claim. Do not ship a fake expiry.

---

## 6. Phase 3 — Fix the Interactive Experience

**Theme:** Right now `asftool` on a fresh machine crashes with a pydantic traceback, and one configuration menu hangs forever. These are the two things a new user hits first.

**Epic 3: "The tool starts, and never traps the user"**

| ID | Story | Points | A/C | Verify | Finds |
|---|---|---|---|---|---|
| **T3.1** | As a new user, `asftool` must start without a `.env` file and tell me what to do | 5 | `Settings.encryption_key` and `Settings.jwt_secret_key` become optional (`str | None = None`); `create_crypto_manager()` returns `None` (or raises a typed `CryptoError`) when no key is set; `cli/main.py` runs a **preflight** before dispatch that detects missing secrets and prints an actionable message: `"Missing ENCRYPTION_KEY. Run: cp .env.example .env  (or: asftool init)"` then exits 1; the preflight must **not** run for `--help`/`--version`; the `doctor` command's "Encryption key: MISSING" row stays informational and does **not** fail the check | `asftool` with no `.env` → one clear message, exit 1, **no traceback**; `asftool --help` → prints help, exit 0 | H-1 |
| **T3.2** | As a user, `asftool --help` must print help instead of entering the menu | 2 | `cli/main.py::_has_subcommand` (or a replacement router) treats `--help`, `-h`, `--version` as "delegate to Typer"; `asftool --help` prints the Typer help and exits 0; `asftool` alone still enters the menu; `asftool auth --help` still works | `asftool --help` → help text, exit 0 | H-1 |
| **T3.3** | As a user, the API-version picker must not hang | 5 | `prompt_select` never loops forever: it caps attempts (e.g. 10) and returns `None`/raises a typed error when exhausted; it accepts **either** a 1-based index **or** an exact match against a choice value; the default is validated against the choices at call time; the `menus/config.py` caller passes the current version as a **value**, and the function converts it to an index; the user can always cancel with `q` or Ctrl-C; a unit test drives `prompt_select` with a simulated "Enter" (default) input and asserts it returns the default's choice within a bounded number of iterations | Unit test: `prompt_select(..., default="v68.0")` with Enter input returns `"v68.0"`; a test that always sends garbage terminates | C-3 |
| **T3.4** | As a user, every `prompt_*` helper must have one consistent contract | 3 | `prompt_text` returns `""` for an empty answer; `prompt_select` returns a **choice value** or `None`; `prompt_confirm` returns a bool; the `default` parameter means the same thing in all three (the value that would be used if the user pressed Enter); a shared `tests/unit/test_prompts.py` covers all three with a simulated `Prompt.ask` | New unit tests pass | C-3, L-2 |
| **T3.5** | As a user, `asftool doctor` must diagnose the alias I am actually using | 3 | `doctor` gains `--alias/-a`; `run_diagnostics(alias=...)` and `_collect_checks(session, verbose, alias)` thread it through; the "Authentication" row reflects that alias; `doctor` without `--alias` still defaults to `default` but says so in the row value | `asftool doctor --alias mcp` reports on `mcp` | M-6 |
| **T3.6** | As a user, the menu must never be able to trap me | 3 | Every interactive prompt in `menus/` accepts a `q`/Ctrl-C escape; `_run_menu_loop` in `cli/main.py` treats a `q` typed at any submenu level as "back"; the menu loop has a global timeout guard on any single prompt (e.g. `asyncio.wait_for` with a generous default) that returns to the parent menu rather than blocking forever; a test simulates a prompt that never returns and asserts the loop recovers | Manual walk-through of all 8 submenus; unit test for the timeout guard | C-3 (hardening) |
| **T3.7** | As a user, my downloads should land in one predictable place | 3 | Decide: `StorageManager.base_dir` becomes `settings.config_dir / "downloads"` (matching the docstrings) **or** the docstrings are corrected to `$CWD/asftool_downloads`; whichever is chosen, `README`, all three service docstrings, and the `asftool_downloads/` `.gitignore` entry are updated consistently in the same commit; the chosen location is printed by `asftool config show` | `asftool config show` prints the download base directory; docs match | M-9 (part) |

**Phase 3 A/C:** tool starts on a clean machine with a helpful message · `--help` works · no prompt can hang · doctor diagnoses the right alias.

---

## 7. Phase 4 — Harden the API Client

**Theme:** The client ignores the server's rate-limit instruction and fans out without a cap. On a real org with hundreds of assets, this produces lockouts.

**Epic 4: "The client respects the server and stays in bounds"**

| ID | Story | Points | A/C | Verify | Finds |
|---|---|---|---|---|---|
| **T4.1** | As a user, a field scan must not fire hundreds of simultaneous requests | 5 | The `FieldImpactService._semaphore` is actually acquired around each per-asset API call (or around a bounded worker pool); a test with 200 fake datasets and a limit of 10 asserts the **maximum observed in-flight** HTTP calls is ≤ 10; the `field_impact_max_concurrent_scans` setting value is wired into the service constructor (currently the service hardcodes `max_concurrent: int = 10` and ignores the setting) | Test fails pre-fix (currently observes 200), passes post-fix | H-3 |
| **T4.2** | As a user, when Salesforce says "wait 300 seconds", I should wait 300 seconds | 5 | A custom wait function reads `SalesforceRateLimitError.retry_after` and honours it, capped at a configurable maximum (default e.g. 900 s); the `Retry-After` header is parsed for **both** delta-seconds and HTTP-date forms; a test with a fake 429 response asserts the first retry delay is ≥ the server's requested delay; the `retry_after` value is surfaced to the user in the CLI error output | Test asserts the computed delay equals the server's `Retry-After` (currently 1.68 s) | H-5 |
| **T4.3** | As a developer, concurrent requests must not share mutable retry state | 3 | `SalesforceClient.retry_client` returns a **new** `AsyncRetrying` instance per call, or the shared instance is proven safe under concurrency with a test that runs N concurrent failing+successful requests and asserts each retried the correct number of times; a comment states the concurrency contract | Concurrency test passes deterministically | Theme: shared `AsyncRetrying` |
| **T4.4** | As a user, a 403 or 404 must tell me what actually went wrong | 3 | `_handle_response` extracts the Salesforce error envelope (`errorCode`, `message`) for **all** 4xx responses, including 403 and 404; `SalesforceAPIError` for 403 carries the API message; `SalesforceNotFoundError` for 404 carries the API message; a test feeds a real 403 body and asserts the message survives into the exception | Test asserts the exception message contains the API's `errorCode` | M-7 |
| **T4.5** | As a user, a 401 must trigger a token refresh, not a hard failure | 3 | On a 401 the client signals "needs refresh" distinctly (e.g. raise `SalesforceAuthError` with a `retryable_auth=True` flag, or attempt exactly **one** refresh + replay through the auth service); the current behaviour — an immediate, unretried `SalesforceAuthError` — is replaced with a single refresh-and-replay; a test asserts exactly one refresh occurred and the replayed request used the new token | Test: 401 → refresh → 200, refresh called once | Auth resilience |
| **T4.6** | As a developer, I want the HTTP client to be testable without a live org | 3 | `SalesforceClient` accepts an injected `httpx.AsyncClient`; the existing `tests/unit/test_client.py` is extended to cover 401/403/404/429/5xx handling, the retry path, and `_build_url`; no test requires network access | New unit tests pass offline | Testability |

**Phase 4 A/C:** concurrency is bounded · `Retry-After` is honoured · error bodies are not discarded · 401 triggers one refresh.

---

## 8. Phase 5 — Fix Lineage & Menus

**Theme:** The Visual Lineage feature has two independent bugs on its main path, and the shared storage helper crashes on a subdirectory.

**Epic 5: "Lineage actually produces a lineage"**

| ID | Story | Points | A/C | Verify | Finds |
|---|---|---|---|---|---|
| **T5.1** | As a user, the lineage menu must not crash when I load an existing impact file | 3 | `search_term` is initialised to `None` before the branch in `menus/lineage.py`; line 88 uses `search_term or "lineage"`; the `'search_term' in locals()` hack at line 95 is removed; a test drives `generate_lineage` down the "existing file" branch with mocked prompts and asserts no `NameError` and that a Mermaid file is produced | Test fails pre-fix (`NameError`), passes post-fix | H-6 |
| **T5.2** | As a user, the impact file the menu looks for must be the file that was actually written | 3 | `analyze_field_async` returns the path it wrote; `menus/lineage.py` uses that **return value** instead of recomputing `storage.field_impact_path(...)` (which embeds a fresh timestamp and therefore never matches); a test asserts the path the menu passes to `_execute_generation` exists on disk | Test: the resolved path `exists()` is `True` (currently `False`) | H-6 |
| **T5.3** | As a user, a partial lineage graph must be labelled as partial | 5 | `LineageService.build_graph` records the IDs whose dependency fetch failed (instead of silently treating them as leaves) and exposes `graph.incomplete_nodes`; the CLI prints a warning listing the assets whose dependencies could not be fetched; the CLI's "Mermaid exported" message is not the only signal | Test: a client that fails on one node asserts the warning is produced | Theme E |
| **T5.4** | As a user, the download cleanup must not crash | 2 | `StorageManager.cleanup_old_downloads` filters to files (`p.is_file()`); a test creates a subdirectory plus files and asserts the cleanup completes and removes only files | Test fails pre-fix (`IsADirectoryError`), passes post-fix | M-8 |
| **T5.5** | As a user, the cleanup feature must be reachable or removed | 2 | Either a CLI command exposes the cleanup (`asftool datasets cleanup --alias X --keep-last N`) or the dead method is deleted; if exposed, it is documented in the README; if deleted, no reference remains in the codebase | `grep -rn "cleanup_old_downloads"` shows exactly one live call site, or zero | M-8 |
| **T5.6** | As a user, a backup to a path I chose must not fail on a missing parent directory | 3 | `DashboardService.backup_dashboard` and `DataflowService.backup_dataflow` create the output parent directory (`output_path.parent.mkdir(parents=True, exist_ok=True)`) before writing — matching what `cli/commands/fields.py:123` already does for the field-impact report; a test writes to a nested non-existent path and asserts success | Test fails pre-fix (`FileNotFoundError`), passes post-fix | M-7 (adjacent) |

**Phase 5 A/C:** both lineage paths work · partial graphs are labelled · cleanup is safe and reachable · backups honour user-chosen paths.

---

## 9. Phase 6 — Housekeeping & Truth

**Theme:** Remove what is not used, correct what is documented, and make the tool's observable behaviour match its own documentation. This phase removes the "false claims" class of defect from the README.

**Epic 6: "The docs tell the truth"**

| ID | Story | Points | A/C | Verify | Finds |
|---|---|---|---|---|---|
| **T6.1** | As a user, the README must describe the tool I actually have | 8 | Every README claim is audited against the code and corrected where it is false. Minimum set: the env-var table (T6.2), the "structured JSON logging" claim (T6.4), the "token auto-refresh" claim (T6.3), the "encryption-key file fallback for headless containers" claim (T6.5 or removed), the API-version default (`v60.0` → `v68.0`), the download-location claim (T3.7), and the dependency-direction claim (verified still true); a reviewer signs off on the diff | Each claim in the README maps to a verified code path | Theme B |
| **T6.2** | As a user, the env vars I read in the README must work | 3 | One convention is chosen and applied everywhere: either `SettingsConfigDict(env_prefix="ASFTOOL_")` + remove the explicit aliases (breaking change → requires a `CHANGELOG` entry and a `0.2.0` minor bump), **or** the README and `.env.example` are corrected to the unprefixed names (no code change). The chosen approach is recorded in `CHANGELOG.md`; a unit test asserts a representative env var is honoured | A test sets `ASFTOOL_SF_API_VERSION` (or the unprefixed `SF_API_VERSION`, per the decision) and asserts `Settings.sf_api_version` reflects it | M-4 |
| **T6.3** | As a user, the token-lifecycle claim must be accurate | 2 | The README's "token auto-refresh" wording matches the T2.5 implementation exactly; if T2.5 was implemented as an observation-window approach, the README says so | README reviewed against `token_store.py` | M-3, Theme B |
| **T6.4** | As a user, the logging must actually be JSON, on stderr, and persisted | 5 | `setup_logging()` is called from `cli/main.py` before dispatching; structlog output is JSON via `JSONRenderer()`; the logger writes to **stderr** (`PrintLoggerFactory(file=sys.stderr)`); the `~/.asftool/asftool.log` file handler is actually attached and the file is created on first log; a test captures stderr and asserts the output parses as JSON; `doctor`'s "Log file" row now reports `True` after a run | Test: captured stderr is valid JSON; `~/.asftool/asftool.log` exists and contains the run's log lines | L-1, L-3 |
| **T6.5** | As a user on a headless host, my token must still be stored securely | 5 | **Either** implement the documented encryption-key file fallback: the master key is stored at `~/.asftool/encryption.key` with `0600` permissions, used only when the OS keyring is unavailable, with a clear warning printed; **or** remove the sentence from the README and add `keyrings.alt` to the `headless` install extra. The two must not disagree | Manual on a keyring-less host: `asftool auth login` then `asftool auth status` succeeds, and the README matches the behaviour | M-1, Theme B |
| **T6.6** | As a maintainer, I want the dead OAuth configuration removed | 5 | `Settings` drops `jwt_secret_key`, `jwt_algorithm`, `access_token_expire_minutes`, `refresh_token_expire_days`, `sf_connected_app_*`, `sf_web_oauth_*`, `sf_device_flow_*`, and the `has_*_credentials` properties — **unless** a decision is taken to implement those flows in this release; `authlib` and `python-jose` are removed from `pyproject.toml` if unused; `generate_jwt_secret()` is removed; `doctor` no longer checks "JWT secret"; `CHANGELOG.md` records the removal as a breaking change for anyone who set those vars | `grep -rn "jwt_secret\|authlib\|jose" asftool/ pyproject.toml` → no live references; `pytest` green | Theme C |
| **T6.7** | As a maintainer, I want unused exception classes removed | 2 | The 16 exception classes in `core/exceptions.py` are pruned to those actually raised or documented for public consumption; `FieldImpactError` (0 references) is either used or removed; `core/__init__.py` `__all__` matches the pruned set; a test asserts every exported name is importable | `pytest` green; `__all__` has no stale entries | Theme C |
| **T6.8** | As a developer, I want no deprecated datetime calls | 2 | `pd.Timestamp.utcnow()` (2 sites) → `pd.Timestamp.now("UTC")`; `datetime.utcnow()` (7 sites) → `datetime.now(UTC)`; `pytest -W error::DeprecationWarning` produces no warnings from `asftool/` | `pytest -W error::DeprecationWarning` green | L-7 |
| **T6.9** | As a developer, I want no leaked file handles | 1 | The two `open(file_path, ...)` calls in `dataset_service.py` use a `with` block; `pytest -W error::ResourceWarning` is clean | `pytest -W error::ResourceWarning` green | L-8 |
| **T6.10** | As a user, my `-o` short flag should mean the same thing everywhere | 1 | The short flags in the `datasets`/`dashboards`/`dataflows` command groups are audited for collisions; `--operation` no longer claims `-o` where `--output` already has it; the README's example commands still work | `asftool datasets upload --help` shows non-colliding short flags | L-4 |

**Phase 6 A/C:** every README claim verified against code · dead configuration and dependencies removed · no deprecation warnings · no resource warnings.

---

## 10. Cross-Phase Work Items

These do not belong to a single phase but must not be forgotten.

| ID | Item | Points | A/C | Finds |
|---|---|---|---|---|
| **X.1** | **Add a contract-fixture lint test**: a test that fails if a new Pydantic model is added to `asftool/core/models/` without a corresponding entry in the `tests/unit/test_wave_contracts.py` mapping. Prevents Theme A from recurring | 3 | Adding a model without a contract entry fails `pytest` with a helpful message | Theme A |
| **X.2** | **Add `asftool init`**: an interactive command that writes a `.env` from `.env.example` with a freshly generated `ENCRYPTION_KEY`, and creates `~/.asftool/` with `0700`. Turns the T3.1 preflight message into a one-command fix | 5 | `asftool init` produces a working `.env`; `asftool` then starts cleanly | H-1 (UX) |
| **X.3** | **Publish a contract test against a live sandbox org** (opt-in, credential-gated): a `@pytest.mark.live` suite that runs the real Wave endpoints and asserts the models parse them. Not in the default CI run; runnable via `pytest -m live` with `SF_ORG_ALIAS` set | 5 | `pytest -m live` passes against a sandbox org; it is marked `@pytest.mark.live` and excluded from the default `addopts` | C-1, C-2, H-4, M-3 |
| **X.4** | **Update `AGENTS.md` to a "applied lessons" format**: each recorded lesson gets a `Status: applied / not-applicable / open` field and links to the task that closed it. Lessons #1, #2, #4, #5 are currently recorded but not applied to `master` | 3 | Every lesson in `AGENTS.md` has a status; the "open" ones appear in the plan | C-1, H-2, H-4 (process) |
| **X.5** | **Add `py.typed`** so the published package ships type information (the package is fully typed and mypy-clean, but the marker is missing) | 1 | `asftool/py.typed` exists and is included in the package data | Typing |
| **X.6** | **Decide the storage root** (product decision, then implementation): `~/.asftool/downloads` vs `$CWD/asftool_downloads`. Record the decision in `docs/decisions/0001-storage-root.md`; then implement it (T3.7) | 1 | The ADR exists and is linked from the README | M-9 |

---

## 11. Backlog — Not Scheduled

Realistically valuable, but not required for a correct release. Listed so they are not lost.

| ID | Item | Points | Notes |
|---|---|---|---|
| B.1 | Restore the `ASFTOOL_` prefix as a *supported alias* in addition to the current names | 3 | Would need a custom `EnvSettingsSource`; only worth it if users outside the team exist |
| B.2 | Implement the JWT-bearer / PKCE / device flows natively (dropping the SF CLI dependency) | 21 | Large; would justify the `authlib`/`python-jose` deps that T6.6 removes |
| B.3 | Cache Salesforce responses on disk to make repeat scans fast | 8 | Needs an invalidation strategy |
| B.4 | `asftool` output as `--format json` on **stdout** (machine-readable) with logs on stderr | 3 | Depends on T6.4 landing first |
| B.5 | Schema-diff between two field-impact reports (what changed between runs) | 8 | Natural extension of the existing report JSON |
| B.6 | `asftool lineage` to a real graph renderer (not just Mermaid) | 13 | Mermaid output already works once T5.1/T5.2 land |
| B.7 | A `Makefile` or `justfile` for the common commands (`make test lint typecheck ci`) | 2 | Developer ergonomics |
| B.8 | Property-based tests (Hypothesis) for the field matcher and pagination helper | 5 | Would harden T1.6 and the fuzzy-match thresholds |
| B.9 | Configurable output format for the lineage JSON (currently `indent=2` only) | 1 | Trivial |
| B.10 | Deprecate and remove `asftool/core/tasks/parallel.py` helpers that nothing calls | 2 | `merge_csv_chunks`/`split_csv_for_parallel` are not referenced by any service |

---

## 12. Traceability Matrix

Every finding in `CODE_REVIEW.md` maps to at least one task.

| Finding | Severity | Tasks | Phase |
|---|---|---|---|
| C-1 `list_orgs` returns `[]` | 🔴 | T2.1, T2.8, T2.9, X.3 | 2 |
| C-2 pagination truncates to page 1 | 🔴 | T1.2, T1.3, T1.5, T1.6, X.3 | 1 |
| C-3 `prompt_select` infinite loop | 🔴 | T3.3, T3.4, T3.6 | 3 |
| H-1 crash without `.env` | 🟠 | T0.2, T3.1, T3.2, X.2 | 0, 3 |
| H-2 `SF_API_VERSION` env leakage | 🟠 | T2.2, T2.9 | 2 |
| H-3 unbounded scan concurrency | 🟠 | T4.1 | 4 |
| H-4 `DataflowJob` cannot parse | 🟠 | T1.4, T1.7, X.3 | 1 |
| H-5 `Retry-After` ignored | 🟠 | T4.2, T4.3 | 4 |
| H-6 lineage `NameError` + path mismatch | 🟠 | T5.1, T5.2, T5.3 | 5 |
| M-1 keyring `NoKeyringError` unhandled | 🟡 | T2.3, T6.5 | 2, 6 |
| M-2 `CryptoManager` padding mismatch | 🟡 | T2.4 | 2 |
| M-3 expiry never detected | 🟡 | T2.5, T6.3, X.3 | 2, 6 |
| M-4 `ASFTOOL_` prefix ignored | 🟡 | T6.2, T6.1 | 6 |
| M-5 refresh failure → surprise login | 🟡 | T2.6 | 2 |
| M-6 `doctor` hardcoded alias | 🟡 | T3.5 | 3 |
| M-7 403/404 lose API detail | 🟡 | T4.4, T5.6 | 4, 5 |
| M-8 `cleanup_old_downloads` crashes | 🟡 | T5.4, T5.5 | 5 |
| M-9 downloads in CWD | 🟡 | T3.7, X.6, T6.1 | 3, 6 |
| L-1 `core/logger.py` dead | 🔵 | T6.4 | 6 |
| L-2 prompt contract inconsistency | 🔵 | T3.4 | 3 |
| L-3 logs to stdout | 🔵 | T6.4 | 6 |
| L-4 `-o` short-flag collision | 🔵 | T6.10 | 6 |
| L-5 no CI / lint errors | 🔵 | T0.1, T0.3, T0.4 | 0 |
| L-6 wrong licence classifier | 🔵 | T0.5 | 0 |
| L-7 deprecated datetime calls | 🔵 | T6.8 | 6 |
| L-8 leaked file handles | 🔵 | T6.9 | 6 |
| Theme A (contract tests) | — | T1.1, T1.7, X.1, X.3 | 1 |
| Theme B (docs vs reality) | — | T6.1, T6.2, T6.3, T6.5 | 6 |
| Theme C (dead config/deps) | — | T6.6, T6.7 | 6 |
| Theme D (pagination drift) | — | T1.6 | 1 |
| Theme E (broad excepts) | — | T5.3, T2.8, T4.5 | 5, 2, 4 |

**Coverage: 26/26 findings + 5 cross-cutting themes = 100%.**

---

## 13. Risk Register for the Plan

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| **T2.5 (token expiry) has no reliable data source** | Medium | High | The plan explicitly allows a fallback (observation window). Do not fabricate an expiry. Escalate to a product decision if neither works. |
| **A contract fix breaks a "working" feature** | Low | Medium | Every model has `extra="allow"`; aliases are additive. Run the full suite plus a live-org smoke test (X.3) before release. |
| **Phase 0 CI is adopted but then bypassed** | Medium | High | Branch protection required on `master`; the AGENTS.md merge workflow already mandates passing CI — make it enforceable. |
| **Scope creep into the OAuth re-implementation (B.2)** | Medium | Medium | T6.6 explicitly *removes* the dead config rather than implementing it. B.2 is out-of-scope backlog. |
| **The 189-point plan (171 task + 18 cross-phase) is too large for one sprint** | High | Medium | Phases 0–2 (73 points) are the release blocker and span three sub-sprints. Phases 3–6 are the following two sprints. Do not start Phase 3 before Phase 2's gate G1 passes. |
| **`AGENTS.md` lessons are recorded but never applied again** | Medium | High | X.4 adds a `Status` field to every lesson. A lesson with `Status: open` must appear in this plan or be closed with a reason. |

---

## 14. Sprint Slicing

For a team of two engineers at 1.0 velocity, 10 points per sprint is a common planning norm. Adjust to your team's real velocity.

**Sprint 1 — Release blocker (73 points, Phases 0–2)**
- Sprint 1a (21 pts, 9 tasks): T0.1–T0.6, T1.1–T1.3 → *gate G0, contract fixtures in place*
- Sprint 1b (24 pts, 6 tasks): T1.4–T1.7, T2.1–T2.2 → *the two Critical API bugs fixed*
- Sprint 1c (28 pts, 7 tasks): T2.3–T2.9 → *gate G1 — auth is correct and fails safely*

**Sprint 2 — Usable product (34 points, Phase 3 + start of Phase 4)**
- T3.1–T3.4 → *gate G2 — the tool starts and never hangs*
- T3.5–T3.7, T4.1, T4.2 → *concurrency and rate limits fixed*

**Sprint 3 — Harden and finish (41 points, Phases 4–5 + cross-phase)**
- T4.3–T4.6, T5.1–T5.3 → *gate G3*
- T5.4–T5.6, X.1, X.3, X.4 → *gate G4 — lineage works, contracts are guarded*

**Sprint 4 — Truth and hygiene (41 points, Phase 6 + remaining cross-phase)**
- T6.1–T6.5, X.2, X.5, X.6 → *gate G5 — docs match reality*
- T6.6–T6.10, X.2, X.5 → *dead code gone, no deprecations*

---

## 15. Definition of Done (Checklist)

Use this verbatim in every PR description.

```markdown
## Definition of Done

- [ ] Task ID(s): T<phase>.<n>
- [ ] Findings closed: <IDs from CODE_REVIEW.md>
- [ ] Acceptance criteria met (link the check)
- [ ] New/updated tests added and they FAIL before the fix
- [ ] `ruff check .` → clean
- [ ] `mypy asftool` → clean
- [ ] `pytest -q` → green
- [ ] README updated if the user-visible contract changed
- [ ] CHANGELOG.md updated (user-visible change or breaking change)
- [ ] `AGENTS.md` lesson status updated if a recorded lesson was applied
- [ ] No secrets, tokens, or org-specific data in the diff
- [ ] No new public API without a docstring and a type hint
```

---

*End of plan. Companion document: `CODE_REVIEW.md`.*
