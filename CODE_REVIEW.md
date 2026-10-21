# Code Review — ASFTool (`origin/master` @ `ddbb69f`)

**Reviewer:** OpenHands AI agent
**Date:** 2026-10-21
**Target:** `origin/master` (`ddbb69f` — "chore(.gitignore): exclude backups and RELEASE.md from master")
**Method:** Full read of all 82 tracked files + empirical execution of every finding (repro scripts in `/tmp/verify_*.py`)
**Taste rating:** **Needs improvement**

---

## 0. Executive Summary

ASFTool has a sound architectural skeleton — clean `cli/ → core/` dependency direction, a good async HTTP client, correct keyring-backed secret storage, and mostly rigorous Pydantic models. The type checker is clean (`mypy`: 50 source files, 0 errors) and 237 of 239 tests pass.

**But the code is not release-ready.** The dominant theme is a *contract drift between the code and the real Salesforce API*: the models, the SF CLI wrapper, and the docs were each written against a slightly different idea of what the platform returns, and nobody reconciled them. The result is that several headline features are **silently broken against a live org** while their unit tests pass, because the tests assert the same wrong assumption the implementation does.

Three findings block a release outright:

1. **`asftool auth list-orgs` always returns "No authorized orgs found"** — the SF CLI response parser reads a key (`orgs`) that does not exist in the actual response.
2. **Dataset and dashboard listing silently truncates to the first page** — the pagination models read `next_page_url` but the API returns `nextPageUrl`, so the loop exits after page 1.
3. **The interactive configuration menu hangs in an infinite loop** when the user presses Enter at the API-version prompt.

Two more are *security-relevant*: the tool is **hard-failed** (unhandled traceback, non-zero exit) on any machine without a `.env` file, and the `SF_API_VERSION` environment variable **leaks into SF CLI subprocesses**, where it actively breaks org resolution.

| Severity | Count |
|---|---|
| 🔴 Critical | 3 |
| 🟠 High | 6 |
| 🟡 Medium | 9 |
| 🔵 Low | 8 |

**Total: 26 findings.**

---

## 1. Verification Baseline

All findings below were reproduced by executing code against the checked-out `origin/master` worktree.

| Check | Command | Result |
|---|---|---|
| Test suite | `pytest -q` | **237 passed, 2 failed** |
| Lint | `ruff check .` | **7 errors** (all in `tests/`) |
| Type check | `mypy asftool` | **Success: no issues found in 50 source files** |
| CLI without `.env` | `asftool --help` | **Traceback, exit 1** |
| Live SF CLI | `sf org list --json --all` | 2 orgs authorised |

The 2 test failures are **not flaky** — they are a deterministic consequence of finding **C-1** (see §3).

> **Note on environment:** the local venv runs Python 3.14.4 / pandas 3.0.5 / tenacity 9.1.4 / typer 0.27.2 — all well ahead of the `requires-python = ">=3.11"` floor. Several findings are forward-compatibility risks that will become hard failures on the next dependency bump.

---

## 2. Architecture Assessment

### What is good

- **Dependency direction is respected.** `core/` never imports from `cli/`. This is stated in the README and holds.
- **Async discipline is real.** `httpx.AsyncClient`, `asyncio.gather` for fan-out, `AsyncIterator` streaming APIs, `ProcessPoolExecutor` for CPU-bound work. No `requests`, no blocking I/O in async paths (with the one exception noted in M-5).
- **Secrets handling is sound in principle.** Tokens are encrypted with a salted PBKDF2-derived key and stored in the OS keyring, never on disk in plaintext. No hardcoded credentials anywhere (verified by grep across `asftool/` and `scripts/`).
- **The `*_async` + thin Typer shim pattern** is applied consistently across all 8 command groups, which is why the interactive menu and the CLI cannot drift in behaviour.
- **Type hints are thorough**, and `mypy` passes with `warn_return_any` and `check_untyped_defs` enabled.

### What is wrong structurally

**The platform contract is defined in three places that do not agree.** This is the root cause of most Critical and High findings.

| Contract element | Defined in | Assumes |
|---|---|---|
| List-response envelope | `core/models/__init__.py` | `next_page_url` (snake_case) |
| List-response envelope | Salesforce Wave API | `nextPageUrl` (camelCase) |
| SF CLI org list | `core/sf_cli.py:463` | `result.orgs` |
| SF CLI org list | Real SF CLI | `result.other` / `.nonScratchOrgs` / `.devHubs` / `.scratchOrgs` |
| SF CLI org display | `core/sf_cli.py:436` | `expirationDate` or `tokenExpiration` |
| SF CLI org display | Real SF CLI | neither — only `apiVersion` |
| Env var names | `README.md:82-89` | `ASFTOOL_`-prefixed |
| Env var names | `core/config.py` | unprefixed, explicit `alias=` |

Every one of these is a place where the code and reality disagree, and in each case **the unit test asserts the code's assumption, so the test passes and the feature is broken in production.**

**Secondary structural issue: `core/logger.py` is dead code.** `setup_logging()` is never called from anywhere in the package — not from `cli/main.py`, not from anywhere. Yet the README advertises "structured JSON logging" and the module docstring claims it writes to `~/.asftool/asftool.log`. In reality, every module does `structlog.get_logger(__name__)` at import time, which uses the *default* structlog configuration: human-readable console output to **stdout**, not JSON, and never persisted to a file. This is a documentation-vs-reality gap that a senior PM would immediately flag ("production ready — structured JSON logging" is a false claim in the README feature list).

---

## 3. Findings

### 🔴 CRITICAL

---

#### C-1 — `asftool auth list-orgs` always reports "No authorized orgs found"

**File:** `asftool/core/sf_cli.py:458-465`

```python
def list_orgs(self) -> list[Any]:
    """List all authorized orgs."""
    result = self._run_command(["org", "list", "--json", "--all"])
    try:
        output = json.loads(result.stdout)
        return output.get("result", {}).get("orgs", [])  # type: ignore[no-any-return]
    except (json.JSONDecodeError, KeyError):
        return []
```

**The `orgs` key does not exist.** The real `sf org list --json --all` response partitions orgs into four arrays:

```
$ sf org list --json --all | jq '.result | keys'
[ "other", "sandboxes", "nonScratchOrgs", "devHubs", "scratchOrgs" ]
```

Verified against a live SF CLI:

```
other -> 1
nonScratchOrgs -> 1
devHubs -> 0
scratchOrgs -> 0
has orgs key? False
```

**Reproduction (against the real CLI):**

```
$ python -c "from asftool.core.sf_cli import SFCLIManager; print(SFCLIManager().list_orgs())"
list_orgs() returned: []
COUNT = 0
```

Two orgs are authorised on this machine. The tool reports zero.

**Impact.** `asftool auth list-orgs` — a documented command (README line 7, "multi-org support") — is **completely non-functional**. The interactive menu's "List authorized orgs" item (`menus/auth.py:83`) shows the same empty table. Worse, it fails *silently*: `SFCLIAuthService.list_orgs()` (`auth/sf_cli_auth.py:317-325`) wraps the call in a bare `except Exception` that logs and returns `[]`, so the user cannot distinguish "you have no orgs" from "the parser is broken". Because the alias discovery is also how a user finds the alias name to pass to `--alias`, this is the **first command a new user runs**.

**Fix.** Concatenate all four arrays, matching what the repo's own `AGENTS.md` lesson #4 already documents:

```python
result_data = output.get("result", {})
orgs = (
    result_data.get("other", [])
    + result_data.get("nonScratchOrgs", [])
    + result_data.get("devHubs", [])
    + result_data.get("scratchOrgs", [])
)
```

> **Process note:** `AGENTS.md` §"Critical Lessons Learned" #4 already records this exact fix. It was written down, never applied to `master`, and the same bug is re-introduced on the `feat/module-a-git-sync` branch. **The lesson-learned document is not being applied to the code it describes.** This is a process finding, not just a code finding.

---

#### C-2 — Dataset and dashboard listing silently truncates to the first page

**Files:**
- `asftool/core/models/__init__.py:189-194` (`DatasetListResponse`)
- `asftool/core/models/__init__.py:282-287` (`DashboardListResponse`)
- Consumers: `core/services/dataset_service.py:63-72`, `core/services/dashboard_service.py:53-61`

```python
class DatasetListResponse(BaseModel):
    """Response for dataset listing."""
    datasets: list[Dataset]
    next_page_url: str | None = None          # <-- no alias
    model_config = ConfigDict(extra="allow", populate_by_name=True)
```

The Salesforce Wave API returns **`nextPageUrl`** (camelCase). The model declares `next_page_url` with no `alias`, and `populate_by_name=True` only allows the *Python* name to be used for construction — it does not map `nextPageUrl` onto `next_page_url`. Because `extra="allow"`, the real `nextPageUrl` is silently swallowed into `model_extra` and `next_page_url` stays `None`.

**Reproduction:**

```python
raw = {"datasets": [], "nextPageUrl": "https://x/wave/datasets?pageToken=abc123"}
m = DatasetListResponse(**raw)
print(m.next_page_url)   # -> None
print(m.model_extra)     # -> {'nextPageUrl': 'https://x/wave/datasets?pageToken=abc123'}
```

End-to-end proof that the pagination loop exits after one request:

```python
# FakeClient returns 2 pages
datasets returned: ['0Fb1']
http calls made (page_tokens): [None]
EXPECTED 2 pages, 2 datasets -> got 1 datasets, 1 calls
```

**Impact.** `asftool datasets list` and `asftool dashboards list` show only the **first 50 assets**, with no error and no indication that more exist. The CLI even prints `Total: 50 datasets` as if that were the whole org. In a real org with hundreds of datasets this is a silent, confidently-wrong answer — the worst failure mode for a data-inventory tool. Any downstream user who trusts the count will conclude the org has 50 datasets.

The same camelCase drift affects the `FieldImpactService` scanner, which reads `resp.get("nextPageUrl")` **correctly** (`field_impact_service.py:283`) — so the field-impact scan paginates properly while the dataset/dashboard listing does not. Two different conventions for the same API in the same codebase, and only one of them is right.

**Fix.** Add the alias, exactly as the repo already does for `DataflowJobListResponse` in `AGENTS.md` #5:

```python
class DatasetListResponse(BaseModel):
    datasets: list[Dataset]
    next_page_url: str | None = Field(default=None, alias="nextPageUrl")
    model_config = ConfigDict(extra="allow", populate_by_name=True)
```

…and the same for `DashboardListResponse`.

---

#### C-3 — The interactive configuration menu enters an infinite loop

**Files:** `asftool/cli/ui.py:175-194` (`prompt_select`), `asftool/cli/menus/config.py:146-152`

```python
def prompt_select(prompt_text: str, choices: list[str], default: str | None = None) -> str:
    ...
    while True:
        try:
            selection = Prompt.ask("Enter your choice", default=str(default) if default else None)
            if selection is None:
                continue
            idx = int(selection) - 1
            if 0 <= idx < len(choices):
                return choices[idx]
            print_error("Invalid selection. Please try again.")
        except ValueError:
            print_error("Please enter a number.")
```

`prompt_select` presents a **numbered list** (`1. v60.0`, `2. v68.0`) but its `default` is a **version string** (`"v68.0"`). When the user presses Enter to accept the shown default, `Prompt.ask` returns `"v68.0"`, `int("v68.0")` raises `ValueError`, and the loop prints "Please enter a number." and asks again — forever. There is no escape, no "cancel" option, and no iteration cap.

**Reproduction (with `Prompt.ask` returning the default, i.e. user pressed Enter):**

```
Calling prompt_select with default='v68.0':
  1. v60.0
  2. v68.0
  ✗ Please enter a number.
  ✗ Please enter a number.
  ✗ Please enter a number.
  ... (unbounded)
```

**Impact.** `asftool` → `7` (Configuration) → `2` (Set API version from org list) is **unrecoverable without killing the process**. This is the *only* in-app way to set the API version, so the documented "Set API version" feature is unusable interactively. `Ctrl-C` is caught by the menu loop (`cli/main.py:181-182`) and merely prints "Cancelled" and continues the menu — but the `while True` is *inside* `prompt_select`, so `Ctrl-C` raises `KeyboardInterrupt` out of `Prompt.ask`… except that `KeyboardInterrupt` is a `BaseException`, not `Exception`, so it is not caught by `except ValueError` and *does* propagate out — the user escapes only by killing the process. A senior PM would classify this as a **blocking usability defect in a headline feature**.

The same function is used at `menus/config.py:148` and is the shared selection helper for every future "pick from a list" menu, so the bug will propagate.

**Fix.** Validate the *default* against the choice list, and cap iterations:

```python
def prompt_select(prompt_text, choices, default=None):
    ...
    for _ in range(10):
        selection = Prompt.ask("Enter your choice", default=None)
        if selection is None:
            continue
        try:
            idx = int(selection) - 1
        except ValueError:
            print_error("Please enter a number.")
            continue
        if 0 <= idx < len(choices):
            return choices[idx]
        print_error("Invalid selection. Please try again.")
    raise ValueError("Too many invalid selections")
```

Separately, the caller should pass a **numeric** default (`str(choices.index(current) + 1)`), not the value string.

---

### 🟠 HIGH

---

#### H-1 — The tool crashes with a raw traceback when `.env` is missing

**File:** `asftool/core/config.py:38-44`, `asftool/core/config.py:145-164`

```python
encryption_key: str = Field(alias="ENCRYPTION_KEY")
jwt_secret_key: str = Field(alias="JWT_SECRET_KEY")
```

Both fields are **required** with no default. `Settings()` therefore raises `pydantic_core.ValidationError` when they are absent. Because `get_settings()` is called from `Session.__init__` (`cli/session.py:34`), which is called from `_run_menu_loop` (`cli/main.py:128`), this happens before any command runs.

**Reproduction (clean directory, no `.env`):**

```
$ asftool --help
Traceback (most recent call last):
  ...
  File "/asftool/core/config.py", line 159, in get_settings
    settings = Settings()
pydantic_core._pydantic_core.ValidationError: 2 validation errors for Settings
ENCRYPTION_KEY
  Field required [type=missing, ...]
JWT_SECRET_KEY
  Field required [type=missing, ...]

EXIT=1
```

Note that `asftool --help` **does not print help** — it enters the menu loop, which dies. The `_has_subcommand` router (`cli/main.py:191-200`) only recognises the nine known subcommand names, so `--help` and `-h` are classified as "no subcommand" and routed to the menu.

**Impact.** Any new user who clones the repo and runs `asftool` — before knowing they need a `.env` — gets a stack trace instead of guidance. The two failing tests in `tests/integration/test_config_cli.py` are a direct consequence:

```
FAILED tests/integration/test_config_cli.py::TestConfigShow::test_show_runs
FAILED tests/integration/test_config_cli.py::TestConfigShow::test_show_reports_persisted_value
E   assert 1 == 0
E   +  where 1 = <Result 2 validation errors for Settings ENCRYPTION_KEY Field required ...>
```

**The test suite has been red on `master` and shipped anyway.** That is a process failure worth naming: `main` has failing tests, and there is no CI workflow (`.github/` does not exist) to catch it.

**Impact on security posture (the reason it matters beyond UX):** `ENCRYPTION_KEY` and `JWT_SECRET_KEY` are **required** even though the *only* implemented auth path is SF CLI, which needs neither. The JWT/PKCE/Connected-App configuration in `Settings` (`config.py:40-57`) is entirely **dead** — `authlib` and `python-jose` are declared dependencies with **zero references** in the codebase. So the tool demands two secrets it does not use, in exchange for security features it does not implement.

**Fix.**
1. Make the two fields optional with a `None` default, and treat the crypto manager as lazily unavailable when `ENCRYPTION_KEY` is unset.
2. Add a `doctor`-style preflight in `main()` that prints an actionable message ("run `asftool init` or copy `.env.example` to `.env`") and exits 1.
3. Route `--help`/`-h`/`--version` to Typer instead of the menu.
4. Either implement the JWT/PKCE flows or delete the dead settings and the `authlib`/`python-jose` dependencies.

---

#### H-2 — `SF_API_VERSION` leaks into SF CLI subprocesses and breaks org resolution

**Files:** `asftool/core/sf_cli.py:106-158` (`_run_command`), `asftool/core/sf_cli.py:160-239` (`_run_command_async`), `asftool/core/config.py:145-164`

Neither `_run_command` nor `_run_command_async` passes an explicit `env=` to `subprocess.run` / `create_subprocess_exec`. The child therefore inherits the **entire** parent environment, including `SF_API_VERSION` — which `Settings` reads from `.env` and which `_apply_api_version_override` (`cli/commands/fields.py:35-49`) writes into `os.environ` at runtime.

SF CLI interprets `SF_API_VERSION` as an override for the org's API version, and when the value is malformed or the version is not supported for that org, org resolution fails outright.

**Reproduction with a polluted environment:**

```
$ SF_API_VERSION=v68.0 sf org display --target-org claritev6 --json
{
  "name": "NamedOrgNotFoundError",
  "message": "Parsing --target-org \n\tNo authorization information found for claritev6.",
  "exitCode": 2,
  "actions": [ "It looks like you mistyped the username or alias. Did you mean \"mcp\"?" ]
}
```

The same command without the env var resolves the org fine.

**Impact.** `asftool fields analyze --api-version v68.0` sets `SF_API_VERSION` in `os.environ` (`cli/commands/fields.py:49`) and then every subsequent SF CLI subprocess in that process inherits it. If the chosen version is not the org's version, *all* auth calls in that invocation fail with a misleading "no authorization information found" error. The user is sent to debug a credential problem when the real cause is their own `--api-version` flag. This is a **self-inflicted, hard-to-diagnose auth failure**.

**Fix.** Filter the variable out of the subprocess environment — exactly as the repo's own `AGENTS.md` lesson #2 prescribes:

```python
env = {k: v for k, v in os.environ.items() if k != "SF_API_VERSION"}
result = subprocess.run(cmd, ..., env=env)
```

---

#### H-3 — The `FieldImpactService` concurrency semaphore is created but never used

**File:** `asftool/core/services/field_impact_service.py:51-64`, and the four `_scan_*` methods

```python
self.max_concurrent = max_concurrent
self._semaphore = asyncio.Semaphore(max_concurrent)   # <-- created, never acquired
```

`self._semaphore` is **never referenced anywhere else in the codebase** (verified: `grep -rn "_semaphore" asftool/` returns only the two definition sites in `runner.py` and this one). The scans use bare `asyncio.gather(*tasks, return_exceptions=True)` over *every* asset, with no cap:

```python
tasks = [self._scan_one_dataset(d, search_term, mode) for d in datasets]
results = await asyncio.gather(*tasks, return_exceptions=True)
```

**Reproduction — 200 datasets, semaphore limit 10:**

```
datasets scanned: 200
MAX CONCURRENT in-flight HTTP calls: 200
semaphore limit was: 10
```

**Impact.** A field-impact scan against a real org fires **one HTTP request per asset simultaneously** — hundreds or thousands of concurrent requests. The consequences are concrete:

- The `httpx` connection pool is capped at 10 (`client.py:74-78`), so requests queue, but the *pending-request* memory and the task count are unbounded.
- Salesforce Wave API aggressively rate-limits (HTTP 429). The client retries with a 30-second-capped exponential backoff (§H-5), so a scan against a 500-asset org becomes a multi-minute stall.
- The configured `field_impact_max_concurrent_scans` setting (`config.py:63-65`, default 10) is **silently ignored** — the user tunes it and observes no change.

**Fix.** Acquire the semaphore inside each per-asset scan coroutine:

```python
async def _scan_one_dataset(self, dataset, search_term, mode):
    async with self._semaphore:
        ...
```

Better: use an `asyncio.Semaphore` as a context manager around the `gather` in a worker-wrapper, so the limit bounds *in-flight* work rather than the task count.

---

#### H-4 — `DataflowJob` and `DataflowJobListResponse` cannot parse the real API response

**Files:** `asftool/core/models/__init__.py:351-369`, `asftool/core/services/dataflow_service.py:68-72`

```python
class DataflowJob(BaseModel):
    id: str
    dataflow_id: str                # no alias
    dataflow_name: str              # no alias
    command: Literal["start", "stop"]
    status: Literal["Queued", "Running", "Success", "Failed", "Cancelled"]
    start_time: datetime | None = None    # no alias
    end_time: datetime | None = None      # no alias
    ...

class DataflowJobListResponse(BaseModel):
    dataflowjobs: list[DataflowJob]       # no alias
```

The real `/wave/dataflowjobs` response is camelCase (`dataflowId`, `dataflowName`, `startTime`, `endTime`) and the collection key is `dataflowJobs`.

**Reproduction:**

```python
raw = {"id":"0Fj...","dataflowId":"0F0...","dataflowName":"MyFlow",
       "command":"start","status":"Success",
       "startTime":"2024-01-01T00:00:00.000Z","endTime":"2024-01-01T01:00:00.000Z"}
DataflowJob(**raw)
# RAISED: ValidationError
#   2 validation errors for DataflowJob
#   dataflow_id   Field required
#   dataflow_name Field required
```

```python
DataflowJobListResponse(**{"dataflowJobs": []})
# RAISED: ValidationError: dataflowjobs Field required
```

**Impact.** `asftool jobs list` and `asftool jobs show <id>` — two documented commands (README lines 12, 53) — **raise `ValidationError` on every real org**. The CLI catches it in the generic `except Exception` and prints `List failed: 2 validation errors for DataflowJob`, which tells the user nothing actionable. `DataflowService.wait_for_dataflow_job()` and `get_dataflow_job_status()` are affected identically, so programmatic polling is broken too.

The unit test at `tests/integration/test_api_endpoints.py:287` asserts against a **snake_case** fixture (`"dataflowjobs": [...]`), i.e. the test encodes the bug. This is the exact failure mode `AGENTS.md` lesson #5 describes — the fix was recorded, and the alias was never added to `master`.

**Fix.**

```python
class DataflowJob(BaseModel):
    id: str
    dataflow_id: str = Field(alias="dataflowId")
    dataflow_name: str = Field(alias="dataflowName")
    command: Literal["start", "stop"]
    status: Literal["Queued", "Running", "Success", "Failed", "Cancelled"]
    start_time: datetime | None = Field(default=None, alias="startTime")
    end_time: datetime | None = Field(default=None, alias="endTime")
    ...
    model_config = ConfigDict(extra="allow", populate_by_name=True)


class DataflowJobListResponse(BaseModel):
    dataflowjobs: list[DataflowJob] = Field(..., alias="dataflowJobs")
    model_config = ConfigDict(extra="allow", populate_by_name=True)
```

**and update the test fixture to the real camelCase payload**, so the test actually guards the behaviour.

---

#### H-5 — HTTP 429 `Retry-After` is captured and then ignored

**File:** `asftool/core/client.py:128-173` and `asftool/core/client.py:89-106`

```python
elif response.status_code == 429:
    retry_after = response.headers.get("Retry-After", "60")
    raise SalesforceRateLimitError(
        "Rate limit exceeded",
        retry_after=int(retry_after) if retry_after.isdigit() else 60,
    )
```

`SalesforceRateLimitError.retry_after` is set correctly. But the retry policy uses a **static** wait function:

```python
self._retry_client = AsyncRetrying(
    wait=wait_exponential_jitter(initial=1, max=30),   # ignores retry_after
    stop=stop_after_attempt(3),
    retry=retry_if_exception_type((..., SalesforceRateLimitError)),
    ...
)
```

The exception's `retry_after` field is **never read by anything**.

**Reproduction — server sends `Retry-After: 300`:**

```
raised: SalesforceRateLimitError | retry_after = 300
attempts made: 3
Retrying ... in 1.68 seconds as it raised SalesforceRateLimitError
Retrying ... in 2.99 seconds as it raised SalesforceRateLimitError
```

The server asked for a 300-second wait. The client waited 1.68 s, then 2.99 s, and gave up.

**Impact.** Every 429 becomes **three rapid-fire requests against an already-throttling server**, which typically deepens the throttling and can trigger a temporary API lockout. This is a textbook retry-storm. Combined with H-3 (unbounded fan-out), a field-impact scan against a large org is a reliable way to get rate-limited and then to hammer through the limit.

**Fix.** Use a custom wait that honours the server's instruction:

```python
def _wait_retry_after(retry_state):
    exc = retry_state.outcome.exception() if retry_state.outcome else None
    if isinstance(exc, SalesforceRateLimitError) and exc.retry_after:
        return min(exc.retry_after, MAX_RETRY_AFTER)
    return wait_exponential_jitter(initial=1, max=30)(retry_state)
```

Also cap `retry_after` to a sane maximum and surface it to the user rather than silently sleeping.

---

#### H-6 — The lineage menu crashes with `NameError` on the "use an existing impact file" path

**File:** `asftool/cli/menus/lineage.py:88`, `asftool/cli/menus/lineage.py:95`

```python
# line 50-54 (inside the else branch — only assigned here)
search_term = prompt_text("Field API name or label to analyze ...")

# line 88 (unconditional)
output_path = await _prompt_output_path(search_term or impact_path or "lineage")
```

`search_term` is assigned **only** in the `else` branch (the "search a field first" path). If the user answers `y` at line 23 ("Use an existing impact file?"), the `if` branch runs, `search_term` is never bound, and line 88 raises:

```
existing-file path : NameError -> cannot access local variable 'search_term' where it is not associated with a value
search-field path  : OK -> 'revenue'
```

Line 95 tries to defend against this with `'search_term' in locals()`, but line 88 executes **first** and is already dead.

**Impact.** The "load an existing impact file" path of the Visual Lineage feature is **completely broken**. The `NameError` is caught by the broad `except Exception` at line 111, so the user just sees "Lineage generation failed: cannot access local variable 'search_term'…" and is returned to the menu with no way forward.

There is a **second, independent bug** on the *other* path. Line 74 recomputes the impact-file path after the analysis has already written it:

```python
impact_path = str(storage.field_impact_path(alias=alias, search_term=search_term))
```

`field_impact_path()` embeds a **timestamp** (`storage.py:62-66`). Recomputing it produces a *different* filename than the one `analyze_field_async` actually wrote:

```
path written by analysis : .../2026-10-21_02-26-25_revenue.json
path recomputed by menu  : .../2026-10-21_02-26-27_revenue.json
MATCH? False
```

So even on the working path, the menu looks for a file that does not exist and reports "Impact file not found".

**Fix.**
1. Initialise `search_term: str | None = None` before the branch, and use `search_term or "lineage"` at line 88.
2. Have `analyze_field_async` **return the path it wrote** (it already does — `-> Path | None`) and use that return value instead of recomputing it:
   ```python
   impact_path = str(await analyze_field_async(...)) if written else None
   ```

---

### 🟡 MEDIUM

---

#### M-1 — `TokenStore` raises an unhandled `NoKeyringError` on any machine without a keyring backend

**File:** `asftool/core/auth/token_store.py:127`, `asftool/core/auth/token_store.py:152-157`

```python
stored = keyring.get_password(self.KEYRING_SERVICE, self._get_keyring_key(alias))
if not stored:
    return None
```

and

```python
try:
    keyring.delete_password(self.KEYRING_SERVICE, self._get_keyring_key(alias))
    return True
except keyring.errors.PasswordDeleteError:
    return False
```

Both `get_password` and `delete_password` are called **outside** any `try` for `keyring.errors.NoKeyringError`. On a headless container without `keyrings.alt` (a completely normal deployment for a CLI that supports device-flow login), the keyring backend is `keyring.backends.fail.Keyring` and every call raises.

**Reproduction:**

```
backend: keyring.backends.fail.Keyring (priority: 0)
load_token RAISED (uncaught): keyring.errors.NoKeyringError No recommended backend was available...
delete_token RAISED (uncaught): keyring.errors.NoKeyringError No recommended backend was available...
```

**Impact.** `asftool auth status` — the first thing a user runs to check their session — **crashes with a traceback** instead of reporting "not authenticated". The `except keyring.errors.PasswordDeleteError` clause is also the wrong exception type: `PasswordDeleteError` is raised when the *entry is absent*, not when the *backend is missing*. So the "no such token" case and the "no keyring" case are conflated into a handler that catches neither correctly.

Note the README claims "Credentials are encrypted and stored in the OS keyring (with **encryption-key file fallback** for headless containers)" (line 69-70). **No such fallback exists in the code** — `grep` for any fallback file logic returns nothing. The documented headless-container path is unimplemented, which is why this bug is reachable at all.

**Fix.** Catch `keyring.errors.NoKeyringError` explicitly and degrade to a clear, actionable message:

```python
try:
    stored = keyring.get_password(self.KEYRING_SERVICE, self._get_keyring_key(alias))
except keyring.errors.NoKeyringError as e:
    raise TokenStoreUnavailableError(
        "No OS keyring backend available. Install 'keyrings.alt' "
        "(pip install 'asftool[headless]') or use a container with a keyring."
    ) from e
```

Either implement the documented encryption-key-file fallback, or remove that sentence from the README.

---

#### M-2 — The `CryptoManager` rejects keys that `Settings` accepts

**Files:** `asftool/core/crypto.py:53-55` vs `asftool/core/config.py:82-92`

```python
# crypto.py
def __init__(self, master_key: str):
    self._master_key = base64.urlsafe_b64decode(master_key.encode())   # strict
```
```python
# config.py — validate_encryption_key
decoded = base64.urlsafe_b64decode(v + "=" * (-len(v) % 4))            # lenient (pads)
```

The validator **pads** the input before decoding; the constructor does **not**. A 43-character unpadded base64 key therefore passes validation and then crashes on use.

**Reproduction:**

```
padded len: 44   unpadded len: 43
CryptoManager(padded)   OK
CryptoManager(unpadded) RAISED: Error Incorrect padding
Settings-style validator ACCEPTS unpadded key -> inconsistency confirmed
```

This matters because the README's own key-generation command produces a **padded** 44-char key:

```bash
python -c "import base64, os; print(base64.urlsafe_b64encode(os.urandom(32)).decode())"
```

so the documented path works — but a user who strips the `=` padding (a common habit, and some secret managers do it automatically) gets a validation error at startup, not at the point of failure.

**Fix.** Use the same padding-tolerant decode in `CryptoManager.__init__`:

```python
self._master_key = base64.urlsafe_b64decode(master_key + "=" * (-len(master_key) % 4))
```

Also note `CryptoManager.__init__` has **no error handling** — a malformed key raises a bare `binascii.Error` that surfaces as an opaque traceback. Wrap it in a `CryptoError`.

---

#### M-3 — Token expiry detection never fires, so the auto-refresh path is dead code

**Files:** `asftool/core/sf_cli.py:429-444`, `asftool/core/auth/token_store.py:45-68`

`SFCLIManager._parse_auth_result` looks for `expirationDate` then `tokenExpiration`:

```python
for field in ("expirationDate", "tokenExpiration"):
    raw = data.get(field)
    ...
```

**Neither field exists** in the current SF CLI `org display --json` response. Verified against the live CLI:

```
all keys: ['accessToken', 'alias', 'apiVersion', 'connectedStatus', 'id', 'instanceUrl', 'username']
expirationDate present: False
tokenExpiration present: False
```

Consequently `expires_at` is always `None`, and `StoredToken.is_expired()` returns `False` for a `None` expiry by design (documented in the docstring at `token_store.py:50-54`).

**Reproduction:**

```
parsed expires_at = None
StoredToken.expires_at = None
is_expired() = False   <- always False when expires_at is None
```

**Impact.** The entire token-refresh machinery — `TokenStore.get_valid_token()`, the `sf_cli_manager.refresh_token()` call, the "Token expired (will auto-refresh on next use)" message in `auth status` — is **permanently unreachable**. The README's claim of "token auto-refresh" (line 7) and "ASFTool gets a fresh token on demand … whenever the stored one is expired" (line 72-73) is not true.

In practice the user is shielded by a second accident: `SFCLIAuthService.get_access_token()` re-logs-in whenever the stored token is missing or expired, and `sf org display` returns a *fresh* token each time. But the mechanism is not the one documented, and the "Token expired" status is never reported.

**Fix.** Either (a) parse the expiry from a source that actually provides it — e.g. `sf org display` does not, so query the org's session via `/services/oauth2/userinfo` or decode the token's lifetime from `sf org list`'s `instanceApiVersionLastRetrieved` — or (b) change the model to record the *observation* time and treat tokens as valid for a bounded window, and correct the README.

---

#### M-4 — `README.md` documents `ASFTOOL_`-prefixed environment variables that the code ignores

**Files:** `README.md:80-89` vs `asftool/core/config.py:34-57`

The README's configuration table says:

| Variable | Description | Default |
|---|---|---|
| `ASFTOOL_APP_NAME` | Application name | `asftool` |
| `ASFTOOL_LOG_LEVEL` | Log level | `INFO` |
| `ASFTOOL_SF_API_VERSION` | Salesforce API version | `v60.0` |
| `ASFTOOL_SF_DEFAULT_DOMAIN` | Login domain | `login.salesforce.com` |

But `Settings.model_config` sets **no `env_prefix`**, and every field carries an explicit alias:

```python
app_name: str = "asftool"                       # no alias -> reads APP_NAME
sf_api_version: str = Field(default="v68.0", alias="SF_API_VERSION")
```

**Reproduction:**

```
app_name   (README: ASFTOOL_APP_NAME): asftool          <- ignored
sf_api_ver (README: ASFTOOL_SF_API_VERSION): v68.0      <- ignored
log_level  (README: ASFTOOL_LOG_LEVEL): INFO            <- ignored
model_config env_prefix: (none)
```

**Impact.** Every `ASFTOOL_*` variable in the README is a no-op. A user who follows the documentation exactly to override the API version will silently get the default. Note the README also claims the default is `v60.0` while the code default is `v68.0` — a second, smaller drift in the same table.

**Fix.** Pick one convention and apply it everywhere. Either add `env_prefix="ASFTOOL_"` to `SettingsConfigDict` and drop the explicit aliases, or delete the `ASFTOOL_` prefix from the README and `.env.example`. Given that `ENCRYPTION_KEY`, `JWT_SECRET_KEY`, and `SF_API_VERSION` are already unprefixed, the lowest-risk fix is to **correct the README and `.env.example`** to match the code. Also fix the `v60.0` → `v68.0` default.

---

#### M-5 — `TokenStore.get_valid_token` swallows a failed refresh and returns `None`, causing a silent re-login

**File:** `asftool/core/auth/token_store.py:188-203`

```python
try:
    auth_result = await sf_cli_manager.refresh_token(alias)
    ...
    return new_token
except Exception as e:
    logger.error("token_refresh_failed", alias=alias, error=str(e))
    return None
```

A `None` return is indistinguishable from "no token stored at all". `SFCLIAuthService.get_access_token()` (`auth/sf_cli_auth.py:216-226`) then interprets it as "no token" and calls `self.login(alias=alias)`, which runs a **full browser-based `sf org login web`** flow.

**Impact.** On a headless server, a transient refresh failure (SF CLI temporarily unavailable, network blip) turns into an attempted **interactive browser login** — which will hang or fail in a non-TTY context. The user gets a browser window they did not ask for, or a confusing timeout, instead of "token refresh failed, run `asftool auth login`".

**Fix.** Distinguish "no token" from "refresh failed":

```python
class TokenRefreshError(OAuthError): ...
...
except Exception as e:
    raise TokenRefreshError(f"Failed to refresh token for alias '{alias}': {e}") from e
```

and let `get_access_token()` surface that distinctly, only falling back to interactive login when `auto_refresh` was explicitly requested *and* the failure is an auth failure.

---

#### M-6 — `asftool doctor` checks a hardcoded `"default"` alias regardless of the session

**File:** `asftool/cli/commands/doctor.py:89-90`

```python
auth_status = await session.auth_service.status("default")
```

`Session()` is constructed with the default alias and there is **no `--alias` option** on the `doctor` command, so the diagnostic always reports on the `"default"` alias even when the user is authenticated as, say, `mcp`. The result is a misleading "Not authenticated" line in the doctor table.

**Impact.** `asftool doctor` is the recommended first command for troubleshooting (`README.md:13`, `asftool doctor` at line 56). Reporting on the wrong alias makes it actively misleading for exactly the multi-org users the tool claims to support.

**Fix.** Add an `--alias` option to `doctor` and thread it through:

```python
@app.callback()
def _doctor_default(ctx, alias: str = typer.Option("default", "--alias", "-a"), verbose: bool = ...):
    ...
    run_diagnostics(verbose=verbose, alias=alias)
```

and use `session.auth_service.status(alias)` in `_collect_checks`.

---

#### M-7 — The `409`/`403` and `404` error responses lose the API's diagnostic message

**File:** `asftool/core/client.py:132-147`

```python
elif response.status_code == 403:
    raise SalesforceAPIError("Access forbidden", status_code=403)
elif response.status_code == 404:
    raise SalesforceNotFoundError("Resource not found")
```

The 4xx branch below (line 148+) carefully parses Salesforce's error envelope (`errorCode` / `message`), but the **403** and **404** branches short-circuit it and discard the body entirely. Salesforce's 403 often carries `INSUFFICIENT_ACCESS`, and the 404 for a Wave asset often carries a specific reason.

**Impact.** Every 403 surfaces as the generic string "Access forbidden" and every 404 as "Resource not found" — indistinguishable from each other in the CLI output. A user debugging a permission problem gets no information about *which* permission or *which* asset.

**Fix.** Route 403 and 404 through the same error-parsing path, keeping the specific exception type:

```python
elif response.status_code == 403:
    detail = self._parse_error_detail(response)   # shared helper
    raise SalesforceAPIError(f"Access forbidden: {detail}", status_code=403)
```

---

#### M-8 — `cleanup_old_downloads` crashes on a subdirectory and is not exposed anywhere

**File:** `asftool/core/storage.py:154-165`

```python
files = sorted(type_dir.glob("*"), key=lambda p: p.stat().st_mtime, reverse=True)
for old_file in files[keep_last:]:
    old_file.unlink()
    removed += 1
```

`glob("*")` returns directories as well as files, and `unlink()` on a directory raises `IsADirectoryError`.

**Reproduction:**

```
cleanup RAISED: IsADirectoryError [Errno 21] Is a directory: '.../myorg/datasets/subdir'
```

**Impact.** Two problems. (1) Any user who happens to have a subdirectory under a download folder gets an unhandled crash instead of a cleanup. (2) More importantly, **the method is never called** — `grep -rn "cleanup_old_downloads"` returns only the definition. It is untested, unreachable dead code in a public API, and it will crash the first time someone wires it up.

**Fix.** Filter to files and make it reachable:

```python
files = [p for p in type_dir.glob("*") if p.is_file()]
```

---

#### M-9 — `asftool` writes downloads into the current working directory, contradicting its own docs

**Files:** `asftool/core/storage.py:21-24` vs `core/services/dataset_service.py:157-159`, `core/services/dashboard_service.py:86-88`, `core/services/dataflow_service.py:118-120`

```python
@property
def base_dir(self) -> Path:
    """Base directory for all downloads (in current working directory)."""
    return Path.cwd() / "asftool_downloads"
```

The service docstrings all say the opposite:

```
If output_path is not provided, generates an organized path using
the storage manager: ~/.asftool/downloads/<alias>/datasets/...
```

**Impact.** Running `asftool datasets extract 0FbXXX` from a random directory litters that directory with an `asftool_downloads/` tree. This is surprising, pollutes whatever the user is working in, and can silently write to a read-only or synchronised location. It also means the tool's output location is not deterministic across invocation directories — a real problem for a tool whose output is meant to be consumed by other tools.

**Fix.** Use the documented `settings.config_dir` (`~/.asftool`) as the base, and update `base_dir` to match the docstrings. If CWD-relative output is intentional, fix the three docstrings and the README instead — but pick one.

---

### 🔵 LOW

---

#### L-1 — `core/logger.py` is dead code; the README's "structured JSON logging" claim is false

**File:** `asftool/core/logger.py` (whole module), `asftool/cli/main.py` (no call site)

`setup_logging()` is never called. Every module uses `structlog.get_logger(__name__)` at import time, which uses structlog's **default** configuration:

```
2026-10-21 02:19:51 [info     ] hello_world                    foo=bar
```

That is human-readable console output on **stdout** — not JSON, not stderr, and not written to `~/.asftool/asftool.log`. Two practical consequences:

1. **stdout pollution.** Log lines are interleaved with the CLI's own output. The repo has a branch named `fix/tui-stderr-log-pollution`, so this class of problem has already caused a real incident — and the fix was never merged into `master`.
2. **No persistent log.** `~/.asftool/asftool.log` is never created, yet `doctor` checks for its existence (`doctor.py:52-53`) and the README documents it.

**Fix.** Call `setup_logging()` from `cli/main.py` before dispatching, or delete the module and correct the README. If keeping it, configure `logger_factory=structlog.PrintLoggerFactory(file=sys.stderr)` and ensure the file handler is actually attached.

---

#### L-2 — `Prompt.ask` with a `default` that is `""` is accepted by `prompt_text` but rejected by `prompt_select`

**Files:** `asftool/cli/ui.py:202-205`, `asftool/cli/ui.py:183-186`

```python
def prompt_text(prompt_text: str, default: str | None = None) -> str:
    result = Prompt.ask(prompt_text, default=default)
    return result or ""
```

`prompt_text` is fine (it coerces `None`/`""` to `""`). `prompt_select` is the one with the defect — it is covered as **C-3** above, but the root cause is a shared inconsistency: `prompt_select` treats `default` as a *displayed value* while treating the input as an *index*. Two different contracts for one parameter name across sibling functions in the same module.

**Fix.** Unify: `prompt_select` should take the default as an index, and the caller should convert.

---

#### L-3 — `structlog` logs go to stdout, which corrupts machine-readable output

**File:** `asftool/core/logger.py:56`

```python
logger_factory=structlog.PrintLoggerFactory(),
```

`PrintLoggerFactory` defaults to `sys.stdout`. Any command whose output is meant to be piped (e.g. `asftool fields analyze --format json` writing to stdout) will have log lines injected into the same stream.

Note that `asftool fields analyze` writes its JSON report to a **file**, not stdout, so this is currently latent rather than active — but the `doctor config` and `config show` commands print structured output to stdout and would be affected once logging is enabled.

**Fix.** `structlog.PrintLoggerFactory(file=sys.stderr)`.

---

#### L-4 — Two `--output` flags on the same command share a short alias with a different option

**File:** `asftool/cli/commands/datasets.py:243-275`

```python
def extract_dataset(
    ...
    output: Path | None = typer.Option(None, "--output", "-o", ...),
    ...
):
def upload_dataset(
    ...
    name: str | None = typer.Option(None, "--name", "-n", ...),
    operation: Literal["Overwrite", "Append"] = typer.Option("Overwrite", "--operation", "-o", ...),
    ...
):
```

`--operation` claims `-o` in `upload`, while `--output` claims `-o` in `extract`. Inconsistent within the same command group. Not a crash, but a papercut for users who muscle-memory `-o`.

**Fix.** Give `--operation` a distinct short flag (e.g. `-op`) or drop it.

---

#### L-5 — `ruff` reports 7 errors on `master`; there is no CI workflow to catch them

**Files:** `tests/integration/test_lineage_cli.py:5`, `tests/unit/test_asset_graph.py:121`, `tests/unit/test_field_impact_service.py:11`, `tests/unit/test_field_matcher.py:3`, `tests/unit/test_lineage.py:3,4,6`

```
tests/integration/test_lineage_cli.py:5:21: F401 `pathlib.Path` imported but unused
tests/unit/test_asset_graph.py:121:9: F841 Local variable `graph` is assigned to but never used
tests/unit/test_field_impact_service.py:11:5: F401 `asftool.core.models.AssetType` imported but unused
tests/unit/test_field_matcher.py:3:8: F401 `pytest` imported but unused
tests/unit/test_lineage.py:3:8: F401 `json` imported but unused
tests/unit/test_lineage.py:4:21: F401 `pathlib.Path` imported but unused
tests/unit/test_lineage.py:6:8: F401 `pytest` imported but unused
```

The project declares `ruff` and `pre-commit` in the `dev` extra (`pyproject.toml:56-64`) but ships **no `.pre-commit-config.yaml`** and **no `.github/` directory**. `pytest` and `mypy` are documented in the README's Development section, but nothing enforces them.

**Impact.** The two failing tests (H-1) shipped to `master` undetected. This is the single highest-leverage *process* gap: every finding in this review is cheap to prevent with a CI gate.

**Fix.** Add `.github/workflows/ci.yml` running `ruff check`, `mypy asftool`, and `pytest` on push and PR. Add `.pre-commit-config.yaml` with the same three hooks.

---

#### L-6 — The package metadata declares the wrong licence

**File:** `pyproject.toml:7`, `pyproject.toml:15`

```toml
license = {text = "Apache License 2.0"}
...
classifiers = [
    ...
    "License :: OSI Approved :: GNU Affero General Public License v3 (AGPLv3)",
]
```

The `LICENSE` file is genuinely Apache 2.0, `license = {text = ...}` says Apache 2.0, and the commit history shows a deliberate migration to Apache 2.0 (`c5d1627 refactor(license,legacy): migrate to Apache 2.0`). **The classifier was missed.** It is the *only* AGPL reference left in the package metadata.

**Impact.** PyPI would render the package as AGPL-3.0 while shipping an Apache-2.0 LICENSE file — a real legal/compliance misstatement in the published metadata, and exactly the kind of thing a senior PM escalates immediately.

**Fix.** Change the classifier to `"License :: OSI Approved :: Apache Software License"`. Also switch to the modern PEP 639 form (`license = "Apache-2.0"` + `license-files = ["LICENSE"]`) when the minimum `setuptools` supports it.

---

#### L-7 — `pd.Timestamp.utcnow()` is deprecated and will break on the next pandas bump

**Files:** `asftool/core/services/dataset_service.py:265`, `asftool/core/services/dataset_service.py:480`

```python
job.completed_at = pd.Timestamp.utcnow().to_pydatetime()
```

Under pandas 3.0.5 (installed here) this emits `Pandas4Warning: Timestamp.utcnow is deprecated and will be removed in a future version. Use Timestamp.now('UTC') instead.` It still works today; it is a scheduled break.

The codebase also uses `datetime.utcnow()` in `core/auth/token_store.py:30,72` and `core/models/__init__.py:208,209,229,230,306` — deprecated since Python 3.12 and emitting `DeprecationWarning` in the current test run (25 warnings).

**Fix.** `pd.Timestamp.now("UTC").to_pydatetime()` and `datetime.now(UTC)`.

---

#### L-8 — `open()` without a context manager leaks a file handle during CSV upload

**Files:** `asftool/core/services/dataset_service.py:413`, `asftool/core/services/dataset_service.py:517`

```python
total_rows = sum(1 for _ in open(file_path, encoding="utf-8")) - 1  # Subtract header
```

The file object is never closed; it is only released when CPython's refcounting garbage-collects it. This is a `ResourceWarning` under `-W error` and a genuine fd leak under PyPy or any non-refcounting runtime.

**Fix.** `with open(file_path, encoding="utf-8") as f: total_rows = sum(1 for _ in f) - 1`.

---

## 4. Cross-Cutting Themes

These are the patterns behind the individual findings — worth fixing at the pattern level, not just the instance level.

### Theme A — "The tests assert the implementation's assumption, so the bug is invisible"

This is the single most important observation in this review. The test suite is **237/239 green** while three headline features are non-functional in production:

| Feature | Test says | Reality says |
|---|---|---|
| `auth list-orgs` | (no test) | Returns `[]` for 2 real orgs |
| `datasets list` | fixture uses `next_page_url` | API returns `nextPageUrl` → 1 page only |
| `jobs list` | fixture uses `dataflowjobs` | API returns `dataflowJobs` → ValidationError |

**Every one of these is a missing contract test against a real (or realistically-shaped) API payload.** The fix is not "add more tests" in the abstract — it is: *for every model that parses a Salesforce response, add a test that feeds the documented camelCase payload and asserts the parsed value.* A single shared `tests/fixtures/wave_responses.py` with verbatim payload shapes, checked into the repo, would have caught C-2 and H-4 at the moment they were written.

### Theme B — Documented behaviour that does not exist

The README and the code disagree in at least six places:

| README claim | Reality |
|---|---|
| "structured JSON logging" (line 15) | Human-readable stdout; `setup_logging()` never called (L-1) |
| "token auto-refresh" (line 7, 72) | `expires_at` always `None`; refresh path unreachable (M-3) |
| "encryption-key file fallback for headless containers" (line 69) | No fallback code exists (M-1) |
| `ASFTOOL_*` env vars (line 82-89) | Ignored (M-4) |
| "API version default `v60.0`" (line 86) | Actual default `v68.0` |
| downloads under `~/.asftool/downloads` | Downloads under `$CWD/asftool_downloads` (M-9) |

For a project whose entire value proposition is *"a trustworthy inventory of my Salesforce analytics assets"*, **the documentation being wrong about the tool's own behaviour is a first-order problem.** A senior PM would treat the README as a product surface.

### Theme C — Dead configuration and dead dependencies

`Settings` carries `jwt_secret_key`, `jwt_algorithm`, `access_token_expire_minutes`, `refresh_token_expire_days`, `sf_connected_app_*`, `sf_web_oauth_*`, `sf_device_flow_*` and the `has_*_credentials` properties. **None of them is used by any code path** — the only implemented auth is SF CLI. Meanwhile `authlib` and `python-jose` are declared dependencies with **zero imports**, and `JWT_SECRET_KEY` is a **required** field (H-1).

The `core/exceptions.py` module defines 16 exception classes; `FieldImpactError` has **zero** references, and `ConfigurationError`, `CryptoError`, `OAuthError`, `TokenExpiredError`, `TokenNotFoundError` exist only in their own module.

**Fix.** Delete the unimplemented settings, the two unused dependencies, and the unused exception classes. The `AGENTS.md` lesson #11 explicitly decided to *remove* adjustable API version logic — the same discipline (YAGNI) should be applied to the dead OAuth configuration.

### Theme D — Duplicated extraction logic has already drifted

`_extract_page_token` is implemented **three times** with two different behaviours:

| Location | Implementation | Behaviour |
|---|---|---|
| `core/services/dataset_service.py:576-580` | `urlparse` + `parse_qs` | URL-decodes the token |
| `core/services/dashboard_service.py:147-152` | `urlparse` + `parse_qs` | URL-decodes the token |
| `core/services/field_impact_service.py:700-711` | `str.split("pageToken=")` | Does **not** URL-decode |

A page token containing `+` or `%` will be mis-parsed by the third version. This is the classic symptom of copy-paste drift and should be consolidated into one `core/pagination.py` helper.

### Theme E — Broad exception handling masks root causes

`except Exception` appears in 20+ places, most commonly in the CLI wrappers:

```python
except Exception as e:
    print_error(f"List failed: {e}")
    raise typer.Exit(1) from e
```

This is *correct* for a top-level CLI boundary (fail with a message, not a traceback). But it is also used in places where it destroys diagnostic information:

- `auth/sf_cli_auth.py:322-325` — `list_orgs` swallows the parse failure and returns `[]` (C-1).
- `field_impact_service.py:480-484` — `list_dataflows` swallows the failure and returns `[]`, which the scan then reports as "0 dataflows scanned" rather than "the dataflow list could not be fetched".
- `lineage_service.py:32-36` — a failed dependency fetch is silently treated as a leaf node, so a **partial graph looks complete**.

The last one is the most dangerous: `asftool lineage generate` will happily produce a diagram that is missing nodes, with no indication that anything failed.

---

## 5. What's Good — Keep This

The review above is heavily negative because bugs dominate the report, but several decisions are genuinely good and should be preserved through the refactor:

1. **`core/` never imports from `cli/`** — the layering is real and should stay.
2. **The `*_async` + thin Typer shim pattern** — it eliminated a whole class of "works in CLI, broken in menu" bugs, and it is applied consistently.
3. **Keyring-backed, encrypted token storage** — the *design* is right (the failure in M-1 is about the *absence* of a backend, not the design).
4. **Pydantic models with `extra="allow"`** — tolerant parsing of unknown fields is correct for a fast-moving platform API; the problem is the *missing* aliases, not the tolerance.
5. **`mypy` clean with `warn_return_any`** — a real quality bar, already met.
6. **Atomic config writes** (`config_store.py:98-115`) via `tempfile` + `os.replace` — correct, and worth keeping.
7. **`ASFToolError` as a base exception** with a domain hierarchy — good structure, just under-used (see Theme C).

---

## 6. Recommended Sequencing

The full task-level plan is in **`DEVELOPMENT_PLAN.md`**. The short version:

| Phase | Theme | Findings addressed |
|---|---|---|
| **0** | Stop the bleeding: CI, fix red tests, pre-commit | L-5, H-1 (test part) |
| **1** | Contract correctness: Salesforce response models | C-2, H-4 |
| **2** | Auth & SF CLI correctness | C-1, H-2, M-1, M-2, M-3, M-5 |
| **3** | Interactive UX: no crash, no hang | C-3, H-1 (runtime), H-6, M-6 |
| **4** | Resilience: rate limits, concurrency, retries | H-3, H-5 |
| **5** | Lineage & menu correctness | H-6, M-7, M-8 |
| **6** | Housekeeping: dead code, docs, packaging | L-1..L-8, M-4, M-9, Theme C, Theme D |

Phases 0–2 are **release blockers**. Phase 3 is required before the interactive menu can be called usable. Phases 4–6 are quality work that should follow once the product is correct.

---

## 7. [RISK ASSESSMENT]

**Overall risk: HIGH.**

| Dimension | Assessment |
|---|---|
| **Correctness** | 🔴 HIGH — three headline features are non-functional against a live org. |
| **Security** | 🟠 MEDIUM — no plaintext secrets, no injection vectors found. But `SF_API_VERSION` env leakage breaks auth, and the keyring-absent path crashes rather than degrading safely. |
| **Data integrity** | 🟠 MEDIUM — silent first-page truncation is a *confidently wrong* answer, the worst failure mode for an inventory tool. |
| **Availability** | 🟠 MEDIUM — unbounded fan-out + ignored `Retry-After` can trip a Salesforce API lockout. |
| **Operability** | 🟠 MEDIUM — no CI, red tests on `master`, dead logging layer, no persistent log. |
| **Maintainability** | 🟡 MEDIUM — clean layering, but duplicated pagination logic has already drifted, and a large dead-configuration surface implies a roadmap that was abandoned, not finished. |

**Human validation required before merge.** Specifically:

1. **Live-org verification** of every contract fix (C-1, C-2, H-4) against a real sandbox org. These bugs were invisible to the test suite precisely because the tests did not use real payloads; a fix validated only against a mock would repeat the mistake. **This cannot be automated.**
2. **A security review of the SF CLI subprocess environment handling** (H-2) once the `env=` filter lands — the change touches how credentials-adjacent processes are spawned.
3. **Product-owner sign-off on the README corrections** (Theme B), because the README is a user-facing contract and the corrections change documented behaviour.

**Verdict: REQUEST CHANGES.** `master` is not mergeable as the release branch. The repository is close — the architecture is right, the type discipline is right, and most of the findings are localised, well-understood corrections. But shipping it would mean shipping a tool that reports "no authorized orgs" to a user with two, and reports 50 datasets to a user with 500.

**One key architectural insight:** *this codebase does not have a testing problem, it has a **contract** problem.* Every Critical and High finding is the same root cause in a different costume — a payload shape, an env-var name, or a response key that was written down in one place and assumed in another, with the assumption encoded into a test that then "verifies" the wrong thing. The single highest-leverage investment is not more unit tests; it is a **checked-in fixture module of verbatim Salesforce API payloads** (one per endpoint), used both as test input and as the basis for the Pydantic models. That one artefact would have prevented C-2, H-4, and the class of bug behind M-7 — and it makes the next platform change a one-file diff instead of an archaeology exercise.

---

> **Improve this review?** If feedback seems incorrect or irrelevant, update the repository's `.agents/skills/custom-codereview-guide.md` (with the `/codereview` trigger), then re-request review. The reviewer reads the guide from the PR head.
>
> **Resolve with AI?** Install the [iterate skill](https://github.com/OpenHands/extensions/tree/main/skills/iterate) and run `/iterate`.
>
> Was this review helpful? React with 👍 or 👎.
