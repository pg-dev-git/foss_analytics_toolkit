# AGENTS.md — Agent Integration Guide

## SDK Usage
```python
from asftool.sdk.engine import ASFToolSDK
async with ASFToolSDK() as sdk:
    results = await sdk.datasets.list()
```

## MCP Setup
Run `asftool mcp start` to expose stdio transport.

## Claude Desktop Config
Add `asftool` to `claude_desktop_config.json` with `mcp start` command.

---

## Critical Lessons Learned & Gotchas

### 1. SF CLI API Version Format Mismatch
**Problem**: SF CLI `sf org list --json --all` returns `instanceApiVersion` as `"67.0"` but Pydantic Settings validator expects `"v67.0"` format (must match regex `^v\d+\.\d+$`).

**Solution**: Always prepend `v` prefix when extracting API version from SF CLI:
```python
version = org.get("instanceApiVersion")  # Returns "67.0"
if version and not version.startswith("v"):
    version = f"v{version}"  # Returns "v67.0"
```

**Where**: `asftool/core/sf_cli.py` → `get_org_api_version()` method

---

### 2. SF_API_VERSION Environment Variable Pollutes SF CLI Subprocesses
**Problem**: When `Settings` loads, it reads `SF_API_VERSION` from `.env` and sets it in `os.environ`. This gets inherited by SF CLI subprocesses, changing their behavior (causes "The requested resource does not exist" errors).

**Solution**: Filter `SF_API_VERSION` from environment passed to SF CLI subprocesses:
```python
# In _run_command_async and _run_command_sync
env = {k: v for k, v in os.environ.items() if k != "SF_API_VERSION"}
process = await asyncio.create_subprocess_exec(..., env=env)
```

**Where**: `asftool/core/sf_cli.py` → `_run_command_async()` and `_run_command_sync()`

---

### 3. Typer Option Empty String vs None
**Problem**: Typer passes empty string `""` for optional `Path` options when not provided, not `None`. This causes pandas `to_csv()` to fail with `TypeError: argument of type 'NoneType' is not a container`.

**Solution**: Normalize empty string to `None` in command handlers:
```python
@app.command("extract")
def extract_dataset(
    output: Path | None = typer.Option(None, "--output", "-o"),
):
    if output == "":  # Typer passes empty string when not provided
        output = None
```

**Where**: `asftool/cli/commands/datasets.py` → `extract_dataset()` command

---

### 4. SF CLI Org List Output Structure
**Problem**: `sf org list --json --all` returns orgs in multiple arrays: `other`, `nonScratchOrgs`, `devHubs`, `scratchOrgs` — NOT a single `orgs` array.

**Solution**: Combine all arrays when parsing:
```python
orgs = (result_data.get("other", []) + 
        result_data.get("nonScratchOrgs", []) + 
        result_data.get("devHubs", []) + 
        result_data.get("scratchOrgs", []))
```

**Where**: `asftool/core/sf_cli.py` → `list_orgs()`, `is_org_authenticated_async()`, `get_org_api_version()`

---

### 5. Pydantic Field Alias for Case Mismatch
**Problem**: Salesforce API returns `dataflowJobs` (camelCase) but Python model used `dataflowjobs` (snake_case), causing validation error.

**Solution**: Use Pydantic `Field` with `alias` parameter:
```python
class DataflowJobListResponse(BaseModel):
    dataflowjobs: list[DataflowJob] = Field(..., alias="dataflowJobs")
    model_config = ConfigDict(populate_by_name=True)
```

**Where**: `asftool/core/models/__init__.py` → `DataflowJobListResponse`

---

### 6. SF CLI Token Expiration Handling
**Problem**: SF CLI tokens expire. Direct API calls with expired tokens return 401 `INVALID_SESSION_ID`.

**Solution**: 
- Store token with `api_version` in keyring for reuse
- `Session.get_client()` reads stored `api_version` from token and creates client with correct API version
- Auto-refresh via `token_store.get_valid_token()` calls `sf_cli.refresh_token()`

**Where**: 
- `asftool/core/auth/token_store.py` → `StoredToken.api_version` field
- `asftool/cli/session.py` → `Session.get_client()`

---

### 7. SF CLI Subprocess Environment on Windows
**Problem**: Windows SF CLI is a `.cmd` file requiring shell execution. Missing `env=os.environ` causes missing `HOME`, `USERPROFILE` needed for SF CLI config lookup.

**Solution**: Pass full environment to both sync and async subprocess calls:
```python
# Windows async
process = await asyncio.create_subprocess_shell(shell_cmd, env=os.environ, ...)

# Windows sync  
result = subprocess.run(shell_cmd, shell=True, env=os.environ, ...)

# Unix async
process = await asyncio.create_subprocess_exec(..., env=os.environ)

# Unix sync
result = subprocess.run(cmd, env=os.environ, ...)
```

**Where**: `asftool/core/sf_cli.py` → `_run_command_async()` and `_run_command_sync()`

---

### 8. Missing `sys` Import in Module
**Problem**: `_run_command_sync` used `sys.platform` but `sys` was only imported inside `_run_command_async`.

**Solution**: Move `import sys` to module top level.

**Where**: `asftool/core/sf_cli.py` → top of file

---

### 9. SF CLI Authentication State
**Key Insight**: SF CLI maintains its own auth state in `~/.sfdx/`. The tool should:
1. Check if SF CLI has valid session (`sf org list --json --all`)
2. Import existing session (`asftool auth import-sf --alias mcp`) to keyring
3. Use imported token for API calls (avoids browser login)

**Commands**:
```bash
# Check auth
asftool auth check-auth --alias mcp

# Import existing SF CLI session
asftool auth import-sf --alias mcp

# Force new login if needed
asftool auth login --force --alias mcp
```

---

### 10. Interactive Mode Dependencies
**Gotcha**: Interactive mode (`asftool` without args) requires `questionary` package for prompts. It's imported in `asftool/cli/menus/lineage.py` at module level.

**Fix**: Add `questionary>=2.0.0` to `pyproject.toml` dependencies.

---

### 11. Removed Adjustable API Version Logic
**Decision**: Removed complex API version selection (`--api-version`, `set-api-version`, `SF_API_VERSION` env var). 

**Rationale**: 
- Org's API version is fixed and known via `sf org list --json --all` (`instanceApiVersion`)
- Configurable version caused confusion and SF CLI subprocess pollution
- Now derived automatically from SF CLI org info at import time

**Removed**:
- `--api-version` flag from `asftool fields analyze`
- `_apply_api_version_override()` function
- `set-api-version` config command
- `SF_API_VERSION` env var usage (except for manual override if needed)

---

## Debugging Checklist

When commands fail with "Resource not found" or "No authenticated session":
1. Verify SF CLI has valid session: `sf org display -o <alias>`
2. Check token in keyring: `asftool auth status --alias <alias>`
3. Re-import session: `asftool auth import-sf --alias <alias>`
4. Force re-login: `asftool auth login --force --alias <alias>`

**Key files to check**:
- `asftool/core/sf_cli.py` - subprocess env filtering, org list parsing
- `asftool/core/auth/token_store.py` - token structure
- `asftool/cli/session.py` - client creation with API version
- `asftool/core/auth/sf_cli_auth.py` - import/login flow
