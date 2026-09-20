# AGENTS.md — Agent Integration Guide

---

## Git & GitHub Workflow (MANDATORY)

### Branching Strategy
- **Never work directly on `main` or `master`**
- Create a new feature branch for each task/feature: `git checkout -b feat/descriptive-name`
- Use conventional branch prefixes:
  - `feat/` — new features
  - `fix/` — bug fixes
  - `refactor/` — code restructuring
  - `docs/` — documentation updates
  - `chore/` — maintenance, dependencies, tooling

### Commit Standards
- **Clean, atomic commits** — one logical change per commit
- **Conventional commit messages**:
  ```
  type(scope): brief description

  Longer explanation if needed.

  Fixes #issue-number
  ```
  Types: `feat`, `fix`, `refactor`, `docs`, `chore`, `test`, `perf`
- Include context in commit body (why, not just what)

### Push & Pull Request Workflow
1. **Push feature branch** to origin: `git push -u origin feat/branch-name`
2. **Create Pull Request** against `main` (or default branch)
3. **PR Requirements**:
   - Clear title and description
   - Link related issues (`Fixes #123`, `Closes #456`)
   - Pass all CI checks (tests, lint, type-check)
   - At least 1 review approval (if team policy)
4. **Merge via GitHub UI** — squash and merge preferred for clean history
5. **Delete branch** after merge (local & remote)

### Git Hygiene
- `git pull --rebase origin main` before starting new work
- Keep branches short-lived (< 1 week ideal)
- Squash fixup commits before PR (`git rebase -i`)
- Never force-push to shared branches
- Use `git commit --amend` for local fixes before push

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

### 12. Git Sync/Revert Implementation Lessons

#### 12.1 SF_DEFAULT_DOMAIN Format in .env
**Problem**: `.env` contained `SF_DEFAULT_DOMAIN=https://claritev6-dev-ed.develop.my.salesforce.com` but code prepends `https://` when building `sf_base_url`, resulting in double `https://`.

**Solution**: Store only the domain in `.env`:
```
# Wrong - causes double https://
SF_DEFAULT_DOMAIN=https://claritev6-dev-ed.develop.my.salesforce.com

# Correct - just the domain
SF_DEFAULT_DOMAIN=claritev6-dev-ed.develop.my.salesforce.com
```

**Where**: `.env` file and `asftool/core/config.py` → `sf_base_url` property

---

#### 12.2 Salesforce Wave API Response Key Inconsistency
**Problem**: Wave API uses different response keys for different asset types instead of a uniform "records" key:
- `/dashboards` → `"dashboards"`
- `/datasets` → `"datasets"`
- `/recipes` → `"recipes"`
- `/dataflows` → `"dataflows"`
- `/lenses` → `"lenses"`

**Solution**: Use an `asset_key_map` in `_fetch_all_assets()`:
```python
asset_key_map = {
    "dashboard": "dashboards",
    "dataset": "datasets",
    "recipe": "recipes",
    "dataflow": "dataflows",
    "lens": "lenses",
}
asset_key = asset_key_map.get(asset_type, "records")
```

**Where**: `asftool/core/git/sync_service.py` → `_fetch_all_assets()`

---

#### 12.3 XMD Endpoint Doesn't Exist
**Problem**: Tried to sync "xmd" asset type via `/xmds` endpoint which doesn't exist. XMDs are accessed per dataset via `/datasets/{id}/versions/{version_id}/xmds/main`.

**Solution**: Remove "xmd" from `ASSET_TYPES` dict and handle XMDs separately per dataset if needed.

**Where**: `asftool/core/git/sync_service.py` → `ASSET_TYPES` dict

---

#### 12.4 Dry-Run Should Not Clone Repositories
**Problem**: `dry_run()` was calling `ensure_workspace()` which clones/initializes repos, even though dry-run should be read-only.

**Solution**: Use `get_workspace_path()` instead of `ensure_workspace()` in `dry_run()` method to only check local files without cloning.

**Where**: `asftool/core/git/sync_service.py` → `dry_run()` method

---

#### 12.5 Missing Remote Repo Handling
**Problem**: Sync fails when remote GitHub repo doesn't exist (e.g., "Repository not found").

**Solution**: In `ensure_workspace()`, catch clone failure and initialize local repo with `git init`, then add remote for future push:
```python
try:
    self._clone_repository(url, path, branch)
except RuntimeError:
    # Initialize local repo
    subprocess.run(["git", "init", str(path)])
    self._checkout_branch(path, branch)
    # Add remote for future push
    subprocess.run(["git", "-C", str(path), "remote", "add", "origin", url])
```

**Where**: `asftool/core/git/workspace.py` → `ensure_workspace()`

---

#### 12.6 Revert Asset Matching Requires Flexible Identifier Resolution
**Problem**: Filenames in Git don't include CRMA IDs (e.g., `dashboard/DTC Sales.json`), but the asset JSON has `label: "DTC Sales"`, `name: "DTC_Sales_SAMPLE"`, `id: "0FKak0000018zeCGAQ"`. Need to match by various identifiers.

**Solution**: Try multiple matching strategies in revert:
- `developerName`, `name`, `label`, `id` exact match
- Common variations: `label.replace(" ", "_")`, `name.replace("_SAMPLE", "")`

**Where**: `asftool/core/git/sync_service.py` → `revert_asset()` method

---

#### 12.7 Bundle Deployment Uses PATCH on Asset Endpoint
**Problem**: The `/wave/{assetType}/{id}/bundle` endpoint doesn't exist for all asset types. The correct approach is PATCH on the asset endpoint (e.g., `/wave/dashboards/{id}`) with only editable fields.

**Solution**: 
- Use PATCH on asset endpoint: `/{asset_endpoint}/{crma_id}`
- Only send editable fields: `label`, `mobileDisabled`, `description`
- Strip read-only fields from payload (`id`, `createdBy`, `createdDate`, `type`, `visibility`, etc.)

**Where**: `asftool/core/git/sync_service.py` → `_deploy_bundle()`

---

#### 12.8 Dulwich Auth Requires Username/Password Tuple
**Problem**: Dulwich's `fetch()`, `pull()`, `push()` don't accept `auth` callback directly. They need `username` and `password` parameters.

**Solution**: Extract credentials from auth handler and pass explicitly:
```python
auth = self._get_auth_for_remote(remote)
if auth:
    username, password = auth(None, None)
    result = dulwich.porcelain.pull(self.repo, remote, branch=branch, username=username, password=password)
```

**Where**: `asftool/core/git/engine.py` → `fetch()`, `pull()`, `push()` methods

---

#### 12.9 Preserve 'id' Field in Normalizer for Deployment
**Problem**: Normalizer was stripping the `id` field (Salesforce ID) which is needed for deployment endpoints.

**Solution**: Add `"id"` to `preserve_fields` in `CRMANormalizerConfig`:
```python
preserve_fields: list[str] = Field(
    default_factory=lambda: [
        "id",  # Keep for deployment
        "developerName",
        "label",
        "name",
        ...
    ],
)
```

**Where**: `asftool/core/git/normalizer.py` → `CRMANormalizerConfig.preserve_fields`

---

#### 12.10 Git Credentials Stored in OS Keyring, Not Config File
**Problem**: The `.asftool-git.yml` config only references a `credentials_alias` — actual credentials are stored in OS keyring.

**Solution**: Use `asftool git-auth login --alias <alias> --provider github --host github.com --username <user> --token <pat> --auth-type pat --non-interactive` to store credentials, or set env vars `ASFTOOL_GIT_TOKEN` and `ASFTOOL_GIT_USERNAME`.

**Where**: `asftool/core/auth/git_auth.py` → `GitAuthService`

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
