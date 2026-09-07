# Configurable Salesforce API Version — Phased Plan

**Branch**: `feature/asftool-refactor`
**Goal**: Make the Salesforce API version configurable. Default to the latest stable (currently `v68.0`). Allow override via env var, per-command flag, or interactive menu. Persist user choice so it survives sessions.

---

## Why this matters

`asftool/core/config.py` currently has a hardcoded default:
```python
sf_api_version: str = Field(default="v60.0", alias="SF_API_VERSION")
```

`v60.0` is from mid-2024. Salesforce ships ~3 versions per year; the latest stable as of late 2026 is `v68.0`. v60 is still supported but missing newer fields/endpoints. New users shouldn't have to dig into `.env` to upgrade.

---

## Architecture constraints (unchanged)

- **Async-first**: `httpx.AsyncClient` for any new "list versions" call.
- **Pydantic models**: extend `Settings` with the new persistence layer type-safe.
- **Resilience**: `tenacity` retry on the new Salesforce metadata call.
- **Layered**: new `core/config_store.py` (config persistence) → `core/config.py` (load) → `cli/` (set/show).

---

## Phase 1: Bump default + version validation (5 min)

### Sub-tasks

#### 1.1 Update default in `core/config.py`
```python
# Bump default; user can still override via SF_API_VERSION env var or config_store.
sf_api_version: str = Field(default="v68.0", alias="SF_API_VERSION")
```

#### 1.2 Add version format validator
```python
@field_validator("sf_api_version")
@classmethod
def validate_sf_api_version(cls, v: str) -> str:
    """Ensure format is v<major>.<minor> with sane bounds."""
    import re
    if not re.match(r"^v\d+\.\d+$", v):
        raise ValueError(f"Invalid API version {v!r}: must match v<major>.<minor>")
    return v
```

#### 1.3 Update `.env.example`
```diff
-SF_API_VERSION=v60.0
+SF_API_VERSION=v68.0
```

### Acceptance
- [ ] Default is `v68.0` out of the box
- [ ] Invalid format (e.g. `60.0`, `v68`, `latest`) raises a clear error
- [ ] Env var override still works
- [ ] Existing tests still pass (no behavior change for those who set `SF_API_VERSION`)

---

## Phase 2: Persistent config store + CLI command (medium)

The user's "interactive mode configuration" implies persistence across sessions. A small JSON file at `~/.asftool/config.json` is the right primitive — same dir as keyring, won't conflict with `.env`, and survives across `asftool` invocations.

### Sub-tasks

#### 2.1 Create `asftool/core/config_store.py` (NEW FILE)
- Pydantic model `UserConfig` with the small set of overridable settings:
  ```python
  class UserConfig(BaseModel):
      sf_api_version: str | None = None
  ```
- `ConfigStore` class with `load() -> UserConfig`, `save(cfg: UserConfig)`, `update(**kwargs)`.
- Default path: `~/.asftool/config.json`.
- Atomic writes (`write to .tmp`, then `rename`).
- Graceful handling of missing file / corrupted JSON → returns empty config.

#### 2.2 Extend `Settings` to merge in `UserConfig`
Order of precedence (highest first):
1. Env var (`SF_API_VERSION`)
2. CLI flag (`--api-version`)
3. `UserConfig` from disk
4. Hardcoded default

Implementation: `get_settings()` helper stays; add `get_user_config()` and merge logic. Don't break the existing `Settings()` constructor — pydantic-settings already supports env vars.

```python
def get_settings() -> Settings:
    base = Settings()
    user = get_user_config()
    if user.sf_api_version and "SF_API_VERSION" not in os.environ:
        base.sf_api_version = user.sf_api_version
    return base
```

#### 2.3 Add `asftool config` Typer command group (NEW FILE `cli/commands/config.py`)
```
asftool config show                      # show all settings
asftool config set-api-version v68.0     # persist
asftool config reset                     # delete ~/.asftool/config.json
```

#### 2.4 Add `--api-version` to all top-level commands (per-command override)
For now, just the analyze and list commands. Pattern: Typer Option that overrides the Settings at runtime.
```python
@app.command()
def analyze(
    search_term: str = ...,
    api_version: str | None = typer.Option(None, "--api-version", help="Override API version"),
    ...
):
    if api_version:
        os.environ["SF_API_VERSION"] = api_version
    _run(analyze_field_async(...))
```

(Ponytail: don't add this flag to every command; just to the ones that hit the API. The `--api-version` on `asftool fields analyze` and `asftool datasets list` is enough; the rest inherit via Settings.)

### Acceptance
- [ ] `asftool config show` prints current effective settings
- [ ] `asftool config set-api-version v68.0` persists to `~/.asftool/config.json`
- [ ] Subsequent invocations use the persisted value (env var overrides it)
- [ ] `asftool fields analyze --api-version v67.0 OpportunityID` uses v67 just for that call
- [ ] Corrupted `config.json` doesn't crash the app; it's replaced with empty config
- [ ] Tests pass; mypy + ruff clean

---

## Phase 3: Interactive menu + version discovery (polish)

### Sub-tasks

#### 3.1 New `Settings` submenu in the interactive menu
- "Configuration" or "Settings" — placed under main menu (key `6`, push Doctor to `7`, push Field Impact to `8` — or keep current order and insert as `9`).
- Items:
  - `1. Show current configuration`
  - `2. Set API version (from list)`
  - `3. Set API version (manual entry)`
  - `4. Reset to defaults`
  - `b. Back`

#### 3.2 Add `SalesforceClient.list_available_api_versions()` (NEW METHOD)
```python
async def list_available_api_versions(self) -> list[str]:
    """Call GET /services/data/ to list available Salesforce API versions.
    Returns the version strings, e.g. ['v60.0', 'v61.0', ..., 'v68.0'].
    """
    # Note: base URL is /services/data/ with NO version suffix.
    url = f"{self.instance_url}/services/data/"
    response = await self.get(url)
    data = response.json()
    return [v["version"] for v in data if "version" in v]
```

Add a test for this in `tests/unit/test_client_field_impact.py` (or a new file `test_client_versions.py`).

#### 3.3 Interactive prompt uses the discovered list
The menu presents a numbered list of available versions. User picks one. Validate the choice against the discovered list before persisting.

#### 3.4 Confirm + persist
Standard prompt_confirm + save flow (same as dataset extract's path confirmation).

### Acceptance
- [ ] `asftool config set-api-version` (interactive) shows a numbered list of versions discovered from the org
- [ ] Manual entry falls back to the same prompt (for users who know the version offhand)
- [ ] Invalid version (not in the list) is rejected with a clear message
- [ ] All 160+ existing tests still pass

---

## Cross-cutting

### Logging
Use `structlog` for the config save/load events. Include `key` and `old/new` values in the log.

### Error handling
- Invalid `config.json` → log warning, return empty config
- Disk write failure → `raise ConfigError`
- Salesforce `/services/data/` call failure → fall back to a hardcoded list `["v60.0", ..., "v68.0"]` (so the menu still works offline)

### Tests
- `tests/unit/test_config_store.py`: round-trip save/load, corrupted file handling, atomic write
- `tests/unit/test_config.py`: Settings merge precedence (env > CLI > file > default)
- `tests/unit/test_client_field_impact.py`: extend with `list_available_api_versions` test
- `tests/integration/test_config_cli.py`: Typer CLI tests for `asftool config show/set/reset`

---

## File summary

| Phase | File | Action |
|-------|------|--------|
| 1 | `asftool/core/config.py` | Edit (default + validator) |
| 1 | `.env.example` | Edit (default value) |
| 2 | `asftool/core/config_store.py` | NEW (UserConfig + ConfigStore) |
| 2 | `asftool/cli/commands/config.py` | NEW (Typer command group) |
| 2 | `asftool/cli/main.py` | Edit (register `config` sub-app) |
| 2 | `tests/unit/test_config_store.py` | NEW |
| 2 | `tests/unit/test_config.py` | NEW (Settings merge) |
| 3 | `asftool/core/client.py` | Edit (add `list_available_api_versions`) |
| 3 | `asftool/cli/menus/config.py` | NEW (interactive menu) |
| 3 | `asftool/cli/menu.py` | Edit (wire config submenu) |
| 3 | `tests/integration/test_config_cli.py` | NEW |

---

## Out of scope (deliberate)

- Auto-bumping the default to whatever Salesforce says is latest (would require a build-time or startup-time network call). We hardcode the default and bump it when Salesforce releases a new version.
- Persisting every Setting (only `sf_api_version` for now; add more if the user asks).
- Per-org API version override (single global setting is enough; users can use env var per shell session).

---

## Next step

Awaiting approval to begin Phase 1.
