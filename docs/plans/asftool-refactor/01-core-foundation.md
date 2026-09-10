# Phase 1: Core Foundation — Rename Package & Config

**Goal:** Rename `tcrm_toolkit` → `asftool`, update pyproject.toml, config, logging paths.

---

## Prerequisites

- Phase 0 complete (TUI deleted, broken OAuth removed)
- On `feature/asftool-refactor` branch

---

## Step 1.1: Rename Package Directory

```bash
mv tcrm_toolkit asftool
```

---

## Step 1.2: Update All Internal Imports

Find and replace all `tcrm_toolkit` → `asftool` imports:

```bash
# Use a tool like sed or just do it manually in your editor
# Files to update:
# - asftool/cli/*.py
# - asftool/cli/commands/*.py
# - asftool/core/*.py
# - asftool/core/**/*.py
# - tests/**/*.py
# - scripts/*.py
```

---

## Step 1.3: Update `pyproject.toml`

```toml
[project]
name = "asftool"
version = "0.1.0"
description = "FOSS Analytics Tool for Salesforce TCRM — Async Python CLI"
readme = "README.md"
requires-python = ">=3.11"
license = {text = "GNU Affero General Public License v3.0"}
authors = [
    {name = "Pedro Gagliardi", email = "pg-dev-git@users.noreply.github.com"}
]
keywords = ["salesforce", "tcrm", "analytics", "tableau", "cli", "async", "foss"]
classifiers = [
    "Development Status :: 3 - Alpha",
    "Intended Audience :: Developers",
    "License :: OSI Approved :: GNU Affero General Public License v3 (AGPLv3)",
    "Programming Language :: Python :: 3.11",
    "Programming Language :: Python :: 3.12",
    "Topic :: Software Development :: Libraries",
    "Topic :: Office/Business :: Financial :: Spreadsheet",
]

dependencies = [
    "typer[all]>=0.12.0",
    "rich>=13.7.0",
    "httpx>=0.27.0",
    "pydantic>=2.7.0",
    "pydantic-settings>=2.3.0",
    "tenacity>=8.2.0",
    "cryptography>=42.0.0",
    "keyring>=24.3.0",
    "structlog>=24.1.0",
    "pandas>=2.2.0",
    "python-dotenv>=1.0.0",
    "authlib>=1.3.0",
    "python-jose[cryptography]>=3.3.0",
    "questionary>=2.0.0",  # NEW: for menus
]

[project.scripts]
asftool = "asftool.cli.main:app"

[project.optional-dependencies]
dev = [
    "pytest>=8.2.0",
    "pytest-asyncio>=0.23.0",
    "pytest-cov>=4.1.0",
    "pytest-mock>=3.12.0",
    "mypy>=1.10.0",
    "ruff>=0.5.0",
    "pre-commit>=3.7.0",
]

[project.urls]
Homepage = "https://github.com/pg-dev-git/foss_analytics_toolkit"
Repository = "https://github.com/pg-dev-git/foss_analytics_toolkit"
Issues = "https://github.com/pg-dev-git/foss_analytics_toolkit/issues"

[build-system]
requires = ["setuptools>=68.0", "wheel"]
build-backend = "setuptools.build_meta"

[tool.setuptools.packages.find]
where = ["."]
include = ["asftool*"]

[tool.pytest.ini_options]
asyncio_mode = "auto"
testpaths = ["tests"]
python_files = ["test_*.py"]
python_classes = ["Test*"]
python_functions = ["test_*"]
addopts = "-v --strict-markers --tb=short"

[tool.mypy]
python_version = "3.11"
warn_return_any = true
warn_unused_configs = true
disallow_untyped_defs = false
disallow_incomplete_defs = false
check_untyped_defs = true
no_implicit_optional = true
strict_optional = true
show_error_codes = true
pretty = true
explicit_package_bases = true
ignore_missing_imports = true

[tool.ruff]
target-version = "py311"
line-length = 100
exclude = ["_legacy", ".venv", "build", "dist"]
select = [
    "E", "W", "F", "I", "B", "C4", "UP", "T20",
]
ignore = ["E501", "B008"]

[tool.ruff.per-file-ignores]
"tests/*" = ["S101", "S106"]

[tool.ruff.format]
quote-style = "double"
indent-style = "space"
skip-magic-trailing-comma = false

[tool.coverage.run]
source = ["asftool"]
omit = ["tests/*", "*/__pycache__/*"]

[tool.coverage.report]
exclude_lines = [
    "pragma: no cover",
    "def __repr__",
    "raise AssertionError",
    "raise NotImplementedError",
    "if __name__ == .__main__.:",
]
```

---

## Step 1.4: Update Config (`core/config.py`)

```python
# Key changes:
# - env_prefix: "ASFTOOL_" (was TCRM_)
# - config_dir: "~/.asftool/" (was ~/.tcrm/)
# - log_file: "~/.asftool/asftool.log"
# - app_name: "asftool"

# Example:
class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="ASFTOOL_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )
    
    app_name: str = "asftool"
    app_version: str = "0.1.0"
    
    @property
    def config_dir(self) -> Path:
        return Path.home() / ".asftool"
    
    @property
    def log_file(self) -> Path:
        return self.config_dir / "asftool.log"
```

---

## Step 1.5: Update Logger (`core/logger.py`)

```python
# Ensure log directory exists
# Log to both stderr (JSON) and ~/.asftool/asftool.log
# Use structlog with JSON renderer
```

---

## Step 1.5: Update Entry Point in `cli/main.py`

```python
# Change: from tcrm_toolkit.cli.main import app
# To:     from asftool.cli.main import app

# Or just ensure the module path matches
```

---

## Step 1.6: Update `scripts/seed_session.py`

```python
# Update imports from tcrm_toolkit → asftool
# Update env var references (TCRM_ → ASFTOOL_)
```

---

## Step 1.7: Update `scripts/verify-cross-platform.py`

```python
# Update import paths
# Update expected CLI command name (tcrm → asftool)
```

---

## Step 1.8: Update Tests

```bash
# Fix all test imports
find tests -name "*.py" -exec sed -i 's/tcrm_toolkit/asftool/g' {} \;
```

---

## Step 1.9: Run Verification

```bash
# Reinstall package
uv sync --extra dev

# Run tests
uv run pytest -v --tb=short

# Test CLI
asftool --help
asftool doctor
```

---

## Acceptance Criteria

- [ ] Package renamed: `tcrm_toolkit` → `asftool`
- [ ] All imports updated
- [ ] `pyproject.toml` has correct name, entry point, deps (questionary added, textual removed)
- [ ] Config uses `ASFTOOL_` prefix, `~/.asftool/` directory
- [ ] Logger writes to `~/.asftool/asftool.log`
- [ ] `asftool --help` works
- [ ] `asftool doctor` runs
- [ ] All 42+ tests pass
- [ ] `uv run ruff check .` passes
- [ ] `uv run mypy asftool` passes

---

## Files to Modify

| File | Changes |
|------|---------|
| `asftool/` (directory) | Renamed from `tcrm_toolkit/` |
| `pyproject.toml` | Name, entry point, deps, mypy/ruff config |
| `asftool/core/config.py` | ASFTOOL_ prefix, ~/.asftool/ paths |
| `asftool/core/logger.py` | Log file path |
| `asftool/cli/main.py` | Entry point |
| `scripts/seed_session.py` | Imports, env vars |
| `scripts/verify-cross-platform.py` | Imports, CLI name |
| `tests/**/*.py` | Import fixes |
| `README.md` | Update usage examples |

---

## Notes

- This phase is mechanical — mostly find/replace
- Keep the architecture: `cli/` → `core/services/` → `core/auth/` + `core/client.py`
- The `core/services/auth_service.py` was already deleted in Phase 0
- Run `uv sync` after pyproject changes to regenerate uv.lock