# Phase 0: Foundation Setup

**Document**: `docs/plans/phases/phase-0-foundation-setup.md`  
**Duration**: 2-3 days  
**Branch**: `feature/phase-0-foundation-setup` (to be created when implementation begins)

---

## 🎯 Objective

Establish the project foundation for the Interactive TUI:
- Add `interactive` optional dependencies to `pyproject.toml`
- Create Docker support for consistent cross-platform environments
- Set up cross-platform configuration patterns
- Create the `tcrm_toolkit/interactive/` module structure
- Verify all dependencies work on Windows, Linux, macOS

---

## 📋 Explicit Requirements

### 1. Update `pyproject.toml` Dependencies

**File**: `pyproject.toml`

Add the following to `[project.optional-dependencies]`:

```toml
[project.optional-dependencies]
interactive = [
    "textual>=0.52.0",
    "textual-dev>=0.1.0",      # Dev tools (CSS inspector, etc.)
    "httpx>=0.27.0",           # For ipapi.co calls (already in main deps)
]

dev = [
    # ... existing dev deps ...
    "pytest-textual>=0.1.0",   # For TUI testing
]
```

**Verify**: Run `uv sync --extra interactive --extra dev` and confirm no conflicts.

---

### 2. Create Docker Support

#### 2.1 Dockerfile (Multi-stage)

**File**: `Dockerfile` (repo root)

```dockerfile
# =============================================================================
# CRMA Toolkit - Interactive TUI Docker Image
# Multi-stage build for minimal production image
# =============================================================================

# ---- Build Stage ----
FROM python:3.12-slim AS builder

# Install system dependencies for building
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    git \
    && rm -rf /var/lib/apt/lists/*

# Install uv for fast dependency management
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Set working directory
WORKDIR /app

# Copy dependency files first (for cache)
COPY pyproject.toml uv.lock* ./

# Install dependencies
RUN uv sync --extra interactive --extra dev --frozen

# Copy source code
COPY . .

# Install package in development mode
RUN uv pip install -e .

# ---- Runtime Stage ----
FROM python:3.12-slim AS runtime

# Install runtime dependencies only
RUN apt-get update && apt-get install -y --no-install-recommends \
    # SF CLI dependencies
    curl \
    gnupg \
    # Terminal support
    procps \
    && rm -rf /var/lib/apt/lists/*

# Install SF CLI (Salesforce CLI)
RUN curl -fsSL https://developer.salesforce.com/media/salesforce-cli/salesforce-cli-linux-x64.tar.xz | tar -xJ -C /usr/local/bin --strip-components=1

# Create non-root user
RUN useradd -m -s /bin/bash tcrm && \
    mkdir -p /home/tcrm/.config /home/tcrm/.local/share && \
    chown -R tcrm:tcrm /home/tcrm

# Copy from builder
COPY --from=builder /app /app
COPY --from=builder /root/.local /home/tcrm/.local

# Set environment
ENV PATH="/home/tcrm/.local/bin:${PATH}"
ENV HOME="/home/tcrm"
ENV USER="tcrm"

# Switch to non-root user
USER tcrm
WORKDIR /home/tcrm

# Default command
ENTRYPOINT ["tcrm"]
CMD ["--help"]
```

#### 2.2 Docker Compose for Development

**File**: `docker-compose.yml` (repo root)

```yaml
version: '3.8'

services:
  dev:
    build:
      context: .
      dockerfile: Dockerfile
      target: builder  # Use builder stage for dev (has dev tools)
    container_name: crma-dev
    volumes:
      - .:/app:cached
      - ~/.ssh:/home/tcrm/.ssh:ro
      - ~/.gitconfig:/home/tcrm/.gitconfig:ro
      # Keyring/socket for auth (Linux)
      - /run/user/${UID}/keyring:/run/user/${UID}/keyring:ro
    environment:
      - DISPLAY=${DISPLAY}
      - WAYLAND_DISPLAY=${WAYLAND_DISPLAY}
      - XDG_RUNTIME_DIR=/run/user/${UID}
      - TERM=xterm-256color
    # For GUI/terminal access
    stdin_open: true
    tty: true
    # Network host for SF CLI localhost callbacks
    network_mode: "host"
    working_dir: /app
    command: sleep infinity  # Keep running for exec

  # Production-like runtime
  prod:
    build:
      context: .
      dockerfile: Dockerfile
      target: runtime
    container_name: crma-prod
    volumes:
      - tcrm-data:/home/tcrm/.local/share/tcrm
      - tcrm-config:/home/tcrm/.config/tcrm
    stdin_open: true
    tty: true
    network_mode: "host"

volumes:
  tcrm-data:
  tcrm-config:
```

#### 2.3 Docker Helper Scripts

**File**: `scripts/docker-dev.sh`

```bash
#!/usr/bin/env bash
# Development helper: start dev container and attach

set -euo pipefail

# Build if needed
docker compose build dev

# Start container
docker compose up -d dev

# Attach with proper terminal
docker compose exec -it dev bash
```

**File**: `scripts/docker-run.sh`

```bash
#!/usr/bin/env bash
# Run TUI in production container

set -euo pipefail

docker compose run --rm prod "$@"
```

Make executable: `chmod +x scripts/docker-*.sh`

---

### 3. Cross-Platform Configuration Patterns

#### 3.1 Platform Detection Utility

**File**: `tcrm_toolkit/core/platform.py` (NEW)

```python
"""Cross-platform utilities for OS detection and paths."""

import os
import sys
import platform
from pathlib import Path
from typing import Literal

OSType = Literal["windows", "linux", "darwin"]


def get_os() -> OSType:
    """Detect current operating system."""
    system = platform.system().lower()
    if system == "windows":
        return "windows"
    elif system == "darwin":
        return "darwin"
    return "linux"


def get_config_dir(app_name: str = "tcrm") -> Path:
    """Get platform-appropriate config directory."""
    os_type = get_os()
    
    if os_type == "windows":
        base = Path(os.environ.get("APPDATA", Path.home() / "AppData" / "Roaming"))
    elif os_type == "darwin":
        base = Path.home() / "Library" / "Application Support"
    else:  # Linux/Unix
        base = Path(os.environ.get("XDG_CONFIG_HOME", Path.home() / ".config"))
    
    return base / app_name


def get_data_dir(app_name: str = "tcrm") -> Path:
    """Get platform-appropriate data directory."""
    os_type = get_os()
    
    if os_type == "windows":
        base = Path(os.environ.get("LOCALAPPDATA", Path.home() / "AppData" / "Local"))
    elif os_type == "darwin":
        base = Path.home() / "Library" / "Application Support"
    else:  # Linux/Unix
        base = Path(os.environ.get("XDG_DATA_HOME", Path.home() / ".local" / "share"))
    
    return base / app_name


def get_cache_dir(app_name: str = "tcrm") -> Path:
    """Get platform-appropriate cache directory."""
    os_type = get_os()
    
    if os_type == "windows":
        base = Path(os.environ.get("LOCALAPPDATA", Path.home() / "AppData" / "Local")) / "Cache"
    elif os_type == "darwin":
        base = Path.home() / "Library" / "Caches"
    else:  # Linux/Unix
        base = Path(os.environ.get("XDG_CACHE_HOME", Path.home() / ".cache"))
    
    return base / app_name


def is_windows() -> bool:
    return get_os() == "windows"


def is_macos() -> bool:
    return get_os() == "darwin"


def is_linux() -> bool:
    return get_os() == "linux"


def get_terminal_size() -> tuple[int, int]:
    """Get terminal size cross-platform."""
    try:
        import shutil
        return shutil.get_terminal_size()
    except Exception:
        return (80, 24)


def supports_true_color() -> bool:
    """Check if terminal supports true color."""
    colorterm = os.environ.get("COLORTERM", "").lower()
    return "truecolor" in colorterm or "24bit" in colorterm
```

#### 3.2 Update Settings for Cross-Platform Paths

**File**: `tcrm_toolkit/core/config.py` (MODIFY)

Add to `Settings` class:

```python
# Cross-platform directories
config_dir: Path = Field(default_factory=lambda: get_config_dir())
data_dir: Path = Field(default_factory=lambda: get_data_dir())
cache_dir: Path = Field(default_factory=lambda: get_cache_dir())

# Ensure directories exist
def __post_init__(self):
    for dir_path in [self.config_dir, self.data_dir, self.cache_dir]:
        dir_path.mkdir(parents=True, exist_ok=True)
```

---

### 4. Create Interactive Module Structure

**Directory**: `tcrm_toolkit/interactive/`

Create the following structure:

```
tcrm_toolkit/interactive/
├── __init__.py
├── app.py                 # Main Textual App (placeholder)
├── session.py             # SessionManager (placeholder)
├── safety.py              # SafetyMonitor (placeholder)
├── tasks.py               # TaskRunner (placeholder)
├── config.py              # TUI config (placeholder)
├── platform.py            # Re-export from core.platform
├── screens/
│   ├── __init__.py
│   ├── main_screen.py
│   ├── login_screen.py
│   ├── org_picker.py
│   └── safety_modal.py
├── widgets/
│   ├── __init__.py
│   ├── data_table.py
│   ├── detail_panel.py
│   ├── progress_panel.py
│   ├── command_palette.py
│   ├── status_bar.py
│   └── task_history.py
├── operations/
│   ├── __init__.py
│   ├── dataset_ops.py
│   ├── dashboard_ops.py
│   └── dataflow_ops.py
└── styles/
    ├── default.css
    ├── dark.css
    └── light.css
```

**File**: `tcrm_toolkit/interactive/__init__.py`

```python
"""Interactive TUI module for CRMA Toolkit."""

from tcrm_toolkit.interactive.app import TCRMApp

__all__ = ["TCRMApp"]
```

**File**: `tcrm_toolkit/interactive/platform.py`

```python
"""Re-export platform utilities."""
from tcrm_toolkit.core.platform import (
    get_os,
    get_config_dir,
    get_data_dir,
    get_cache_dir,
    is_windows,
    is_macos,
    is_linux,
    get_terminal_size,
    supports_true_color,
    OSType,
)

__all__ = [
    "get_os",
    "get_config_dir",
    "get_data_dir",
    "get_cache_dir",
    "is_windows",
    "is_macos",
    "is_linux",
    "get_terminal_size",
    "supports_true_color",
    "OSType",
]
```

---

### 5. Verify Cross-Platform Compatibility

Create a verification script:

**File**: `scripts/verify-cross-platform.py`

```python
#!/usr/bin/env python
"""Verify cross-platform compatibility of the codebase."""

import sys
import platform
import subprocess
from pathlib import Path

def check_python_version():
    """Check Python version >= 3.11."""
    version = sys.version_info
    assert version.major == 3 and version.minor >= 11, f"Python 3.11+ required, got {version}"
    print(f"✅ Python {version.major}.{version.minor}.{version.micro}")

def check_imports():
    """Verify all critical imports work."""
    imports = [
        ("textual", "textual"),
        ("rich", "rich"),
        ("httpx", "httpx"),
        ("pydantic", "pydantic"),
        ("pydantic_settings", "pydantic_settings"),
        ("keyring", "keyring"),
        ("cryptography", "cryptography"),
        ("pandas", "pandas"),
        ("structlog", "structlog"),
        ("tenacity", "tenacity"),
        ("typer", "typer"),
    ]
    
    for name, module in imports:
        try:
            __import__(module)
            print(f"✅ {name}")
        except ImportError as e:
            print(f"❌ {name}: {e}")
            return False
    return True

def check_sf_cli():
    """Check SF CLI availability."""
    try:
        result = subprocess.run(["sf", "--version"], capture_output=True, text=True, timeout=10)
        if result.returncode == 0:
            print(f"✅ SF CLI: {result.stdout.strip()}")
        else:
            print(f"⚠️  SF CLI not found (install from https://developer.salesforce.com/tools/sfdxcli)")
    except FileNotFoundError:
        print(f"⚠️  SF CLI not found (install from https://developer.salesforce.com/tools/sfdxcli)")
    except Exception as e:
        print(f"⚠️  SF CLI check failed: {e}")

def check_platform_utils():
    """Test platform utilities."""
    sys.path.insert(0, str(Path(__file__).parent.parent))
    from tcrm_toolkit.core.platform import get_os, get_config_dir, get_data_dir
    
    os_type = get_os()
    print(f"✅ OS detected: {os_type}")
    print(f"✅ Config dir: {get_config_dir()}")
    print(f"✅ Data dir: {get_data_dir()}")

def main():
    print(f"🔍 Cross-platform verification for {platform.system()} {platform.machine()}")
    print("=" * 60)
    
    check_python_version()
    print()
    check_imports()
    print()
    check_sf_cli()
    print()
    check_platform_utils()
    print()
    print("=" * 60)
    print("✅ All checks passed!")

if __name__ == "__main__":
    main()
```

---

### 6. GitHub Actions for Multi-Platform CI

**File**: `.github/workflows/ci.yml`

```yaml
name: CI

on:
  push:
    branches: [main, refactor/**]
  pull_request:
    branches: [main]

jobs:
  test:
    runs-on: ${{ matrix.os }}
    strategy:
      matrix:
        os: [ubuntu-latest, windows-latest, macos-latest]
        python-version: ["3.11", "3.12", "3.13"]
    
    steps:
      - uses: actions/checkout@v4
      
      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: ${{ matrix.python-version }}
      
      - name: Install uv
        uses: astral-sh/setup-uv@v3
      
      - name: Install dependencies
        run: uv sync --extra interactive --extra dev
      
      - name: Run linting
        run: uv run ruff check .
      
      - name: Run type checking
        run: uv run mypy tcrm_toolkit
      
      - name: Run tests
        run: uv run pytest -v --tb=short
      
      - name: Verify cross-platform
        run: uv run python scripts/verify-cross-platform.py

  docker:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      
      - name: Build Docker image
        run: docker build -t crma-toolkit:test .
      
      - name: Test Docker image
        run: docker run --rm crma-toolkit:test --help
```

---

## ✅ Acceptance Criteria

| Check | Verification |
|-------|--------------|
| Dependencies install | `uv sync --extra interactive --extra dev` succeeds on all OSes |
| Docker builds | `docker build -t crma-toolkit .` succeeds |
| Docker runs | `docker run --rm crma-toolkit --help` shows help |
| Platform utils work | `python scripts/verify-cross-platform.py` passes |
| Module structure exists | `tcrm_toolkit/interactive/` with all subdirs |
| No hardcoded paths | `grep -r "C:\\\\|/home/" tcrm_toolkit/ --include="*.py" | grep -v test` returns empty |
| CI passes | GitHub Actions green on ubuntu, windows, macos |

---

## 🔧 Coding Agent Instructions

1. **Start with `pyproject.toml`** - Add dependencies first, verify install
2. **Create Dockerfile** - Test build locally before committing
3. **Create platform.py** - This is the foundation for all cross-platform code
4. **Create module structure** - Empty files with proper `__init__.py` exports
5. **Run verification script** - Must pass on your development machine
6. **Test Docker** - Both `docker-dev.sh` and `docker-run.sh` should work

**Key Patterns to Follow**:
- Use `pathlib.Path` everywhere
- Import platform utilities from `tcrm_toolkit.core.platform`
- Use `asyncio.subprocess` not `subprocess` directly in async code
- Use `concurrent.futures.ProcessPoolExecutor` for CPU-bound work
- Never assume `/tmp` or `/home` exists on Windows

---

## 📝 Architecture Decisions (Log in `architecture-decisions.md`)

- [x] Decision: Use `uv` for dependency management in Docker
- [x] Decision: Multi-stage Docker build for minimal runtime image
- [x] Decision: Non-root user in Docker for security
- [x] Decision: Host network mode for SF CLI localhost callbacks
- [x] Decision: `pathlib.Path` + platform utils for all paths

---

*End of Phase 0 Document*