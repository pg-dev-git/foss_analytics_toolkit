# Phase 9: Doctor, Polish & Documentation

**Goal:** Production-grade diagnostics, cross-platform verification, and updated docs.

---

## Prerequisites

- Phases 0-8 complete
- All CLI commands working

---

## Files to Create/Modify

```
asftool/cli/commands/doctor.py          # Doctor command
asftool/scripts/verify-cross-platform.py # Updated verification
README.md                                # Updated usage
```

---

## Step 9.1: Create `asftool/cli/commands/doctor.py`

```python
"""Doctor command — comprehensive diagnostics."""

import asyncio
import platform
import shutil
import sys
from pathlib import Path

import typer
from rich.console import Console
from rich.table import Table

from asftool.cli.session import Session
from asftool.cli.ui import console, print_error, print_header, print_info, print_success, print_warning
from asftool.core.auth import SFCLIAuthError
from asftool.core.config import get_settings

app = typer.Typer(help="System diagnostics")


def _run(coro):
    return asyncio.run(coro)


@app.command()
def doctor(
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose output"),
):
    """Run comprehensive system diagnostics."""
    async def _doctor():
        print_header("ASFTool Doctor Diagnostics")
        console.print()

        checks = []

        # 1. Python version
        py_version = f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
        checks.append(("Python", f"{py_version} ({platform.platform()})", sys.version_info >= (3, 11)))

        # 2. UV package manager
        uv_path = shutil.which("uv")
        checks.append(("UV", uv_path or "NOT FOUND", uv_path is not None))

        # 3. SF CLI
        session = Session()
        sf_cli_available = session.auth_service.sf_cli.is_available()
        sf_cli_path = session.auth_service.sf_cli._cli_path if sf_cli_available else "NOT FOUND"
        checks.append(("SF CLI", sf_cli_path, sf_cli_available))

        # 4. Config directory
        config_dir = get_settings().config_dir
        config_exists = config_dir.exists()
        checks.append(("Config dir", str(config_dir), config_exists))

        # 5. Log file
        log_file = get_settings().log_file
        log_exists = log_file.exists()
        checks.append(("Log file", str(log_file), log_exists))

        # 6. Encryption key
        settings = get_settings()
        has_encryption = bool(settings.encryption_key)
        checks.append(("Encryption key", "SET" if has_encryption else "MISSING", has_encryption))

        # 7. JWT secret
        has_jwt = bool(settings.jwt_secret_key)
        checks.append(("JWT secret", "SET" if has_jwt else "MISSING", has_jwt))

        # 8. Keyring backend
        try:
            import keyring
            kr_backend = keyring.get_keyring().__class__.__name__
            checks.append(("Keyring backend", kr_backend, True))
        except Exception as e:
            checks.append(("Keyring backend", f"ERROR: {e}", False))

        # 9. Network connectivity (Salesforce)
        try:
            import httpx
            async with httpx.AsyncClient(timeout=5.0) as client:
                resp = await client.get("https://login.salesforce.com")
                net_ok = resp.status_code == 200
        except Exception:
            net_ok = False
        checks.append(("Salesforce connectivity", "OK" if net_ok else "FAILED", net_ok))

        # 10. Auth status
        auth_status = await session.auth_service.status("default")
        auth_ok = auth_status["authenticated"] and not auth_status["token_expired"]
        checks.append((
            "Authentication",
            f"{auth_status.get('username', 'Not logged in')}" if auth_status["authenticated"] else "Not authenticated",
            auth_ok,
        ))

        # Display results
        table = Table(title="System Checks", show_header=True)
        table.add_column("Check", style="cyan")
        table.add_column("Value", style="white")
        table.add_column("Status", style="bold", width=10)

        all_pass = True
        for name, value, passed in checks:
            status = "[green]✓ PASS[/green]" if passed else "[red]✗ FAIL[/red]"
            if not passed:
                all_pass = False
            table.add_row(name, value, status)

        console.print(table)
        console.print()

        if all_pass:
            print_success("All checks passed!")
        else:
            print_warning("Some checks failed — see above")
            if not verbose:
                print_info("Run with --verbose for more details")

        await session.close()

        if not all_pass:
            raise typer.Exit(1)

    _run(_doctor())


@app.command("config")
def show_config():
    """Show current configuration."""
    settings = get_settings()
    print_header("Configuration")
    print_info(f"Config dir: {settings.config_dir}")
    print_info(f"Log file: {settings.log_file}")
    print_info(f"API version: {settings.sf_api_version}")
    print_info(f"Default domain: {settings.sf_default_domain}")
    print_info(f"Encryption key: {'SET' if settings.encryption_key else 'MISSING'}")
    print_info(f"JWT secret: {'SET' if settings.jwt_secret_key else 'MISSING'}")
```

---

## Step 9.2: Update `asftool/scripts/verify-cross-platform.py`

```python
#!/usr/bin/env python
"""Cross-platform verification script."""

import asyncio
import platform
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from asftool.core.config import get_settings
from asftool.core.auth import SFCLIAuthService
from asftool.core.crypto import create_crypto_manager


async def verify():
    """Run cross-platform verification."""
    print("=" * 60)
    print("ASFTool Cross-Platform Verification")
    print("=" * 60)
    print(f"Platform: {platform.platform()}")
    print(f"Python: {sys.version}")
    print()

    checks = []

    # Config
    settings = get_settings()
    config_dir = settings.config_dir
    config_dir.mkdir(parents=True, exist_ok=True)
    checks.append(("Config directory", config_dir.exists()))

    # Crypto
    crypto = create_crypto_manager()
    test_data = "test secret"
    encrypted = crypto.encrypt(test_data)
    decrypted = crypto.decrypt(encrypted)
    checks.append(("Crypto roundtrip", decrypted == test_data))

    # SF CLI
    auth = SFCLIAuthService(settings, crypto)
    sf_cli_ok = auth.sf_cli.is_available()
    checks.append(("SF CLI available", sf_cli_ok))

    # Keyring
    try:
        import keyring
        kr = keyring.get_keyring()
        checks.append(("Keyring backend", True))
        if hasattr(kr, 'name'):
            print(f"  Keyring: {kr.name}")
    except Exception as e:
        checks.append(("Keyring backend", False))
        print(f"  Keyring error: {e}")

    # Results
    print()
    print("Results:")
    all_pass = True
    for name, passed in checks:
        status = "PASS" if passed else "FAIL"
        if not passed:
            all_pass = False
        print(f"  [{status}] {name}")

    print()
    if all_pass:
        print("All verification checks PASSED")
        return 0
    else:
        print("Some verification checks FAILED")
        return 1


if __name__ == "__main__":
    sys.exit(asyncio.run(verify()))
```

---

## Step 9.3: Update `README.md`

```markdown
# ASFTool — FOSS Analytics Tool for Salesforce TCRM

Modern async Python CLI for Salesforce Tableau CRM (TCRM) Analytics.

## Features

- **SF CLI Authentication** — Web login, device flow, auto-refresh, multi-org
- **Interactive Menu** — "Always running OS" style menu (`asftool`)
- **Dataset Operations** — List, extract (parallel), upload (parallel), delete
- **Dashboard Operations** — List, backup JSON
- **Dataflow Operations** — List, backup, start, stop
- **Data Manager Jobs** — List, show
- **Cross-Platform** — Windows, Linux, macOS
- **Production Ready** — Structured logging, retries, parallelism

## Installation

```bash
# From source
git clone https://github.com/pg-dev-git/foss_analytics_toolkit
cd foss_analytics_toolkit
uv sync --extra dev
uv pip install -e .
```

## Quick Start

```bash
# Authenticate (opens browser)
asftool auth login

# Or device flow for headless/SSH
asftool auth login --device

# Check status
asftool auth status

# Interactive menu (always running OS style)
asftool

# Or direct commands
asftool datasets list
asftool datasets extract <id> -o data.csv
asftool datasets upload <id> data.csv
asftool dashboards list
asftool dataflows list
asftool jobs list

# Diagnostics
asftool doctor
```

## Configuration

Environment variables (prefix `ASFTOOL_`):

| Variable | Description | Default |
|----------|-------------|---------|
| `ASFTOOL_ENCRYPTION_KEY` | Base64-encoded 32-byte key for token encryption | Auto-generated |
| `ASFTOOL_JWT_SECRET_KEY` | JWT secret for internal tokens | Auto-generated |
| `ASFTOOL_SF_API_VERSION` | Salesforce API version | `v60.0` |
| `ASFTOOL_SF_DEFAULT_DOMAIN` | Default Salesforce domain | `login` |
| `ASFTOOL_LOG_LEVEL` | Log level | `INFO` |

Generate keys:
```bash
python -c "import base64, secrets; print(base64.urlsafe_b64encode(secrets.token_bytes(32)).decode())"
```

## Architecture

```
asftool/
├── cli/                    # Typer commands + Rich menus
│   ├── commands/           # auth, datasets, dashboards, dataflows, jobs, doctor
│   ├── menus/              # Interactive menu wiring
│   └── session.py          # Session bridge (auth + services)
├── core/
│   ├── auth/               # SF CLI auth (sf_cli, sf_cli_auth, token_store)
│   ├── services/           # Business logic (dataset, dashboard, dataflow)
│   ├── client.py           # Async HTTP client with retries
│   ├── config.py           # Pydantic Settings
│   ├── crypto.py           # Fernet + dynamic salt
│   ├── tasks/              # TaskRunner + parallel helpers
│   └── models/             # Pydantic models
└── tests/
```

## Parallelism

- **SAQL Queries**: `asyncio.Semaphore(10)` — I/O bound, concurrent
- **CSV Merge**: `ProcessPoolExecutor` — CPU bound, true parallelism
- **Base64 Encoding**: `ProcessPoolExecutor` — CPU bound
- **Progress**: Async callbacks, non-blocking

## Testing

```bash
# Unit tests
uv run pytest tests/unit/ -v

# Integration tests (mocked)
uv run pytest tests/integration/ -v

# All tests
uv run pytest -v --tb=short
```

## Live Testing (with real org)

```bash
# Seed session with live credentials
TCRM_ACCESS_TOKEN="..." TCRM_INSTANCE_URL="..." TCRM_USERNAME="..." \
uv run python scripts/seed_session.py

# Or use SF CLI (recommended)
sf org login web --alias myorg
asftool auth login --alias myorg
```

## License

GNU AGPL v3.0
```

---

## Step 9.4: Final Verification

```bash
# Run all tests
uv run pytest -v --tb=short

# Lint
uv run ruff check .

# Type check
uv run mypy asftool

# Cross-platform verify
uv run python scripts/verify-cross-platform.py

# Doctor
asftool doctor

# Interactive menu
asftool

# Subcommands
asftool --help
asftool auth --help
asftool datasets --help
```

---

## Acceptance Criteria

- [ ] `asftool doctor` runs all checks and shows pretty table
- [ ] `asftool doctor --verbose` shows more detail
- [ ] `asftool doctor config` shows configuration
- [ ] `scripts/verify-cross-platform.py` passes on current platform
- [ ] `README.md` has correct usage examples for `asftool`
- [ ] All 42+ tests pass
- [ ] `uv run ruff check .` passes
- [ ] `uv run mypy asftool` passes
- [ ] Package installs with `uv pip install -e .`
- [ ] `asftool` (no args) launches interactive menu

---

## Notes

- This is the "ship it" phase — make it feel professional
- The doctor command is a great first impression for users
- Cross-platform verify should be in CI eventually
- README is the project's front door — keep it current