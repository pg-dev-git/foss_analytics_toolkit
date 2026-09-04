# ASFTool — FOSS Analytics Tool for Salesforce TCRM

Modern async Python CLI for Salesforce Tableau CRM (TCRM) Analytics. Rebuilds the legacy
`FOSS_Toolkit.py` with a `typer` CLI plus an interactive "always running OS" menu — no TUI
framework needed.

## Features

- **SF CLI Authentication** — web login (opens browser) or device flow for headless/SSH; token auto-refresh and multi-org support
- **Interactive Menu** — `asftool` alone drops you into a menu loop, like the original toolkit
- **Dataset Operations** — list, extract to CSV, upload CSV, delete, show details
- **Dashboard Operations** — list, backup JSON, show details
- **Dataflow Operations** — list, backup, start, stop, show details
- **Data Manager Jobs** — list, show job details
- **Diagnostics** — `asftool doctor` runs a full environment check
- **Cross-Platform** — Windows, Linux, macOS
- **Production Ready** — structured JSON logging, retries, async + multiprocessing parallelism

## Installation

```bash
git clone https://github.com/pg-dev-git/foss_analytics_toolkit
cd foss_analytics_toolkit

uv sync --extra dev       # install with dev extras (tests, lint, typecheck)
```

Requires Python 3.11+ and the [Salesforce CLI](https://developer.salesforce.com/tools/sfdxcli) (`sf`).

## Quick Start

```bash
# Authenticate (opens a browser)
asftool auth login

# Headless / SSH? Use the device flow
asftool auth login --device

# Check the current session
asftool auth status

# Interactive menu (always-running style)
asftool

# Or run single commands
asftool datasets list
asftool datasets extract 0FbXXX -o data.csv
asftool datasets upload 0FbXXX data.csv
asftool dashboards list
asftool dashboards backup 0FKXXX -o dashboard.json
asftool dataflows list
asftool dataflows start 03CXXX
asftool jobs list

# Environment diagnostics
asftool doctor
asftool doctor config
```

## How Authentication Works

ASFTool uses the **Salesforce CLI** (`sf`) as its only auth path — no Connected App, no
manual OAuth callback server. This mirrors the approach that worked reliably in the legacy
toolkit:

1. `asftool auth login` runs `sf org login web --alias default` (opens the browser).
2. The access token, instance URL, and username are captured from
   `sf org display --target-org default --json`.
3. Credentials are encrypted and stored in the OS keyring (with encryption-key file
   fallback for headless containers).

Because SF CLI owns the refresh token, ASFTool gets a fresh token on demand with
`sf org display` whenever the stored one is expired.

## Configuration

Copy `.env.example` to `.env` for local development. ASFTool reads these environment
variables (with `ASFTOOL_` prefix):

| Variable | Description | Default |
|----------|-------------|---------|
| `ASFTOOL_APP_NAME` | Application name | `asftool` |
| `ASFTOOL_APP_VERSION` | Application version | `0.1.0` |
| `ASFTOOL_LOG_LEVEL` | Log level | `INFO` |
| `ASFTOOL_DEBUG` | Debug mode | `false` |
| `ASFTOOL_SF_API_VERSION` | Salesforce API version | `v60.0` |
| `ASFTOOL_SF_DEFAULT_DOMAIN` | Login domain | `login.salesforce.com` |
| `ENCRYPTION_KEY` | Base64 32-byte key for token encryption | required |
| `JWT_SECRET_KEY` | Internal token signing secret (≥32 chars) | required |

Generate keys:

```bash
python -c "import base64, os; print(base64.urlsafe_b64encode(os.urandom(32)).decode())"
```

Persistent state lives under `~/.asftool/`: configuration directory, log file
(`asftool.log`), and (optionally) an encryption-key fallback file.

## Architecture

```
asftool/
├── cli/                    # Presentation: Typer commands + Rich menus
│   ├── main.py             # Entry point: subcommand dispatch or menu loop
│   ├── session.py          # Session bridge (auth + client lifecycle)
│   ├── commands/           # auth, datasets, dashboards, dataflows, jobs, doctor
│   ├── menus/              # Interactive menu wiring (one module per domain)
│   └── ui.py               # Rich helpers (tables, prompts, progress)
├── core/                   # Reusable, UI-agnostic layer
│   ├── auth/               # SF CLI auth (sf_cli, sf_cli_auth, token_store)
│   ├── services/           # Business logic (dataset, dashboard, dataflow)
│   ├── tasks/              # TaskRunner + parallel helpers (ProcessPoolExecutor)
│   ├── client.py           # Async HTTP client (httpx + tenacity retries)
│   ├── config.py           # Pydantic settings
│   ├── crypto.py           # Dynamic-salt encryption + keyring
│   ├── logger.py           # structlog JSON → stderr + ~/.asftool/asftool.log
│   └── models/             # Pydantic models
├── tests/                  # Unit + mocked integration tests
├── scripts/                # Dev tooling (cross-platform verification, …)
└── pyproject.toml
```

Dependency direction: `cli/ → core/tasks/ → core/services/ → core/`. Nothing below
`core/` may import from `cli/`.

## Parallelism

Dataset extraction/upload pipelines mix async I/O with CPU-bound work:

- **SAQL queries / upload parts** — `asyncio.Semaphore` + `httpx` for concurrent I/O
- **CSV merge / base64 encode** — `ProcessPoolExecutor` (true multiprocessing) via `TaskRunner`
- **Progress** — non-blocking callbacks that feed Rich progress bars

The helpers live in `asftool/core/tasks/` and are picklable, so they are safe to pass to
process pools.

## Development

```bash
uv run pytest -v --tb=short        # run all tests
uv run pytest tests/unit/ -v       # unit tests only
uv run pytest tests/integration/ -v  # mocked integration tests
uv run ruff check .                # lint
uv run mypy asftool                # typecheck
uv run python scripts/verify-cross-platform.py  # platform sanity checks
```

### Live testing against a real org

```bash
# Option A — recommended: authenticate via SF CLI
sf org login web --alias myorg
asftool auth login --alias myorg

# Option B — seed a session from existing env credentials (scripts/seed_session.py)
```

## License

GNU Affero General Public License v3.0
