# ASFTool — Analytics REST API Software Tool (ASFT)

ASFTool is a lightweight, high-performance async Python CLI for the Salesforce Analytics REST API. It rebuilds the legacy `FOSS_Toolkit.py` with a `typer` CLI plus an interactive "always running OS" menu — no TUI framework needed.

## Features

- **SF CLI Authentication** — web login (opens browser) or device flow for headless/SSH; multi-org support
- **Interactive Menu** — `asftool` alone drops you into a menu loop, like the original toolkit
- **Dataset Operations** — list, extract to CSV, upload CSV, delete, show details
- **Dashboard Operations** — list, backup JSON, show details
- **Dataflow Operations** — list, backup, start, stop, show details
- **Data Manager Jobs** — list, show job details
- **Diagnostics** — `asftool doctor` runs a full environment check
- **Cross-Platform** — Windows, Linux, macOS
- **Production Ready** — structured JSON logging to stderr and `~/.asftool/asftool.log`, retries that honour the server's `Retry-After`, bounded parallel scans, async + multiprocessing parallelism

## Installation

```bash
git clone https://github.com/pg-dev-git/foss_analytics_toolkit
cd foss_analytics_toolkit

uv sync --extra dev       # install with dev extras (tests, lint, typecheck)
```

Requires Python 3.11+ and the [Salesforce CLI](https://developer.salesforce.com/tools/sfdxcli) (`sf`).

ASFT is designed to be product-agnostic: it talks to the Salesforce Analytics REST API directly, without binding to any specific Salesforce product brand (Tableau CRM, Einstein Analytics, or otherwise).

## Quick Start

```bash
# Create a working configuration (.env with a generated encryption key)
asftool init

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

# Where is everything stored?
asftool config show
```

## How Authentication Works

ASFTool uses the **Salesforce CLI** (`sf`) as its only auth path — no Connected App, no
manual OAuth callback server. This mirrors the approach that worked reliably in the legacy
toolkit:

1. `asftool auth login` runs `sf org login web --alias default` (opens the browser).
2. The access token, instance URL, and username are captured from
   `sf org display --target-org default --json`.
3. Credentials are encrypted and stored in the OS keyring.

Because SF CLI owns the refresh token, ASFTool gets a fresh token on demand with
`sf org display`.

> **How expiry is detected.** The SF CLI does not report a token expiry — `sf org
> display` has no such field, and the access token is opaque rather than a decodable
> JWT. ASFTool therefore records when a token was last observed and treats it as
> expired once it is older than an hour (`MAX_TOKEN_AGE` in
> `asftool/core/auth/token_store.py`), at which point it is refreshed. When the
> platform does supply an expiry, that value is used instead.

> **What happens on a 401.** If the platform rejects a request with a 401 — which
> happens when Salesforce invalidates a token that still looks fresh to the token
> store — the token is refreshed and the request is repeated once. The request is
> only repeated when repeating it is safe: a read, or a `PUT`/`DELETE` that names
> one specific asset. A request that *acts* (starting a dataflow, creating a
> dashboard, processing a Data Manager job) is not repeated, because the platform
> may have already performed the action. The policy is recorded in
> `docs/decisions/0002-replay-policy.md`.

> **Headless hosts without a keyring:** install the fallback backend with
> `pip install 'asftool[headless]'` (pulls in `keyrings.alt`). Without a keyring
> backend, `asftool auth status` reports that the token store is unavailable rather
> than failing with a traceback.

## Configuration

Copy `.env.example` to `.env` for local development. ASFTool reads these environment
variables:

| Variable | Description | Default |
|----------|-------------|---------|
| `ENCRYPTION_KEY` | Base64 32-byte key for token encryption | required¹ |
| `SF_API_VERSION` | Salesforce API version | `v68.0` |
| `SF_DEFAULT_DOMAIN` | Login domain | `login.salesforce.com` |
| `APP_NAME` | Application name | `asftool` |
| `APP_VERSION` | Application version | `0.1.0` |
| `LOG_LEVEL` | Log level | `INFO` |
| `DEBUG` | Debug mode | `false` |
| `FIELD_IMPACT_MAX_CONCURRENT_SCANS` | Parallel asset scans during a field scan | `10` |

> The variable names are **not** prefixed with `ASFTOOL_`. Earlier versions of this
> README documented `ASFTOOL_*` names that the code never read; the table above lists
> the names the code actually uses. `asftool config show` prints the values in effect.

¹ Required to store tokens: it encrypts what goes into the OS keyring. Without it,
`asftool` exits with a message telling you to run `asftool init` or create a `.env`.

Generate keys:

```bash
python -c "import base64, os; print(base64.urlsafe_b64encode(os.urandom(32)).decode())"
```

Persistent state lives under `~/.asftool/`:

| Path | Contents |
|------|----------|
| `~/.asftool/config.json` | Persisted user settings (e.g. the API version) |
| `~/.asftool/asftool.log` | Structured JSON log |
| `~/.asftool/downloads/<alias>/` | Extracted datasets, backups, lineage output |

`asftool config show` prints all of these paths.

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

All three checks (`pytest`, `ruff`, `mypy`) also run in CI on every pull
request. See [CHANGELOG.md](CHANGELOG.md) for the release history.

### Contract tests against a real org

`tests/live/` holds contract tests that call the real Analytics REST API
and assert the models still parse what the platform returns. They are
excluded from the default run because they need credentials and a real
org:

```bash
# needs SF_AUTH_TOKEN and SF_DEFAULT_DOMAIN in .env
uv run pytest -m live
```

The checked-in fixtures in `tests/fixtures/wave_responses.py` were
captured from a live org and sanitized; these tests are what tells you
when the platform has moved on from them.

### Live testing against a real org

```bash
# Option A — recommended: authenticate via SF CLI
sf org login web --alias myorg
asftool auth login --alias myorg

# Option B — seed a session from existing env credentials (scripts/seed_session.py)
```

## License

Apache License 2.0
