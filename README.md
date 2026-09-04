# TCRM Toolkit

Salesforce Tableau CRM (TCRM) Analytics Toolkit - A modern, async Python CLI for managing TCRM datasets, dashboards, and dataflows.

## Features

- **Async Architecture**: Built on `httpx` with automatic retries and circuit breakers
- **Secure Authentication**: Web PKCE, Device Authorization Flow, and JWT Bearer flows
- **Credential Security**: Dynamic salt encryption with OS keyring integration
- **Type Safety**: 100% typed with Pydantic models and mypy validation
- **Modern CLI**: Rich terminal UI with progress bars and formatted tables

## Installation

```bash
# Clone the repository
git clone https://github.com/pg-dev-git/foss_analytics_toolkit
cd foss_analytics_toolkit

# Install with uv (recommended) or pip
uv sync --extra dev
# or
pip install -e ".[dev]"
```

## Configuration

Copy `.env.example` to `.env` and configure:

```bash
cp .env.example .env
# Edit .env with your settings
```

## Usage

```bash
# Authenticate
tcrm auth login

# List datasets
tcrm datasets list

# Extract dataset to CSV
tcrm datasets extract <DATASET_ID>

# Upload CSV to dataset
tcrm datasets upload <DATASET_ID> <CSV_FILE>

# List dashboards
tcrm dashboards list

# Backup dashboard
tcrm dashboards backup <DASHBOARD_ID>

# List dataflows
tcrm dataflows list

# Start/stop dataflow
tcrm dataflows start <DATAFLOW_ID>
tcrm dataflows stop <DATAFLOW_ID>
```

## Architecture

```
tcrm_toolkit/
├── core/           # Reusable SDK layer (UI-agnostic)
│   ├── config.py   # Pydantic settings
│   ├── crypto.py   # Encryption with dynamic salts
│   ├── client.py   # Async HTTP client with retries
│   ├── models/     # Pydantic data models
│   └── services/   # Domain services (auth, dataset, dashboard, dataflow)
└── cli/            # Presentation layer only
    ├── main.py     # Typer entry point
    ├── commands/   # CLI command implementations
    └── ui.py       # Rich formatting
```

## Development

```bash
# Run tests
pytest

# Type checking
mypy tcrm_toolkit

# Linting
ruff check tcrm_toolkit
ruff format tcrm_toolkit
```

## License

GNU Affero General Public License v3.0