# Repository Guidelines for `pg-dev-git/foss_analytics_toolkit`

## Overview
FOSS Analytics Toolkit is a Python-based utility toolkit designed to expand the usability of Tableau CRM (TCRM) / Salesforce Analytics. It provides automated tasks and command-line utilities for datasets, dashboards, dataflows, and data manager jobs.

## Repository Structure
- `FOSS_Toolkit.py`: Main interactive CLI entry point.
- `dataset_tasks/`: Modules for dataset management, extraction, backup, and CSV uploads.
- `dashboards_tasks/`: Modules for listing and backing up dashboards.
- `dataflow_tasks/`: Modules for managing and backing up dataflows.
- `data_manager_tasks/`: Modules for tracking Data Manager jobs.
- `misc_tasks/`: Utilities for authentication, cryptography/encryption, system metrics, and initial checks.
- `toolkit_data/`: Directory storing configuration and encrypted auth files.

## Setup & Dependencies
- Python 3.8 or 3.9 recommended.
- Install dependencies:
  ```bash
  pip install -r requirements.txt
  ```
- Key dependencies: `modin[ray]`, `psutil`, `requests`, and `cryptography`.

## Running the Application
- Launch the interactive CLI tool:
  ```bash
  python3 FOSS_Toolkit.py
  ```

## Testing & Linting
- There are no automated test suites, testing frameworks, linting tools, or CI workflows (`Makefile`, `pyproject.toml`, `.pre-commit-config.yaml`, etc.) defined in this repository.

## Gotchas & Workflows
- **Authentication**: Supports Web Login and Connected App authentication. Configuration and credentials are encrypted on first run and stored in `toolkit_data/`.
- **CSV Uploads**: The only date format supported when uploading CSV files is `yyyy/mm/dd`.
- **Memory Usage**: Large data extractions may require significant RAM or SWAP space when working with Modin/pandas dataframes.
