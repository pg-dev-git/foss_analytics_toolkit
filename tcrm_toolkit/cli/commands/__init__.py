"""CLI commands package."""

from tcrm_toolkit.cli.commands.auth import app as auth_app
from tcrm_toolkit.cli.commands.dashboards import app as dashboards_app
from tcrm_toolkit.cli.commands.dataflows import app as dataflows_app
from tcrm_toolkit.cli.commands.datasets import app as datasets_app
from tcrm_toolkit.cli.commands.jobs import app as jobs_app

__all__ = [
    "auth_app",
    "datasets_app",
    "dashboards_app",
    "dataflows_app",
    "jobs_app",
]
