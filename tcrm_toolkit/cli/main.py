"""Main CLI entry point — STUB during Phase 0 cleanup. Full impl in Phase 3."""

import typer
from tcrm_toolkit.cli.commands.auth import app as auth_app
from tcrm_toolkit.cli.commands.datasets import app as datasets_app
from tcrm_toolkit.cli.commands.dashboards import app as dashboards_app
from tcrm_toolkit.cli.commands.dataflows import app as dataflows_app
from tcrm_toolkit.cli.commands.jobs import app as jobs_app

app = typer.Typer(
    name="tcrm",
    help="Salesforce TCRM Analytics Toolkit (STUB — see Phase 3 for full menu loop)",
    add_completion=False,
    no_args_is_help=True,
)

app.add_typer(auth_app, name="auth")
app.add_typer(datasets_app, name="datasets")
app.add_typer(dashboards_app, name="dashboards")
app.add_typer(dataflows_app, name="dataflows")
app.add_typer(jobs_app, name="jobs")
