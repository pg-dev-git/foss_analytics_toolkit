"""Field Impact Analysis CLI commands.

Follows the same *_async wrapper + thin Typer shim pattern used by
the dataset/auth/dashboard/dataflow commands.
"""

import asyncio
from pathlib import Path
from typing import Literal

import typer

from asftool.cli.session import Session
from asftool.cli.ui import console, print_error, print_header, print_info, print_success
from asftool.core.config import get_settings
from asftool.core.services import FieldImpactService

app = typer.Typer(help="Field impact analysis across TCRM assets")


def _run(coro):
    return asyncio.run(coro)


# ---------------------------------------------------------------------------
# Async wrappers (used by interactive menu and Typer commands)
# ---------------------------------------------------------------------------


async def analyze_field_async(
    search_term: str,
    mode: str = "both",
    threshold: int = 85,
    application: str | None = None,
    output: Path | None = None,
    fmt: Literal["table", "json", "summary"] = "table",
    include_datasets: bool = True,
    include_dashboards: bool = True,
    include_dataflows: bool = True,
    include_replicated: bool = True,
) -> None:
    session = Session()
    try:
        from asftool.core.models import MatchMode

        async with session.client_context() as client:
            service = FieldImpactService(client, session.settings)
            report = await service.analyze_field_impact(
                search_term=search_term,
                match_mode=MatchMode(mode),
                fuzzy_threshold=threshold,
                include_datasets=include_datasets,
                include_dashboards=include_dashboards,
                include_dataflows=include_dataflows,
                include_replicated=include_replicated,
                application_id=application,
            )

            if fmt == "summary":
                summary = report.to_summary_dict()
                print_header(f"Field Impact: '{search_term}'")
                print_info(f"Mode: {summary['match_mode']}  |  Threshold: {threshold}")
                print_success(
                    f"Assets: {summary['total_assets_scanned']}  |  "
                    f"Matches: {summary['total_matches']} "
                    f"({summary['exact_matches']} exact + {summary['fuzzy_matches']} fuzzy)"
                )
                for asset_type, count in summary["by_asset_type"].items():
                    print_info(f"  - {asset_type}: {count}")
                if summary.get("error_count"):
                    print_error(f"Errors during scan: {summary['error_count']}")
            elif fmt == "json":
                import json

                output_path = output or Path.home() / ".asftool" / "downloads" / f"field_impact_{search_term}.json"
                output_path.parent.mkdir(parents=True, exist_ok=True)
                output_path.write_text(report.model_dump_json(indent=2))
                print_success(f"Report saved to {output_path}")
            else:
                # Default table format
                print_header(f"Field Impact: '{search_term}' (mode={mode}, threshold={threshold})")
                summary = report.to_summary_dict()
                print_info(
                    f"Assets scanned: {summary['total_assets_scanned']}  |  "
                    f"Matches: {summary['total_matches']}  |  Execution: {summary['execution_time_ms']}ms"
                )
                if summary.get("error_count"):
                    print_error(f"Errors: {summary['error_count']}")
                    for err in report.errors:
                        print_error(f"  - {err}")
    except typer.Exit:
        pass
    except Exception as e:
        print_error(f"Analysis failed: {e}")
        raise typer.Exit(1) from e
    finally:
        await session.close()


# ---------------------------------------------------------------------------
# Typer commands
# ---------------------------------------------------------------------------


@app.command()
def analyze(
    search_term: str = typer.Argument(..., help="Field API name or label"),
    mode: Literal["exact", "fuzzy", "both"] = typer.Option(
        "both", "--mode", "-m", help="Match mode: exact, fuzzy, or both"
    ),
    threshold: int = typer.Option(85, "--threshold", "-t", help="Fuzzy threshold 0-100"),
    application: str | None = typer.Option(
        None, "--app", "-a", help="Limit to application dependency graph"
    ),
    output: Path | None = typer.Option(
        None, "--output", "-o", help="Output JSON path (optional)"
    ),
    fmt: Literal["table", "json", "summary"] = typer.Option(
        "table", "--format", "-f", help="Output format"
    ),
    no_datasets: bool = typer.Option(False, "--no-datasets", help="Skip dataset scanning"),
    no_dashboards: bool = typer.Option(False, "--no-dashboards", help="Skip dashboard scanning"),
    no_dataflows: bool = typer.Option(False, "--no-dataflows", help="Skip dataflow scanning"),
    no_replicated: bool = typer.Option(False, "--no-replicated", help="Skip replicated dataset scanning"),
) -> None:
    """Analyze where a field is used across all TCRM assets."""
    _run(
        analyze_field_async(
            search_term=search_term,
            mode=mode,
            threshold=threshold,
            application=application,
            output=output,
            fmt=fmt,
            include_datasets=not no_datasets,
            include_dashboards=not no_dashboards,
            include_dataflows=not no_dataflows,
            include_replicated=not no_replicated,
        )
    )
