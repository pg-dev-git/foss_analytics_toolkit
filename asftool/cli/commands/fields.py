"""Field Impact Analysis CLI commands.

Follows the same *_async wrapper + thin Typer shim pattern used by
the dataset/auth/dashboard/dataflow commands.
"""

import asyncio
import os
import re
from pathlib import Path
from typing import Literal

import typer

from asftool.cli.session import Session
from asftool.cli.ui import console, print_error, print_header, print_info, print_success
from asftool.core.config import get_settings
from asftool.core.services import FieldImpactService
from asftool.core.storage import get_storage_manager

# ponytail: sanitize non-ASCII (emoji) for CLI display only; JSON report preserves UTF-8.


def _sanitize_for_display(text: str) -> str:
    # Replace emoji and non-standard chars with '?' for safe terminal display.
    return re.sub(r"[^\w\s.,;:!?\-@=+/()\[\]{}<>\$\%\'\"]", "?", text)

app = typer.Typer(help="Field impact analysis across TCRM assets")


def _run(coro):
    return asyncio.run(coro)


def _apply_api_version_override(api_version: str | None) -> None:
    """Set SF_API_VERSION in os.environ for this process if --api-version was given.

    Per-phase-2 precedence: --api-version > env var > ~/.asftool/config.json > default.
    Setting it in os.environ here is what makes the chain work: get_settings()
    reads os.environ first, before checking the persisted config.
    """
    if not api_version:
        return
    if not re.match(r"^v\d+\.\d+$", api_version):
        print_error(
            f"Invalid --api-version {api_version!r}: must match v<major>.<minor>"
        )
        raise typer.Exit(1)
    os.environ["SF_API_VERSION"] = api_version


# Stage labels for progress output.
_STAGE_LABELS = {
    "datasets": "Datasets",
    "dashboards": "Dashboards",
    "dataflows": "Dataflows",
    "replicated_datasets": "Replicated datasets",
    "building_graph": "Application graph",
}


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
) -> Path | None:
    """Run the field analyzer.

    Returns the path of the saved JSON report (always written, regardless of fmt).
    """
    session = Session()
    try:
        from asftool.core.models import MatchMode

        settings = get_settings()

        async def _progress(stage: str, count: int, total: int) -> None:
            # Strip the "_complete" suffix to look up the friendly label.
            base = stage.removesuffix("_complete")
            label = _STAGE_LABELS.get(base, base)
            if stage.endswith("_complete"):
                print_info(f"  → {label}: {count} asset(s) scanned")
            elif stage == "building_graph" and count == total and total > 0:
                print_info("  → Application graph built")
            else:
                print_info(f"  [{count + 1}/{total}] {label}...")

        async with session.client_context() as client:
            service = FieldImpactService(client, session.settings)
            print_header(f"Field Impact: '{search_term}'")
            print_info(f"Mode: {mode}  |  Threshold: {threshold}  |  Org: {session.alias}")
            print_info("Scanning...")

            report = await service.analyze_field_impact(
                search_term=search_term,
                match_mode=MatchMode(mode),
                fuzzy_threshold=threshold,
                include_datasets=include_datasets,
                include_dashboards=include_dashboards,
                include_dataflows=include_dataflows,
                include_replicated=include_replicated,
                application_id=application,
                progress_callback=_progress,
            )

            # Always save a JSON report via StorageManager.
            storage = get_storage_manager(settings)
            output_path = output or storage.field_impact_path(
                alias=session.alias, search_term=search_term
            )
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text(report.model_dump_json(indent=2), encoding="utf-8")

            # Summary block.
            summary = report.to_summary_dict()
            console.print()
            print_success(
                f"Scan complete in {summary['execution_time_ms']}ms — "
                f"{summary['total_matches']} match(es) across "
                f"{summary['total_assets_scanned']} asset(s)"
            )
            print_info(
                f"  {summary['exact_matches']} exact + {summary['fuzzy_matches']} fuzzy"
            )
            for asset_type, count in summary["by_asset_type"].items():
                print_info(f"  - {asset_type}: {count} matched")
            if summary.get("error_count"):
                print_error(f"Errors during scan: {summary['error_count']}")
                for err in report.errors:
                    print_error(f"  - {err}")

            console.print()
            print_success(f"Report saved: {output_path}")

            # Optional detailed inline listing (table format only).
            if fmt == "table":
                for ds in report.details.datasets:
                    if ds.match_count > 0:
                        display_name = _sanitize_for_display(ds.display_name)
                        print_info(
                            f"  Dataset '{display_name}': {ds.match_count} match(es)"
                        )
                        for m in ds.matches[:5]:
                            safe_name = _sanitize_for_display(str(m.field_api_name))
                            safe_type = str(getattr(m, "match_type", "unknown"))
                            score_display = getattr(m, "match_score", 0)
                            print_info(f"    - {safe_name}  (match={safe_type}, score={score_display})")
                for db in report.details.dashboards:
                    if db.match_count > 0:
                        display_name = _sanitize_for_display(db.display_name)
                        print_info(
                            f"  Dashboard '{display_name}': {db.match_count} match(es)"
                        )
                for df in report.details.dataflows:
                    if df.match_count > 0:
                        display_name = _sanitize_for_display(df.display_name)
                        print_info(
                            f"  Dataflow '{display_name}': {df.match_count} match(es)"
                        )
                for rd in report.details.replicated_datasets:
                    if rd.match_count > 0:
                        display_name = _sanitize_for_display(rd.display_name)
                        print_info(
                            f"  Replicated '{display_name}': {rd.match_count} match(es)"
                        )
            return output_path
    except typer.Exit:
        return None
    except Exception as e:
        # ponytail: terminal encoding ceiling (emoji from real TCRM data)
        print_error(f"Analysis failed: {e}")
        import sys
        if isinstance(e, UnicodeEncodeError):
            enc = getattr(sys.stdout, "encoding", None) or "unknown"
            print_error(
                f"Encoding error (terminal encoding: {enc}). " +
                "Your terminal cannot display some characters in the data. " +
                "Use '--format json --output <path>' to save a UTF-8 JSON report.")
        else:
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
        None, "--output", "-o", help="Output JSON path (default: organized storage)"
    ),
    fmt: Literal["table", "json", "summary"] = typer.Option(
        "table", "--format", "-f", help="Output format"
    ),
    api_version: str | None = typer.Option(
        None,
        "--api-version",
        help="Override Salesforce API version (e.g. v68.0). "
        "Takes precedence over SF_API_VERSION env var and ~/.asftool/config.json.",
    ),
    no_datasets: bool = typer.Option(False, "--no-datasets", help="Skip dataset scanning"),
    no_dashboards: bool = typer.Option(False, "--no-dashboards", help="Skip dashboard scanning"),
    no_dataflows: bool = typer.Option(False, "--no-dataflows", help="Skip dataflow scanning"),
    no_replicated: bool = typer.Option(False, "--no-replicated", help="Skip replicated dataset scanning"),
) -> None:
    """Analyze where a field is used across all TCRM assets."""
    _apply_api_version_override(api_version)
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
