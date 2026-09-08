"""Lineage CLI commands — SVG, Mermaid, and JSON exports."""

import asyncio
import json
from pathlib import Path

import typer
from rich.console import Console

from asftool.cli.session import Session
from asftool.cli.ui import (
    print_error,
    print_info,
    print_lineage_success,
    print_success,
)
from asftool.core.services.lineage_service import LineageService

app = typer.Typer(help="Visual lineage mapping & SVG diagram generation")
console = Console()


def _run(coro):
    return asyncio.run(coro)


@app.command("generate")
def generate(
    asset_id: str = typer.Argument(..., help="Root TCRM asset ID"),
    format: str = typer.Option(
        "svg", "--format", "-f", help="Output format: svg | mermaid | json"
    ),
    output: str = typer.Option(
        "lineage_output", "--output", "-o", help="Output file path (without extension)"
    ),
):
    """Generate dependency diagram for a TCRM asset."""

    async def generate_async(asset_id: str, fmt: str, out: str) -> None:
        session = Session()
        try:
            service = LineageService(session)
            print_info(f"Fetching dependencies for asset: {asset_id}")
            graph = await service.build_graph(asset_id)
            if fmt == "svg":
                path = service.render_svg(graph, out)
                print_lineage_success(f"SVG exported: {path}")
            elif fmt == "mermaid":
                mmd = service.render_mermaid(graph)
                out_path = f"{out}.mmd"
                Path(out_path).write_text(mmd, encoding="utf-8")
                print_lineage_success(f"Mermaid exported: {out_path}")
            elif fmt == "json":
                payload = service.to_node_edge_json(graph)
                out_path = f"{out}.json"
                Path(out_path).write_text(json.dumps(payload, indent=2), encoding="utf-8")
                print_lineage_success(f"JSON exported: {out_path}")
            else:
                print_error(f"Unknown format: {fmt}")
                raise typer.Exit(1)
        except Exception as exc:
            print_error(f"Lineage generation failed: {exc}")
            raise typer.Exit(1) from exc
        finally:
            await session.close()

    _run(generate_async(asset_id, format, output))
