"""Lineage / Visual Dependency Mapping interactive menu."""

from pathlib import Path

import questionary

from asftool.cli.menu import Menu, MenuItem
from asftool.cli.session import Session
from asftool.cli.ui import print_error, print_info, print_lineage_success, print_warning


async def generate_lineage() -> None:
    """Interactive lineage generation with format multi-select."""
    from asftool.cli.commands.lineage import generate_async
    from asftool.cli.session import Session
    from asftool.cli.ui import print_error, print_info

    session = Session()
    try:
        print_info("=== Visual Lineage Mapping ===")

        # Step 1: Get asset ID with validation
        asset_id = await _prompt_asset_id(session)
        if not asset_id:
            print_info("Cancelled.")
            return

        # Step 2: Multi-select format
        formats = await _prompt_formats()
        if not formats:
            print_info("Cancelled.")
            return

        # Step 3: Output path
        output_path = await _prompt_output_path(asset_id)
        if not output_path:
            print_info("Cancelled.")
            return

        # Step 4: Confirm and execute
        formats_str = ", ".join(formats)
        if not await _confirm_generation(asset_id, formats_str, output_path):
            print_info("Cancelled.")
            return

        # Step 5: Generate for each format
        await _execute_generation(session, asset_id, formats, output_path)

    except Exception as e:
        print_error(f"Lineage generation failed: {e}")
    finally:
        await session.close()


async def _prompt_asset_id(session: Session) -> str | None:
    """Prompt for and validate asset ID."""
    from asftool.cli.ui import prompt_text

    while True:
        asset_id = prompt_text("Enter TCRM Asset ID (Dataset/Dashboard/Dataflow/Recipe/Lens ID)")
        if not asset_id:
            return None

        # Basic validation: non-empty, reasonable length
        asset_id = asset_id.strip()
        if len(asset_id) < 5:
            print_error("Asset ID too short. Please enter a valid Salesforce ID (15 or 18 chars).")
            continue

        # Optional: verify asset exists via API (lightweight check)
        try:
            client = await session.get_client()
            # Try to get dependencies; if 404, asset doesn't exist
            from asftool.core.exceptions import SalesforceNotFoundError
            try:
                await client.get_dependencies(asset_id)
            except SalesforceNotFoundError:
                print_warning(f"Asset '{asset_id}' not found or no dependencies. Continue anyway?")
                if not await _confirm_continue():
                    continue
        except Exception as e:
            # Network/auth issues - warn but allow continuing
            print_warning(f"Could not verify asset ({e}). Continue anyway?")
            if not await _confirm_continue():
                continue

        return asset_id


async def _prompt_formats() -> list[str] | None:
    """Multi-select format picker using questionary checkboxes."""
    try:
        choices = [
            questionary.Choice("SVG (vector diagram)", value="svg"),
            questionary.Choice("Mermaid (.mmd for Markdown)", value="mermaid"),
            questionary.Choice("JSON (nodes/edges for React Flow/D3.js)", value="json"),
        ]

        selected = await questionary.checkbox(
            "Select output formats (space to select, enter to confirm):",
            choices=choices,
            instruction="[Space] select/deselect  [Enter] confirm",
        ).ask_async()

        if selected is None:
            return None  # User cancelled (Ctrl+C)

        if not selected:
            print_error("No format selected. Please choose at least one.")
            return await _prompt_formats()

        return selected

    except Exception as e:
        print_error(f"Format selection failed: {e}")
        return None


async def _prompt_output_path(asset_id: str) -> str | None:
    """Prompt for output path with sensible default."""
    from asftool.cli.ui import prompt_text

    # Default: lineage_<asset_id> in current dir
    default_path = f"lineage_{asset_id}"

    output_path = prompt_text("Output path (without extension)", default=default_path)
    if output_path is None:
        return None

    output_path = output_path.strip()
    if not output_path:
        return None

    return output_path


async def _confirm_generation(asset_id: str, formats: str, output_path: str) -> bool:
    """Confirm before executing."""
    from asftool.cli.ui import prompt_confirm

    return prompt_confirm(
        f"Generate lineage for asset '{asset_id}'?\n"
        f"  Formats: {formats}\n"
        f"  Output prefix: {output_path}\n"
        f"Proceed?",
        default=True,
    )


async def _confirm_continue() -> bool:
    """Simple yes/no confirmation."""
    from asftool.cli.ui import prompt_confirm
    return prompt_confirm("Continue?", default=True)


async def _execute_generation(
    session: Session,
    asset_id: str,
    formats: list[str],
    output_path: str,
) -> None:
    """Execute lineage generation for each selected format."""
    from asftool.core.services.lineage_service import LineageService

    # Build graph once
    print_info("Fetching dependencies and building graph...")
    try:
        client = await session.get_client()
        service = LineageService(client)
        graph = await service.build_graph(asset_id)

        if not graph.nodes:
            print_warning("No dependencies found for this asset.")
            return

        print_info(f"Graph built: {len(graph.nodes)} nodes, {len(graph.edges)} edges")

    except Exception as e:
        print_error(f"Failed to build dependency graph: {e}")
        return

    # Generate each format
    for fmt in formats:
        out = f"{output_path}"
        try:
            if fmt == "svg":
                # Check for dot binary before attempting
                if not _check_graphviz_available():
                    print_error(
                        "Graphviz 'dot' binary not found. SVG generation requires Graphviz.\n"
                        "  Install: sudo apt install graphviz (Ubuntu) / brew install graphviz (macOS)\n"
                        "  Skipping SVG. Mermaid/JSON still available."
                    )
                    continue

                path = service.render_svg(graph, out)
                print_lineage_success(f"SVG exported: {path}")

            elif fmt == "mermaid":
                mmd = service.render_mermaid(graph)
                out_path = f"{out}.mmd"
                Path(out_path).write_text(mmd, encoding="utf-8")
                print_lineage_success(f"Mermaid exported: {out_path}")

            elif fmt == "json":
                import json
                payload = service.to_node_edge_json(graph)
                out_path = f"{out}.json"
                Path(out_path).write_text(json.dumps(payload, indent=2), encoding="utf-8")
                print_lineage_success(f"JSON exported: {out_path}")

        except Exception as e:
            print_error(f"Failed to generate {fmt.upper()}: {e}")


def _check_graphviz_available() -> bool:
    """Check if Graphviz 'dot' binary is available."""
    import shutil
    return shutil.which("dot") is not None


def lineage_operations(menu: Menu) -> None:
    """Wire up the lineage submenu."""
    menu.add(MenuItem("1", "Generate lineage diagram", handler=generate_lineage))
    menu.add(MenuItem("b", "Back", exit_after=True))