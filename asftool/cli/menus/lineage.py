"""Lineage / Visual Dependency Mapping interactive menu."""

from pathlib import Path

import questionary

from asftool.cli.menu import Menu, MenuItem
from asftool.cli.session import Session
from asftool.cli.ui import print_error, print_info, print_lineage_success, print_warning


async def generate_lineage() -> None:
    """Interactive lineage generation with format multi-select."""
    from asftool.cli.session import Session

    session = Session()
    try:
        print_info("=== Visual Lineage Mapping ===")
        # Step 1: User selects input source — either impact JSON file or live impact analysis
        from asftool.cli.ui import prompt_text

        # Offer two paths: load an existing impact file, or run a field search first
        mode = prompt_text("Use an existing impact file? Type 'y' to load one, or 'n' to search a field first", default="n")
        impact_path: str | None = None
        if mode and mode.lower() in ("y", "yes", "1", "true"):
            impact_path_raw = prompt_text("Path to the impact JSON file (short name is fine — e.g., 'opportunity_impact')")
            if not impact_path_raw:
                print_info("Cancelled.")
                return
            impact_path_raw = impact_path_raw.strip()
            # Smart resolve: try as-is, then against known download directory
            p = Path(impact_path_raw)
            if not p.exists():
                # Try resolving in the standard downloads area
                download_dir = Path("asftool_downloads/default/field_impact")
                alt = download_dir / impact_path_raw
                if alt.exists():
                    impact_path = str(alt)
                else:
                    # Check if the basename exists anywhere in downloads
                    alt2 = download_dir / p.name
                    if alt2.exists():
                        impact_path = str(alt2)
                    else:
                        impact_path = impact_path_raw
            else:
                impact_path = impact_path_raw
        else:
            # Run field impact analysis interactively
            search_term = prompt_text("Field API name or label to analyze (e.g., 'revenue', 'customer_id')")
            if not search_term:
                print_info("Cancelled.")
                return
            search_term = search_term.strip()

            # Build impact report via service
            from asftool.cli.commands.fields import analyze_field_async
            try:
                print_info(f"Running field impact analysis for: '{search_term}'...")
                # Call the async analysis command directly
                await analyze_field_async(
                    search_term=search_term,
                    mode="both",
                    fmt="json",
                )
                # After analysis completes, infer the output path from session storage
                # (No extra prompt — the user just gave a search term; we'll use the default path automatically)
                # Note: analyze_field_async writes to a default path; we resolve it here.
                from asftool.core.config import get_settings
                from asftool.core.storage import get_storage_manager
                alias = session.alias
                settings = get_settings()
                storage = get_storage_manager(settings)
                impact_path = str(storage.field_impact_path(alias=alias, search_term=search_term))
                print_info(f"Using generated impact file: {impact_path}")
            except Exception as exc:
                print_error(f"Field impact analysis failed: {exc}")
                return

        # Step 2: Multi-select format
        formats = await _prompt_formats()
        if not formats:
            print_info("Cancelled.")
            return

        # Step 3: Pick a clean output name — not a full path
        print_info("Give this output a short name — like 'sales_lineage' or 'opportunity_map'. We'll add .svg / .mmd / .json automatically.")
        output_path = await _prompt_output_path(search_term or impact_path or "lineage")
        if not output_path:
            print_info("Cancelled.")
            return

        # Quick confirmation — tell them exactly what they're getting
        formats_str = ", ".join(formats)
        source_name = impact_path if impact_path else (search_term if 'search_term' in locals() else "lineage")
        # Extract just the filename, not the full path, for a clean message
        import os
        clean_name = os.path.basename(str(source_name))
        print_info(f"You'll get: {formats_str} files. Source: '{clean_name}'. Output name: '{output_path}'.")
        proceed = await _confirm_continue()
        if not proceed:
            print_info("No problem — cancelled.")
            return

        # Step 5: Generate for each format
        if not impact_path:
            print_error("No impact file or search term provided. Please select a source.")
            return
        await _execute_generation(session, impact_path, formats, output_path)

    except Exception as e:
        print_error(f"Lineage generation failed: {e}")
    finally:
        await session.close()


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


async def _prompt_output_path(default_name: str = "lineage") -> str | None:
    """Prompt for output path with sensible default."""
    from asftool.cli.ui import prompt_text

    # Default: lineage_<asset_id> in current dir
    default_path = f"lineage_{default_name}"

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
        f"Generate a visual lineage diagram from '{asset_id}'?\n"
        f"  Formats: {formats}\n"
        f"  Output name: {output_path}\n"
        f"Looks good? (y/n)",
        default=True,
    )


async def _confirm_continue() -> bool:
    """Simple yes/no confirmation."""
    from asftool.cli.ui import prompt_confirm

    return prompt_confirm("Continue?", default=True)


async def _execute_generation(
    session: Session,
    impact_path: str,
    formats: list[str],
    output_path: str,
) -> None:
    """Execute lineage generation from impact JSON file."""
    from asftool.core.services.lineage_service import LineageService
    from asftool.core.storage import get_storage_manager

    # Check impact file exists
    if not Path(impact_path).exists():
        print_error(f"Impact file not found: {impact_path}")
        print_info("To create an impact file, run: asftool fields analyze-field-impact")
        return

    # Build graph from impact file (no API dependency needed)
    try:
        service = LineageService(None)
        graph = service.build_graph_from_impact(impact_path)

        if not graph.nodes:
            print_warning("No dependencies found for this asset.")
            return

        print_info(f"Graph built: {len(graph.nodes)} nodes, {len(graph.edges)} edges")

    except FileNotFoundError as exc:
        print_error(str(exc))
        return
    except ValueError as exc:
        print_error(f"Invalid impact data: {exc}")
        print_info("The file may not be a valid Field Impact Analysis JSON output.")
        return
    except Exception as exc:
        print_error(f"Failed to build lineage graph from impact: {exc}")
        return

    # Resolve output paths using StorageManager
    settings = session.settings
    storage = get_storage_manager(settings)
    alias = session.alias

    # Generate each format with proper organized paths
    for fmt in formats:
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

                out_path = storage.lineage_path(alias=alias, output_name=output_path, extension="svg")
                path = service.render_svg(graph, str(out_path))
                print_lineage_success(f"SVG exported: {path}")

            elif fmt == "mermaid":
                out_path = storage.lineage_path(alias=alias, output_name=output_path, extension="mmd")
                mmd = service.render_mermaid(graph)
                out_path.write_text(mmd, encoding="utf-8")
                print_lineage_success(f"Mermaid exported: {out_path}")

            elif fmt == "json":
                import json
                out_path = storage.lineage_path(alias=alias, output_name=output_path, extension="json")
                payload = service.to_node_edge_json(graph)
                out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
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
