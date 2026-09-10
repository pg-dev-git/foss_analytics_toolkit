## Summary

Implements Phase 6 of the ASFTool refactor: Visual Data Lineage & Dependency Mapping module.

### Core Implementation (`asftool/core/`)
- **Models** (`core/models/lineage.py`): `LineageNode`, `LineageEdge`, `LineageGraph` with traversal helpers (upstream/downstream)
- **Client** (`core/client.py`): Added `get_dependencies(asset_id)` targeting Salesforce Wave `/wave/dependencies/{asset_id}` endpoint
- **Service** (`core/services/lineage_service.py`): 
  - Recursive `build_graph()` traverses Analytics REST API dependency endpoints
  - `render_svg()` — Graphviz with custom node shapes/colors per asset type (Dataset=cylinder, Recipe=ellipse, Dataflow=box3d, Dashboard=rect, Lens=diamond)
  - `render_mermaid()` — Raw `.mmd` flowchart markup
  - `to_node_edge_json()` — Standardized `{nodes, edges}` payload for React Flow/D3.js frontends

### CLI (`asftool/cli/`)
- **Command** (`cli/commands/lineage.py`): `lineage generate <asset_id> --format svg|mermaid|json --output <path>`
- **UI** (`cli/ui.py`): Added `print_lineage_success()` for consistent messaging
- **Registration** (`cli/main.py`): `lineage` subcommand group registered

### Tests
- `tests/unit/test_lineage.py`: 6 passing tests (models + rendering)
- `tests/integration/test_lineage_cli.py`: 2 passing CLI invocation tests

### Known limitation
- OS-level `graphviz` binary (`dot`) required for SVG output; missing in this CI environment. Mermaid/JSON formats work without it.

### Related
Follows the 10-phase plan in `docs/plans/asftool-refactor/` (Phase 6).