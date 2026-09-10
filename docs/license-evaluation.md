# License Evaluation — ASFTool (ASFT)

Prepared: 2026-07-23  |  Project: ASFTool — Analytics REST API Software Tool

## Current license
GNU Affero General Public License v3 (AGPLv3), 19 November 2007 (`LICENSE`).

## Why AGPLv3 was chosen
The original `FOSS_Toolkit.py` used AGPLv3. It protects against proprietary redistribution of server-side modifications — relevant when the tool interacts with Salesforce Analytics REST API endpoints over a network.

## Alternatives considered

| License | Compatibility with dependencies (MIT/Apache/BSD) | Suitability for CLI wrapper | Network clause (relevant?) |
|---|---|---|---|
| **MIT** | Excellent — permissive, no copyleft conflict | Very good — simple, commercial-friendly | None |
| **Apache-2.0** | Excellent — permissive, patent grant included | Very good — standard for modern Python CLIs | None |
| **BSD-3** | Excellent — permissive | Good — minimal restrictions | None |
| **AGPLv3 (current)** | Requires derivatives to remain AGPL | Strong copyleft — may limit commercial integration | Mandatory source release for remote interaction |

## Dependency compatibility check
All `pyproject.toml` dependencies (`typer`, `rich`, `httpx`, `pydantic`, `pandas`, etc.) use MIT, Apache-2.0, BSD-3, or similar permissive licenses. AGPLv3 is compatible with them — no license conflict.

## Recommendation for a CLI-only project
ASFT is a lightweight async CLI wrapper, not a network server distributing software to remote users. The AGPLv3 network-interaction clause (Section 13) provides little practical benefit here. For broader adoption and easier integration in enterprise environments, **MIT or Apache-2.0** is more appropriate.

If keeping AGPLv3, add a clear `COPYRIGHT` file and source-file headers to satisfy Section 5 requirements.

Next step: user confirmation before changing `LICENSE`.
