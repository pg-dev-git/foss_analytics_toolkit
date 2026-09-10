# ASFTool — Analytics REST API Software Tool (ASFT)

Release: v0.2.0 — Branding & Naming Standardization

ASFTool is a lightweight, high-performance async Python CLI for the Salesforce Analytics REST API. It interacts directly with the REST endpoints, without binding to any Salesforce product brand (Tableau CRM, Einstein Analytics, or otherwise).

## What's changed in this release

- All internal and user-facing references to "TCRM" / "Tableau CRM" replaced with "ASFT".
- Package renamed consistency: `asftool` entrypoint, `ASFTOOL_*` environment variables, `~/.asftool/` config directory.
- CLI help strings, menu banners, and core service docstrings updated to reference the generic Analytics REST API.
- Legacy duplicate directories (`dataset_tasks/`, `dashboards_tasks/`, etc.) removed from `master`; preserved under `_legacy/`.
- Log file rotated: `tcrm.log` → `asftool.log`.

## Quick start

```bash
# Install
uv sync --extra dev

# Authenticate
asftool auth login

# Check environment
asftool doctor

# Interactive menu
asftool
```

## Migration from older versions

If you have a leftover `tcrm` binary or `tcrm-toolkit` package installed, remove it before using `asftool`:

```bash
where.exe tcrm   # (Windows) then delete the shim
py -m pip uninstall tcrm-toolkit
```

Configuration paths changed: `~/.tcrm/` → `~/.asftool/`. Environment variables use `ASFTOOL_` instead of `TCRM_`.

## License

Apache License 2.0
