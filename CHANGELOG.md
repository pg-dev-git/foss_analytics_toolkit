# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

- Test suite no longer depends on the developer's ambient environment: the two
  `asftool config show` tests failed on any machine without a `.env` file.
- Package metadata now declares Apache-2.0. The classifier still said AGPLv3
  while the `LICENSE` file said Apache 2.0.

### Changed

- Lint findings cleared across the test suite; `ruff check .` is clean and
  enforced in CI.
- Package metadata migrated to the PEP 639 licence form
  (`License-Expression: Apache-2.0`).

### Added

- CI workflow running `ruff`, `mypy` and `pytest` on every push to `master`
  and every pull request, on Python 3.11 and 3.12.
- `pre-commit` configuration for lint, formatting and basic file hygiene.

## [0.1.0]

### Added

- Initial release: SF CLI authentication, interactive menu, and the
  `datasets` / `dashboards` / `dataflows` / `jobs` / `fields` / `config` /
  `lineage` / `doctor` command groups.

[Unreleased]: https://github.com/pg-dev-git/foss_analytics_toolkit/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/pg-dev-git/foss_analytics_toolkit/releases/tag/v0.1.0
