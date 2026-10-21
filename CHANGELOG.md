# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

- `asftool auth list-orgs` reported "No authorized orgs found" on a machine
  with authorized orgs. The SF CLI does not return a single `orgs` array; it
  partitions orgs across `other`, `nonScratchOrgs`, `devHubs`, `scratchOrgs`
  and `sandboxes`, and the same org routinely appears in several of them.
  Results are now de-duplicated by org id, and a response that cannot be
  parsed raises instead of returning an empty list.
- `asftool datasets list` and `asftool dashboards list` silently stopped
  after the first page. The response models did not map the platform's
  `nextPageUrl`, so it was discarded and the loop exited.
- The pagination cursor is sent as `page`, not `pageToken`. The platform
  ignores an unknown query parameter and returns page 1 again, so the client
  never advanced.
- `asftool jobs list` and `asftool jobs show` raised a `ValidationError` on
  every real org. A dataflow job carries `label`, `status`, `jobType`,
  `startDate`, `executedDate` and `duration` -- not the `dataflowId` /
  `dataflowName` / `command` / `startTime` / `endTime` the model required.
- `asftool` on a machine with no `.env` died with a pydantic traceback
  before printing anything. It now prints one actionable message and exits 1.
- `asftool --help` entered the interactive menu instead of printing help.
- The interactive API-version picker could not be escaped: pressing Enter
  fed a version string into `int()` and looped forever. It now accepts
  either the number or the value, caps its attempts, and treats Ctrl-C and
  EOF as a cancel.
- The Visual Lineage menu crashed with `NameError` on the "load an existing
  impact file" path, and looked for an impact file it had never written
  (the path was recomputed with a fresh timestamp).
- A failed dependency fetch was treated as "this asset is a leaf", so a
  partial lineage graph looked exactly like a complete one. Incomplete nodes
  are now recorded and the CLI warns which ones are missing.
- A field scan fanned out one request per asset with no limit, because the
  semaphore was created but never acquired. Scans are now bounded by
  `FIELD_IMPACT_MAX_CONCURRENT_SCANS`, which was previously ignored.
- A 429's `Retry-After` was parsed and then read by nothing, so the client
  retried after 1.7 s against a server that had asked for 300 s. The wait
  strategy now honours the server's instruction.
- A 403 or 404 discarded the response body and reported a generic
  "Access forbidden" or "Resource not found". Both now carry the
  platform's own error code and message.
- Token expiry was never detected, so the refresh path was unreachable. The
  SF CLI reports no expiry, so the token is now tracked by observation time
  and refreshed once it is older than an hour.
- A failed token refresh was indistinguishable from "no token", and made the
  tool start an interactive browser login the user never asked for. It now
  reports the refresh failure.
- A host with no OS keyring backend crashed `asftool auth status` with a
  raw `NoKeyringError`. It now reports that the token store is unavailable,
  with instructions.
- `CryptoManager` rejected an encryption key that `Settings` accepted: the
  validator padded the key before decoding, the constructor did not. Both
  now decode leniently, and a malformed key raises `CryptoError`.
- `cleanup_old_downloads` crashed with `IsADirectoryError` on a subdirectory.
- The dashboard and dataflow backups failed with `FileNotFoundError` when
  the chosen output path had no parent directory.
- `asftool doctor` always reported authentication for a hardcoded `default`
  alias, whatever the user was authenticated as. It now takes `--alias`.
- The `structured JSON logging` the README advertised did not exist:
  `setup_logging()` was never called, and the output was human-readable text
  on stdout. It is now JSON on stderr and persisted to
  `~/.asftool/asftool.log`.
- Package metadata declared AGPLv3 while the `LICENSE` file was Apache 2.0.
- The test suite depended on the developer's ambient environment: the two
  `asftool config show` tests failed on any machine without a `.env` file.
- Deprecated `datetime.utcnow()` and `pd.Timestamp.utcnow()` calls replaced
  with the timezone-aware equivalents; a leaked file handle closed.

### Changed

- Downloads now live under `~/.asftool/downloads` instead of
  `$CWD/asftool_downloads`, so the output location does not depend on
  where the tool is run from. See `docs/decisions/0001-storage-root.md`.
- The README, `.env.example` and the code now agree on the environment
  variable names. The documented `ASFTOOL_*` names were never read.
- The README's API version default is now `v68.0`, matching the code.
- The README's token-lifecycle description now matches the implementation.
- `-o` means `--output` in every command; `--operation` no longer claims it.
- Lint findings cleared across the test suite; `ruff check .` is clean and
  enforced in CI.
- Package metadata migrated to the PEP 639 licence form
  (`License-Expression: Apache-2.0`).

### Added

- CI workflow running `ruff`, `mypy` and `pytest` on every push to `master`
  and every pull request, on Python 3.11 and 3.12.
- `pre-commit` configuration for lint, formatting and basic file hygiene.
- Checked-in Salesforce API response fixtures
  (`tests/fixtures/wave_responses.py`), captured from a live org and
  sanitized, plus contract tests that feed every response model the matching
  payload. A guard test fails if a response model is added without one.
- Opt-in contract tests against a real org (`pytest -m live`).
- `asftool init`: writes a `.env` with a generated encryption key and
  creates `~/.asftool`.
- `asftool datasets cleanup`: removes old downloads, keeping the most
  recent ones per type.
- A `headless` install extra providing the `keyrings.alt` backend.
- Shared pagination helpers (`asftool/core/pagination.py`), replacing three
  drifted copies of the same logic.

## [0.1.0]

### Added

- Initial release: SF CLI authentication, interactive menu, and the
  `datasets` / `dashboards` / `dataflows` / `jobs` / `fields` / `config` /
  `lineage` / `doctor` command groups.

[Unreleased]: https://github.com/pg-dev-git/foss_analytics_toolkit/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/pg-dev-git/foss_analytics_toolkit/releases/tag/v0.1.0
