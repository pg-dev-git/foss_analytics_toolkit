# Encoding Fix Plan — Emoji in CLI Output

**Branch**: `feature/asftool-refactor`
**Root cause**: real Salesforce dashboard label (`"Canadian Sales 🇨🇦"`) has a non-ASCII emoji. The CLI's `rich.console.Console` uses the terminal's default encoding (`charmap` = `cp1252` on Windows cmd.exe). When `console.print()` writes a line containing that emoji, Python raises `UnicodeEncodeError: 'charmap' codec can't encode...`.

**Not a crawler logic bug** — the crawl completes correctly (65 assets, 191 matches, 5 exact, 186 fuzzy). The crash is at the display layer, not the data layer.

---

## Phase 1: Force UTF-8 at environment + CLI init (lazy)

### Sub-tasks (2 steps, ~3 lines each)

1. **`.env` and `.env.example`** — add `PYTHONIOENCODING=utf-8`. This is std lib (`os.environ`); Python's stdin/stdout encoding picks this up at startup. It's the native mechanism for forcing UTF-8 on Windows.
2. **`asftool/cli/ui.py`** — add `console.output_encoding = "utf-8"` in module init (or in the `console` setup). `rich.console` supports this natively.

This is the minimal, correct fix — it doesn't sanitize data (which would hide real user information), it doesn't add mock encoding layers, and it uses the native encoding mechanism.

---

## Phase 2: Explicit encoding in file writes (lazy)

### Sub-tasks

3. **`asftool/core/storage.py`** — confirm `write_text` uses `encoding="utf-8"` (Python 3 default, but being explicit avoids surprises on systems with a different locale).
4. **`asftool/core/services/field_impact_service.py`** — confirm `report.model_dump_json()` uses `indent=2` with default encoding (already UTF-8). The JSON output is fine; add a `# ponytail: UTF-8 enforced by PYTHONIOENCODING` comment in the service file for documentation.

---

## Acceptance
- [x] `.env` has `PYTHONIOENCODING=utf-8`
- [x] `.env.example` updated
- [x] `cli/ui.py` has `console.output_encoding = "utf-8"`
- [x] Live test (`test_field_live.py`) completes without encoding error
- [x] Existing tests still pass
- [x] No mock encoding framework added; no data sanitization added

---

## Skipped (deliberate, YAGNI)
- No custom `encoding_wrapper` class or decorator. `PYTHONIOENCODING` is the std lib mechanism.
- No data sanitization (stripping emoji from labels). The user's request was to show output properly, not to hide real user content.
- No `console.force_terminal()` call. Native encoding is sufficient.

## Ceiling / upgrade path
`# ponytail: PYTHONIOENCODING is sufficient; if user runs without .env (no PYTHONIOENCODING set), the terminal locale must be UTF-8. Upgrade if needed: add a pre-flight check in cli/main.py that warns when the locale is not UTF-8.`
