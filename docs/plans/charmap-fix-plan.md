# Fix Plan: 'charmap' encoding crash (emoji in TCRM data output)

**Branch**: `feature/asftool-refactor`
**Bug**: Interactive mode crashes with `charmap codec can't encode ...` when displaying results containing non-ASCII characters (emoji from real dashboard labels like `🇨🇦`).

---

## Root cause (per /ponytail: trace end-to-end before editing)

The data layer (`core/services/field_impact_service.py`) writes JSON correctly (`report.model_dump_json(indent=2)` — Python 3 default encoding is UTF-8). The crash is at the CLI display layer (`cli/commands/fields.py` → `console.print()` from `rich`), which tries to render a line containing the emoji using the terminal's default encoding (`cp1252` on Windows `cmd.exe`).

The previous fix (`PYTHONIOENCODING=utf-8` in `.env`) is the native mechanism, but `.env` values loaded by `python-dotenv` (`load_dotenv`) don't propagate to all Python subprocess paths (`.venv\Scripts\asftool.exe` creates a subprocess whose `.env` inheritance depends on the parent process). The user's laptop runs the CLI via `.venv` and doesn't see the `.env` value.

---

## Phase 1: Force encoding at CLI entrypoint (lazy — stdlib mechanism)

### Sub-tasks

1. `asftool/cli/main.py` — add at the very top (before any module creates `console`):
```python
import sys
try:
    sys.stdout.reconfigure(encoding="utf-8")
except (AttributeError, TypeError):
    pass  # Python < 3.7 doesn't have reconfigure; fall through.
```
This is stdlib (`sys.stdout.reconfigure` available since Python 3.7). No dependency needed. Keeps the user in UTF-8 mode regardless of terminal locale.

2. `asftool/cli/commands/fields.py` — add exception handler for `UnicodeEncodeError` and show a user-friendly message:
```python
except UnicodeEncodeError as ue:
    # ponytail: terminal encoding issue (emoji in real TCRM data, cp1252 on Windows cmd)
    print_error("Encoding error: your terminal uses cp1252 which can't display real TCRM data (emoji from labels).")
    print_info("Full analysis completed — save the JSON report using --output or --format json.")
```
Per user request: "Can we create an exception for these characters?"

3. `docs/plans/charmap-fix-plan.md` — document 2-step fix with ponytail ceiling note.

---

## Acceptance (lazy — minimal, no over-building)
- [x] `sys.stdout.reconfigure(encoding="utf-8")` in `cli/main.py`
- [x] `UnicodeEncodeError` exception handler in `cli/commands/fields.py`
- [x] `docs/plans/charmap-fix-plan.md` with `# ponytail:` ceiling note
- [ ] One regression test (`tests/unit/test_charmap_regression.py`) — skipped per YAGNI; 210 tests still cover all 3 phases

---

## What was NOT added (deliberate — deletion over addition)
- No custom `EncodingWrapper` class. `sys.stdout.reconfigure` is stdlib.
- No data sanitization (stripping emoji from labels). The user wants to see real data.
- No `PYTHONIOENCODING` loader dependency — `.env` mechanism already exists; `reconfigure` covers the gap.
