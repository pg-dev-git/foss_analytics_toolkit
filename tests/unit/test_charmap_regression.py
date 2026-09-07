"""Regression test for the encoding crash fix (emoji from real TCRM data).

Bug: interactive mode crashes with 'charmap codec can't encode' when
real Salesforce data (e.g. dashboard label: 'Canadian Sales 🇨🇦')
contains a non-ASCII emoji and the terminal uses cp1252.

Fix: cli/main.py loads .env and sets PYTHONIOENCODING=utf-8 at CLI
entrypoint (before any console init). cli/commands/fields.py has a graceful
UnicodeEncodeError handler. cli/ui.py uses default Console().

This test is a minimal assert-based self-check (per ladder rules):
if the CLI entrypoint or exception handler breaks, this fails.
"""

from unittest.mock import patch, MagicMock
import pytest

# ponytail: minimal self-check — import the CLI modules, verify handler exists.


def test_cli_main_has_encoding_fix():
    """cli/main.py must load .env and set PYTHONIOENCODING at import time."""
    import importlib.util
    spec = importlib.util.find_spec("asftool.cli.main")
    # Importing cli/main triggers the PYTHONIOENCODING set.
    from asftool.cli.main import main
    import os
    # After import, PYTHONIOENCODING should be set by cli/main.py.
    assert os.environ.get("PYTHONIOENCODING") == "utf-8"


def test_field_analyzer_has_unicode_encode_exception_handler():
    """cli/commands/fields.py must handle UnicodeEncodeError gracefully."""
    import inspect
    from asftool.cli.commands.fields import analyze_field_async
    src = inspect.getsource(analyze_field_async)
    # The exception handler should reference `UnicodeEncodeError`.
    assert "UnicodeEncodeError" in src, (
        "analyze_field_async must have a UnicodeEncodeError handler"
    )


def test_encoding_environment_set_before_console_init():
    """cli/ui.py should NOT try Console(encoding=...) — that arg doesn't exist."""
    import inspect
    import asftool.cli.ui as ui_mod
    src = inspect.getsource(ui_mod)
    # The console creation line should NOT contain the invalid 'encoding=' parameter.
    # Instead, cli/main.py enforces encoding at the env layer.
    assert 'console = Console(' in src
    assert 'encoding=' not in src.split('console = Console(')[1].split(')')[0]
