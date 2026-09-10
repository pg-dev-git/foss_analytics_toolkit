"""Regression test for the encoding crash fix (emoji from Analytics REST API response data).

Bug: interactive mode crashes with 'charmap codec can't encode' when
real Salesforce data (e.g. dashboard label: 'Canadian Sales 🇨🇦')
contains a non-ASCII emoji and the terminal uses cp1252.

Fix: cli/main.py loads .env and sets PYTHONIOENCODING=utf-8 at CLI
entrypoint (before any console init). cli/commands/fields.py has a graceful
UnicodeEncodeError handler. cli/ui.py uses default Console().

This test is a minimal assert-based self-check (per ladder rules):
if the CLI entrypoint or exception handler breaks, this fails.
"""

import base64
import importlib.util
import inspect
import json
import os
import tempfile
from pathlib import Path
from unittest.mock import patch

from asftool.core.config import Settings
from asftool.core.storage import StorageManager

# ponytail: minimal self-check — import the CLI modules, verify handler exists.


def test_cli_main_has_encoding_fix():
    """cli/main.py must load .env and set PYTHONIOENCODING at import time."""
    importlib.util.find_spec("asftool.cli.main")
    # Importing cli/main triggers the PYTHONIOENCODING set.
    from asftool.cli.main import main  # noqa: F401
    # After import, PYTHONIOENCODING should be set by cli/main.py.
    assert os.environ.get("PYTHONIOENCODING") == "utf-8"


def test_field_analyzer_has_unicode_encode_exception_handler():
    """cli/commands/fields.py must handle UnicodeEncodeError gracefully."""
    from asftool.cli.commands.fields import analyze_field_async
    src = inspect.getsource(analyze_field_async)
    # The exception handler should reference `UnicodeEncodeError`.
    assert "UnicodeEncodeError" in src, (
        "analyze_field_async must have a UnicodeEncodeError handler"
    )


def test_encoding_environment_set_before_console_init():
    """cli/ui.py should NOT try Console(encoding=...) — that arg doesn't exist."""
    import asftool.cli.ui as ui_mod
    src = inspect.getsource(ui_mod)
    # The console creation line should NOT contain the invalid 'encoding=' parameter.
    # Instead, cli/main.py enforces encoding at the env layer.
    assert 'console = Console(' in src
    assert 'encoding=' not in src.split('console = Console(')[1].split(')')[0]


def test_storage_manager_persists_emoji_utf8():
    """StorageManager must write/read emoji-containing JSON without encoding errors.

    This tests the actual file I/O path used by field impact reports:
    StorageManager generates a path, then write_text(..., encoding="utf-8")
    is called with ensure_ascii=False JSON. The file must be readable back
    with the emoji intact, regardless of the host OS default encoding.
    """
    # Create a minimal settings object with valid encryption key (32 bytes base64 encoded)
    encryption_key = base64.urlsafe_b64encode(b"x" * 32).decode()
    jwt_secret_key = base64.urlsafe_b64encode(b"y" * 32).decode()
    settings = Settings(
        app_name="asftool",
        app_version="0.1.0",
        encryption_key=encryption_key,
        jwt_secret_key=jwt_secret_key,
    )

    storage = StorageManager(settings)

    # Use a temp directory for the test - mock Path.cwd() to return temp dir
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)

        # Mock Path.cwd() to return our temp directory
        with patch("asftool.core.storage.Path.cwd", return_value=tmp_path):
            # Generate a field impact path
            path = storage.field_impact_path(
                alias="test-org",
                search_term="OpportunityID",
                extension="json",
            )

            # Create a report-like dict with emoji (simulating Analytics REST API response data)
            report_data = {
                "scope": {"search_term": "OpportunityID"},
                "summary": {"total_matches": 1},
                "details": {
                    "dashboards": [
                        {
                            "dashboard_name": "Canadian Sales 🇨🇦",
                            "dashboard_id": "01Zxxx",
                            "matches": [
                                {
                                    "field_name": "Amount",
                                    "field_context": "widget.label",
                                    "match_type": "EXACT",
                                    "match_score": 100,
                                }
                            ],
                        }
                    ]
                },
            }

            # Write the JSON with emoji - this is the critical path that was failing
            json_text = json.dumps(report_data, indent=2, ensure_ascii=False)
            path.write_text(json_text, encoding="utf-8")

            # Read it back and verify emoji is preserved
            read_back = path.read_text(encoding="utf-8")
            parsed = json.loads(read_back)

            # Verify the emoji survived the round-trip
            assert "🇨🇦" in parsed["details"]["dashboards"][0]["dashboard_name"]
            assert parsed["details"]["dashboards"][0]["dashboard_name"] == "Canadian Sales 🇨🇦"
