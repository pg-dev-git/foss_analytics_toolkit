"""Tests that short flags mean the same thing across the command groups.

`-o` meant `--output` in `datasets extract` and `--operation` in
`datasets upload`. Muscle memory would therefore do the wrong thing
depending on which command the user was running.
"""

import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]


def _help(command: list[str]) -> str:
    result = subprocess.run(
        [sys.executable, "-m", "asftool.cli.main", *command, "--help"],
        capture_output=True,
        text=True,
        cwd=str(REPO_ROOT),
        timeout=60,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    return result.stdout


class TestShortFlagConsistency:
    @pytest.mark.parametrize(
        "command",
        [
            ["datasets", "extract"],
            ["dashboards", "backup"],
            ["dataflows", "backup"],
            ["fields", "analyze"],
            ["lineage", "generate"],
        ],
    )
    def test_output_uses_dash_o(self, command):
        assert "-o" in _help(command)

    def test_operation_does_not_use_dash_o(self):
        """`--operation` must not claim `-o`."""
        help_text = _help(["datasets", "upload"])
        # The option is listed, but without the colliding short flag.
        assert "--operation" in help_text
        assert "-o," not in help_text.replace("--operation", "")

    def test_upload_still_accepts_the_long_operation_flag(self):
        """Removing the short flag must not remove the option."""
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "asftool.cli.main",
                "datasets",
                "upload",
                "--help",
            ],
            capture_output=True,
            text=True,
            cwd=str(REPO_ROOT),
            timeout=60,
        )
        assert "--operation" in result.stdout
