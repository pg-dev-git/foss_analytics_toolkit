"""Tests that short flags mean the same thing across the command groups.

`-o` meant `--output` in `datasets extract` and `--operation` in
`datasets upload`. Muscle memory would therefore do the wrong thing
depending on which command the user was running.
"""

import pytest
from typer.testing import CliRunner

from asftool.cli.main import app


@pytest.fixture
def runner():
    return CliRunner()


def _help(runner, command: list[str]) -> str:
    result = runner.invoke(app, [*command, "--help"])
    assert result.exit_code == 0, result.stdout
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
    def test_output_uses_dash_o(self, runner, command):
        assert "-o" in _help(runner, command)

    def test_operation_does_not_use_dash_o(self, runner):
        """`--operation` must not claim `-o`."""
        help_text = _help(runner, ["datasets", "upload"])
        assert "--operation" in help_text
        # The short flag would be rendered as "-o," next to the option.
        assert "-o," not in help_text.replace("--operation", "")

    def test_upload_still_accepts_the_long_operation_flag(self, runner):
        """Removing the short flag must not remove the option."""
        assert "--operation" in _help(runner, ["datasets", "upload"])
