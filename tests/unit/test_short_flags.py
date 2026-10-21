"""Tests that short flags mean the same thing across the command groups.

`-o` meant `--output` in `datasets extract` and `--operation` in
`datasets upload`. Muscle memory would therefore do the wrong thing
depending on which command the user was running.
"""

import pytest
import typer.main

from asftool.cli.main import app


def _click_command(group: str, subcommand: str):
    """The Click command object for a command.

    The option flags are read from the command object rather than from
    rendered help text: Rich truncates the help table to the terminal
    height, so the text is not the same on a 200-row terminal and on a
    25-row CI runner.
    """
    root = typer.main.get_command(app)
    return root.commands[group].commands[subcommand]


def _option_flags(command, name: str) -> list[str]:
    for param in command.params:
        if param.name == name:
            return list(param.opts) + list(param.secondary_opts)
    return []


class TestShortFlagConsistency:
    @pytest.mark.parametrize(
        "group,subcommand",
        [
            ("datasets", "extract"),
            ("dashboards", "backup"),
            ("dataflows", "backup"),
            ("fields", "analyze"),
            ("lineage", "generate"),
        ],
    )
    def test_output_uses_dash_o(self, group, subcommand):
        flags = _option_flags(_click_command(group, subcommand), "output")
        assert "-o" in flags

    def test_operation_does_not_use_dash_o(self):
        """`--operation` must not claim `-o`."""
        flags = _option_flags(_click_command("datasets", "upload"), "operation")
        assert "--operation" in flags
        assert "-o" not in flags

    def test_operation_still_exists(self):
        """Removing the short flag must not remove the option."""
        flags = _option_flags(_click_command("datasets", "upload"), "operation")
        assert flags
