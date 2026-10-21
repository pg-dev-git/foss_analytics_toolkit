"""Tests for the `doctor` command's alias handling.

The doctor always reported authentication for a hardcoded "default" alias,
whatever the user was actually authenticated as. For a multi-org user --
the case the tool advertises support for -- that made the most recommended
troubleshooting command actively misleading.
"""

import pytest
import typer.main
from typer.testing import CliRunner

from asftool.cli.main import app


@pytest.fixture
def runner():
    return CliRunner()


def _click_command(group: str, subcommand: str | None = None):
    """The Click command object for a command group.

    The option flags are read from the command object rather than from
    rendered help text: Rich truncates the help table to the terminal
    height, so the text is not the same on a 200-row terminal and on a
    25-row CI runner.
    """
    root = typer.main.get_command(app)
    command = root.commands[group]
    if subcommand is not None:
        command = command.commands[subcommand]
    return command


def _option_flags(command, name: str) -> list[str]:
    for param in command.params:
        if param.name == name:
            return list(param.opts) + list(param.secondary_opts)
    return []


class TestDoctorAlias:
    def test_alias_option_exists(self):
        flags = _option_flags(_click_command("doctor"), "alias")
        assert "--alias" in flags

    def test_short_alias_option_exists(self):
        flags = _option_flags(_click_command("doctor"), "alias")
        assert "-a" in flags

    def test_reports_on_the_requested_alias(self, runner, monkeypatch):
        """The Authentication row names the alias that was asked for."""
        from asftool.cli.commands import doctor

        captured: dict[str, str] = {}

        async def _fake_collect(session, verbose, alias="default"):
            captured["alias"] = alias
            return [("Authentication", f"{alias}: not authenticated", True)]

        monkeypatch.setattr(doctor, "_collect_checks", _fake_collect)

        result = runner.invoke(app, ["doctor", "--alias", "some-org"])

        assert result.exit_code == 0, result.stdout
        assert captured["alias"] == "some-org"

    def test_defaults_to_the_default_alias(self, runner, monkeypatch):
        from asftool.cli.commands import doctor

        captured: dict[str, str] = {}

        async def _fake_collect(session, verbose, alias="default"):
            captured["alias"] = alias
            return [("Authentication", f"{alias}: not authenticated", True)]

        monkeypatch.setattr(doctor, "_collect_checks", _fake_collect)

        result = runner.invoke(app, ["doctor"])

        assert result.exit_code == 0, result.stdout
        assert captured["alias"] == "default"


class TestCollectChecksSignature:
    def test_collect_checks_accepts_an_alias(self):
        import inspect

        from asftool.cli.commands.doctor import _collect_checks

        params = inspect.signature(_collect_checks).parameters
        assert "alias" in params
        assert params["alias"].default == "default"
