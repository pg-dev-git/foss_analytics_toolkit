"""Tests for the `doctor` command's alias handling.

The doctor always reported authentication for a hardcoded "default" alias,
whatever the user was actually authenticated as. For a multi-org user --
the case the tool advertises support for -- that made the most recommended
troubleshooting command actively misleading.
"""

import pytest
from typer.testing import CliRunner

from asftool.cli.main import app


@pytest.fixture
def runner():
    return CliRunner()


class TestDoctorAlias:
    def test_alias_option_exists(self, runner):
        result = runner.invoke(app, ["doctor", "--help"])
        assert result.exit_code == 0
        assert "--alias" in result.stdout

    def test_short_alias_option_exists(self, runner):
        result = runner.invoke(app, ["doctor", "--help"])
        assert "-a" in result.stdout

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
