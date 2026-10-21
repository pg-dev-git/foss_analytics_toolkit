"""Tests for the `doctor` command's alias handling.

The doctor always reported authentication for a hardcoded "default" alias,
whatever the user was actually authenticated as. For a multi-org user --
the case the tool advertises support for -- that made the most recommended
troubleshooting command actively misleading.
"""

import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]


def _run(args: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, "-m", "asftool.cli.main", *args],
        capture_output=True,
        text=True,
        cwd=str(REPO_ROOT),
        timeout=90,
    )


class TestDoctorAlias:
    def test_alias_option_exists(self):
        result = _run(["doctor", "--help"])
        assert result.returncode == 0
        assert "--alias" in result.stdout

    def test_short_alias_option_exists(self):
        result = _run(["doctor", "--help"])
        assert "-a" in result.stdout

    def test_reports_on_the_requested_alias(self):
        """The Authentication row names the alias that was asked for."""
        result = _run(["doctor", "--alias", "some-org"])

        output = result.stdout + result.stderr
        assert "some-org" in output
        # It must not claim to be reporting on "default".
        assert "default: not authenticated" not in output

    def test_defaults_to_the_default_alias(self):
        result = _run(["doctor"])

        output = result.stdout + result.stderr
        assert "default" in output


class TestCollectChecksSignature:
    def test_collect_checks_accepts_an_alias(self):
        import inspect

        from asftool.cli.commands.doctor import _collect_checks

        params = inspect.signature(_collect_checks).parameters
        assert "alias" in params
        assert params["alias"].default == "default"
