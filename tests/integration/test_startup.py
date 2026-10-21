"""Tests for the CLI's startup behaviour.

`asftool` on a machine with no `.env` used to die with a pydantic
traceback before printing anything, and `asftool --help` entered the
interactive menu instead of printing help. Both are the first thing a new
user meets.
"""

import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]


def _run(args: list[str], env: dict[str, str] | None = None) -> subprocess.CompletedProcess:
    """Run the CLI in an empty directory with a controlled environment.

    The working directory must not be the repository, or the project's own
    ``.env`` would be picked up and the test would not be testing what it
    claims to.
    """
    import os
    import tempfile

    full_env = dict(os.environ)
    full_env.pop("ENCRYPTION_KEY", None)
    # Pin the terminal size. Rich truncates its help table to the terminal
    # height, so the rendered help differs between a developer's terminal
    # and a CI runner.
    full_env["COLUMNS"] = "200"
    full_env["LINES"] = "100"
    if env:
        full_env.update(env)

    with tempfile.TemporaryDirectory() as empty_dir:
        return subprocess.run(
            [sys.executable, "-m", "asftool.cli.main", *args],
            capture_output=True,
            text=True,
            cwd=empty_dir,
            env=full_env,
            timeout=60,
        )


class TestStartupWithoutConfiguration:
    def test_no_env_gives_one_clear_message_not_a_traceback(self, tmp_path):
        """The tool must not die with a stack trace on a fresh machine."""
        result = _run([])
        output = result.stdout + result.stderr
        assert "Traceback" not in output
        assert "ValidationError" not in output
        assert "ENCRYPTION_KEY" in output

    def test_no_env_exits_non_zero(self):
        result = _run([])
        assert result.returncode == 1

    def test_help_is_not_blocked_by_missing_configuration(self):
        """`--help` must work even with no configuration at all."""
        result = _run(["--help"])
        assert result.returncode == 0
        assert "Usage" in result.stdout

    def test_short_help_flag_works(self):
        result = _run(["-h"])
        assert result.returncode == 0
        assert "Usage" in result.stdout

    def test_doctor_works_without_configuration(self):
        """`doctor` is the command run precisely because something is wrong."""
        result = _run(["doctor"])
        output = result.stdout + result.stderr
        assert "Traceback" not in output
        assert "ASFTool Doctor Diagnostics" in output

    def test_config_show_works_without_configuration(self):
        result = _run(["config", "show"])
        output = result.stdout + result.stderr
        assert "Traceback" not in output
        assert "Configuration" in output

    def test_subcommand_help_works(self):
        result = _run(["datasets", "--help"])
        assert result.returncode == 0
        assert "Usage" in result.stdout

    def test_help_lists_the_command_groups(self):
        """The help must name the available command groups."""
        result = _run(["--help"])
        assert result.returncode == 0
        assert "datasets" in result.stdout


class TestPreflight:
    def test_reports_the_missing_variable(self, monkeypatch):
        from asftool.cli.main import _preflight

        monkeypatch.setattr(
            "asftool.core.config.get_settings",
            lambda: type(
                "S", (), {"encryption_key": None}
            )(),
        )
        message = _preflight()
        assert message is not None
        assert "ENCRYPTION_KEY" in message
        assert "asftool init" in message

    def test_passes_when_configured(self, monkeypatch):
        from asftool.cli.main import _preflight

        monkeypatch.setattr(
            "asftool.core.config.get_settings",
            lambda: type(
                "S", (), {"encryption_key": "k"}
            )(),
        )
        assert _preflight() is None


class TestHelpRouting:
    @pytest.mark.parametrize(
        "argv",
        [
            ["asftool", "--help"],
            ["asftool", "-h"],
            ["asftool", "--version"],
        ],
    )
    def test_help_flags_are_recognised(self, argv):
        from asftool.cli.main import _wants_help

        assert _wants_help(argv) is True

    def test_subcommand_is_not_help(self):
        from asftool.cli.main import _wants_help

        assert _wants_help(["asftool", "datasets", "list"]) is False
