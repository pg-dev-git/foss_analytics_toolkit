"""Tests for the structured logging setup.

The README advertised "structured JSON logging" and the doctor command
checked for ``~/.asftool/asftool.log``, but ``setup_logging()`` was never
called from anywhere. Every module used structlog's default
configuration: human-readable output on stdout, never written to a file.

With stdout claimed by the CLI's own output, log lines were interleaved
with the results the user asked for.
"""

import base64
import json
import logging
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]

# A valid 32-byte encryption key, so the CLI gets past its own validation.
VALID_ENCRYPTION_KEY = base64.urlsafe_b64encode(b"t" * 32).decode()


@pytest.fixture
def logging_setup(tmp_path):
    """Configure logging into a temporary file and restore the defaults."""
    import structlog

    from asftool.core.logger import setup_logging

    log_file = tmp_path / "asftool.log"
    # structlog caches loggers on first use; reset so the new
    # configuration takes effect.
    structlog.reset_defaults()
    setup_logging(log_file=log_file, level="INFO")
    yield log_file
    structlog.reset_defaults()
    logging.getLogger().handlers.clear()


class TestStructuredLogging:
    def test_output_is_json(self, logging_setup, caplog):
        import structlog

        with caplog.at_level(logging.INFO):
            structlog.get_logger("test").info("an_event", key="value")

        record = json.loads(caplog.records[-1].getMessage())
        assert record["event"] == "an_event"
        assert record["key"] == "value"
        assert record["level"] == "info"
        assert "timestamp" in record

    def test_logs_do_not_go_to_stdout(self, logging_setup, capfd):
        """stdout belongs to the CLI's own output."""
        import structlog

        structlog.get_logger("test").info("an_event")

        captured = capfd.readouterr()
        assert captured.out == ""

    def test_log_file_is_created_and_written(self, logging_setup):
        import structlog

        structlog.get_logger("test").info("persisted_event")

        assert logging_setup.exists()
        record = json.loads(logging_setup.read_text(encoding="utf-8").strip())
        assert record["event"] == "persisted_event"

    def test_log_file_parent_directory_is_created(self, tmp_path):
        import structlog

        from asftool.core.logger import setup_logging

        structlog.reset_defaults()
        nested = tmp_path / "a" / "b" / "asftool.log"
        try:
            setup_logging(log_file=nested, level="INFO")
            assert nested.parent.exists()
        finally:
            structlog.reset_defaults()
            logging.getLogger().handlers.clear()


def _cli_env(tmp_path: Path) -> dict:
    import os

    env = dict(os.environ)
    env["HOME"] = str(tmp_path)
    env["ENCRYPTION_KEY"] = VALID_ENCRYPTION_KEY
    return env


class TestMainConfiguresLogging:
    def test_cli_run_creates_the_log_file(self, tmp_path):
        """A real CLI run must configure logging."""
        subprocess.run(
            [sys.executable, "-m", "asftool.cli.main", "config", "show"],
            capture_output=True,
            text=True,
            cwd=str(tmp_path),
            env=_cli_env(tmp_path),
            timeout=60,
        )

        log_file = tmp_path / ".asftool" / "asftool.log"
        assert log_file.exists(), "the CLI did not create the log file"

    def test_cli_stdout_is_not_polluted_by_log_lines(self, tmp_path):
        """Machine-readable output must stay clean."""
        result = subprocess.run(
            [sys.executable, "-m", "asftool.cli.main", "config", "show"],
            capture_output=True,
            text=True,
            cwd=str(tmp_path),
            env=_cli_env(tmp_path),
            timeout=60,
        )

        for line in result.stdout.splitlines():
            if line.strip():
                # Every stdout line is a rendered CLI message, not a log.
                assert not line.startswith("{"), f"log record on stdout: {line}"
