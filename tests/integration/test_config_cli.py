"""Integration tests for the ``asftool config`` command group (Phase 2)."""

import json
from pathlib import Path

import pytest
from typer.testing import CliRunner

from asftool.cli.commands.config import app as config_app
from asftool.core.config_store import ConfigStore


@pytest.fixture
def tmp_config(monkeypatch, tmp_path: Path):
    """Point the global ConfigStore at a temp file so tests are isolated."""
    target = tmp_path / "config.json"
    monkeypatch.setattr(ConfigStore, "DEFAULT_PATH", target)
    return target


@pytest.fixture
def runner():
    return CliRunner()


class TestConfigShow:
    def test_show_runs(self, runner, tmp_config):
        result = runner.invoke(config_app, ["show"])
        assert result.exit_code == 0
        # Output should mention "Configuration" and the API version line.
        assert "Configuration" in result.stdout
        assert "API version" in result.stdout

    def test_show_reports_persisted_value(self, runner, tmp_config):
        tmp_config.write_text(
            json.dumps({"sf_api_version": "v68.0"}), encoding="utf-8"
        )
        result = runner.invoke(config_app, ["show"])
        assert result.exit_code == 0
        assert "v68.0" in result.stdout
        assert "persisted" in result.stdout.lower()


class TestConfigSetApiVersion:
    def test_persists_value(self, runner, tmp_config):
        result = runner.invoke(config_app, ["set-api-version", "v68.0"])
        assert result.exit_code == 0
        assert tmp_config.exists()
        data = json.loads(tmp_config.read_text())
        assert data == {"sf_api_version": "v68.0"}

    def test_rejects_bad_format(self, runner, tmp_config):
        result = runner.invoke(config_app, ["set-api-version", "60.0"])
        assert result.exit_code == 1
        # File should not be created on rejection.
        assert not tmp_config.exists()
        assert "Invalid" in result.stdout

    def test_overwrites_existing(self, runner, tmp_config):
        tmp_config.write_text(
            json.dumps({"sf_api_version": "v60.0"}), encoding="utf-8"
        )
        result = runner.invoke(config_app, ["set-api-version", "v68.0"])
        assert result.exit_code == 0
        data = json.loads(tmp_config.read_text())
        assert data["sf_api_version"] == "v68.0"


class TestConfigReset:
    def test_deletes_file(self, runner, tmp_config):
        tmp_config.write_text(
            json.dumps({"sf_api_version": "v68.0"}), encoding="utf-8"
        )
        result = runner.invoke(config_app, ["reset"])
        assert result.exit_code == 0
        assert not tmp_config.exists()

    def test_reset_missing_is_noop(self, runner, tmp_config):
        result = runner.invoke(config_app, ["reset"])
        assert result.exit_code == 0
