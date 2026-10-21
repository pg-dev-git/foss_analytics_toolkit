"""Tests for `asftool init`.

The preflight tells a user with no configuration to run `asftool init`, so
that command has to exist and actually produce a working configuration.
"""

import os
import stat
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]


def _run(args: list[str], cwd: Path, env: dict) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, "-m", "asftool.cli.main", *args],
        capture_output=True,
        text=True,
        cwd=str(cwd),
        env=env,
        timeout=60,
    )


@pytest.fixture
def clean_env(tmp_path):
    """An environment with no ASFTool configuration."""
    env = dict(os.environ)
    env.pop("ENCRYPTION_KEY", None)
    env.pop("JWT_SECRET_KEY", None)
    env["HOME"] = str(tmp_path)
    return env


class TestInit:
    def test_creates_a_dot_env(self, tmp_path, clean_env):
        result = _run(["init"], tmp_path, clean_env)

        assert result.returncode == 0, result.stdout + result.stderr
        env_file = tmp_path / ".env"
        assert env_file.exists()

    def test_generated_key_is_valid(self, tmp_path, clean_env):
        _run(["init"], tmp_path, clean_env)

        from asftool.core.config import Settings

        settings = Settings(
            **dict(
                line.split("=", 1)
                for line in (tmp_path / ".env")
                .read_text(encoding="utf-8")
                .splitlines()
                if line and not line.startswith("#") and "=" in line
            )
        )
        # The validator decodes the key and checks its length.
        assert settings.encryption_key

    def test_env_file_is_private(self, tmp_path, clean_env):
        """The file holds the encryption key."""
        _run(["init"], tmp_path, clean_env)

        mode = stat.S_IMODE((tmp_path / ".env").stat().st_mode)
        assert mode == 0o600

    def test_refuses_to_overwrite_without_force(self, tmp_path, clean_env):
        (tmp_path / ".env").write_text("EXISTING=1", encoding="utf-8")

        result = _run(["init"], tmp_path, clean_env)

        assert result.returncode == 1
        assert (tmp_path / ".env").read_text(encoding="utf-8") == "EXISTING=1"

    def test_force_overwrites(self, tmp_path, clean_env):
        (tmp_path / ".env").write_text("EXISTING=1", encoding="utf-8")

        result = _run(["init", "--force"], tmp_path, clean_env)

        assert result.returncode == 0
        assert "EXISTING=1" not in (tmp_path / ".env").read_text(encoding="utf-8")

    def test_tool_starts_after_init(self, tmp_path, clean_env):
        """The whole point: after init, the tool runs."""
        _run(["init"], tmp_path, clean_env)

        result = _run(["config", "show"], tmp_path, clean_env)

        assert result.returncode == 0
        assert "Configuration" in result.stdout

    def test_init_is_exempt_from_the_preflight(self, tmp_path, clean_env):
        """`init` must work with no configuration at all."""
        result = _run(["init"], tmp_path, clean_env)
        assert "Missing ENCRYPTION_KEY" not in result.stdout
