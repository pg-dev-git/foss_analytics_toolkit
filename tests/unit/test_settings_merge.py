"""Tests for the Settings merge precedence (env > persisted > default).

Phase 2 of the api-version plan. Verifies the rule:
  .env / os.environ > ~/.asftool/config.json > hardcoded default

We test the extracted ``_resolve_sf_api_version`` helper directly
(rather than instantiating Settings, which needs encryption keys from
.env). The helper has all the same merge logic; Settings just calls it.
"""

from pathlib import Path

import pytest

from asftool.core.config import (
    _resolve_sf_api_version,
    _sf_api_version_from_env_sources,
)


class TestMergePrecedence:
    def test_default_when_no_env_no_persisted(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path, chdir_to_tmp
    ):
        """No env, no persisted file -> the hardcoded default wins."""
        monkeypatch.delenv("SF_API_VERSION", raising=False)
        result = _resolve_sf_api_version(
            from_settings="v68.0",  # pydantic-settings default
            from_user_config=None,
        )
        assert result == "v68.0"

    def test_env_var_overrides_persisted(
        self, monkeypatch: pytest.MonkeyPatch, chdir_to_tmp
    ):
        """os.environ > persisted file."""
        monkeypatch.setenv("SF_API_VERSION", "v60.0")
        result = _resolve_sf_api_version(
            from_settings="v60.0",  # what pydantic-settings read
            from_user_config="v68.0",  # what the user persisted
        )
        assert result == "v60.0"  # env wins

    def test_persisted_overrides_default_when_no_env(
        self, monkeypatch: pytest.MonkeyPatch, chdir_to_tmp
    ):
        """No env, but persisted file present -> file wins over default."""
        monkeypatch.delenv("SF_API_VERSION", raising=False)
        result = _resolve_sf_api_version(
            from_settings="v68.0",  # default since no env
            from_user_config="v65.0",  # what the user persisted
        )
        assert result == "v65.0"  # persisted wins over default

    def test_invalid_persisted_value_ignored(
        self, monkeypatch: pytest.MonkeyPatch, chdir_to_tmp
    ):
        """If the persisted value is bad, ConfigStore.load() returns None
        and we fall through to the default. Verify the helper handles None
        from_user_config correctly.
        """
        monkeypatch.delenv("SF_API_VERSION", raising=False)
        result = _resolve_sf_api_version(
            from_settings="v68.0",
            from_user_config=None,  # bad value was filtered out
        )
        assert result == "v68.0"

    def test_dotenv_overrides_persisted(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path, chdir_to_tmp
    ):
        """.env file value beats persisted file."""
        # Write a .env with SF_API_VERSION=v60.0 in the tmp dir.
        (Path.cwd() / ".env").write_text("SF_API_VERSION=v60.0\n", encoding="utf-8")
        monkeypatch.delenv("SF_API_VERSION", raising=False)
        result = _resolve_sf_api_version(
            from_settings="v60.0",
            from_user_config="v68.0",
        )
        assert result == "v60.0"  # .env wins

    def test_handles_none_from_settings_gracefully(
        self, monkeypatch: pytest.MonkeyPatch, chdir_to_tmp
    ):
        """Defensive: if from_settings is None, use default unless persisted
        provides a value."""
        monkeypatch.delenv("SF_API_VERSION", raising=False)
        # Persisted wins over default.
        result = _resolve_sf_api_version(
            from_settings=None, from_user_config="v67.0"
        )
        assert result == "v67.0"
        # Default applies when both are None.
        result = _resolve_sf_api_version(from_settings=None, from_user_config=None)
        assert result == "v68.0"


@pytest.fixture
def chdir_to_tmp(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Run the test with cwd=tmp_path so the .env lookup in
    _sf_api_version_from_env_sources is isolated."""
    monkeypatch.chdir(tmp_path)
    yield


class TestEnvSourcesHelper:
    def test_returns_none_when_neither_source_set(
        self, monkeypatch: pytest.MonkeyPatch, chdir_to_tmp
    ):
        monkeypatch.delenv("SF_API_VERSION", raising=False)
        # No .env in cwd.
        assert _sf_api_version_from_env_sources() is None

    def test_returns_env_var_value(
        self, monkeypatch: pytest.MonkeyPatch, chdir_to_tmp
    ):
        monkeypatch.setenv("SF_API_VERSION", "v65.0")
        assert _sf_api_version_from_env_sources() == "v65.0"

    def test_returns_dotenv_value(
        self, monkeypatch: pytest.MonkeyPatch, chdir_to_tmp
    ):
        monkeypatch.delenv("SF_API_VERSION", raising=False)
        (Path.cwd() / ".env").write_text("SF_API_VERSION=v64.0\n", encoding="utf-8")
        assert _sf_api_version_from_env_sources() == "v64.0"

    def test_env_var_takes_priority_over_dotenv(
        self, monkeypatch: pytest.MonkeyPatch, chdir_to_tmp
    ):
        monkeypatch.setenv("SF_API_VERSION", "v65.0")
        (Path.cwd() / ".env").write_text("SF_API_VERSION=v64.0\n", encoding="utf-8")
        assert _sf_api_version_from_env_sources() == "v65.0"

    def test_dotenv_with_other_keys_still_works(
        self, monkeypatch: pytest.MonkeyPatch, chdir_to_tmp
    ):
        monkeypatch.delenv("SF_API_VERSION", raising=False)
        (Path.cwd() / ".env").write_text(
            "OTHER_KEY=foo\n# comment\nSF_API_VERSION=v66.0\n", encoding="utf-8"
        )
        assert _sf_api_version_from_env_sources() == "v66.0"
