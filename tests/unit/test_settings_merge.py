"""Tests for the Settings merge precedence (env > persisted > default).

Phase 2 of the api-version plan. Verifies the rule:
  env var > ~/.asftool/config.json > hardcoded default

We test the extracted ``_resolve_sf_api_version`` helper directly
(rather than instantiating Settings, which needs encryption keys from
.env). The helper has all the same merge logic; Settings just calls it.
"""

import pytest

from asftool.core.config import _resolve_sf_api_version


class TestMergePrecedence:
    def test_default_when_no_env_no_persisted(
        self, monkeypatch: pytest.MonkeyPatch
    ):
        """No env, no persisted file -> the hardcoded default wins."""
        monkeypatch.delenv("SF_API_VERSION", raising=False)
        result = _resolve_sf_api_version(
            from_settings="v68.0",  # pydantic-settings default
            from_user_config=None,
        )
        assert result == "v68.0"

    def test_env_overrides_persisted(self, monkeypatch: pytest.MonkeyPatch):
        """Env var > persisted file."""
        monkeypatch.setenv("SF_API_VERSION", "v60.0")
        result = _resolve_sf_api_version(
            from_settings="v60.0",  # what pydantic-settings read
            from_user_config="v68.0",  # what the user persisted
        )
        assert result == "v60.0"  # env wins

    def test_persisted_overrides_default_when_no_env(
        self, monkeypatch: pytest.MonkeyPatch
    ):
        """No env, but persisted file present -> file wins over default."""
        monkeypatch.delenv("SF_API_VERSION", raising=False)
        result = _resolve_sf_api_version(
            from_settings="v68.0",  # default since no env
            from_user_config="v65.0",  # what the user persisted
        )
        assert result == "v65.0"  # persisted wins over default

    def test_invalid_persisted_value_ignored(
        self, monkeypatch: pytest.MonkeyPatch
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

    def test_env_present_falls_through_even_if_persisted_set(
        self, monkeypatch: pytest.MonkeyPatch
    ):
        """Even if a persisted value exists, env var wins (per rule)."""
        monkeypatch.setenv("SF_API_VERSION", "v70.0")
        result = _resolve_sf_api_version(
            from_settings="v70.0",
            from_user_config="v65.0",
        )
        assert result == "v70.0"

    def test_handles_none_from_settings_gracefully(
        self, monkeypatch: pytest.MonkeyPatch
    ):
        """Defensive: if from_settings is None (shouldn't happen, but…), use
        default unless persisted provides a value."""
        monkeypatch.delenv("SF_API_VERSION", raising=False)
        # Persisted wins over default.
        result = _resolve_sf_api_version(
            from_settings=None, from_user_config="v67.0"
        )
        assert result == "v67.0"
        # Default applies when both are None.
        result = _resolve_sf_api_version(from_settings=None, from_user_config=None)
        assert result == "v68.0"
