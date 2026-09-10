"""Tests for the sf_api_version field on Settings.

Phase 1: default = v68.0, format validator rejects anything not matching
v<major>.<minor>.
"""

import base64

import pytest
from pydantic import ValidationError

from asftool.core.config import Settings


def _enc_keys():
    return {
        "encryption_key": base64.urlsafe_b64encode(b"x" * 32).decode(),
        "jwt_secret_key": base64.urlsafe_b64encode(b"y" * 32).decode(),
    }


class TestSfApiVersionDefault:
    def test_default_field_definition(self):
        """The default value on the Settings model is v68.0.

        We don't instantiate Settings() to check this because pydantic-settings
        will read .env (which may have its own SF_API_VERSION). The default
        is what kicks in when no env var and no .env value is present.
        """
        field = Settings.model_fields["sf_api_version"]
        assert field.default == "v68.0"

    def test_env_override_still_works(self, monkeypatch):
        monkeypatch.setenv("SF_API_VERSION", "v65.0")
        # Also clear .env-derived value by setting the env var explicitly.
        s = Settings(**_enc_keys())
        assert s.sf_api_version == "v65.0"


class TestSfApiVersionValidator:
    @pytest.mark.parametrize("good", ["v60.0", "v68.0", "v99.99", "v1.0"])
    def test_accepts_valid_format(self, monkeypatch, good):
        monkeypatch.setenv("SF_API_VERSION", good)
        s = Settings(**_enc_keys())
        assert s.sf_api_version == good

    @pytest.mark.parametrize(
        "bad",
        [
            "60.0",         # missing v prefix
            "v68",          # missing minor
            "v68.0.1",      # extra component
            "v",            # no version
            "latest",       # not a version
            "",             # empty
            "V68.0",        # uppercase V
        ],
    )
    def test_rejects_invalid_format(self, monkeypatch, bad):
        monkeypatch.setenv("SF_API_VERSION", bad)
        with pytest.raises(ValidationError) as exc_info:
            Settings(**_enc_keys())
        assert "SF_API_VERSION" in str(exc_info.value) or "sf_api_version" in str(exc_info.value)
