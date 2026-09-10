"""Unit tests for asftool.core.sf_cli (SF CLI subprocess wrapper).

These tests focus on the parts that don't require SF CLI to be
installed: result parsing and field-name handling. The subprocess
integration is covered by tests/integration/test_api_endpoints.py
where the client is mocked.
"""


import pytest

from asftool.core.sf_cli import SFCLIManager


def test_parse_auth_result_extracts_expiration_date() -> None:
    """SF CLI's 'org display --json' returns 'expirationDate' (not 'tokenExpiration').

    The bug fixed in commit 869db7b: the old code looked for
    'tokenExpiration' which never exists, so the token was saved with
    expires_at=None, and the next request was treated as expired,
    triggering a re-login cycle.
    """
    mgr = SFCLIManager()  # no SF CLI calls yet
    data = {
        "accessToken": "tok-abc",
        "instanceUrl": "https://example.my.salesforce.com",
        "refreshToken": "ref-xyz",
        "username": "user@example.com",
        "expirationDate": "2026-09-05T18:30:00.000+0000",
    }
    result = mgr._parse_auth_result(data, alias="default")

    assert result.access_token == "tok-abc"
    assert result.instance_url == "https://example.my.salesforce.com"
    assert result.refresh_token == "ref-xyz"
    assert result.username == "user@example.com"
    assert result.alias == "default"
    assert result.expires_at is not None
    assert result.expires_at.year == 2026
    assert result.expires_at.month == 9
    assert result.expires_at.day == 5


def test_parse_auth_result_falls_back_to_token_expiration() -> None:
    """Older sfdx CLI and some custom forks use 'tokenExpiration'.

    Ensure backwards compatibility if a future SF CLI version reverts
    the field name, or if a custom build still uses the legacy name.
    """
    mgr = SFCLIManager()
    data = {
        "accessToken": "tok",
        "instanceUrl": "https://example.my.salesforce.com",
        "tokenExpiration": "2026-09-05T18:30:00.000+0000",
    }
    result = mgr._parse_auth_result(data, alias="default")

    assert result.access_token == "tok"
    assert result.expires_at is not None
    assert result.expires_at.year == 2026


def test_parse_auth_result_handles_z_suffix() -> None:
    """'Z' (Zulu) suffix must be accepted as UTC."""
    mgr = SFCLIManager()
    data = {
        "accessToken": "tok",
        "instanceUrl": "https://example.my.salesforce.com",
        "expirationDate": "2026-09-05T18:30:00Z",
    }
    result = mgr._parse_auth_result(data, alias="default")
    assert result.expires_at is not None
    assert result.expires_at.tzinfo is not None  # has a tz


def test_parse_auth_result_handles_missing_or_unparseable_expiry() -> None:
    """Unknown / unparseable expiry must not raise; result has expires_at=None.

    This is the second half of the bug fixed in 869db7b: when the
    field name is unknown (or value is garbage), the parser must
    yield expires_at=None so the cycle can be broken. The companion
    rule in StoredToken.is_expired trusts None and lets the request
    through.
    """
    mgr = SFCLIManager()

    # No expiry field at all
    no_field = {"accessToken": "t", "instanceUrl": "https://e"}
    assert mgr._parse_auth_result(no_field, "default").expires_at is None

    # Unparseable value
    bad_value = {
        "accessToken": "t",
        "instanceUrl": "https://e",
        "expirationDate": "not-a-date",
    }
    assert mgr._parse_auth_result(bad_value, "default").expires_at is None

    # Empty string
    empty = {
        "accessToken": "t",
        "instanceUrl": "https://e",
        "expirationDate": "",
    }
    assert mgr._parse_auth_result(empty, "default").expires_at is None


def test_parse_auth_result_requires_mandatory_fields() -> None:
    """If accessToken or instanceUrl is missing, raise SFCLIError."""
    from asftool.core.sf_cli import SFCLIError

    mgr = SFCLIManager()

    with pytest.raises(SFCLIError, match="Missing access token"):
        mgr._parse_auth_result({"instanceUrl": "https://e"}, "default")

    with pytest.raises(SFCLIError, match="Missing access token"):
        mgr._parse_auth_result({}, "default")

    with pytest.raises(SFCLIError, match="Missing access token"):
        mgr._parse_auth_result({"accessToken": ""}, "default")
