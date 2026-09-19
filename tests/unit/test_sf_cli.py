"""Unit tests for asftool.core.sf_cli (SF CLI subprocess wrapper).

These tests focus on the parts that don't require SF CLI to be
installed: result parsing and field-name handling. The subprocess
integration is covered by tests/integration/test_api_endpoints.py
where the client is mocked.
"""


import json
import pytest
from unittest.mock import AsyncMock, Mock, patch

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


# --- Tests for is_org_authenticated ---


def test_is_org_authenticated_returns_false_when_cli_not_available() -> None:
    """When SF CLI is not available, is_org_authenticated should return False."""
    mgr = SFCLIManager()
    with patch.object(mgr, "is_available", return_value=False):
        assert mgr.is_org_authenticated("default") is False
        assert mgr.is_org_authenticated("myorg") is False


def test_is_org_authenticated_returns_true_for_connected_org() -> None:
    """When org is in list with connectedStatus=Connected, return True."""
    mgr = SFCLIManager()
    with patch.object(mgr, "is_available", return_value=True):
        with patch.object(mgr, "list_orgs", return_value=[
            {"alias": "default", "connectedStatus": "Connected", "username": "user@example.com"},
            {"alias": "other", "connectedStatus": "Disconnected", "username": "other@example.com"},
        ]):
            assert mgr.is_org_authenticated("default") is True
            assert mgr.is_org_authenticated("other") is False


def test_is_org_authenticated_returns_false_for_missing_alias() -> None:
    """When alias not in list, return False."""
    mgr = SFCLIManager()
    with patch.object(mgr, "is_available", return_value=True):
        with patch.object(mgr, "list_orgs", return_value=[
            {"alias": "default", "connectedStatus": "Connected"},
        ]):
            assert mgr.is_org_authenticated("nonexistent") is False


def test_is_org_authenticated_returns_false_on_exception() -> None:
    """Any exception during check should return False (fail-safe)."""
    mgr = SFCLIManager()
    with patch.object(mgr, "is_available", return_value=True):
        with patch.object(mgr, "list_orgs", side_effect=Exception("CLI error")):
            assert mgr.is_org_authenticated("default") is False


@pytest.mark.asyncio
async def test_is_org_authenticated_async_returns_false_when_cli_not_available() -> None:
    """When SF CLI is not available, async version should return False."""
    mgr = SFCLIManager()
    with patch.object(mgr, "is_available", return_value=False):
        result = await mgr.is_org_authenticated_async("default")
        assert result is False


@pytest.mark.asyncio
async def test_is_org_authenticated_async_returns_true_for_connected_org() -> None:
    """When org is connected in async org list, return True."""
    mgr = SFCLIManager()
    with patch.object(mgr, "is_available", return_value=True):
        # Mock _run_command_async to return a successful org list
        mock_result = Mock()
        mock_result.stdout = '{"status": 0, "result": {"orgs": [{"alias": "default", "connectedStatus": "Connected", "username": "user@example.com"}]}}'
        with patch.object(mgr, "_run_command_async", new_callable=AsyncMock) as mock_run:
            mock_run.return_value = mock_result
            result = await mgr.is_org_authenticated_async("default")
            assert result is True


@pytest.mark.asyncio
async def test_is_org_authenticated_async_returns_false_for_missing_alias() -> None:
    """When alias not in async org list, return False."""
    mgr = SFCLIManager()
    with patch.object(mgr, "is_available", return_value=True):
        mock_result = Mock()
        mock_result.stdout = '{"status": 0, "result": {"orgs": [{"alias": "other", "connectedStatus": "Connected"}]}}'
        with patch.object(mgr, "_run_command_async", new_callable=AsyncMock) as mock_run:
            mock_run.return_value = mock_result
            result = await mgr.is_org_authenticated_async("default")
            assert result is False


@pytest.mark.asyncio
async def test_get_sf_cli_orgs_returns_connected_orgs() -> None:
    """get_sf_cli_orgs returns only connected orgs with expected fields."""
    mgr = SFCLIManager()
    with patch.object(mgr, "is_available", return_value=True):
        mock_result = Mock()
        mock_result.stdout = json.dumps({
            "status": 0,
            "result": {
                "orgs": [
                    {"alias": "prod", "username": "user@prod.com", "instanceUrl": "https://prod.my.salesforce.com", "connectedStatus": "Connected"},
                    {"alias": "dev", "username": "user@dev.com", "instanceUrl": "https://dev.my.salesforce.com", "connectedStatus": "Connected"},
                    {"alias": "old", "username": "user@old.com", "instanceUrl": "https://old.my.salesforce.com", "connectedStatus": "Disconnected"},
                ]
            }
        })
        with patch.object(mgr, "_run_command_async", new_callable=AsyncMock) as mock_run:
            mock_run.return_value = mock_result
            result = await mgr.get_sf_cli_orgs()
            
            assert len(result) == 2
            assert result[0]["alias"] == "prod"
            assert result[0]["username"] == "user@prod.com"
            assert result[0]["instance_url"] == "https://prod.my.salesforce.com"
            assert result[0]["connected_status"] == "Connected"
            assert result[1]["alias"] == "dev"
            # Disconnected org should be filtered out


@pytest.mark.asyncio
async def test_get_sf_cli_orgs_returns_empty_when_cli_not_available() -> None:
    """get_sf_cli_orgs returns empty list when SF CLI not available."""
    mgr = SFCLIManager()
    with patch.object(mgr, "is_available", return_value=False):
        result = await mgr.get_sf_cli_orgs()
        assert result == []


@pytest.mark.asyncio
async def test_get_sf_cli_orgs_returns_empty_on_exception() -> None:
    """get_sf_cli_orgs returns empty list on any exception."""
    mgr = SFCLIManager()
    with patch.object(mgr, "is_available", return_value=True):
        with patch.object(mgr, "_run_command_async", new_callable=AsyncMock) as mock_run:
            mock_run.side_effect = Exception("CLI error")
            result = await mgr.get_sf_cli_orgs()
            assert result == []
