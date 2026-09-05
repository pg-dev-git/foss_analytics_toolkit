"""Unit tests for asftool.core.auth.token_store (StoredToken + TokenStore).

Focuses on the StoredToken.is_expired semantics, which had a critical
bug: returning True when expires_at was None caused the menu loop
to infinitely re-authenticate (see commit 869db7b).
"""

from datetime import UTC, datetime, timedelta

from asftool.core.auth.token_store import StoredToken


def test_is_expired_false_when_no_expires_at() -> None:
    """If we don't know the expiry, trust SF CLI — return False.

    The pre-fix behavior was: return True. That made the auth cycle
    unrecoverable: the only way out was a manual re-login. After the
    fix, an unknown expiry is treated as 'fresh enough to try'; if
    the actual API call gets 401, that's a real signal we can act on.
    """
    token = StoredToken(
        access_token="t",
        instance_url="https://example.my.salesforce.com",
        expires_at=None,
    )
    assert token.is_expired() is False


def test_is_expired_false_when_expires_at_unparseable() -> None:
    """A garbage expiry string is also 'unknown', not 'expired'."""
    token = StoredToken(
        access_token="t",
        instance_url="https://example.my.salesforce.com",
        expires_at="not-a-real-date",
    )
    assert token.is_expired() is False


def test_is_expired_false_when_expiry_in_future() -> None:
    """Future expiry + buffer → not expired."""
    future = (datetime.now(UTC) + timedelta(hours=1)).isoformat()
    token = StoredToken(
        access_token="t",
        instance_url="https://example.my.salesforce.com",
        expires_at=future,
    )
    assert token.is_expired() is False


def test_is_expired_true_when_expiry_in_past() -> None:
    """Past expiry → expired."""
    past = (datetime.now(UTC) - timedelta(hours=1)).isoformat()
    token = StoredToken(
        access_token="t",
        instance_url="https://example.my.salesforce.com",
        expires_at=past,
    )
    assert token.is_expired() is True


def test_is_expired_true_within_buffer_window() -> None:
    """Default 60-second buffer: token expiring in 30s is treated as expired."""
    soon = (datetime.now(UTC) + timedelta(seconds=30)).isoformat()
    token = StoredToken(
        access_token="t",
        instance_url="https://example.my.salesforce.com",
        expires_at=soon,
    )
    assert token.is_expired(buffer_seconds=60) is True
    # With zero buffer, it's not yet expired
    assert token.is_expired(buffer_seconds=0) is False


def test_is_expired_handles_z_suffix() -> None:
    """Z (Zulu) suffix must be parsed as UTC, not local."""
    # 1 hour in the future, expressed as Z-suffixed UTC string
    future_utc = datetime.now(UTC) + timedelta(hours=1)
    future_str = future_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
    token = StoredToken(
        access_token="t",
        instance_url="https://example.my.salesforce.com",
        expires_at=future_str,
    )
    assert token.is_expired() is False
