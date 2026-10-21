"""Unit tests for asftool.core.auth.token_store (StoredToken + TokenStore).

Focuses on the StoredToken.is_expired semantics, which had a critical
bug: returning True when expires_at was None caused the menu loop
to infinitely re-authenticate (see commit 869db7b).
"""

from datetime import UTC, datetime, timedelta

import pytest

from asftool.core.auth.token_store import (
    MAX_TOKEN_AGE,
    StoredToken,
    TokenStore,
)
from asftool.core.exceptions import TokenRefreshError, TokenStoreUnavailableError


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


# ---------------------------------------------------------------------------
# Observation window
#
# The SF CLI does not report a token expiry: `sf org display` has no such
# field and the access token is opaque rather than a decodable JWT. Without
# a fallback the whole refresh machinery was unreachable.
# ---------------------------------------------------------------------------


def test_observed_at_is_set_on_creation() -> None:
    token = StoredToken(access_token="t", instance_url="https://example.com")
    assert token.observed_at is not None


def test_fresh_token_without_expiry_is_not_expired() -> None:
    """A token just observed is within the window, so it is usable."""
    token = StoredToken(access_token="t", instance_url="https://example.com")
    assert token.is_expired() is False


def test_stale_token_without_expiry_is_expired() -> None:
    """A token observed longer ago than the window is treated as expired."""
    observed = datetime.now(UTC) - timedelta(seconds=MAX_TOKEN_AGE + 60)
    token = StoredToken(
        access_token="t",
        instance_url="https://example.com",
        observed_at=observed.isoformat(),
    )
    assert token.is_expired() is True


def test_explicit_expiry_takes_precedence_over_observation_window() -> None:
    """A platform-provided expiry is authoritative."""
    observed = datetime.now(UTC) - timedelta(seconds=MAX_TOKEN_AGE + 60)
    future = datetime.now(UTC) + timedelta(hours=1)
    token = StoredToken(
        access_token="t",
        instance_url="https://example.com",
        expires_at=future.isoformat(),
        observed_at=observed.isoformat(),
    )
    assert token.is_expired() is False


def test_token_with_neither_expiry_nor_observation_is_not_expired() -> None:
    """Nothing to go on -- trust it rather than loop forever."""
    token = StoredToken(
        access_token="t",
        instance_url="https://example.com",
        observed_at="",
    )
    token.observed_at = None
    assert token.is_expired() is False


# ---------------------------------------------------------------------------
# Keyring unavailable
# ---------------------------------------------------------------------------


def _store() -> TokenStore:
    class _NullCrypto:
        def encrypt(self, data: str):  # pragma: no cover - not used here
            raise NotImplementedError

        def decrypt(self, data):  # pragma: no cover - not used here
            raise NotImplementedError

    return TokenStore(_NullCrypto())  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_load_token_raises_on_missing_keyring(monkeypatch) -> None:
    """A headless host must get an actionable error, not a traceback."""
    import keyring

    def _raise(*args, **kwargs):
        raise keyring.errors.NoKeyringError("No recommended backend was available")

    monkeypatch.setattr(keyring, "get_password", _raise)

    with pytest.raises(TokenStoreUnavailableError) as exc_info:
        await _store().load_token("default")
    assert "keyrings.alt" in str(exc_info.value)


@pytest.mark.asyncio
async def test_delete_token_raises_on_missing_keyring(monkeypatch) -> None:
    import keyring

    def _raise(*args, **kwargs):
        raise keyring.errors.NoKeyringError("No recommended backend was available")

    monkeypatch.setattr(keyring, "delete_password", _raise)

    with pytest.raises(TokenStoreUnavailableError):
        await _store().delete_token("default")


@pytest.mark.asyncio
async def test_delete_token_returns_false_when_entry_absent(monkeypatch) -> None:
    """A missing entry is not an error."""
    import keyring

    def _raise(*args, **kwargs):
        raise keyring.errors.PasswordDeleteError("No such password")

    monkeypatch.setattr(keyring, "delete_password", _raise)

    assert await _store().delete_token("default") is False


# ---------------------------------------------------------------------------
# Refresh failure is not "no token"
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_refresh_failure_raises_instead_of_returning_none(monkeypatch) -> None:
    """A failed refresh must not be mistaken for an empty store.

    Returning None made the caller start an interactive browser login --
    which the user never asked for and which cannot work headless.
    """

    from asftool.core.auth.token_store import StoredToken as _Token

    stale = _Token(
        access_token="old",
        instance_url="https://example.com",
        observed_at=(datetime.now(UTC) - timedelta(seconds=MAX_TOKEN_AGE + 60)).isoformat(),
    )

    class _FakeCrypto:
        def encrypt(self, data: str):
            raise NotImplementedError

        def decrypt(self, data):
            raise NotImplementedError

    store = TokenStore(_FakeCrypto())  # type: ignore[arg-type]

    async def _load(alias: str = "default"):
        return stale

    monkeypatch.setattr(store, "load_token", _load)

    class _FailingManager:
        async def refresh_token(self, alias: str):
            raise RuntimeError("SF CLI unavailable")

    with pytest.raises(TokenRefreshError) as exc_info:
        await store.get_valid_token("default", _FailingManager())  # type: ignore[arg-type]
    assert "asftool auth login" in str(exc_info.value)


@pytest.mark.asyncio
async def test_no_token_returns_none(monkeypatch) -> None:
    """No token at all is still None, not an error."""
    store = TokenStore(None)  # type: ignore[arg-type]

    async def _load(alias: str = "default"):
        return None

    monkeypatch.setattr(store, "load_token", _load)

    class _Manager:
        async def refresh_token(self, alias: str):  # pragma: no cover
            raise AssertionError("should not refresh when there is no token")

    assert await store.get_valid_token("default", _Manager()) is None  # type: ignore[arg-type]
