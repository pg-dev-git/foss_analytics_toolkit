"""Tests for the forced token refresh and its wiring into the client.

``SFCLIAuthService.get_access_token`` returns the *stored* token whenever
it has not yet expired. That is the right behaviour when starting a
session, but it is the wrong one after the platform has just rejected
that token with a 401: the store still believes the token is good, so it
would hand back the very token that just failed.

``refresh_access_token`` exists to force a refresh regardless of what the
store thinks.
"""

import base64
from unittest.mock import AsyncMock, MagicMock

import pytest

from asftool.core.auth.sf_cli_auth import SFCLIAuthError, SFCLIAuthService
from asftool.core.auth.token_store import StoredToken
from asftool.core.config import Settings
from asftool.core.exceptions import TokenRefreshError
from asftool.core.sf_cli import SFCLIAuthResult


@pytest.fixture
def settings():
    """Settings that do not depend on the developer's .env file."""
    encryption_key = base64.urlsafe_b64encode(b"x" * 32).decode()
    return Settings.model_construct(
        encryption_key=encryption_key,
        sf_api_version="v60.0",
        sf_default_domain="test.salesforce.com",
    )


def build_service(settings, sf_cli):
    """Build an auth service with a stubbed SF CLI and token store."""
    service = SFCLIAuthService(settings, crypto_manager=None, sf_cli_manager=sf_cli)
    service.token_store = MagicMock()
    return service


def make_stored_token() -> StoredToken:
    """A stored token that the store considers perfectly fresh."""
    return StoredToken(
        access_token="stale_token",
        instance_url="https://example.my.salesforce.com",
        alias="default",
        username="user@example.com",
    )


class TestRefreshAccessToken:
    """The forced refresh."""

    async def test_it_returns_the_new_token(self, settings):
        sf_cli = MagicMock()
        sf_cli.refresh_token = AsyncMock(
            return_value=SFCLIAuthResult(
                access_token="fresh_token",
                instance_url="https://example.my.salesforce.com",
                alias="default",
                username="user@example.com",
            )
        )
        service = build_service(settings, sf_cli)
        service.token_store.load_token = AsyncMock(return_value=make_stored_token())
        service.token_store.save_token = AsyncMock()

        token = await service.refresh_access_token(alias="default")

        assert token == "fresh_token"

    async def test_it_forces_the_sf_cli_to_refresh(self, settings):
        sf_cli = MagicMock()
        sf_cli.refresh_token = AsyncMock(
            return_value=SFCLIAuthResult(
                access_token="fresh_token",
                instance_url="https://example.my.salesforce.com",
                alias="default",
            )
        )
        service = build_service(settings, sf_cli)
        service.token_store.load_token = AsyncMock(return_value=make_stored_token())
        service.token_store.save_token = AsyncMock()

        await service.refresh_access_token(alias="default")

        sf_cli.refresh_token.assert_awaited_once_with("default")

    async def test_it_stores_the_new_token(self, settings):
        """The next session must start with the refreshed token."""
        sf_cli = MagicMock()
        sf_cli.refresh_token = AsyncMock(
            return_value=SFCLIAuthResult(
                access_token="fresh_token",
                instance_url="https://example.my.salesforce.com",
                alias="default",
                username="user@example.com",
            )
        )
        service = build_service(settings, sf_cli)
        service.token_store.load_token = AsyncMock(return_value=make_stored_token())
        service.token_store.save_token = AsyncMock()

        await service.refresh_access_token(alias="default")

        service.token_store.save_token.assert_awaited_once()
        stored = service.token_store.save_token.await_args.args[0]
        assert stored.access_token == "fresh_token"

    async def test_it_does_not_trust_the_stored_token(self, settings):
        """The stored token is fresh, but it is the one that just failed."""
        sf_cli = MagicMock()
        sf_cli.refresh_token = AsyncMock(
            return_value=SFCLIAuthResult(
                access_token="fresh_token",
                instance_url="https://example.my.salesforce.com",
                alias="default",
            )
        )
        service = build_service(settings, sf_cli)
        service.token_store.load_token = AsyncMock(return_value=make_stored_token())
        service.token_store.save_token = AsyncMock()

        token = await service.refresh_access_token(alias="default")

        assert token != "stale_token"

    async def test_a_failed_refresh_is_reported(self, settings):
        """A refresh failure must be an error, not a silent fallback."""
        sf_cli = MagicMock()
        sf_cli.refresh_token = AsyncMock(side_effect=RuntimeError("SF CLI missing"))
        service = build_service(settings, sf_cli)
        service.token_store.load_token = AsyncMock(return_value=make_stored_token())
        service.token_store.save_token = AsyncMock()

        with pytest.raises(TokenRefreshError) as exc_info:
            await service.refresh_access_token(alias="default")

        assert "SF CLI missing" in str(exc_info.value)

    async def test_a_failed_refresh_stores_nothing(self, settings):
        sf_cli = MagicMock()
        sf_cli.refresh_token = AsyncMock(side_effect=RuntimeError("boom"))
        service = build_service(settings, sf_cli)
        service.token_store.load_token = AsyncMock(return_value=make_stored_token())
        service.token_store.save_token = AsyncMock()

        with pytest.raises(TokenRefreshError):
            await service.refresh_access_token(alias="default")

        service.token_store.save_token.assert_not_awaited()

    async def test_no_stored_token_is_an_error(self, settings):
        """There is nothing to refresh, and no alias to refresh it for."""
        sf_cli = MagicMock()
        sf_cli.refresh_token = AsyncMock()
        service = build_service(settings, sf_cli)
        service.token_store.load_token = AsyncMock(return_value=None)

        with pytest.raises(SFCLIAuthError):
            await service.refresh_access_token(alias="default")

    async def test_the_instance_url_is_kept_when_the_refresh_omits_it(self, settings):
        """A partial refresh result must not lose the instance URL."""
        sf_cli = MagicMock()
        sf_cli.refresh_token = AsyncMock(
            return_value=SFCLIAuthResult(
                access_token="fresh_token",
                instance_url="",
                alias="default",
            )
        )
        service = build_service(settings, sf_cli)
        service.token_store.load_token = AsyncMock(return_value=make_stored_token())
        service.token_store.save_token = AsyncMock()

        await service.refresh_access_token(alias="default")

        stored = service.token_store.save_token.await_args.args[0]
        assert stored.instance_url == "https://example.my.salesforce.com"

    async def test_the_token_is_stored_under_the_requested_alias(self, settings):
        """The SF CLI can report a different alias than the one requested.

        The SF CLI reports the alias the org was first authorised under.
        A user who runs the tool with ``--alias default`` therefore gets
        a refresh result carrying the org's own alias. Storing the token
        under *that* alias would put it under a key the next lookup never
        reads, and the session would look "not authenticated" again.
        """
        sf_cli = MagicMock()
        sf_cli.refresh_token = AsyncMock(
            return_value=SFCLIAuthResult(
                access_token="fresh_token",
                instance_url="https://example.my.salesforce.com",
                alias="some-other-alias",
            )
        )
        service = build_service(settings, sf_cli)
        service.token_store.load_token = AsyncMock(return_value=make_stored_token())
        service.token_store.save_token = AsyncMock()

        await service.refresh_access_token(alias="default")

        stored = service.token_store.save_token.await_args.args[0]
        assert stored.alias == "default"


class TestSessionWiring:
    """The CLI session must give the client a way to refresh."""

    async def test_the_client_gets_a_refresher(self):
        """A client built by the session can recover from a 401."""
        from asftool.cli.session import Session

        session = Session(alias="default")
        auth = MagicMock()
        auth.get_access_token = AsyncMock(return_value="stale_token")
        auth.get_instance_url = AsyncMock(return_value="https://example.my.salesforce.com")
        auth.refresh_access_token = AsyncMock(return_value="fresh_token")
        session._auth_service = auth

        client = await session.get_client()

        assert client._token_refresher is not None
        assert await client._token_refresher() == "fresh_token"
        auth.refresh_access_token.assert_awaited_once_with(alias="default")

    async def test_the_refresher_uses_the_session_alias(self):
        from asftool.cli.session import Session

        session = Session(alias="myorg")
        auth = MagicMock()
        auth.get_access_token = AsyncMock(return_value="stale_token")
        auth.get_instance_url = AsyncMock(return_value="https://example.my.salesforce.com")
        auth.refresh_access_token = AsyncMock(return_value="fresh_token")
        session._auth_service = auth

        client = await session.get_client()
        await client._token_refresher()

        auth.refresh_access_token.assert_awaited_once_with(alias="myorg")
