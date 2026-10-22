"""Tests for the client's 401 refresh-and-replay behaviour.

A 401 used to be a hard failure: the client raised
``SalesforceAuthError`` immediately, even when the token was one minute
old and a refresh would have recovered the session.

These tests drive the real request path through an ``httpx.MockTransport``,
so no network is involved and the assertions are about what actually went
out on the wire.
"""

import asyncio
import base64

import httpx
import pytest

from asftool.core.client import SalesforceClient
from asftool.core.config import Settings
from asftool.core.exceptions import SalesforceAuthError

DATASET_ID = "0Fb010000000000000"


@pytest.fixture
def settings():
    """Settings that do not depend on the developer's .env file."""
    encryption_key = base64.urlsafe_b64encode(b"x" * 32).decode()
    return Settings.model_construct(
        encryption_key=encryption_key,
        sf_api_version="v60.0",
        sf_default_domain="test.salesforce.com",
    )


def build_client(settings, handler, token_refresher=None):
    """Build a client whose HTTP layer is a mock transport."""
    transport = httpx.MockTransport(handler)
    http_client = httpx.AsyncClient(
        transport=transport,
        headers={"Authorization": "Bearer stale_token"},
    )
    return SalesforceClient(
        access_token="stale_token",
        instance_url="https://test.salesforce.com",
        settings=settings,
        token_refresher=token_refresher,
        http_client=http_client,
    )


class TestRefreshAndReplay:
    """A 401 must lead to one refresh and one replay."""

    async def test_a_401_is_refreshed_and_the_request_replayed(self, settings):
        """The headline case: 401 -> refresh -> 200."""
        seen: list[str] = []

        def handler(request: httpx.Request) -> httpx.Response:
            seen.append(request.headers.get("Authorization", ""))
            if len(seen) == 1:
                return httpx.Response(401, json=[{"errorCode": "INVALID_SESSION_ID"}])
            return httpx.Response(200, json={"datasets": []})

        refreshes = []

        async def refresher() -> str:
            refreshes.append("called")
            return "fresh_token"

        client = build_client(settings, handler, refresher)

        response = await client.get("/wave/datasets")

        assert response.status_code == 200
        assert refreshes == ["called"], "the token must be refreshed exactly once"
        assert seen == ["Bearer stale_token", "Bearer fresh_token"], (
            "the replay must carry the new token"
        )
        assert client.access_token == "fresh_token"

    async def test_the_refresh_happens_exactly_once(self, settings):
        """Two 401s in a row must not cause two refreshes."""
        responses = [401, 401]

        def handler(request: httpx.Request) -> httpx.Response:
            status = responses.pop(0) if responses else 200
            return httpx.Response(status, json=[])

        refreshes = []

        async def refresher() -> str:
            refreshes.append("called")
            return "fresh_token"

        client = build_client(settings, handler, refresher)

        with pytest.raises(SalesforceAuthError):
            await client.get("/wave/datasets")

        assert len(refreshes) == 1, (
            f"expected exactly one refresh, got {len(refreshes)}"
        )

    async def test_a_successful_replay_returns_the_body(self, settings):
        def handler(request: httpx.Request) -> httpx.Response:
            if request.headers.get("Authorization") == "Bearer fresh_token":
                return httpx.Response(200, json={"datasets": [{"id": DATASET_ID}]})
            return httpx.Response(401, json=[])

        async def refresher() -> str:
            return "fresh_token"

        client = build_client(settings, handler, refresher)
        body = (await client.get("/wave/datasets")).json()
        assert body == {"datasets": [{"id": DATASET_ID}]}


class TestUnsafeRequestsAreNotReplayed:
    """A request that could do the work twice must not be repeated."""

    async def test_a_post_is_not_replayed(self, settings):
        """Replaying a dataflow start could start it twice."""
        calls = []

        def handler(request: httpx.Request) -> httpx.Response:
            calls.append(request.method)
            return httpx.Response(401, json=[])

        refreshes = []

        async def refresher() -> str:
            refreshes.append("called")
            return "fresh_token"

        client = build_client(settings, handler, refresher)

        with pytest.raises(SalesforceAuthError):
            await client.post("/wave/dataflowjobs", json={"dataflowId": "0Fb"})

        assert calls == ["POST"], "the POST must be sent exactly once"
        assert refreshes == ["called"], "the token is still refreshed"

    async def test_a_patch_is_not_replayed(self, settings):
        """The PATCH processes a data-manager job; repeating it is not safe."""
        calls = []

        def handler(request: httpx.Request) -> httpx.Response:
            calls.append(request.method)
            return httpx.Response(401, json=[])

        async def refresher() -> str:
            return "fresh_token"

        client = build_client(settings, handler, refresher)

        with pytest.raises(SalesforceAuthError):
            await client.patch("/sobjects/InsightsExternalData/0Fb030000000000AAA", json={})

        assert calls == ["PATCH"]

    async def test_a_delete_of_a_collection_is_not_replayed(self, settings):
        """A DELETE without an asset id could hit whatever exists at the time."""
        calls = []

        def handler(request: httpx.Request) -> httpx.Response:
            calls.append(request.method)
            return httpx.Response(401, json=[])

        async def refresher() -> str:
            return "fresh_token"

        client = build_client(settings, handler, refresher)

        with pytest.raises(SalesforceAuthError):
            await client.delete("/wave/datasets")

        assert calls == ["DELETE"]

    async def test_a_delete_of_an_asset_is_replayed(self, settings):
        """A DELETE aimed at one specific id is safe to repeat."""
        calls = []

        def handler(request: httpx.Request) -> httpx.Response:
            calls.append(request.method)
            if len(calls) == 1:
                return httpx.Response(401, json=[])
            return httpx.Response(204)

        async def refresher() -> str:
            return "fresh_token"

        client = build_client(settings, handler, refresher)

        response = await client.delete(f"/wave/datasets/{DATASET_ID}")

        assert response.status_code == 204
        assert calls == ["DELETE", "DELETE"]


class TestWithoutARefresher:
    """A client with no refresher must behave exactly as before."""

    async def test_a_401_still_raises(self, settings):
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(401, json=[])

        client = build_client(settings, handler)

        with pytest.raises(SalesforceAuthError):
            await client.get("/wave/datasets")

    async def test_the_request_is_sent_only_once(self, settings):
        calls = []

        def handler(request: httpx.Request) -> httpx.Response:
            calls.append(request.method)
            return httpx.Response(401, json=[])

        client = build_client(settings, handler)

        with pytest.raises(SalesforceAuthError):
            await client.get("/wave/datasets")

        assert calls == ["GET"]


class TestWhenTheRefreshFails:
    """A failed refresh must not be papered over."""

    async def test_a_failing_refresher_leaves_the_401_raised(self, settings):
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(401, json=[])

        async def refresher() -> str:
            raise RuntimeError("the SF CLI is not installed")

        client = build_client(settings, handler, refresher)

        with pytest.raises(SalesforceAuthError):
            await client.get("/wave/datasets")

    async def test_a_failing_refresher_does_not_replay(self, settings):
        """A stale token is not retried; that is how a loop starts."""
        calls = []

        def handler(request: httpx.Request) -> httpx.Response:
            calls.append(request.method)
            return httpx.Response(401, json=[])

        async def refresher() -> str:
            raise RuntimeError("boom")

        client = build_client(settings, handler, refresher)

        with pytest.raises(SalesforceAuthError):
            await client.get("/wave/datasets")

        assert calls == ["GET"], "the request must not be replayed with a stale token"

    async def test_an_empty_token_is_treated_as_a_failure(self, settings):
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(401, json=[])

        async def refresher() -> str:
            return ""

        client = build_client(settings, handler, refresher)

        with pytest.raises(SalesforceAuthError):
            await client.get("/wave/datasets")

        assert client.access_token == "stale_token"


class TestConcurrentRequestsShareOneRefresh:
    """Concurrent 401s must not each trigger their own refresh."""

    async def test_concurrent_401s_refresh_once(self, settings):
        """Ten requests that all get a 401 produce exactly one refresh."""
        request_count = 0
        refreshes = []

        async def handler(request: httpx.Request) -> httpx.Response:
            nonlocal request_count
            request_count += 1
            # Every request fails with a 401 until the token is fresh.
            if request.headers.get("Authorization") != "Bearer fresh_token":
                await asyncio.sleep(0)
                return httpx.Response(401, json=[])
            return httpx.Response(200, json=[])

        async def refresher() -> str:
            refreshes.append("called")
            await asyncio.sleep(0.01)
            return "fresh_token"

        client = build_client(settings, handler, refresher)

        results = await asyncio.gather(
            *(client.get("/wave/datasets") for _ in range(10))
        )

        assert all(r.status_code == 200 for r in results)
        assert len(refreshes) == 1, (
            f"expected one shared refresh, got {len(refreshes)}"
        )

    async def test_the_replayed_requests_carry_the_new_token(self, settings):
        tokens: list[str] = []

        async def handler(request: httpx.Request) -> httpx.Response:
            token = request.headers.get("Authorization", "")
            tokens.append(token)
            if token != "Bearer fresh_token":
                return httpx.Response(401, json=[])
            return httpx.Response(200, json=[])

        async def refresher() -> str:
            await asyncio.sleep(0.01)
            return "fresh_token"

        client = build_client(settings, handler, refresher)

        await asyncio.gather(*(client.get("/wave/datasets") for _ in range(5)))

        assert tokens.count("Bearer fresh_token") == 5, (
            "every replay must use the refreshed token"
        )


class TestInjectedHttpClient:
    """The injected client is the caller's to close."""

    async def test_close_does_not_close_an_injected_client(self, settings):
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json=[])

        client = build_client(settings, handler)
        http_client = client.client

        await client.close()

        assert http_client.is_closed is False
        await http_client.aclose()

    async def test_a_client_we_own_is_closed(self, settings):
        client = SalesforceClient(
            access_token="token",
            instance_url="https://test.salesforce.com",
            settings=settings,
        )
        http_client = client.client

        await client.close()

        assert http_client.is_closed is True
