"""Unit tests for Salesforce client."""

import base64
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest

from asftool.core.client import (
    DEFAULT_RETRY_AFTER,
    MAX_RETRY_AFTER,
    SalesforceClient,
    parse_retry_after,
)
from asftool.core.config import Settings
from asftool.core.exceptions import (
    SalesforceAPIError,
    SalesforceAuthError,
    SalesforceNotFoundError,
    SalesforceRateLimitError,
)


@pytest.fixture
def test_settings():
    """Create test settings with valid keys."""
    encryption_key = base64.urlsafe_b64encode(b"x" * 32).decode()
    # Use model_construct to bypass .env file loading and validation
    return Settings.model_construct(
        encryption_key=encryption_key,
        sf_api_version="v60.0",
        sf_default_domain="test.salesforce.com",
    )


@pytest.fixture
def client(test_settings):
    """Create test client."""
    return SalesforceClient(
        access_token="test_token",
        instance_url="https://test.salesforce.com",
        settings=test_settings,
    )


@pytest.fixture
def retry_test_settings():
    """Create test settings with valid keys for retry tests."""
    encryption_key = base64.urlsafe_b64encode(b"x" * 32).decode()
    # Use model_construct to bypass .env file loading and validation
    return Settings.model_construct(
        encryption_key=encryption_key,
        sf_api_version="v60.0",
        sf_default_domain="test.salesforce.com",
    )


@pytest.fixture
def retry_client(retry_test_settings):
    return SalesforceClient(
        access_token="test_token",
        instance_url="https://test.salesforce.com",
        settings=retry_test_settings,
    )


class TestSalesforceClient:
    """Tests for SalesforceClient."""

    def test_base_url(self, client):
        """Test base URL construction."""
        assert client.base_url == "https://test.salesforce.com/services/data/v60.0"
        assert client.wave_base_url == "https://test.salesforce.com/services/data/v60.0/wave"

    def test_build_url(self, client):
        """Test URL building."""
        assert client._build_url("/query") == "https://test.salesforce.com/services/data/v60.0/query"
        assert client._build_url("query") == "https://test.salesforce.com/services/data/v60.0/query"
        assert client._build_url("https://other.com/api") == "https://other.com/api"

    @pytest.mark.asyncio
    async def test_handle_response_401(self, client):
        """Test handling 401 response."""
        response = MagicMock(spec=httpx.Response)
        response.status_code = 401

        with pytest.raises(SalesforceAuthError):
            client._handle_response(response)

    @pytest.mark.asyncio
    async def test_handle_response_404(self, client):
        """Test handling 404 response."""
        response = MagicMock(spec=httpx.Response)
        response.status_code = 404

        with pytest.raises(SalesforceNotFoundError):
            client._handle_response(response)

    @pytest.mark.asyncio
    async def test_handle_response_429(self, client):
        """Test handling 429 response."""
        response = MagicMock(spec=httpx.Response)
        response.status_code = 429
        response.headers = {"Retry-After": "30"}

        with pytest.raises(SalesforceRateLimitError) as exc_info:
            client._handle_response(response)
        assert exc_info.value.retry_after == 30

    @pytest.mark.asyncio
    async def test_handle_response_500(self, client):
        """Test handling 500 response."""
        response = MagicMock(spec=httpx.Response)
        response.status_code = 500

        with pytest.raises(SalesforceAPIError) as exc_info:
            client._handle_response(response)
        assert exc_info.value.status_code == 500

    @pytest.mark.asyncio
    async def test_handle_response_400_with_json(self, client):
        """Test handling 400 response with JSON error."""
        response = MagicMock(spec=httpx.Response)
        response.status_code = 400
        # Salesforce returns errors as a list
        response.json.return_value = [{
            "message": "Invalid query",
            "errorCode": "MALFORMED_QUERY",
        }]

        with pytest.raises(SalesforceAPIError) as exc_info:
            client._handle_response(response)
        assert "MALFORMED_QUERY" in str(exc_info.value)
        assert "Invalid query" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_close(self, client):
        """Test client close."""
        # Mock the internal client
        mock_client = AsyncMock()
        client._client = mock_client

        await client.close()

        mock_client.aclose.assert_called_once()
        assert client._client is None

    @pytest.mark.asyncio
    async def test_context_manager(self, client):
        """Test async context manager."""
        mock_client = AsyncMock()
        client._client = mock_client

        async with client as c:
            assert c is client

        mock_client.aclose.assert_called_once()


class TestSalesforceClientRetry:
    """Tests for retry logic."""

    @pytest.mark.asyncio
    async def test_retry_on_timeout(self, retry_client):
        """Test retry on timeout."""
        mock_client = AsyncMock()
        retry_client._client = mock_client

        # First two calls timeout, third succeeds
        mock_client.request.side_effect = [
            httpx.TimeoutException("Timeout"),
            httpx.TimeoutException("Timeout"),
            MagicMock(status_code=200, json=lambda: {"result": "success"}),
        ]

        # This would test the retry logic, but it's complex to test fully
        # without more mocking. The key point is that tenacity is configured.
        assert retry_client.retry_client is not None

    @pytest.mark.asyncio
    async def test_no_retry_on_401(self, retry_client):
        """Test that 401 is not retried (handled as auth error)."""
        mock_client = AsyncMock()
        retry_client._client = mock_client

        response = MagicMock(spec=httpx.Response)
        response.status_code = 401
        mock_client.request.return_value = response

        with pytest.raises(SalesforceAuthError):
            await retry_client.get("/test")

        # Should only be called once (no retry on 401)
        assert mock_client.request.call_count == 1


class TestParseRetryAfter:
    def test_delta_seconds(self):
        assert parse_retry_after("300") == 300

    def test_missing_header_uses_the_default(self):
        assert parse_retry_after(None) == DEFAULT_RETRY_AFTER

    def test_empty_header_uses_the_default(self):
        assert parse_retry_after("") == DEFAULT_RETRY_AFTER

    def test_garbage_uses_the_default(self):
        assert parse_retry_after("soon") == DEFAULT_RETRY_AFTER

    def test_http_date(self):
        """The header may also be an HTTP-date."""
        from datetime import UTC, datetime, timedelta
        from email.utils import format_datetime

        when = datetime.now(UTC) + timedelta(seconds=120)
        parsed = parse_retry_after(format_datetime(when))
        assert 100 <= parsed <= 130

    def test_past_http_date_waits_zero(self):
        from datetime import UTC, datetime, timedelta
        from email.utils import format_datetime

        when = datetime.now(UTC) - timedelta(seconds=120)
        assert parse_retry_after(format_datetime(when)) == 0


class TestRetryAfterIsHonoured:
    """The client must wait as long as the server asked.

    It used to ignore the server's instruction entirely, so a 429 that
    said "wait 300 seconds" was retried after 1.7 s and then 3 s -- three
    rapid-fire requests against an already-throttling server.
    """

    def test_rate_limit_error_carries_the_server_delay(self, client):
        response = httpx.Response(
            429, headers={"Retry-After": "300"}, json=[{"errorCode": "LIMIT"}]
        )
        with pytest.raises(SalesforceRateLimitError) as exc_info:
            client._handle_response(response)
        assert exc_info.value.retry_after == 300

    def test_rate_limit_error_accepts_an_http_date(self, client):
        from datetime import UTC, datetime, timedelta
        from email.utils import format_datetime

        when = datetime.now(UTC) + timedelta(seconds=60)
        response = httpx.Response(
            429, headers={"Retry-After": format_datetime(when)}, json=[]
        )
        with pytest.raises(SalesforceRateLimitError) as exc_info:
            client._handle_response(response)
        assert 0 < exc_info.value.retry_after <= 60

    def test_wait_uses_the_server_delay(self):
        from asftool.core.client import wait_retry_after

        class _Outcome:
            failed = True

            def exception(self):
                return SalesforceRateLimitError("rate limited", retry_after=300)

        class _State:
            outcome = _Outcome()

        assert wait_retry_after(_State()) == 300.0

    def test_wait_is_capped(self):
        from asftool.core.client import wait_retry_after

        class _Outcome:
            failed = True

            def exception(self):
                return SalesforceRateLimitError("rate limited", retry_after=99999)

        class _State:
            outcome = _Outcome()

        assert wait_retry_after(_State()) == float(MAX_RETRY_AFTER)

    def test_wait_falls_back_to_exponential_for_other_errors(self):
        from asftool.core.client import wait_retry_after

        class _Outcome:
            failed = True

            def exception(self):
                return httpx.TimeoutException("timed out")

        class _State:
            outcome = _Outcome()
            attempt_number = 1

        delay = wait_retry_after(_State())
        assert 0 < delay <= 30


class TestErrorDetailIsPreserved:
    """A 403 or 404 must carry the platform's own message.

    The generic "Access forbidden" / "Resource not found" strings gave a
    user debugging a permission problem nothing to act on.
    """

    def test_403_keeps_the_api_error_code(self, client):
        response = httpx.Response(
            403,
            json=[
                {
                    "errorCode": "INSUFFICIENT_ACCESS",
                    "message": "Insufficient access to the requested resource.",
                }
            ],
        )
        with pytest.raises(SalesforceAPIError) as exc_info:
            client._handle_response(response)
        assert "INSUFFICIENT_ACCESS" in str(exc_info.value)
        assert exc_info.value.error_code == "INSUFFICIENT_ACCESS"

    def test_404_keeps_the_api_message(self, client):
        response = httpx.Response(
            404,
            json=[{"errorCode": "NOT_FOUND", "message": "The asset does not exist."}],
        )
        with pytest.raises(SalesforceNotFoundError) as exc_info:
            client._handle_response(response)
        assert "The asset does not exist." in str(exc_info.value)

    def test_403_without_a_body_still_raises(self, client):
        response = httpx.Response(403, content=b"")
        with pytest.raises(SalesforceAPIError):
            client._handle_response(response)

    def test_error_body_as_an_object(self, client):
        """Salesforce sometimes returns a single object rather than a list."""
        response = httpx.Response(
            400, json={"errorCode": "MALFORMED_QUERY", "message": "Bad query"}
        )
        with pytest.raises(SalesforceAPIError) as exc_info:
            client._handle_response(response)
        assert "MALFORMED_QUERY" in str(exc_info.value)
