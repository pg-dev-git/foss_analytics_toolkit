"""Async HTTP client for Salesforce API with retry logic and circuit breaker."""

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Any, Optional

import httpx
import structlog
from tenacity import (
    AsyncRetrying,
    after_log,
    before_sleep_log,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential_jitter,
)

from asftool.core.config import Settings, get_settings
from asftool.core.crypto import CryptoManager
from asftool.core.exceptions import (
    SalesforceAPIError,
    SalesforceAuthError,
    SalesforceNotFoundError,
    SalesforceRateLimitError,
)
from asftool.core.pagination import build_page_params

logger = structlog.get_logger(__name__)

#: The longest we will honour a server's Retry-After for. A server asking
#: for an hour is not a situation the CLI should sit through silently.
MAX_RETRY_AFTER = 900  # seconds

#: Fallback delay when the server says nothing useful.
DEFAULT_RETRY_AFTER = 60  # seconds


def parse_retry_after(value: str | None) -> int:
    """Parse a ``Retry-After`` header into seconds.

    The header has two forms: delta-seconds (``"300"``) and an HTTP-date
    (``"Wed, 21 Oct 2026 07:28:00 GMT"``). Salesforce uses both.

    Returns:
        The number of seconds to wait, or ``DEFAULT_RETRY_AFTER`` when the
        value is missing or unusable.
    """
    if not value:
        return DEFAULT_RETRY_AFTER

    value = value.strip()
    if value.isdigit():
        return int(value)

    try:
        from email.utils import parsedate_to_datetime

        when = parsedate_to_datetime(value)
        if when is None:
            return DEFAULT_RETRY_AFTER
        if when.tzinfo is None:
            when = when.replace(tzinfo=UTC)
        seconds = (when - datetime.now(UTC)).total_seconds()
        return max(0, int(seconds))
    except (TypeError, ValueError):
        return DEFAULT_RETRY_AFTER


def wait_retry_after(retry_state: Any) -> float:
    """Wait strategy that honours the server's ``Retry-After``.

    The default exponential backoff ignored the delay the server asked for,
    so a 429 that said "wait 300 seconds" was retried after 1.7 s and then
    3 s -- three rapid-fire requests against an already-throttling server.

    The wait is capped at :data:`MAX_RETRY_AFTER` so a server cannot park
    the CLI indefinitely.
    """
    outcome = retry_state.outcome
    if outcome is not None and outcome.failed:
        exception = outcome.exception()
        if isinstance(exception, SalesforceRateLimitError):
            delay = min(exception.retry_after, MAX_RETRY_AFTER)
            logger.warning(
                "rate_limit_backoff",
                retry_after=exception.retry_after,
                waiting_for=delay,
            )
            return float(max(0, delay))
    return wait_exponential_jitter(initial=1, max=30)(retry_state)


class SalesforceClient:
    """Async HTTP client for Salesforce REST API with resilience patterns."""

    def __init__(
        self,
        access_token: str,
        instance_url: str,
        settings: Settings | None = None,
    ):
        """Initialize the client with authentication and configuration.

        Args:
            access_token: OAuth access token for authentication
            instance_url: Salesforce instance URL (e.g., https://na100.salesforce.com)
            settings: Optional settings override
        """
        self.access_token = access_token
        self.instance_url = instance_url.rstrip("/")
        self.settings = settings or get_settings()
        self._client: httpx.AsyncClient | None = None
        self._retry_client: AsyncRetrying | None = None

    @property
    def base_url(self) -> str:
        """Get the base API URL for this instance."""
        return f"{self.instance_url}/services/data/{self.settings.sf_api_version}"

    @property
    def wave_base_url(self) -> str:
        """Get the Wave/Analytics API base URL."""
        return f"{self.base_url}/wave"

    @property
    def client(self) -> httpx.AsyncClient:
        """Get or create the underlying HTTP client."""
        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(
                    connect=10.0,
                    read=60.0,
                    write=30.0,
                    pool=10.0,
                ),
                limits=httpx.Limits(
                    max_connections=10,
                    max_keepalive_connections=5,
                    keepalive_expiry=30.0,
                ),
                headers={
                    "Authorization": f"Bearer {self.access_token}",
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                    "User-Agent": f"{self.settings.app_name}/{self.settings.app_version}",
                },
                follow_redirects=True,
            )
        return self._client

    @property
    def retry_client(self) -> AsyncRetrying:
        """Get or create the retry client with configured policies."""
        if self._retry_client is None:
            self._retry_client = AsyncRetrying(
                wait=wait_retry_after,
                stop=stop_after_attempt(3),
                retry=retry_if_exception_type((
                    httpx.TimeoutException,
                    httpx.NetworkError,
                    httpx.RemoteProtocolError,
                    SalesforceRateLimitError,
                )),
                before_sleep=before_sleep_log(logger, logging.WARNING),
                after=after_log(logger, logging.INFO),
                reraise=True,
            )
        return self._retry_client

    async def close(self) -> None:
        """Close the underlying HTTP client."""
        if self._client:
            await self._client.aclose()
            self._client = None

    async def __aenter__(self) -> "SalesforceClient":
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.close()

    def _build_url(self, path: str) -> str:
        """Build full URL from path."""
        if path.startswith("http"):
            return path
        if path.startswith("/"):
            return f"{self.base_url}{path}"
        return f"{self.base_url}/{path}"

    @staticmethod
    def _parse_error_detail(response: httpx.Response) -> tuple[str, str]:
        """Extract ``(error_code, message)`` from a Salesforce error body.

        Salesforce returns errors either as a list of objects or as a single
        object, and not every error carries both fields.
        """
        try:
            error_data = response.json()
        except Exception:
            return ("UNKNOWN_ERROR", f"Request failed: {response.status_code}")

        if isinstance(error_data, list) and error_data:
            error = error_data[0]
        elif isinstance(error_data, dict):
            error = error_data
        else:
            return ("UNKNOWN_ERROR", f"Request failed: {response.status_code}")

        if not isinstance(error, dict):
            return ("UNKNOWN_ERROR", f"Request failed: {response.status_code}")

        error_code = error.get("errorCode", "UNKNOWN_ERROR")
        error_msg = error.get("message", "Unknown error")
        return (str(error_code), str(error_msg))

    def _handle_response(self, response: httpx.Response) -> httpx.Response:
        """Handle response and raise appropriate exceptions."""
        if response.status_code == 401:
            raise SalesforceAuthError("Authentication failed or token expired")
        elif response.status_code == 403:
            # Keep the platform's own message: a 403 usually names the
            # missing permission, which is the only actionable part.
            error_code, error_msg = self._parse_error_detail(response)
            raise SalesforceAPIError(
                f"Access forbidden: {error_code}: {error_msg}",
                status_code=403,
                error_code=error_code,
            )
        elif response.status_code == 404:
            error_code, error_msg = self._parse_error_detail(response)
            raise SalesforceNotFoundError(
                f"Resource not found: {error_code}: {error_msg}"
            )
        elif response.status_code == 429:
            # Rate limited - extract retry-after header if present
            retry_after = parse_retry_after(response.headers.get("Retry-After"))
            raise SalesforceRateLimitError(
                "Rate limit exceeded",
                retry_after=retry_after,
            )
        elif response.status_code >= 500:
            raise SalesforceAPIError(
                f"Server error: {response.status_code}",
                status_code=response.status_code,
            )
        elif response.status_code >= 400:
            error_code, error_msg = self._parse_error_detail(response)
            raise SalesforceAPIError(
                f"{error_code}: {error_msg}",
                status_code=response.status_code,
                error_code=error_code,
            )
        return response

    async def _request(
        self,
        method: str,
        path: str,
        **kwargs: Any,
    ) -> httpx.Response:
        """Make an HTTP request with retry logic."""
        url = self._build_url(path)

        async def _make_request() -> httpx.Response:
            response = await self.client.request(method, url, **kwargs)
            return self._handle_response(response)

        return await self.retry_client(_make_request)

    # Convenience methods
    async def get(self, path: str, params: dict | None = None, **kwargs: Any) -> httpx.Response:
        """GET request."""
        return await self._request("GET", path, params=params, **kwargs)

    async def post(
        self,
        path: str,
        json: dict | None = None,
        data: Any = None,
        **kwargs: Any,
    ) -> httpx.Response:
        """POST request."""
        return await self._request("POST", path, json=json, data=data, **kwargs)

    async def patch(self, path: str, json: dict | None = None, **kwargs: Any) -> httpx.Response:
        """PATCH request."""
        return await self._request("PATCH", path, json=json, **kwargs)

    async def delete(self, path: str, **kwargs: Any) -> httpx.Response:
        """DELETE request."""
        return await self._request("DELETE", path, **kwargs)

    # Salesforce-specific API methods
    async def query(self, soql: str) -> Any:
        """Execute a SOQL query."""
        response = await self.get("/query", params={"q": soql})
        return response.json()

    async def query_all(self, soql: str) -> Any:
        """Execute a SOQL query including deleted/archived records."""
        response = await self.get("/queryAll", params={"q": soql})
        return response.json()

    async def saql_query(self, query: str) -> Any:
        """Execute a SAQL query against Wave/Analytics."""
        payload = {"query": query, "queryLanguage": "SAQL"}
        response = await self.post(f"{self.wave_base_url}/query", json=payload)
        return response.json()

    # Dataset methods
    async def list_datasets(
        self,
        page_size: int = 50,
        sort: str = "Mru",
        page_token: str | None = None,
    ) -> Any:
        """List datasets with pagination."""
        params = build_page_params(page_token, page_size=page_size, sort=sort)
        response = await self.get(f"{self.wave_base_url}/datasets", params=params)
        return response.json()

    async def get_dataset(self, dataset_id: str) -> Any:
        """Get dataset details by ID."""
        response = await self.get(f"{self.wave_base_url}/datasets/{dataset_id}")
        return response.json()

    async def get_dataset_version(self, dataset_id: str, version_id: str) -> Any:
        """Get specific dataset version."""
        response = await self.get(f"{self.wave_base_url}/datasets/{dataset_id}/versions/{version_id}")
        return response.json()

    async def get_dataset_xmd(self, dataset_id: str, version_id: str) -> Any:
        """Get dataset XMD (Extended Metadata)."""
        response = await self.get(
            f"{self.wave_base_url}/datasets/{dataset_id}/versions/{version_id}/xmds/main"
        )
        return response.json()

    async def delete_dataset(self, dataset_id: str) -> httpx.Response:
        """Delete a dataset."""
        return await self.delete(f"{self.wave_base_url}/datasets/{dataset_id}")

    async def get_dataset_dependencies(self, dataset_id: str) -> Any:
        """Get dataset dependencies (downstream dataflows/dashboards)."""
        response = await self.get(f"{self.wave_base_url}/dependencies/{dataset_id}")
        return response.json()

    # Dashboard methods
    async def list_dashboards(
        self,
        page_size: int = 50,
        sort: str = "Mru",
        page_token: str | None = None,
    ) -> Any:
        """List dashboards with pagination."""
        params = build_page_params(page_token, page_size=page_size, sort=sort)
        response = await self.get(f"{self.wave_base_url}/dashboards", params=params)
        return response.json()

    async def get_dashboard(self, dashboard_id: str) -> Any:
        """Get dashboard details by ID."""
        response = await self.get(f"{self.wave_base_url}/dashboards/{dashboard_id}")
        return response.json()

    async def get_dashboard_datasets(self, dashboard_id: str) -> Any:
        """Get datasets used in a dashboard."""
        response = await self.get(f"{self.wave_base_url}/dashboards/{dashboard_id}/datasets")
        return response.json()

    async def delete_dashboard(self, dashboard_id: str) -> httpx.Response:
        """Delete a dashboard."""
        return await self.delete(f"{self.wave_base_url}/dashboards/{dashboard_id}")

    # Dataflow methods
    async def list_dataflows(self) -> Any:
        """List all dataflows."""
        response = await self.get(f"{self.wave_base_url}/dataflows")
        return response.json()

    async def get_dataflow(self, dataflow_id: str) -> Any:
        """Get dataflow details by ID."""
        response = await self.get(f"{self.wave_base_url}/dataflows/{dataflow_id}")
        return response.json()

    async def start_dataflow(self, dataflow_id: str) -> Any:
        """Start a dataflow execution."""
        payload = {"dataflowId": dataflow_id, "command": "start"}
        response = await self.post(f"{self.wave_base_url}/dataflowjobs", json=payload)
        return response.json()

    async def stop_dataflow(self, dataflow_id: str) -> Any:
        """Stop a running dataflow."""
        payload = {"dataflowId": dataflow_id, "command": "stop"}
        response = await self.post(f"{self.wave_base_url}/dataflowjobs", json=payload)
        return response.json()

    async def list_dataflow_jobs(self) -> Any:
        """List dataflow jobs."""
        response = await self.get(f"{self.wave_base_url}/dataflowjobs")
        return response.json()

    async def get_dataflow_definition(self, dataflow_id: str) -> Any:
        """Get full dataflow definition (JSON recipe) for field scanning."""
        response = await self.get(f"{self.wave_base_url}/dataflows/{dataflow_id}")
        return response.json()

    # Field Impact Analysis methods
    async def get_dependencies(self, asset_id: str) -> Any:
        """Query the Wave Dependencies API for upstream/downstream links."""
        response = await self.get(f"{self.wave_base_url}/dependencies/{asset_id}")
        return response.json()

    async def list_applications(self) -> Any:
        """List all Analytics applications."""
        response = await self.get(f"{self.wave_base_url}/applications")
        return response.json()

    async def list_available_api_versions(self) -> list[str]:
        """List Salesforce API versions available to this org.

        Calls ``GET /services/data/`` (no version suffix) and returns
        the list of version strings, e.g. ``['v60.0', 'v61.0', ...]``.

        Note: this is a metadata-level call, not under the wave API. It
        does NOT go through the wave_base_url.
        """
        url = f"{self.instance_url}/services/data/"
        response = await self.get(url)
        data = response.json()
        # Salesforce returns a list of dicts, each with 'version' and 'url'.
        if not isinstance(data, list):
            return []
        return [v["version"] for v in data if isinstance(v, dict) and "version" in v]

    async def get_application_dependencies(self, application_id: str) -> Any:
        """Get asset dependencies for an application (builds the dependency graph)."""
        response = await self.get(
            f"{self.wave_base_url}/applications/{application_id}/dependencies"
        )
        return response.json()

    async def get_dashboard_full(self, dashboard_id: str) -> Any:
        """Get full dashboard JSON including widgets, steps, and field references."""
        response = await self.get(f"{self.wave_base_url}/dashboards/{dashboard_id}")
        return response.json()

    async def list_replicated_datasets(self) -> Any:
        """List all replicated datasets."""
        response = await self.get(f"{self.wave_base_url}/replicatedDatasets")
        return response.json()

    async def get_replicated_dataset_fields(self, replicated_dataset_id: str) -> Any:
        """Get fields for a replicated dataset (connected object fields).

        Returns 404 for non-existent replicated datasets (caller must handle).
        """
        response = await self.get(
            f"{self.wave_base_url}/replicatedDatasets/{replicated_dataset_id}/fields"
        )
        return response.json()

    # Data Manager / External Data methods
    async def create_insights_external_data(
        self,
        edgemart_alias: str,
        metadata_json: str,
        operation: str = "Overwrite",
    ) -> Any:
        """Create an InsightsExternalData job for CSV upload."""
        payload = {
            "Format": "Csv",
            "EdgemartAlias": edgemart_alias,
            "Operation": operation,
            "Action": "None",
            "MetadataJson": metadata_json,
        }
        response = await self.post("/sobjects/InsightsExternalData", json=payload)
        return response.json()

    async def upload_insights_external_data_part(
        self,
        external_data_id: str,
        part_number: int,
        data_file_base64: str,
    ) -> Any:
        """Upload a part of the CSV data."""
        payload = {
            "DataFile": data_file_base64,
            "InsightsExternalDataId": external_data_id,
            "PartNumber": part_number,
        }
        response = await self.post("/sobjects/InsightsExternalDataPart", json=payload)
        return response.json()

    async def process_insights_external_data(self, external_data_id: str) -> Any:
        """Process the uploaded data (trigger Data Manager job)."""
        payload = {"Action": "Process"}
        response = await self.patch(f"/sobjects/InsightsExternalData/{external_data_id}", json=payload)
        return response.json()

    # Limits
    async def get_limits(self) -> Any:
        """Get API limits."""
        response = await self.get("/limits")
        return response.json()

    # Streaming/chunked upload for large files
    async def upload_large_file_streaming(
        self,
        edgemart_alias: str,
        metadata_json: str,
        file_path: str,
        chunk_size: int = 50000,
        operation: str = "Overwrite",
    ) -> AsyncIterator[dict[str, Any]]:
        """Stream upload a large CSV file in chunks.

        Yields progress updates for each chunk.
        """
        import pandas as pd

        # Create the external data job
        job = await self.create_insights_external_data(
            edgemart_alias=edgemart_alias,
            metadata_json=metadata_json,
            operation=operation,
        )
        external_data_id = job["id"]
        yield {"status": "created", "job_id": external_data_id}

        # Read and upload in chunks
        part_number = 1
        for chunk in pd.read_csv(file_path, chunksize=chunk_size):
            csv_data = chunk.to_csv(index=False)
            import base64
            data_file_base64 = base64.b64encode(csv_data.encode()).decode()

            await self.upload_insights_external_data_part(
                external_data_id=external_data_id,
                part_number=part_number,
                data_file_base64=data_file_base64,
            )
            yield {"status": "uploaded_part", "part": part_number, "rows": len(chunk)}
            part_number += 1

        # Process the data
        result = await self.process_insights_external_data(external_data_id)
        yield {"status": "processing", "result": result}


@asynccontextmanager
async def create_client(
    access_token: str,
    instance_url: str,
    settings: Settings | None = None,
) -> AsyncIterator[SalesforceClient]:
    """Context manager for creating and closing a SalesforceClient."""
    client = SalesforceClient(access_token, instance_url, settings)
    try:
        yield client
    finally:
        await client.close()


@asynccontextmanager
async def create_client_from_sf_cli(
    alias: str = "default",
    settings: Settings | None = None,
    crypto_manager: Optional["CryptoManager"] = None,
) -> AsyncIterator[SalesforceClient]:
    """
    Context manager for creating a SalesforceClient using SF CLI authentication.

    Args:
        alias: SF CLI org alias
        settings: Optional settings override
        crypto_manager: Optional CryptoManager for token encryption

    Yields:
        Authenticated SalesforceClient

    Example:
        async with create_client_from_sf_cli("myorg") as client:
            datasets = await client.list_datasets()
    """
    from asftool.core.auth.sf_cli_auth import SFCLIAuthService
    from asftool.core.config import get_settings
    from asftool.core.crypto import create_crypto_manager

    settings = settings or get_settings()
    crypto = crypto_manager or create_crypto_manager()
    auth_service = SFCLIAuthService(settings, crypto)

    access_token = await auth_service.get_access_token(alias)
    instance_url = await auth_service.get_instance_url(alias)

    client = SalesforceClient(access_token, instance_url, settings)
    try:
        yield client
    finally:
        await client.close()
