"""Async HTTP client for Salesforce API with retry logic and circuit breaker."""

import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator

import httpx
import structlog
from tenacity import (
    AsyncRetrying,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential_jitter,
    before_sleep_log,
    after_log,
)

from tcrm_toolkit.core.config import Settings, get_settings
from tcrm_toolkit.core.exceptions import (
    SalesforceAPIError,
    SalesforceAuthError,
    SalesforceRateLimitError,
    SalesforceNotFoundError,
)

logger = structlog.get_logger(__name__)


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
                wait=wait_exponential_jitter(initial=1, max=30),
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

    def _handle_response(self, response: httpx.Response) -> httpx.Response:
        """Handle response and raise appropriate exceptions."""
        if response.status_code == 401:
            raise SalesforceAuthError("Authentication failed or token expired")
        elif response.status_code == 403:
            raise SalesforceAPIError("Access forbidden", status_code=403)
        elif response.status_code == 404:
            raise SalesforceNotFoundError("Resource not found")
        elif response.status_code == 429:
            # Rate limited - extract retry-after header if present
            retry_after = response.headers.get("Retry-After", "60")
            raise SalesforceRateLimitError(
                "Rate limit exceeded",
                retry_after=int(retry_after) if retry_after.isdigit() else 60,
            )
        elif response.status_code >= 500:
            raise SalesforceAPIError(
                f"Server error: {response.status_code}",
                status_code=response.status_code,
            )
        elif response.status_code >= 400:
            # Try to parse error details from response
            try:
                error_data = response.json()
                # Salesforce returns errors as a list
                if isinstance(error_data, list) and error_data:
                    error = error_data[0]
                    error_msg = error.get("message", "Unknown error")
                    error_code = error.get("errorCode", "UNKNOWN_ERROR")
                else:
                    error_msg = error_data.get("message", "Unknown error")
                    error_code = error_data.get("errorCode", "UNKNOWN_ERROR")
                raise SalesforceAPIError(
                    f"{error_code}: {error_msg}",
                    status_code=response.status_code,
                    error_code=error_code,
                )
            except SalesforceAPIError:
                # Re-raise SalesforceAPIError (e.g., from the raise above)
                raise
            except Exception:
                raise SalesforceAPIError(
                    f"Request failed: {response.status_code}",
                    status_code=response.status_code,
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
    async def query(self, soql: str) -> dict[str, Any]:
        """Execute a SOQL query."""
        response = await self.get("/query", params={"q": soql})
        return response.json()

    async def query_all(self, soql: str) -> dict[str, Any]:
        """Execute a SOQL query including deleted/archived records."""
        response = await self.get("/queryAll", params={"q": soql})
        return response.json()

    async def saql_query(self, query: str) -> dict[str, Any]:
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
    ) -> dict[str, Any]:
        """List datasets with pagination."""
        params = {"pageSize": page_size, "sort": sort}
        if page_token:
            params["pageToken"] = page_token
        response = await self.get(f"{self.wave_base_url}/datasets", params=params)
        return response.json()

    async def get_dataset(self, dataset_id: str) -> dict[str, Any]:
        """Get dataset details by ID."""
        response = await self.get(f"{self.wave_base_url}/datasets/{dataset_id}")
        return response.json()

    async def get_dataset_version(self, dataset_id: str, version_id: str) -> dict[str, Any]:
        """Get specific dataset version."""
        response = await self.get(f"{self.wave_base_url}/datasets/{dataset_id}/versions/{version_id}")
        return response.json()

    async def get_dataset_xmd(self, dataset_id: str, version_id: str) -> dict[str, Any]:
        """Get dataset XMD (Extended Metadata)."""
        response = await self.get(
            f"{self.wave_base_url}/datasets/{dataset_id}/versions/{version_id}/xmds/main"
        )
        return response.json()

    async def delete_dataset(self, dataset_id: str) -> httpx.Response:
        """Delete a dataset."""
        return await self.delete(f"{self.wave_base_url}/datasets/{dataset_id}")

    async def get_dataset_dependencies(self, dataset_id: str) -> dict[str, Any]:
        """Get dataset dependencies (downstream dataflows/dashboards)."""
        response = await self.get(f"{self.wave_base_url}/dependencies/{dataset_id}")
        return response.json()

    # Dashboard methods
    async def list_dashboards(
        self,
        page_size: int = 50,
        sort: str = "Mru",
        page_token: str | None = None,
    ) -> dict[str, Any]:
        """List dashboards with pagination."""
        params = {"pageSize": page_size, "sort": sort}
        if page_token:
            params["pageToken"] = page_token
        response = await self.get(f"{self.wave_base_url}/dashboards", params=params)
        return response.json()

    async def get_dashboard(self, dashboard_id: str) -> dict[str, Any]:
        """Get dashboard details by ID."""
        response = await self.get(f"{self.wave_base_url}/dashboards/{dashboard_id}")
        return response.json()

    async def get_dashboard_datasets(self, dashboard_id: str) -> dict[str, Any]:
        """Get datasets used in a dashboard."""
        response = await self.get(f"{self.wave_base_url}/dashboards/{dashboard_id}/datasets")
        return response.json()

    async def delete_dashboard(self, dashboard_id: str) -> httpx.Response:
        """Delete a dashboard."""
        return await self.delete(f"{self.wave_base_url}/dashboards/{dashboard_id}")

    # Dataflow methods
    async def list_dataflows(self) -> dict[str, Any]:
        """List all dataflows."""
        response = await self.get(f"{self.wave_base_url}/dataflows")
        return response.json()

    async def get_dataflow(self, dataflow_id: str) -> dict[str, Any]:
        """Get dataflow details by ID."""
        response = await self.get(f"{self.wave_base_url}/dataflows/{dataflow_id}")
        return response.json()

    async def start_dataflow(self, dataflow_id: str) -> dict[str, Any]:
        """Start a dataflow execution."""
        payload = {"dataflowId": dataflow_id, "command": "start"}
        response = await self.post(f"{self.wave_base_url}/dataflowjobs", json=payload)
        return response.json()

    async def stop_dataflow(self, dataflow_id: str) -> dict[str, Any]:
        """Stop a running dataflow."""
        payload = {"dataflowId": dataflow_id, "command": "stop"}
        response = await self.post(f"{self.wave_base_url}/dataflowjobs", json=payload)
        return response.json()

    async def list_dataflow_jobs(self) -> dict[str, Any]:
        """List dataflow jobs."""
        response = await self.get(f"{self.wave_base_url}/dataflowjobs")
        return response.json()

    # Data Manager / External Data methods
    async def create_insights_external_data(
        self,
        edgemart_alias: str,
        metadata_json: str,
        operation: str = "Overwrite",
    ) -> dict[str, Any]:
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
    ) -> dict[str, Any]:
        """Upload a part of the CSV data."""
        payload = {
            "DataFile": data_file_base64,
            "InsightsExternalDataId": external_data_id,
            "PartNumber": part_number,
        }
        response = await self.post("/sobjects/InsightsExternalDataPart", json=payload)
        return response.json()

    async def process_insights_external_data(self, external_data_id: str) -> dict[str, Any]:
        """Process the uploaded data (trigger Data Manager job)."""
        payload = {"Action": "Process"}
        response = await self.patch(f"/sobjects/InsightsExternalData/{external_data_id}", json=payload)
        return response.json()

    # Limits
    async def get_limits(self) -> dict[str, Any]:
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
    crypto_manager: "CryptoManager" | None = None,
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
    from tcrm_toolkit.core.auth.sf_cli_auth import SFCLIAuthService
    from tcrm_toolkit.core.config import get_settings
    from tcrm_toolkit.core.crypto import CryptoManager, create_crypto_manager

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