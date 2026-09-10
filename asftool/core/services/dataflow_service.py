"""Dataflow service for ASFTool."""

from pathlib import Path
from typing import Any

import structlog

from asftool.core.client import SalesforceClient
from asftool.core.config import Settings, get_settings
from asftool.core.exceptions import DataflowError
from asftool.core.models import (
    Dataflow,
    DataflowJob,
    DataflowJobListResponse,
    DataflowListResponse,
)
from asftool.core.storage import get_storage_manager

logger = structlog.get_logger(__name__)


class DataflowService:
    """Service for dataflow operations."""

    def __init__(
        self,
        client: SalesforceClient,
        settings: Settings | None = None,
    ):
        """Initialize the dataflow service."""
        self.client = client
        self.settings = settings or get_settings()
        self.storage = get_storage_manager(self.settings)

    # =========================================================================
    # Listing and Retrieval
    # =========================================================================

    async def list_dataflows(self) -> list[Dataflow]:
        """List all dataflows."""
        response = await self.client.list_dataflows()
        data = DataflowListResponse(**response)
        return data.dataflows

    async def get_dataflow(self, dataflow_id: str) -> Dataflow:
        """Get dataflow details by ID."""
        response = await self.client.get_dataflow(dataflow_id)
        return Dataflow(**response)

    # =========================================================================
    # Execution Control
    # =========================================================================

    async def start_dataflow(self, dataflow_id: str) -> DataflowJob:
        """Start a dataflow execution."""
        response = await self.client.start_dataflow(dataflow_id)
        return DataflowJob(**response)

    async def stop_dataflow(self, dataflow_id: str) -> DataflowJob:
        """Stop a running dataflow."""
        response = await self.client.stop_dataflow(dataflow_id)
        return DataflowJob(**response)

    # =========================================================================
    # Job Monitoring
    # =========================================================================

    async def list_dataflow_jobs(self) -> list[DataflowJob]:
        """List all dataflow jobs."""
        response = await self.client.list_dataflow_jobs()
        data = DataflowJobListResponse(**response)
        return data.dataflowjobs

    async def get_dataflow_job_status(self, job_id: str) -> DataflowJob | None:
        """Get status of a specific dataflow job."""
        jobs = await self.list_dataflow_jobs()
        for job in jobs:
            if job.id == job_id:
                return job
        return None

    async def wait_for_dataflow_job(
        self,
        job_id: str,
        poll_interval: int = 10,
        timeout: int = 3600,
    ) -> DataflowJob:
        """Wait for a dataflow job to complete."""
        import asyncio

        start_time = asyncio.get_event_loop().time()

        while True:
            job = await self.get_dataflow_job_status(job_id)
            if not job:
                raise DataflowError(f"Job {job_id} not found")

            if job.status in ("Success", "Failed", "Cancelled"):
                return job

            if asyncio.get_event_loop().time() - start_time > timeout:
                raise DataflowError(f"Job {job_id} timed out after {timeout}s")

            await asyncio.sleep(poll_interval)

    # =========================================================================
    # Backup
    # =========================================================================

    async def backup_dataflow(
        self,
        dataflow_id: str,
        output_path: Path | None = None,
        alias: str = "default",
    ) -> Any:
        """Backup current dataflow definition.

        If output_path is not provided, generates an organized path using
        the storage manager: ~/.asftool/downloads/<alias>/dataflows/
        <timestamp>_<name>_<id>.json
        """
        # Get the dataflow definition
        response = await self.client.get(
            f"{self.client.wave_base_url}/dataflows/{dataflow_id}"
        )
        definition = response.json()

        # Get dataflow info for naming
        dataflow = await self.get_dataflow(dataflow_id)

        if output_path is None:
            output_path = self.storage.dataflow_path(
                alias=alias,
                dataflow_name=dataflow.name,
                dataflow_id=dataflow_id,
            )

        import json
        output_path.write_text(json.dumps(definition, indent=2, ensure_ascii=False), encoding="utf-8")
        logger.info("dataflow_backup_saved", path=str(output_path))

        return definition
