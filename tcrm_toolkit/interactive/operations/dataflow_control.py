"""Dataflow start/stop/monitor operations."""

import asyncio
from collections.abc import Callable
from typing import Any

import structlog

from tcrm_toolkit.core.services.dataflow_service import DataflowService
from tcrm_toolkit.interactive.tasks import TaskRunner

logger = structlog.get_logger(__name__)


class DataflowController:
    """Control dataflow execution with job monitoring."""

    def __init__(
        self,
        session,
        task_runner: TaskRunner,
        progress_callback: Callable[[dict[str, Any]], None] | None = None,
    ):
        self.session = session
        self.task_runner = task_runner
        self.progress_callback = progress_callback

    async def start_dataflow(self, dataflow_id: str) -> dict[str, Any]:
        """Start dataflow and return job info."""
        async with self.session.client_context() as client:
            service = DataflowService(client, self.session.settings)

            if self.progress_callback:
                self.progress_callback({"status": "starting", "dataflow_id": dataflow_id})

            job = await service.start_dataflow(dataflow_id)

            if self.progress_callback:
                self.progress_callback({"status": "started", "job_id": job.id})

            return {
                "job_id": job.id,
                "dataflow_id": dataflow_id,
                "status": job.status,
            }

    async def stop_dataflow(self, dataflow_id: str) -> dict[str, Any]:
        """Stop running dataflow."""
        async with self.session.client_context() as client:
            service = DataflowService(client, self.session.settings)

            if self.progress_callback:
                self.progress_callback({"status": "stopping", "dataflow_id": dataflow_id})

            job = await service.stop_dataflow(dataflow_id)

            if self.progress_callback:
                self.progress_callback({"status": "stopped", "job_id": job.id})

            return {
                "job_id": job.id,
                "dataflow_id": dataflow_id,
                "status": job.status,
            }

    async def wait_for_job(
        self,
        job_id: str,
        poll_interval: int = 10,
        timeout: int = 3600,
    ) -> dict[str, Any]:
        """Wait for dataflow job to complete with progress updates."""
        async with self.session.client_context() as client:
            service = DataflowService(client, self.session.settings)

            start_time = asyncio.get_event_loop().time()

            while True:
                job = await service.get_dataflow_job_status(job_id)
                if not job:
                    raise ValueError(f"Job {job_id} not found")

                if self.progress_callback:
                    self.progress_callback({
                        "status": "polling",
                        "job_id": job_id,
                        "job_status": job.status,
                    })

                if job.status in ("Success", "Failed", "Cancelled"):
                    if self.progress_callback:
                        self.progress_callback({
                            "status": "completed",
                            "job_id": job_id,
                            "final_status": job.status,
                        })
                    return {
                        "job_id": job.id,
                        "status": job.status,
                        "dataflow_name": job.dataflow_name,
                    }

                if asyncio.get_event_loop().time() - start_time > timeout:
                    raise TimeoutError(f"Job {job_id} timed out after {timeout}s")

                await asyncio.sleep(poll_interval)
