"""Parallel dataset upload with multiprocessing for CSV processing."""

import asyncio
import math
import tempfile
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pandas as pd
import structlog

from tcrm_toolkit.core.models import UploadProgress
from tcrm_toolkit.core.services.dataset_service import DatasetService
from tcrm_toolkit.interactive.tasks import (
    TaskRunner,
    process_csv_chunk,
    split_csv_for_parallel,
)

logger = structlog.get_logger(__name__)


def _process_chunks_parallel(chunk_args: list[tuple]) -> list[dict[str, Any]]:
    """Top-level helper to process multiple chunks in process pool."""
    return [process_csv_chunk(args) for args in chunk_args]


class ParallelDatasetUploader:
    """
    Upload large CSV to dataset using parallel chunk processing.

    Strategy:
    1. Read CSV metadata (first row)
    2. Create InsightsExternalData job
    3. Split CSV into chunks for parallel base64 encoding
    4. Process chunks in ProcessPoolExecutor (CPU-bound)
    5. Upload parts sequentially (API requirement)
    6. Trigger processing
    """

    def __init__(
        self,
        session,
        task_runner: TaskRunner,
        progress_callback: Callable[[UploadProgress], None] | None = None,
    ):
        self.session = session
        self.task_runner = task_runner
        self.progress_callback = progress_callback

    async def upload(
        self,
        dataset_id: str,
        file_path: Path,
        dataset_name: str | None = None,
        operation: str = "Overwrite",
        chunk_size: int = 50000,
        max_process_workers: int = 4,
    ) -> dict[str, Any]:
        """
        Upload CSV to dataset with parallel chunk processing.
        """
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        async with self.session.client_context() as client:
            service = DatasetService(client, self.session.settings)

            if not dataset_name:
                dataset = await service.get_dataset(dataset_id)
                dataset_name = dataset.name

            first_chunk = pd.read_csv(file_path, nrows=1)
            metadata_json = service._generate_metadata_json(first_chunk, dataset_name)

            job_response = await client.create_insights_external_data(
                edgemart_alias=dataset_name,
                metadata_json=metadata_json,
                operation=operation,
            )
            external_data_id = job_response["id"]

            total_rows = sum(1 for _ in open(file_path)) - 1
            total_parts = math.ceil(total_rows / chunk_size)

            logger.info(
                "parallel_upload_started",
                dataset_id=dataset_id,
                external_data_id=external_data_id,
                total_rows=total_rows,
                total_parts=total_parts,
            )

            progress = UploadProgress(
                total_rows=total_rows,
                uploaded_rows=0,
                current_part=0,
                total_parts=total_parts,
                status="uploading",
            )

            with tempfile.TemporaryDirectory() as tmpdir:
                chunk_paths = await self.task_runner.run_in_process_pool(
                    split_csv_for_parallel,
                    str(file_path),
                    max_process_workers,
                    tmpdir,
                )

                chunk_args = [(path, i, total_parts) for i, path in enumerate(chunk_paths)]

                processed_chunks = await self.task_runner.run_in_process_pool(
                    _process_chunks_parallel,
                    chunk_args,
                )

                uploaded_rows = 0
                for _i, chunk_result in enumerate(processed_chunks):
                    await client.upload_insights_external_data_part(
                        external_data_id=external_data_id,
                        part_number=chunk_result["part_number"],
                        data_file_base64=chunk_result["data_file_base64"],
                    )

                    uploaded_rows += chunk_result["rows"]
                    progress.uploaded_rows = uploaded_rows
                    progress.current_part = chunk_result["part_number"]

                    if self.progress_callback:
                        if asyncio.iscoroutinefunction(self.progress_callback):
                            await self.progress_callback(progress)
                        else:
                            self.progress_callback(progress)

            progress.status = "processing"
            if self.progress_callback:
                if asyncio.iscoroutinefunction(self.progress_callback):
                    await self.progress_callback(progress)
                else:
                    self.progress_callback(progress)

            result = await client.process_insights_external_data(external_data_id)

            progress.status = "completed"
            if self.progress_callback:
                if asyncio.iscoroutinefunction(self.progress_callback):
                    await self.progress_callback(progress)
                else:
                    self.progress_callback(progress)

            logger.info(
                "parallel_upload_completed",
                dataset_id=dataset_id,
                external_data_id=external_data_id,
                rows=uploaded_rows,
            )

            return {
                "external_data_id": external_data_id,
                "dataset_id": dataset_id,
                "rows": uploaded_rows,
                "status": "completed",
                "result": result,
            }
