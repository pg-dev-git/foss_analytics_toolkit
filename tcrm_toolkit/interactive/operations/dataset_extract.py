"""Parallel dataset extraction with multiprocessing for large datasets."""

import asyncio
import math
import shutil
import tempfile
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pandas as pd
import structlog

from tcrm_toolkit.core.models import ExtractionProgress
from tcrm_toolkit.core.services.dataset_service import DatasetService
from tcrm_toolkit.interactive.tasks import TaskRunner, merge_csv_chunks

logger = structlog.get_logger(__name__)


class ParallelDatasetExtractor:
    """
    Extract large datasets using parallel SAQL queries and multiprocessing merge.
    
    Strategy:
    1. Get total row count via SAQL
    2. Calculate optimal chunk size (50k-150k rows)
    3. Run SAQL queries in parallel (async, I/O-bound) with semaphore
    4. Save each chunk to temp CSV
    5. Merge chunks using ProcessPoolExecutor (CPU-bound)
    6. Stream progress updates throughout
    """

    def __init__(
        self,
        session,
        task_runner: TaskRunner,
        progress_callback: Callable[[ExtractionProgress], None] | None = None,
    ):
        self.session = session
        self.task_runner = task_runner
        self.progress_callback = progress_callback
        self._temp_dir: Path | None = None

    async def extract(
        self,
        dataset_id: str,
        output_path: Path,
        max_concurrent_queries: int = 10,
    ) -> dict[str, Any]:
        """
        Extract dataset to CSV with parallel processing.

        Args:
            dataset_id: Dataset ID to extract
            output_path: Output CSV file path
            max_concurrent_queries: Max parallel SAQL queries

        Returns:
            Dict with extraction stats
        """
        self._temp_dir = Path(tempfile.mkdtemp(prefix=f"tcrm_extract_{dataset_id}_"))

        try:
            async with self.session.client_context() as client:
                service = DatasetService(client, self.session.settings)

                # Get dataset info
                dataset = await service.get_dataset(dataset_id)
                version_id = dataset.current_version_id
                if not version_id:
                    raise ValueError(f"Dataset {dataset_id} has no current version")

                # Get XMD for field list
                xmd = await service.get_dataset_xmd(dataset_id, version_id)
                fields = service._extract_fields_from_xmd(xmd)

                if not fields:
                    raise ValueError("No valid fields found in dataset XMD")

                # Get total row count
                total_rows = await service.get_row_count(dataset_id, version_id)

                if total_rows == 0:
                    pd.DataFrame(columns=fields).to_csv(output_path, index=False)
                    return {"rows": 0, "chunks": 0, "output": str(output_path)}

                # Calculate chunking
                chunk_size = service._calculate_chunk_size(total_rows)
                total_chunks = math.ceil(total_rows / chunk_size)

                logger.info(
                    "parallel_extract_started",
                    dataset_id=dataset_id,
                    total_rows=total_rows,
                    chunk_size=chunk_size,
                    total_chunks=total_chunks,
                )

                progress = ExtractionProgress(
                    total_rows=total_rows,
                    processed_rows=0,
                    current_chunk=0,
                    total_chunks=total_chunks,
                    status="running",
                )

                semaphore = asyncio.Semaphore(max_concurrent_queries)

                async def extract_chunk(chunk_num: int) -> tuple[int, Path | None]:
                    async with semaphore:
                        offset = chunk_num * chunk_size
                        saql = service._build_saql_query(
                            dataset_id, version_id, fields, offset, chunk_size
                        )

                        response = await client.saql_query(saql)
                        records = response.get("results", {}).get("records", [])

                        if records:
                            chunk_df = pd.DataFrame(records)
                            assert self._temp_dir is not None
                            chunk_path = self._temp_dir / f"chunk_{chunk_num:04d}.csv"
                            chunk_df.to_csv(chunk_path, index=False)
                            return len(records), chunk_path

                        return 0, None

                chunk_tasks = [extract_chunk(i) for i in range(total_chunks)]

                chunk_results = []
                for i, coro in enumerate(asyncio.as_completed(chunk_tasks)):
                    rows, chunk_path = await coro
                    chunk_results.append((rows, chunk_path))

                    progress.processed_rows += rows
                    progress.current_chunk = i + 1

                    if self.progress_callback:
                        if asyncio.iscoroutinefunction(self.progress_callback):
                            await self.progress_callback(progress)
                        else:
                            self.progress_callback(progress)

                successful_chunks = [p for r, p in chunk_results if r > 0 and p is not None]
                total_processed = sum(r for r, _ in chunk_results)

                progress.status = "merging"
                if self.progress_callback:
                    if asyncio.iscoroutinefunction(self.progress_callback):
                        await self.progress_callback(progress)
                    else:
                        self.progress_callback(progress)

                merge_result = await self.task_runner.run_in_process_pool(
                    merge_csv_chunks,
                    [str(p) for p in successful_chunks],
                    str(output_path),
                )

                progress.status = "completed"
                progress.current_chunk = total_chunks
                if self.progress_callback:
                    if asyncio.iscoroutinefunction(self.progress_callback):
                        await self.progress_callback(progress)
                    else:
                        self.progress_callback(progress)

                logger.info(
                    "parallel_extract_completed",
                    dataset_id=dataset_id,
                    total_rows=total_processed,
                    chunks=len(successful_chunks),
                )

                return {
                    "rows": total_processed,
                    "chunks": len(successful_chunks),
                    "output": str(output_path),
                    "merge_result": merge_result,
                }

        finally:
            if self._temp_dir and self._temp_dir.exists():
                shutil.rmtree(self._temp_dir, ignore_errors=True)
