# Phase 3: Operations & Background Tasks

**Document**: `docs/plans/phases/phase-3-operations-background-tasks.md`  
**Duration**: 1 week  
**Branch**: `feature/phase-3-operations-background-tasks` (to be created when implementation begins)  
**Depends on**: Phase 2 complete

---

## 🎯 Objective

Implement all write operations with background execution, progress tracking, and **parallel dataset extraction/upload** using multiprocessing for CPU-bound work and async for I/O-bound work — matching the performance of the legacy multiprocessing implementation.

---

## 📋 Explicit Requirements

### 1. TaskRunner - Background Task Infrastructure

**File**: `tcrm_toolkit/interactive/tasks.py`

```python
"""Background task runner with progress tracking and history."""

import asyncio
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Optional
from concurrent.futures import ProcessPoolExecutor

import structlog
from textual import work
from textual.message import Message
from textual.widget import Widget

logger = structlog.get_logger(__name__)


class TaskStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class TaskProgress:
    """Progress update for a task."""
    task_id: str
    status: TaskStatus
    current: int = 0
    total: int = 0
    message: str = ""
    details: dict = field(default_factory=dict)
    started_at: datetime = field(default_factory=datetime.utcnow)
    completed_at: Optional[datetime] = None
    error: Optional[str] = None
    
    @property
    def percent(self) -> float:
        if self.total == 0:
            return 0.0
        return min(100.0, (self.current / self.total) * 100)
    
    @property
    def is_finished(self) -> bool:
        return self.status in (TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED)


@dataclass
class TaskResult:
    """Final result of a task."""
    task_id: str
    status: TaskStatus
    result: Any = None
    error: Optional[str] = None
    started_at: datetime = field(default_factory=datetime.utcnow)
    completed_at: datetime = field(default_factory=datetime.utcnow)
    metadata: dict = field(default_factory=dict)


class TaskProgressMessage(Message):
    """Message for task progress updates."""
    def __init__(self, progress: TaskProgress):
        self.progress = progress
        super().__init__()


class TaskCompletedMessage(Message):
    """Message for task completion."""
    def __init__(self, result: TaskResult):
        self.result = result
        super().__init__()


class TaskRunner(Widget):
    """
    Background task runner with:
    - Async task execution (I/O-bound)
    - ProcessPoolExecutor for CPU-bound work (multiprocessing)
    - Progress tracking via messages
    - Task history with persistence
    - Cancellation support
    - Max concurrent tasks limit
    """
    
    def __init__(
        self,
        max_concurrent: int = 3,
        max_history: int = 100,
        process_pool_size: int = None,
    ):
        super().__init__()
        self.max_concurrent = max_concurrent
        self.max_history = max_history
        self.process_pool_size = process_pool_size or min(4, (asyncio.cpu_count() or 4))
        
        self._tasks: dict[str, asyncio.Task] = {}
        self._progress: dict[str, TaskProgress] = {}
        self._history: list[TaskResult] = []
        self._process_pool: ProcessPoolExecutor | None = None
        self._semaphore = asyncio.Semaphore(max_concurrent)
    
    @property
    def process_pool(self) -> ProcessPoolExecutor:
        """Lazy-initialize process pool."""
        if self._process_pool is None:
            self._process_pool = ProcessPoolExecutor(max_workers=self.process_pool_size)
        return self._process_pool
    
    async def run_task(
        self,
        coro_factory: Callable[[], asyncio.coroutine],
        task_id: str | None = None,
        name: str = "Task",
        progress_callback: Callable[[TaskProgress], None] | None = None,
    ) -> TaskResult:
        """
        Run a coroutine as a background task.
        
        Args:
            coro_factory: Factory that returns a coroutine (not awaited)
            task_id: Optional task ID (generated if not provided)
            name: Human-readable task name
            progress_callback: Optional callback for progress updates
            
        Returns:
            TaskResult when complete
        """
        task_id = task_id or str(uuid.uuid4())[:8]
        
        # Wait for semaphore (max concurrent limit)
        async with self._semaphore:
            # Create progress tracker
            progress = TaskProgress(
                task_id=task_id,
                status=TaskStatus.RUNNING,
                message=f"Starting {name}...",
            )
            self._progress[task_id] = progress
            self.post_message(TaskProgressMessage(progress))
            
            if progress_callback:
                progress_callback(progress)
            
            # Create and run task
            task = asyncio.create_task(self._run_task_impl(
                task_id, name, coro_factory, progress, progress_callback
            ))
            self._tasks[task_id] = task
            
            try:
                result = await task
                return result
            finally:
                self._tasks.pop(task_id, None)
    
    async def _run_task_impl(
        self,
        task_id: str,
        name: str,
        coro_factory: Callable,
        progress: TaskProgress,
        progress_callback: Callable | None,
    ) -> TaskResult:
        """Internal task implementation with error handling."""
        try:
            # Run the coroutine
            coro = coro_factory()
            result = await coro
            
            # Mark completed
            progress.status = TaskStatus.COMPLETED
            progress.current = progress.total
            progress.message = f"{name} completed"
            progress.completed_at = datetime.utcnow()
            
            task_result = TaskResult(
                task_id=task_id,
                status=TaskStatus.COMPLETED,
                result=result,
                completed_at=progress.completed_at,
            )
            
        except asyncio.CancelledError:
            progress.status = TaskStatus.CANCELLED
            progress.message = f"{name} cancelled"
            progress.completed_at = datetime.utcnow()
            
            task_result = TaskResult(
                task_id=task_id,
                status=TaskStatus.CANCELLED,
                error="Cancelled",
                completed_at=progress.completed_at,
            )
            raise
            
        except Exception as e:
            logger.error("task_failed", task_id=task_id, name=name, error=str(e))
            progress.status = TaskStatus.FAILED
            progress.message = f"{name} failed: {e}"
            progress.error = str(e)
            progress.completed_at = datetime.utcnow()
            
            task_result = TaskResult(
                task_id=task_id,
                status=TaskStatus.FAILED,
                error=str(e),
                completed_at=progress.completed_at,
            )
        
        # Update progress
        self.post_message(TaskProgressMessage(progress))
        if progress_callback:
            progress_callback(progress)
        
        # Add to history
        self._add_to_history(task_result)
        
        # Notify completion
        self.post_message(TaskCompletedMessage(task_result))
        
        return task_result
    
    def _add_to_history(self, result: TaskResult) -> None:
        """Add result to history, trim if needed."""
        self._history.append(result)
        if len(self._history) > self.max_history:
            self._history = self._history[-self.max_history:]
    
    def get_progress(self, task_id: str) -> TaskProgress | None:
        return self._progress.get(task_id)
    
    def get_all_progress(self) -> list[TaskProgress]:
        return list(self._progress.values())
    
    def get_history(self) -> list[TaskResult]:
        return list(self._history)
    
    async def cancel_task(self, task_id: str) -> bool:
        """Cancel a running task."""
        task = self._tasks.get(task_id)
        if task and not task.done():
            task.cancel()
            return True
        return False
    
    async def cancel_all(self) -> int:
        """Cancel all running tasks."""
        count = 0
        for task in self._tasks.values():
            if not task.done():
                task.cancel()
                count += 1
        return count
    
    async def run_in_process_pool(self, func: Callable, *args, **kwargs) -> Any:
        """
        Run a CPU-bound function in the process pool.
        
        Use this for: pandas operations, CSV merging, data transformations.
        The function must be picklable (top-level function, not lambda/closure).
        """
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self.process_pool, func, *args, **kwargs)
    
    async def close(self) -> None:
        """Cleanup resources."""
        # Cancel all running tasks
        await self.cancel_all()
        
        # Wait for tasks to finish cancellation
        if self._tasks:
            await asyncio.gather(*self._tasks.values(), return_exceptions=True)
        
        # Shutdown process pool
        if self._process_pool:
            self._process_pool.shutdown(wait=True)
            self._process_pool = None


# =============================================================================
# Parallel Dataset Extraction Helpers (CPU-bound, run in process pool)
# =============================================================================

def merge_csv_chunks(chunk_paths: list[str], output_path: str) -> dict:
    """
    Merge multiple CSV chunks into single file.
    
    Runs in process pool to bypass GIL for pandas concat.
    """
    import pandas as pd
    
    chunks = []
    total_rows = 0
    
    for path in chunk_paths:
        df = pd.read_csv(path)
        chunks.append(df)
        total_rows += len(df)
    
    if chunks:
        combined = pd.concat(chunks, ignore_index=True)
        combined.to_csv(output_path, index=False)
    
    return {
        "output_path": output_path,
        "total_rows": total_rows,
        "chunks_merged": len(chunks),
    }


def process_csv_chunk(args: tuple) -> dict:
    """
    Process a single CSV chunk (for upload).
    
    Args: (chunk_data, chunk_index, total_chunks)
    """
    import pandas as pd
    import base64
    
    chunk_data, chunk_index, total_chunks = args
    df = pd.read_csv(chunk_data)
    csv_bytes = df.to_csv(index=False).encode()
    b64 = base64.b64encode(csv_bytes).decode()
    
    return {
        "part_number": chunk_index + 1,
        "data_file_base64": b64,
        "rows": len(df),
    }


def split_csv_for_parallel(input_path: str, num_chunks: int, output_dir: str) -> list[str]:
    """
    Split large CSV into chunks for parallel processing.
    
    Returns list of chunk file paths.
    """
    import pandas as pd
    from pathlib import Path
    
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Read in chunks to avoid memory issues
    chunk_paths = []
    chunk_size = 100000  # 100k rows per chunk
    
    for i, chunk in enumerate(pd.read_csv(input_path, chunksize=chunk_size)):
        if i >= num_chunks:
            # Merge remaining into last chunk
            break
        chunk_path = output_dir / f"chunk_{i:04d}.csv"
        chunk.to_csv(chunk_path, index=False)
        chunk_paths.append(str(chunk_path))
    
    return chunk_paths
```

---

### 2. Parallel Dataset Extraction

**File**: `tcrm_toolkit/interactive/operations/dataset_extract.py`

```python
"""Parallel dataset extraction with multiprocessing for large datasets."""

import asyncio
import math
import tempfile
from pathlib import Path
from typing import Any, Callable

import structlog
from tcrm_toolkit.core.services.dataset_service import DatasetService
from tcrm_toolkit.core.models import ExtractionProgress
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
        # Create temp directory for chunks
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
                    # Create empty CSV with headers
                    import pandas as pd
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
                
                # Progress tracking
                progress = ExtractionProgress(
                    total_rows=total_rows,
                    processed_rows=0,
                    current_chunk=0,
                    total_chunks=total_chunks,
                    status="running",
                )
                
                # Semaphore for concurrent SAQL queries
                semaphore = asyncio.Semaphore(max_concurrent_queries)
                
                async def extract_chunk(chunk_num: int) -> tuple[int, Path]:
                    """Extract single chunk."""
                    async with semaphore:
                        offset = chunk_num * chunk_size
                        saql = service._build_saql_query(
                            dataset_id, version_id, fields, offset, chunk_size
                        )
                        
                        response = await client.saql_query(saql)
                        records = response.get("results", {}).get("records", [])
                        
                        if records:
                            import pandas as pd
                            chunk_df = pd.DataFrame(records)
                            chunk_path = self._temp_dir / f"chunk_{chunk_num:04d}.csv"
                            chunk_df.to_csv(chunk_path, index=False)
                            return len(records), chunk_path
                        
                        return 0, None
                
                # Extract all chunks in parallel
                chunk_tasks = [
                    extract_chunk(i) for i in range(total_chunks)
                ]
                
                chunk_results = []
                for i, coro in enumerate(asyncio.as_completed(chunk_tasks)):
                    rows, chunk_path = await coro
                    chunk_results.append((rows, chunk_path))
                    
                    # Update progress
                    progress.processed_rows += rows
                    progress.current_chunk = i + 1
                    
                    if self.progress_callback:
                        self.progress_callback(progress)
                
                # Filter successful chunks
                successful_chunks = [p for r, p in chunk_results if r > 0 and p]
                total_processed = sum(r for r, _ in chunk_results)
                
                # Merge chunks in process pool (CPU-bound)
                progress.status = "merging"
                if self.progress_callback:
                    self.progress_callback(progress)
                
                merge_result = await self.task_runner.run_in_process_pool(
                    merge_csv_chunks,
                    successful_chunks,
                    str(output_path),
                )
                
                progress.status = "completed"
                progress.current_chunk = total_chunks
                if self.progress_callback:
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
            # Cleanup temp directory
            if self._temp_dir and self._temp_dir.exists():
                import shutil
                shutil.rmtree(self._temp_dir, ignore_errors=True)
```

---

### 3. Parallel Dataset Upload

**File**: `tcrm_toolkit/interactive/operations/dataset_upload.py`

```python
"""Parallel dataset upload with multiprocessing for CSV processing."""

import asyncio
import math
import tempfile
from pathlib import Path
from typing import Callable

import structlog
from tcrm_toolkit.core.services.dataset_service import DatasetService
from tcrm_toolkit.core.models import UploadProgress
from tcrm_toolkit.interactive.tasks import TaskRunner, split_csv_for_parallel, process_csv_chunk

logger = structlog.get_logger(__name__)


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
            
            # Get dataset name if not provided
            if not dataset_name:
                dataset = await service.get_dataset(dataset_id)
                dataset_name = dataset.name
            
            # Generate metadata from first row
            import pandas as pd
            first_chunk = pd.read_csv(file_path, nrows=1)
            metadata_json = service._generate_metadata_json(first_chunk, dataset_name)
            
            # Create external data job
            job_response = await client.create_insights_external_data(
                edgemart_alias=dataset_name,
                metadata_json=metadata_json,
                operation=operation,
            )
            external_data_id = job_response["id"]
            
            # Count total rows
            total_rows = sum(1 for _ in open(file_path)) - 1
            total_parts = math.ceil(total_rows / chunk_size)
            
            logger.info(
                "parallel_upload_started",
                dataset_id=dataset_id,
                external_data_id=external_data_id,
                total_rows=total_rows,
                total_parts=total_parts,
            )
            
            # Progress tracking
            progress = UploadProgress(
                total_rows=total_rows,
                uploaded_rows=0,
                current_part=0,
                total_parts=total_parts,
                status="uploading",
            )
            
            # Split CSV into chunks for parallel base64 encoding
            with tempfile.TemporaryDirectory() as tmpdir:
                chunk_paths = await self.task_runner.run_in_process_pool(
                    split_csv_for_parallel,
                    str(file_path),
                    max_process_workers,
                    tmpdir,
                )
                
                # Process chunks in parallel (base64 encoding is CPU-bound)
                chunk_args = [(path, i, total_parts) for i, path in enumerate(chunk_paths)]
                
                processed_chunks = await self.task_runner.run_in_process_pool(
                    self._process_chunks_parallel,
                    chunk_args,
                )
                
                # Upload parts sequentially (API requires sequential part numbers)
                for i, chunk_result in enumerate(processed_chunks):
                    await client.upload_insights_external_data_part(
                        external_data_id=external_data_id,
                        part_number=chunk_result["part_number"],
                        data_file_base64=chunk_result["data_file_base64"],
                    )
                    
                    progress.uploaded_rows += chunk_result["rows"]
                    progress.current_part = i + 1
                    
                    if self.progress_callback:
                        self.progress_callback(progress)
            
            # Process the data
            progress.status = "processing"
            if self.progress_callback:
                self.progress_callback(progress)
            
            result = await client.process_insights_external_data(external_data_id)
            
            progress.status = "completed"
            progress.current_part = total_parts
            if self.progress_callback:
                self.progress_callback(progress)
            
            logger.info(
                "parallel_upload_completed",
                dataset_id=dataset_id,
                external_data_id=external_data_id,
                rows=progress.uploaded_rows,
            )
            
            return {
                "rows": progress.uploaded_rows,
                "parts": total_parts,
                "result": result,
            }
    
    @staticmethod
    def _process_chunks_parallel(chunk_args: list[tuple]) -> list[dict]:
        """Process multiple chunks in parallel (runs in process pool)."""
        from concurrent.futures import ProcessPoolExecutor
        from tcrm_toolkit.interactive.tasks import process_csv_chunk
        
        with ProcessPoolExecutor() as executor:
            results = list(executor.map(process_csv_chunk, chunk_args))
        
        return results
```

---

### 4. Dashboard Backup/Restore Operations

**File**: `tcrm_toolkit/interactive/operations/dashboard_backup.py`

```python
"""Dashboard backup and restore operations."""

import asyncio
import json
from pathlib import Path
from typing import Callable

import structlog
from tcrm_toolkit.core.services.dashboard_service import DashboardService
from tcrm_toolkit.interactive.tasks import TaskRunner

logger = structlog.get_logger(__name__)


class DashboardBackupManager:
    """Manage dashboard backup and restore operations."""
    
    def __init__(
        self,
        session,
        task_runner: TaskRunner,
        progress_callback: Callable[[dict], None] | None = None,
    ):
        self.session = session
        self.task_runner = task_runner
        self.progress_callback = progress_callback
    
    async def backup_dashboard(
        self,
        dashboard_id: str,
        output_path: Path,
    ) -> dict[str, Any]:
        """Backup single dashboard to JSON file."""
        async with self.session.client_context() as client:
            service = DashboardService(client, self.session.settings)
            
            if self.progress_callback:
                self.progress_callback({"status": "fetching", "dashboard_id": dashboard_id})
            
            backup = await service.backup_dashboard(dashboard_id, output_path)
            
            if self.progress_callback:
                self.progress_callback({"status": "completed", "path": str(output_path)})
            
            return {
                "dashboard_id": dashboard_id,
                "dashboard_name": backup.dashboard_name,
                "path": str(output_path),
            }
    
    async def backup_all_dashboards(
        self,
        output_dir: Path,
        pattern: str | None = None,
    ) -> dict[str, Any]:
        """Backup all dashboards to directory."""
        output_dir.mkdir(parents=True, exist_ok=True)
        
        async with self.session.client_context() as client:
            service = DashboardService(client, self.session.settings)
            dashboards = await service.list_dashboards()
            
            if pattern:
                import fnmatch
                dashboards = [d for d in dashboards if fnmatch.fnmatch(d.label, pattern)]
            
            results = []
            for i, dashboard in enumerate(dashboards):
                if self.progress_callback:
                    self.progress_callback({
                        "status": "backing_up",
                        "current": i + 1,
                        "total": len(dashboards),
                        "dashboard": dashboard.label,
                    })
                
                try:
                    output_path = output_dir / f"{dashboard.name}.json"
                    await service.backup_dashboard(dashboard.id, output_path)
                    results.append({
                        "id": dashboard.id,
                        "name": dashboard.name,
                        "label": dashboard.label,
                        "path": str(output_path),
                        "status": "success",
                    })
                except Exception as e:
                    results.append({
                        "id": dashboard.id,
                        "name": dashboard.name,
                        "label": dashboard.label,
                        "error": str(e),
                        "status": "failed",
                    })
            
            if self.progress_callback:
                self.progress_callback({"status": "completed", "results": results})
            
            return {
                "total": len(dashboards),
                "successful": sum(1 for r in results if r["status"] == "success"),
                "failed": sum(1 for r in results if r["status"] == "failed"),
                "results": results,
            }
    
    async def restore_dashboard(
        self,
        backup_path: Path,
        new_name: str | None = None,
    ) -> dict[str, Any]:
        """Restore dashboard from backup file."""
        async with self.session.client_context() as client:
            service = DashboardService(client, self.session.settings)
            
            if self.progress_callback:
                self.progress_callback({"status": "restoring", "path": str(backup_path)})
            
            dashboard = await service.restore_dashboard(backup_path, new_name)
            
            if self.progress_callback:
                self.progress_callback({"status": "completed", "dashboard_id": dashboard.id})
            
            return {
                "dashboard_id": dashboard.id,
                "dashboard_name": dashboard.name,
                "source": str(backup_path),
            }
```

---

### 5. Dataflow Control Operations

**File**: `tcrm_toolkit/interactive/operations/dataflow_control.py`

```python
"""Dataflow start/stop/monitor operations."""

import asyncio
from typing import Callable

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
        progress_callback: Callable[[dict], None] | None = None,
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
```

---

### 6. Task History Panel Widget

**File**: `tcrm_toolkit/interactive/widgets/task_history.py`

```python
"""Task history panel for viewing past operations."""

from textual import on
from textual.app import ComposeResult
from textual.containers import Container, Vertical
from textual.widgets import DataTable, Label, Static, TabbedContent, TabPane

from tcrm_toolkit.interactive.tasks import TaskRunner, TaskResult, TaskStatus


class TaskHistoryPanel(Static):
    """Panel showing task history with filtering."""
    
    def __init__(self, task_runner: TaskRunner):
        super().__init__(id="task-history")
        self.task_runner = task_runner
        self._filter_status: TaskStatus | None = None
    
    def compose(self) -> ComposeResult:
        yield Vertical(
            Label("Task History", id="history-title"),
            TabbedContent(
                TabPane("All", id="tab-all"),
                TabPane("Running", id="tab-running"),
                TabPane("Completed", id="tab-completed"),
                TabPane("Failed", id="tab-failed"),
                id="history-tabs"
            ),
            DataTable(id="history-table", cursor_type="row", zebra_stripes=True),
            id="history-container"
        )
    
    async def on_mount(self) -> None:
        table = self.query_one("#history-table", DataTable)
        table.add_columns("Time", "Task", "Status", "Duration", "Details")
        table.zebra_stripes = True
        await self.refresh_history()
    
    @on(TabbedContent.TabActivated, "#history-tabs")
    async def on_tab_changed(self, event: TabbedContent.TabActivated) -> None:
        tab_map = {
            "tab-all": None,
            "tab-running": TaskStatus.RUNNING,
            "tab-completed": TaskStatus.COMPLETED,
            "tab-failed": TaskStatus.FAILED,
        }
        self._filter_status = tab_map.get(event.tab.id)
        await self.refresh_history()
    
    async def refresh_history(self) -> None:
        """Refresh history table."""
        table = self.query_one("#history-table", DataTable)
        table.clear()
        
        history = self.task_runner.get_history()
        
        if self._filter_status:
            history = [r for r in history if r.status == self._filter_status]
        
        # Show most recent first
        for result in reversed(history[-100:]):
            duration = ""
            if result.completed_at and result.started_at:
                delta = result.completed_at - result.started_at
                duration = f"{delta.total_seconds():.1f}s"
            
            status_style = {
                TaskStatus.COMPLETED: "[green]",
                TaskStatus.FAILED: "[red]",
                TaskStatus.CANCELLED: "[yellow]",
                TaskStatus.RUNNING: "[blue]",
            }.get(result.status, "")
            
            details = result.error or str(result.result)[:50] if result.result else ""
            
            table.add_row(
                result.started_at.strftime("%H:%M:%S"),
                result.task_id,
                f"{status_style}{result.status.value}[/]",
                duration,
                details,
            )
```

---

### 7. Progress Panel Widget

**File**: `tcrm_toolkit/interactive/widgets/progress_panel.py`

```python
"""Progress panel for showing running task progress."""

from textual import on
from textual.app import ComposeResult
from textual.containers import Container, Vertical
from textual.widgets import ProgressBar, Label, Static, DataTable

from tcrm_toolkit.interactive.tasks import TaskRunner, TaskProgress, TaskStatus


class ProgressPanel(Static):
    """Panel showing active task progress bars."""
    
    def __init__(self, task_runner: TaskRunner):
        super().__init__(id="progress-panel")
        self.task_runner = task_runner
    
    def compose(self) -> ComposeResult:
        yield Vertical(
            Label("Active Tasks", id="progress-title"),
            DataTable(id="progress-table", cursor_type="row"),
            id="progress-container"
        )
    
    async def on_mount(self) -> None:
        table = self.query_one("#progress-table", DataTable)
        table.add_columns("Task", "Status", "Progress", "Details")
        # Start update timer
        self.set_interval(1.0, self.update_progress)
    
    def update_progress(self) -> None:
        """Update progress table from task runner."""
        table = self.query_one("#progress-table", DataTable)
        table.clear()
        
        for progress in self.task_runner.get_all_progress():
            if progress.is_finished:
                continue
            
            pct = progress.percent
            bar = "█" * int(pct / 5) + "░" * (20 - int(pct / 5))
            
            table.add_row(
                progress.task_id,
                progress.status.value,
                f"{bar} {pct:.1f}%",
                progress.message,
            )
```

---

### 8. Integrate Operations into Main Screen

**File**: `tcrm_toolkit/interactive/screens/main_screen.py` (EXTEND)

```python
# Add context menu handling and operation triggers

@on(DataTable.RowSelected, "#datasets-table")
async def on_dataset_row_selected(self, event: DataTable.RowSelected) -> None:
    """Handle dataset row selection - show context menu."""
    table = event.data_table
    row_key = table.get_row_at(event.cursor_row).key
    
    # Find dataset in browser
    browser = self.query_one("#datasets-browser", DataBrowser)
    dataset = next((r for r in browser._filtered_rows if browser.get_row_id(r) == row_key), None)
    
    if dataset:
        detail = self.query_one("#detail-panel", DetailPanel)
        detail.show_dataset(dataset)
        
        # Show context menu on right-click or Ctrl+M
        # (Implementation in Phase 4)

async def action_extract_dataset(self) -> None:
    """Extract selected dataset."""
    # Get selected dataset from browser
    browser = self.query_one("#datasets-browser", DataBrowser)
    table = browser.query_one("#data-table", DataTable)
    
    if table.cursor_row >= 0:
        row_key = table.get_row_at(table.cursor_row).key
        dataset = next((r for r in browser._filtered_rows if browser.get_row_id(r) == row_key), None)
        
        if dataset:
            # Show file picker for output (simplified - use default path)
            output_path = Path.cwd() / f"{dataset.name}.csv"
            
            # Run extraction in background
            from tcrm_toolkit.interactive.operations.dataset_extract import ParallelDatasetExtractor
            extractor = ParallelDatasetExtractor(
                self.session,
                self.app.task_runner,
                progress_callback=self._on_extract_progress,
            )
            
            self.app.task_runner.run_task(
                lambda: extractor.extract(dataset.id, output_path),
                name=f"Extract {dataset.name}",
            )

def _on_extract_progress(self, progress: ExtractionProgress) -> None:
    """Handle extraction progress updates."""
    # Update progress panel
    progress_panel = self.query_one("#progress-panel", ProgressPanel)
    # ProgressPanel updates via TaskRunner messages
    pass
```

---

### 9. Command Palette Actions

**File**: `tcrm_toolkit/interactive/widgets/command_palette.py`

```python
"""Command palette for quick action access."""

from textual import on
from textual.app import ComposeResult
from textual.containers import Container, Vertical
from textual.screen import ModalScreen
from textual.widgets import Input, Label, ListItem, ListView, Static


class CommandPalette(ModalScreen[str]):
    """Command palette (Ctrl+P) for fuzzy action search."""
    
    COMMANDS = [
        ("Extract Dataset", "extract_dataset", "📥"),
        ("Upload Dataset", "upload_dataset", "📤"),
        ("Backup Dashboard", "backup_dashboard", "💾"),
        ("Restore Dashboard", "restore_dashboard", "📂"),
        ("Start Dataflow", "start_dataflow", "▶️"),
        ("Stop Dataflow", "stop_dataflow", "⏹️"),
        ("Switch Organization", "switch_org", "🔐"),
        ("Refresh Current View", "refresh", "🔄"),
        ("Open Settings", "settings", "⚙️"),
        ("View Task History", "task_history", "📋"),
        ("Check Connection Safety", "safety_check", "🛡️"),
        ("Quit", "quit", "❌"),
    ]
    
    def __init__(self):
        super().__init__()
        self._filtered_commands = self.COMMANDS
    
    def compose(self) -> ComposeResult:
        yield Container(
            Vertical(
                Static("⌘ Command Palette", id="palette-title"),
                Input(placeholder="Type to search commands...", id="palette-input"),
                ListView(
                    *[ListItem(Label(f"{icon}  {label}"), id=f"cmd-{action}") 
                      for label, action, icon in self.COMMANDS],
                    id="palette-list"
                ),
                id="palette-container"
            ),
            id="palette-dialog"
        )
    
    async def on_mount(self) -> None:
        self.query_one("#palette-input", Input).focus()
    
    @on(Input.Changed, "#palette-input")
    def on_input_changed(self, event: Input.Changed) -> None:
        query = event.value.lower()
        list_view = self.query_one("#palette-list", ListView)
        
        if query:
            self._filtered_commands = [
                (label, action, icon) for label, action, icon in self.COMMANDS
                if query in label.lower() or query in action.lower()
            ]
        else:
            self._filtered_commands = self.COMMANDS
        
        # Rebuild list
        list_view.clear()
        for label, action, icon in self._filtered_commands:
            list_view.append(ListItem(Label(f"{icon}  {label}"), id=f"cmd-{action}"))
    
    @on(ListView.Selected, "#palette-list")
    def on_command_selected(self, event: ListView.Selected) -> None:
        if event.item.id and event.item.id.startswith("cmd-"):
            action = event.item.id[4:]
            self.dismiss(action)
    
    def on_key(self, event) -> None:
        if event.key == "escape":
            self.dismiss(None)
```

---

## ✅ Acceptance Criteria

| Feature | Verification |
|---------|--------------|
| TaskRunner executes async tasks | `task_runner.run_task()` runs coroutine in background |
| ProcessPoolExecutor works | `run_in_process_pool()` executes CPU-bound function |
| Parallel extraction | 1M+ row dataset extracts faster than sequential |
| Progress updates | ProgressPanel shows real-time progress bars |
| Task history | Completed tasks appear in history panel |
| Cancellation | Running tasks can be cancelled |
| Dashboard backup | Single and bulk backup work |
| Dataflow control | Start/stop/wait work with progress |
| Command palette | Ctrl+P shows searchable commands |

---

## 🔧 Coding Agent Instructions

### Implementation Order
1. **tasks.py** - TaskRunner with ProcessPoolExecutor (core infrastructure)
2. **dataset_extract.py** - ParallelDatasetExtractor with SAQL parallelization
3. **dataset_upload.py** - ParallelDatasetUploader with chunk processing
4. **dashboard_backup.py** - Backup/restore operations
5. **dataflow_control.py** - Start/stop/monitor
6. **progress_panel.py** - Progress display widget
7. **task_history.py** - History panel widget
8. **command_palette.py** - Ctrl+P command palette
9. **main_screen.py** - Integrate operations, context menus

### Parallel Processing Patterns

**For I/O-bound (SAQL queries, HTTP requests):**
```python
semaphore = asyncio.Semaphore(10)  # Limit concurrency
async with semaphore:
    result = await client.saql_query(saql)
```

**For CPU-bound (pandas concat, base64 encoding, CSV processing):**
```python
# Define top-level function (picklable)
def merge_csv_chunks(chunk_paths, output_path):
    import pandas as pd
    combined = pd.concat([pd.read_csv(p) for p in chunk_paths])
    combined.to_csv(output_path, index=False)

# Run in process pool
await task_runner.run_in_process_pool(merge_csv_chunks, paths, output)
```

### Key Principles
- **Async for I/O**: Network requests, API calls, file reads
- **Multiprocessing for CPU**: pandas operations, data transformations, encoding
- **Semaphore for rate limiting**: Respect Salesforce API limits
- **Progress callbacks**: Update UI without blocking
- **Temp directories**: Clean up automatically with `tempfile.TemporaryDirectory()`

### Testing
```bash
# Test parallel extraction
tcrm  # Launch TUI
# Navigate to datasets
# Select large dataset
# Press 'E' or use command palette "Extract Dataset"
# Watch progress panel for parallel chunk downloads
# Verify output CSV has all rows

# Test parallel upload
# Prepare large CSV (100k+ rows)
# Use command palette "Upload Dataset"
# Watch progress for chunk processing + upload

# Test task history
# Complete several operations
# Press Ctrl+P -> "View Task History"
# Verify all tasks listed with status
```

---

## 📝 Architecture Decisions (Log in `architecture-decisions.md`)

- [x] Decision: TaskRunner uses ProcessPoolExecutor for CPU-bound work
- [x] Decision: Semaphore limits concurrent SAQL queries (default 10)
- [x] Decision: Chunk size 50k-150k rows based on dataset size
- [x] Decision: Temp directory for chunk files, auto-cleanup
- [x] Decision: Sequential part upload (API requirement) after parallel prep
- [x] Decision: Progress via Textual messages (not callbacks) for decoupling
- [x] Decision: Command palette as ModalScreen with fuzzy search

---

*End of Phase 3 Document*