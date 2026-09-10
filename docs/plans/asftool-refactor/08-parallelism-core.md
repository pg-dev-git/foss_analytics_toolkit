# Phase 8: Parallelism Core — TaskRunner + Parallel Helpers

**Goal:** Extract `TaskRunner` and parallel utilities from TUI code into `core/tasks/` — TUI-free, reusable by CLI.

---

## Prerequisites

- Phase 0 complete (TUI deleted)
- `interactive/tasks.py` still exists in git history

---

## Files to Create

```
asftool/core/tasks/
├── __init__.py
├── runner.py              # TaskRunner (async + ProcessPoolExecutor)
├── parallel.py            # merge_csv_chunks, process_csv_chunk, split_csv_for_parallel
```

---

## Step 8.1: Create `asftool/core/tasks/runner.py`

Based on `interactive/tasks.py` but **TUI-free** (no Textual Widget inheritance, no Messages).

```python
"""Background task runner with ProcessPoolExecutor for CPU-bound work."""

import asyncio
import os
import uuid
from collections.abc import Callable
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any

import structlog

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
    completed_at: datetime | None = None
    error: str | None = None

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
    error: str | None = None
    started_at: datetime = field(default_factory=datetime.utcnow)
    completed_at: datetime = field(default_factory=datetime.utcnow)
    metadata: dict = field(default_factory=dict)


class TaskRunner:
    """
    Background task runner with:
    - Async task execution (I/O-bound)
    - ProcessPoolExecutor for CPU-bound work (multiprocessing)
    - Progress tracking via callbacks
    - Task history
    - Cancellation support
    - Max concurrent tasks limit
    """

    def __init__(
        self,
        max_concurrent: int = 3,
        max_history: int = 100,
        process_pool_size: int | None = None,
    ):
        self.max_concurrent = max_concurrent
        self.max_history = max_history
        self.process_pool_size = process_pool_size or min(4, (os.cpu_count() or 4))

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
        coro_factory: Callable[[], Any],
        task_id: str | None = None,
        name: str = "Task",
        progress_callback: Callable[[TaskProgress], None] | None = None,
    ) -> TaskResult:
        """Run a coroutine as a background task."""
        task_id = task_id or str(uuid.uuid4())[:8]

        async with self._semaphore:
            progress = TaskProgress(
                task_id=task_id,
                status=TaskStatus.RUNNING,
                message=f"Starting {name}...",
            )
            self._progress[task_id] = progress

            if progress_callback:
                progress_callback(progress)

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
            coro = coro_factory()
            result = await coro

            progress.status = TaskStatus.COMPLETED
            progress.current = progress.total or progress.current
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

        if progress_callback:
            progress_callback(progress)

        self._add_to_history(task_result)
        return task_result

    def _add_to_history(self, result: TaskResult) -> None:
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
        task = self._tasks.get(task_id)
        if task and not task.done():
            task.cancel()
            return True
        return False

    async def cancel_all(self) -> int:
        count = 0
        for task in self._tasks.values():
            if not task.done():
                task.cancel()
                count += 1
        return count

    async def run_in_process_pool(self, func: Callable, *args, **kwargs) -> Any:
        """Run a CPU-bound function in the process pool."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self.process_pool, func, *args, **kwargs)

    async def close(self) -> None:
        """Cleanup resources."""
        await self.cancel_all()
        if self._tasks:
            await asyncio.gather(*self._tasks.values(), return_exceptions=True)
        if self._process_pool:
            self._process_pool.shutdown(wait=True)
            self._process_pool = None
```

---

## Step 8.2: Create `asftool/core/tasks/parallel.py`

```python
"""Parallel processing utilities for dataset operations."""

import base64
from pathlib import Path
from typing import Any

import pandas as pd


def merge_csv_chunks(chunk_paths: list[str], output_path: str) -> dict[str, Any]:
    """
    Merge multiple CSV chunks into single file.

    Called in ProcessPoolExecutor (CPU-bound).
    """
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


def process_csv_chunk(args: tuple[str, int, int]) -> dict[str, Any]:
    """
    Process a single CSV chunk for upload (base64 encode).

    Called in ProcessPoolExecutor (CPU-bound).

    Args:
        args: (chunk_path, chunk_index, total_chunks)
    """
    chunk_data, chunk_index, total_chunks = args
    df = pd.read_csv(chunk_data)
    csv_bytes = df.to_csv(index=False).encode()
    b64 = base64.b64encode(csv_bytes).decode()

    return {
        "part_number": chunk_index + 1,
        "data_file_base64": b64,
        "rows": len(df),
    }


def _process_chunks_parallel(chunk_args: list[tuple]) -> list[dict[str, Any]]:
    """Top-level helper to process multiple chunks in process pool."""
    return [process_csv_chunk(args) for args in chunk_args]


def split_csv_for_parallel(
    input_path: str,
    num_chunks: int,
    output_dir: str,
) -> list[str]:
    """
    Split large CSV into chunks for parallel processing.

    Called in ProcessPoolExecutor (CPU-bound).

    Returns list of chunk file paths.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    chunk_paths = []
    chunk_size = 100000  # 100k rows per chunk

    for i, chunk in enumerate(pd.read_csv(input_path, chunksize=chunk_size)):
        if i >= num_chunks:
            break
        chunk_path = output_dir / f"chunk_{i:04d}.csv"
        chunk.to_csv(chunk_path, index=False)
        chunk_paths.append(str(chunk_path))

    return chunk_paths
```

---

## Step 8.3: Create `asftool/core/tasks/__init__.py`

```python
"""Core tasks module — parallelism utilities."""

from asftool.core.tasks.runner import TaskRunner, TaskProgress, TaskResult, TaskStatus
from asftool.core.tasks.parallel import (
    merge_csv_chunks,
    process_csv_chunk,
    split_csv_for_parallel,
    _process_chunks_parallel,
)

__all__ = [
    "TaskRunner",
    "TaskProgress",
    "TaskResult",
    "TaskStatus",
    "merge_csv_chunks",
    "process_csv_chunk",
    "split_csv_for_parallel",
    "_process_chunks_parallel",
]
```

---

## Step 8.4: Update Services to Use Core Tasks

In `asftool/core/services/dataset_service.py`, replace any direct `ProcessPoolExecutor` usage with `TaskRunner` from `core.tasks`.

```python
# At top of dataset_service.py
from asftool.core.tasks import TaskRunner, merge_csv_chunks, _process_chunks_parallel, split_csv_for_parallel

# In extract_dataset method, use TaskRunner for merge:
runner = TaskRunner(process_pool_size=4)
merge_result = await runner.run_in_process_pool(
    merge_csv_chunks,
    [str(p) for p in successful_chunks],
    str(output_path),
)
await runner.close()
```

---

## Step 8.5: Update CLI to Use TaskRunner for Progress

In `asftool/cli/commands/datasets.py` extract/upload commands, create a `TaskRunner` for progress tracking if needed.

```python
# For parallel operations, use TaskRunner
runner = TaskRunner(max_concurrent=1, process_pool_size=4)
# ... use runner.run_in_process_pool for CPU-bound parts
await runner.close()
```

---

## Acceptance Criteria

- [ ] `asftool/core/tasks/` module exists with runner.py, parallel.py
- [ ] No Textual dependencies (no Widget, no Messages)
- [ ] `TaskRunner` works with async callbacks for progress
- [ ] `merge_csv_chunks`, `process_csv_chunk`, `split_csv_for_parallel` are picklable (top-level functions)
- [ ] `DatasetService` uses `TaskRunner` for parallel merge
- [ ] `uv run pytest tests/unit/test_* -v` passes
- [ ] `uv run ruff check asftool/core/tasks/` passes

---

## Notes

- This extracts the proven parallelism pattern from TUI into reusable core
- Top-level functions in `parallel.py` are required for pickling in ProcessPoolExecutor
- `TaskRunner` is now TUI-agnostic — can be used by CLI, tests, or future TUI
- Process pool size defaults to min(4, CPU count) — configurable