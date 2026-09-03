"""Background task runner with progress tracking and history."""

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
        """
        Run a coroutine as a background task.
        """
        task_id = task_id or str(uuid.uuid4())[:8]

        async with self._semaphore:
            progress = TaskProgress(
                task_id=task_id,
                status=TaskStatus.RUNNING,
                message=f"Starting {name}...",
            )
            self._progress[task_id] = progress
            self.post_message(TaskProgressMessage(progress))

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

        self.post_message(TaskProgressMessage(progress))
        if progress_callback:
            progress_callback(progress)

        self._add_to_history(task_result)
        self.post_message(TaskCompletedMessage(task_result))

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
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self.process_pool, func, *args, **kwargs)

    async def close(self) -> None:
        await self.cancel_all()
        if self._tasks:
            await asyncio.gather(*self._tasks.values(), return_exceptions=True)
        if self._process_pool:
            self._process_pool.shutdown(wait=True)
            self._process_pool = None


def merge_csv_chunks(chunk_paths: list[str], output_path: str) -> dict:
    """Merge multiple CSV chunks into single file."""
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
    """Process a single CSV chunk for upload."""
    import base64

    import pandas as pd

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
    from pathlib import Path

    import pandas as pd

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

