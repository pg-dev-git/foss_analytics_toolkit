"""Background task runner with ProcessPoolExecutor for CPU-bound work.

TUI-agnostic: no Textual Widget inheritance, no Messages. Pure async +
multiprocessing primitives so the runner can be used by the CLI, future
TUI, or tests.
"""

import asyncio
import os
import uuid
from collections.abc import Callable
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

import structlog

logger = structlog.get_logger(__name__)


class TaskStatus(StrEnum):
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
    details: dict[str, Any] = field(default_factory=dict)
    started_at: datetime = field(default_factory=lambda: datetime.now(UTC))
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
    started_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    completed_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    metadata: dict[str, Any] = field(default_factory=dict)


class TaskRunner:
    """Background task runner.

    Combines an asyncio.Semaphore (to cap concurrency) with a
    ProcessPoolExecutor (to offload CPU-bound work to a worker pool).
    Progress is delivered via a sync callback; the runner itself is
    fully async.
    """

    def __init__(
        self,
        max_concurrent: int = 3,
        max_history: int = 100,
        process_pool_size: int | None = None,
    ):
        self.max_concurrent = max_concurrent
        self.max_history = max_history
        self.process_pool_size = process_pool_size or min(4, os.cpu_count() or 4)

        self._tasks: dict[str, asyncio.Task[Any]] = {}
        self._progress: dict[str, TaskProgress] = {}
        self._history: list[TaskResult] = []
        self._process_pool: ProcessPoolExecutor | None = None
        self._semaphore = asyncio.Semaphore(max_concurrent)

    @property
    def process_pool(self) -> ProcessPoolExecutor:
        """Lazy-initialize the process pool."""
        if self._process_pool is None:
            self._process_pool = ProcessPoolExecutor(max_workers=self.process_pool_size)
        return self._process_pool

    async def run_task(
        self,
        coro_factory: Callable[[], Any],
        task_id: str | None = None,
        name: str = "Task",
        progress_callback: Callable[[TaskProgress], Any] | None = None,
    ) -> TaskResult:
        """Run a coroutine as a background task with semaphore-bounded concurrency."""
        task_id = task_id or str(uuid.uuid4())[:8]

        async with self._semaphore:
            progress = TaskProgress(
                task_id=task_id,
                status=TaskStatus.RUNNING,
                message=f"Starting {name}...",
            )
            self._progress[task_id] = progress
            if progress_callback:
                self._invoke_cb(progress_callback, progress)

            task = asyncio.create_task(
                self._run_task_impl(task_id, name, coro_factory, progress, progress_callback)
            )
            self._tasks[task_id] = task
            try:
                return await task
            finally:
                self._tasks.pop(task_id, None)

    async def _run_task_impl(
        self,
        task_id: str,
        name: str,
        coro_factory: Callable[..., Any],
        progress: TaskProgress,
        progress_callback: Callable[[TaskProgress], Any] | None,
    ) -> TaskResult:
        try:
            result = await coro_factory()

            progress.status = TaskStatus.COMPLETED
            progress.current = progress.total or progress.current
            progress.message = f"{name} completed"
            progress.completed_at = datetime.now(UTC)
            task_result = TaskResult(
                task_id=task_id,
                status=TaskStatus.COMPLETED,
                result=result,
                completed_at=progress.completed_at,
            )
        except asyncio.CancelledError:
            progress.status = TaskStatus.CANCELLED
            progress.message = f"{name} cancelled"
            progress.completed_at = datetime.now(UTC)
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
            progress.completed_at = datetime.now(UTC)
            task_result = TaskResult(
                task_id=task_id,
                status=TaskStatus.FAILED,
                error=str(e),
                completed_at=progress.completed_at,
            )

        if progress_callback:
            self._invoke_cb(progress_callback, progress)
        self._add_to_history(task_result)
        return task_result

    @staticmethod
    def _invoke_cb(cb: Callable[..., Any], progress: TaskProgress) -> None:
        """Invoke a progress callback, awaiting it if it's a coroutine function."""
        result = cb(progress)
        if asyncio.iscoroutine(result):
            # The callback returned a coroutine; schedule it on the running loop.
            # We can't await here (we may be sync), so we just create a task and
            # let it run in the background. Errors are logged but don't fail
            # the task.
            try:
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    loop.create_task(result)
                else:
                    asyncio.run(result)
            except RuntimeError:
                # No loop — fall back to running synchronously.
                asyncio.run(result)

    def _add_to_history(self, result: TaskResult) -> None:
        self._history.append(result)
        if len(self._history) > self.max_history:
            self._history = self._history[-self.max_history :]

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

    async def run_in_process_pool(self, func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        """Run a CPU-bound function in the process pool.

        `func` must be a top-level (picklable) function.
        """
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self.process_pool, func, *args, **kwargs)

    async def close(self) -> None:
        """Cancel pending tasks and shut the process pool down."""
        await self.cancel_all()
        if self._tasks:
            await asyncio.gather(*self._tasks.values(), return_exceptions=True)
        if self._process_pool is not None:
            self._process_pool.shutdown(wait=True)
            self._process_pool = None
