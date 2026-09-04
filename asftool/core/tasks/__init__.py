"""Core tasks module — parallelism utilities.

Provides TaskRunner (async + ProcessPoolExecutor) and picklable
parallel helpers (merge_csv_chunks, process_csv_chunk,
split_csv_for_parallel, _process_chunks_parallel).

These are TUI-agnostic — used by services and the CLI alike.
"""

from asftool.core.tasks.parallel import (
    _process_chunks_parallel,
    merge_csv_chunks,
    process_csv_chunk,
    split_csv_for_parallel,
)
from asftool.core.tasks.runner import TaskProgress, TaskResult, TaskRunner, TaskStatus

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
