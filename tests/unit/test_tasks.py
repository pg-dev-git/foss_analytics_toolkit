"""Unit tests for the core tasks module (TaskRunner + parallel helpers)."""

import asyncio

import pandas as pd
import pytest

from asftool.core.tasks import (
    TaskProgress,
    TaskRunner,
    TaskStatus,
    merge_csv_chunks,
    process_csv_chunk,
    split_csv_for_parallel,
)


def _sample_df(rows: int) -> pd.DataFrame:
    return pd.DataFrame(
        {"id": list(range(rows)), "name": [f"row-{i}" for i in range(rows)]}
    )


class TestTaskRunner:
    """Tests for TaskRunner async task execution."""

    @pytest.mark.asyncio
    async def test_run_task_success(self):
        """A completed task returns its result and reports COMPLETED."""
        runner = TaskRunner(max_concurrent=2, process_pool_size=1)

        async def _work() -> int:
            await asyncio.sleep(0.01)
            return 42

        result = await runner.run_task(_work, name="test-task")
        await runner.close()

        assert result.status == TaskStatus.COMPLETED
        assert result.result == 42

    @pytest.mark.asyncio
    async def test_run_task_failure(self):
        """A failing task returns FAILED with the error message, no raise."""
        runner = TaskRunner(max_concurrent=2, process_pool_size=1)

        async def _boom() -> None:
            raise ValueError("kaboom")

        result = await runner.run_task(_boom, name="boom-task")
        await runner.close()

        assert result.status == TaskStatus.FAILED
        assert result.error == "kaboom"

    @pytest.mark.asyncio
    async def test_progress_callback(self):
        """Progress callback is invoked with RUNNING then COMPLETED."""
        runner = TaskRunner(max_concurrent=2, process_pool_size=1)
        seen: list[TaskStatus] = []

        def _on_progress(p: TaskProgress) -> None:
            seen.append(p.status)

        async def _work() -> str:
            return "ok"

        await runner.run_task(_work, name="progress-task", progress_callback=_on_progress)
        await runner.close()

        assert TaskStatus.RUNNING in seen
        assert TaskStatus.COMPLETED in seen

    @pytest.mark.asyncio
    async def test_history_tracked(self):
        """Completed tasks are recorded in history (bounded by max_history)."""
        runner = TaskRunner(max_history=5, process_pool_size=1)

        async def _noop() -> None:
            return None

        for _ in range(7):
            await runner.run_task(_noop, name="h")

        await runner.close()
        assert len(runner.get_history()) == 5  # bounded


class TestParallelHelpers:
    """Tests for the picklable parallel helpers."""

    def test_merge_csv_chunks(self, tmp_path):
        """Merging disjoint chunks concatenates rows."""
        a = tmp_path / "a.csv"
        b = tmp_path / "b.csv"
        out = tmp_path / "out.csv"
        _sample_df(2).to_csv(a, index=False)
        _sample_df(3).to_csv(b, index=False)

        result = merge_csv_chunks([str(a), str(b)], str(out))

        assert result["total_rows"] == 5
        assert result["chunks_merged"] == 2
        merged = pd.read_csv(out)
        assert len(merged) == 5

    def test_process_csv_chunk(self, tmp_path):
        """Chunk is base64-encoded with 1-based part number."""
        f = tmp_path / "chunk.csv"
        _sample_df(4).to_csv(f, index=False)

        result = process_csv_chunk((str(f), 0, 3))

        assert result["part_number"] == 1
        assert result["rows"] == 4
        assert result["data_file_base64"]

    def test_split_csv_for_parallel(self, tmp_path):
        """Large CSV is split into bounded chunk files."""
        src = tmp_path / "big.csv"
        _sample_df(250_000).to_csv(src, index=False)

        chunk_paths = split_csv_for_parallel(
            str(src), num_chunks=3, output_dir=str(tmp_path / "chunks")
        )

        # 250k rows @ 100k chunk_size → exactly 3 chunks
        assert len(chunk_paths) == 3
        assert all(__import__("os").path.exists(p) for p in chunk_paths)
