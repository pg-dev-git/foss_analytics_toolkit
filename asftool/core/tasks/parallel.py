"""Parallel processing utilities for dataset operations.

All functions here are top-level (importable) and picklable, so they
can be passed to ProcessPoolExecutor via TaskRunner.run_in_process_pool().
"""

import base64
from pathlib import Path
from typing import Any

import pandas as pd


def merge_csv_chunks(chunk_paths: list[str], output_path: str) -> dict[str, Any]:
    """Merge multiple CSV chunk files into a single output CSV.

    Called in ProcessPoolExecutor (CPU-bound: pandas concat + to_csv).

    Args:
        chunk_paths: List of CSV file paths to merge.
        output_path: Destination CSV file path.

    Returns:
        Dict with output_path, total_rows, and chunks_merged count.
    """
    chunks: list[pd.DataFrame] = []
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
    """Read a CSV chunk, base64-encode it, return upload-ready payload.

    Called in ProcessPoolExecutor (CPU-bound: pandas read + base64).

    Args:
        args: (chunk_path, chunk_index, total_chunks)

    Returns:
        Dict with part_number (1-based), base64-encoded data, and row count.
    """
    chunk_data, chunk_index, _total_chunks = args
    df = pd.read_csv(chunk_data)
    csv_bytes = df.to_csv(index=False).encode()
    b64 = base64.b64encode(csv_bytes).decode()

    return {
        "part_number": chunk_index + 1,
        "data_file_base64": b64,
        "rows": len(df),
    }


def _process_chunks_parallel(chunk_args: list[tuple[str, int, int]]) -> list[dict[str, Any]]:
    """Process all chunks sequentially. Use as a single ProcessPoolExecutor job.

    Splits work across processes by submitting a single call to
    ``run_in_process_pool`` rather than one call per chunk — fewer
    inter-process round-trips for the same total work.
    """
    return [process_csv_chunk(args) for args in chunk_args]


def split_csv_for_parallel(
    input_path: str,
    num_chunks: int,
    output_dir: str,
    chunk_size: int = 100_000,
) -> list[str]:
    """Split a large CSV into fixed-size chunk files.

    Called in ProcessPoolExecutor (CPU-bound: pandas IO).

    Args:
        input_path: Source CSV path.
        num_chunks: Maximum number of chunks to produce (caps the output).
        output_dir: Directory to write chunk files into (created if needed).
        chunk_size: Rows per chunk.

    Returns:
        List of chunk file paths.
    """
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    chunk_paths: list[str] = []
    for i, chunk in enumerate(pd.read_csv(input_path, chunksize=chunk_size)):
        if i >= num_chunks:
            break
        chunk_path = out_dir / f"chunk_{i:04d}.csv"
        chunk.to_csv(chunk_path, index=False)
        chunk_paths.append(str(chunk_path))

    return chunk_paths
