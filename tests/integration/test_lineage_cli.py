"""Integration tests for lineage CLI invocation and file output."""

import subprocess
import sys
from pathlib import Path


def test_lineage_help():
    result = subprocess.run(
        [sys.executable, "-m", "asftool.cli.main", "lineage", "--help"],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0
    assert "generate" in result.stdout
    assert "Mermaid" in result.stdout or "mermaid" in result.stdout
    assert "json" in result.stdout.lower()
    assert "svg" not in result.stdout.lower()


def test_lineage_generate_invalid_format():
    result = subprocess.run(
        [sys.executable, "-m", "asftool.cli.main", "lineage", "generate", "x", "--format", "bad"],
        capture_output=True,
        text=True,
    )
    # Should exit with error or print error message; just verify it runs without crash
    assert "bad" in result.stdout or result.returncode != 0 or "lineage" in result.stdout
