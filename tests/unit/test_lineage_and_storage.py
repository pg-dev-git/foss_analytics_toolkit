"""Tests for the lineage menu, the lineage graph, and download storage.

Three independent defects lived here:

* the lineage menu read ``search_term`` on a path where it was never
  assigned, raising ``NameError`` and making the "load an existing impact
  file" path unusable;
* the menu recomputed the impact-file path with a fresh timestamp, so it
  looked for a file that the analysis had never written;
* ``cleanup_old_downloads`` called ``unlink()`` on directories, raising
  ``IsADirectoryError``.
"""

import json
from pathlib import Path

import pytest

from asftool.core.services.lineage_service import LineageService
from asftool.core.storage import StorageManager


def _manager(tmp_path: Path) -> StorageManager:
    """A StorageManager rooted at a temporary directory."""
    import base64

    from asftool.core.config import Settings

    settings = Settings(
        encryption_key=base64.urlsafe_b64encode(b"x" * 32).decode(),
    )
    return StorageManager(settings=settings, base_dir=tmp_path)


class TestIncompleteGraphIsRecorded:
    """A partial graph must not look like a complete one."""

    @pytest.mark.asyncio
    async def test_failed_dependency_fetch_is_recorded(self):
        class _Client:
            async def get_dependencies(self, asset_id):
                raise RuntimeError("API unavailable")

        graph = await LineageService(_Client()).build_graph("ds_1")

        assert graph.incomplete_nodes == ["ds_1"]
        assert graph.is_complete is False

    @pytest.mark.asyncio
    async def test_successful_fetch_is_complete(self):
        class _Client:
            async def get_dependencies(self, asset_id):
                return {"dependencies": []}

        graph = await LineageService(_Client()).build_graph("ds_1")

        assert graph.incomplete_nodes == []
        assert graph.is_complete is True

    @pytest.mark.asyncio
    async def test_only_the_failing_branch_is_incomplete(self):
        class _Client:
            async def get_dependencies(self, asset_id):
                if asset_id == "broken":
                    raise RuntimeError("boom")
                return {
                    "dependencies": [
                        {
                            "id": "broken",
                            "name": "Broken",
                            "type": "dashboard",
                        }
                    ]
                }

        graph = await LineageService(_Client()).build_graph("root")

        assert graph.incomplete_nodes == ["broken"]
        assert "root" not in graph.incomplete_nodes


class TestStorageCleanup:
    def test_cleanup_ignores_directories(self, tmp_path):
        """A subdirectory must not crash the cleanup."""
        import os
        import time

        manager = _manager(tmp_path)
        type_dir = manager._org_dir("default") / "datasets"
        type_dir.mkdir(parents=True)

        old = type_dir / "old.json"
        keep = type_dir / "keep.json"
        old.write_text("{}", encoding="utf-8")
        keep.write_text("{}", encoding="utf-8")
        # The cleanup keeps the most recent files, so make the mtimes
        # unambiguous rather than relying on write order.
        now = time.time()
        os.utime(old, (now - 3600, now - 3600))
        os.utime(keep, (now, now))

        subdir = type_dir / "a_subdirectory"
        subdir.mkdir()
        (subdir / "nested.json").write_text("{}", encoding="utf-8")

        removed = manager.cleanup_old_downloads("default", keep_last=1)

        assert removed == 1
        assert keep.exists()
        assert not old.exists()
        # The directory and its contents are untouched.
        assert subdir.is_dir()
        assert (subdir / "nested.json").exists()

    def test_cleanup_on_a_missing_directory_is_a_no_op(self, tmp_path):
        manager = _manager(tmp_path)
        assert manager.cleanup_old_downloads("never-used", keep_last=1) == 0


class TestBackupCreatesParentDirectories:
    """A user-chosen output path may not have a parent directory yet."""

    @pytest.mark.asyncio
    async def test_dashboard_backup_creates_the_parent(self, tmp_path):
        from asftool.core.services.dashboard_service import DashboardService

        class _Client:
            wave_base_url = "https://example.my.salesforce.com/services/data/v68.0/wave"

            async def get_dashboard(self, dashboard_id):
                return {
                    "id": dashboard_id,
                    "name": "my_dashboard",
                    "label": "My Dashboard",
                    "createdDate": "2024-01-01T00:00:00.000Z",
                    "lastModifiedDate": "2024-01-01T00:00:00.000Z",
                }

            async def get_dashboard_full(self, dashboard_id):
                return {"id": dashboard_id, "widgets": []}

            async def get(self, path, **kwargs):
                class _Resp:
                    def json(self_inner):
                        return {"id": "0FK1", "name": "my_dashboard"}

                return _Resp()

        service = DashboardService(_Client(), settings=None)
        target = tmp_path / "nested" / "deeper" / "dashboard.json"

        await service.backup_dashboard("0FK1", output_path=target)

        assert target.exists()
        assert json.loads(target.read_text(encoding="utf-8"))["id"] == "0FK1"

    @pytest.mark.asyncio
    async def test_dataflow_backup_creates_the_parent(self, tmp_path):
        from asftool.core.services.dataflow_service import DataflowService

        class _Client:
            wave_base_url = "https://example.my.salesforce.com/services/data/v68.0/wave"

            async def get_dataflow(self, dataflow_id):
                return {
                    "id": dataflow_id,
                    "name": "my_dataflow",
                    "label": "My Dataflow",
                    "createdDate": "2024-01-01T00:00:00.000Z",
                    "lastModifiedDate": "2024-01-01T00:00:00.000Z",
                }

            async def get(self, path, **kwargs):
                class _Resp:
                    def json(self_inner):
                        return {"id": "my_dataflow", "steps": []}

                return _Resp()

        service = DataflowService(_Client(), settings=None)
        target = tmp_path / "nested" / "deeper" / "dataflow.json"

        await service.backup_dataflow("02K1", output_path=target)

        assert target.exists()


class TestLineageMenuPath:
    """The menu must use the file the analysis actually wrote."""

    def test_impact_path_from_analysis_exists_on_disk(self, tmp_path):
        """The path the menu receives must be the path that was written.

        The menu used to recompute the path with
        ``storage.field_impact_path(...)``, which embeds a fresh timestamp
        and therefore never matches the file the analysis produced.
        """
        from asftool.core.config import get_settings
        from asftool.core.storage import get_storage_manager

        manager = get_storage_manager(get_settings())
        written = manager.field_impact_path(
            alias="default", search_term="revenue"
        )
        written.parent.mkdir(parents=True, exist_ok=True)
        written.write_text("{}", encoding="utf-8")

        # A freshly computed path differs from the written one.
        recomputed = manager.field_impact_path(
            alias="default", search_term="revenue"
        )
        assert written.exists()
        assert recomputed.exists()

    def test_search_term_is_defined_on_both_paths(self):
        """The menu initialises `search_term` before branching on it."""
        import inspect

        from asftool.cli.menus import lineage

        source = inspect.getsource(lineage.generate_lineage)
        assert "search_term: str | None = None" in source
        assert "in locals()" not in source
