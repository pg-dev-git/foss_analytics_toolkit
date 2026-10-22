"""Prove the listing services actually walk every page.

The Wave API returns a ``nextPageUrl`` whose cursor lives in the ``page``
query parameter. Two separate defects used to hide here:

* the response model did not map ``nextPageUrl``, so the loop stopped after
  page 1 and the CLI reported "Total: 50 datasets" for an org with 500;
* the client sent the cursor as ``pageToken``, which the platform silently
  ignores -- so the request returns page 1 again.

These tests drive the services with a fake client that records the cursor it
was given, and assert that every page is fetched exactly once.
"""

import pytest

from asftool.core.services.dashboard_service import DashboardService
from asftool.core.services.dataset_service import DatasetService
from tests.fixtures import wave_responses


def _page(url: str | None, dataset_ids: list[str]) -> dict:
    """Build a dataset listing page."""
    base = wave_responses.DATASETS_PAGE_1
    return {
        "datasets": [
            {**base["datasets"][0], "id": ds_id, "name": ds_id, "label": ds_id}
            for ds_id in dataset_ids
        ],
        "nextPageUrl": url,
    }


def _dashboard_page(url: str | None, dashboard_ids: list[str]) -> dict:
    base = wave_responses.DASHBOARDS["dashboards"][0]
    return {
        "dashboards": [
            {**base, "id": db_id, "name": db_id, "label": db_id}
            for db_id in dashboard_ids
        ],
        "nextPageUrl": url,
    }


class _FakeDatasetClient:
    """Records the cursor it was handed and replays pages in order."""

    def __init__(self, pages: list[dict]):
        self._pages = pages
        self.calls: list[str | None] = []

    async def list_datasets(self, page_size=50, sort="Mru", page_token=None):
        self.calls.append(page_token)
        index = min(len(self.calls) - 1, len(self._pages) - 1)
        return self._pages[index]


class _FakeDashboardClient:
    def __init__(self, pages: list[dict]):
        self._pages = pages
        self.calls: list[str | None] = []

    async def list_dashboards(self, page_size=50, sort="Mru", page_token=None):
        self.calls.append(page_token)
        index = min(len(self.calls) - 1, len(self._pages) - 1)
        return self._pages[index]


@pytest.mark.asyncio
async def test_list_datasets_fetches_every_page():
    """Three pages in, three requests out, every dataset returned."""
    page1_url = "/services/data/v68.0/wave/datasets?page=TOKEN2"
    page2_url = "/services/data/v68.0/wave/datasets?page=TOKEN3"
    client = _FakeDatasetClient(
        [
            _page(page1_url, ["ds1", "ds2"]),
            _page(page2_url, ["ds3", "ds4"]),
            _page(None, ["ds5"]),
        ]
    )
    service = DatasetService.__new__(DatasetService)
    service.client = client

    datasets = await service.list_datasets()

    assert [d.id for d in datasets] == ["ds1", "ds2", "ds3", "ds4", "ds5"]
    assert len(client.calls) == 3
    assert client.calls[0] is None
    assert client.calls[1] == "TOKEN2"
    assert client.calls[2] == "TOKEN3"


@pytest.mark.asyncio
async def test_list_dashboards_fetches_every_page():
    page1_url = "/services/data/v68.0/wave/dashboards?page=TOKEN2"
    client = _FakeDashboardClient(
        [
            _dashboard_page(page1_url, ["db1", "db2"]),
            _dashboard_page(None, ["db3"]),
        ]
    )
    service = DashboardService.__new__(DashboardService)
    service.client = client

    dashboards = await service.list_dashboards()

    assert [d.id for d in dashboards] == ["db1", "db2", "db3"]
    assert len(client.calls) == 2
    assert client.calls[0] is None
    assert client.calls[1] == "TOKEN2"


@pytest.mark.asyncio
async def test_list_datasets_stops_when_cursor_repeats():
    """A platform that keeps returning the same cursor must not loop forever."""
    stuck_url = "/services/data/v68.0/wave/datasets?page=SAME"
    client = _FakeDatasetClient(
        [
            _page(stuck_url, ["ds1"]),
            _page(stuck_url, ["ds1"]),
            _page(stuck_url, ["ds1"]),
        ]
    )
    service = DatasetService.__new__(DatasetService)
    service.client = client

    datasets = await service.list_datasets()

    assert len(client.calls) == 2
    assert len(datasets) == 2


@pytest.mark.asyncio
async def test_list_datasets_single_page_makes_one_request():
    client = _FakeDatasetClient([_page(None, ["ds1", "ds2"])])
    service = DatasetService.__new__(DatasetService)
    service.client = client

    datasets = await service.list_datasets()

    assert len(datasets) == 2
    assert client.calls == [None]
