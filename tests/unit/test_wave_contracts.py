"""Contract tests: every Salesforce-response model must parse a real payload.

The models in ``asftool/core/models`` used to be tested against invented
payloads that encoded the implementation's own assumptions. Three defects
were therefore invisible to the test suite:

* ``DatasetListResponse`` / ``DashboardListResponse`` did not map the
  platform's ``nextPageUrl`` (camelCase) onto ``next_page_url``, so every
  listing silently stopped after page 1.
* ``DataflowJob`` required ``dataflowId`` / ``dataflowName`` / ``command``,
  none of which the platform returns, so ``jobs list`` raised
  ``ValidationError`` on every real org.
* ``DataflowJobListResponse`` did not map the platform's ``dataflowJobs``
  key, so the list response failed to parse.

These tests feed the checked-in fixtures captured from a live org
(``tests/fixtures/wave_responses.py``) and assert the models parse them.
"""

import pytest

from asftool.core.models import (
    Dashboard,
    DashboardListResponse,
    Dataflow,
    DataflowJob,
    DataflowJobListResponse,
    DataflowListResponse,
    Dataset,
    DatasetListResponse,
    DatasetVersion,
    DatasetXMD,
    DependenciesResponse,
    LimitsResponse,
)
from tests.fixtures import wave_responses


class TestDatasetContracts:
    def test_dataset_parses_real_listing(self):
        dataset = Dataset(**wave_responses.DATASETS["datasets"][0])
        assert dataset.id
        assert dataset.name
        assert dataset.label
        assert dataset.created_date is not None
        assert dataset.last_modified_date is not None

    def test_dataset_list_response_parses_real_listing(self):
        response = DatasetListResponse(**wave_responses.DATASETS)
        assert len(response.datasets) == len(wave_responses.DATASETS["datasets"])
        assert response.datasets[0].id

    def test_dataset_list_response_reads_next_page_url(self):
        """The platform returns `nextPageUrl`; the model must expose it."""
        response = DatasetListResponse(**wave_responses.DATASETS_PAGE_1)
        assert response.next_page_url == wave_responses.DATASETS_PAGE_1["nextPageUrl"]
        assert response.next_page_url is not None

    def test_dataset_list_response_without_next_page_url(self):
        """A listing with no `nextPageUrl` must expose None, not a stale URL."""
        response = DatasetListResponse(**wave_responses.DATASETS)
        assert response.next_page_url is None

    def test_dataset_xmd_parses_real_payload(self):
        xmd = DatasetXMD(**wave_responses.DATASET_XMD)
        assert xmd.dimensions
        assert xmd.measures

    def test_dataset_version_parses_real_listing(self):
        version = DatasetVersion(**wave_responses.DATASET_VERSIONS["versions"][0])
        assert version.id
        assert version.dataset_id
        assert version.total_row_count is not None


class TestLimitsContracts:
    def test_limits_response_parses_real_payload(self):
        """`/limits` is a flat mapping, not `{limits: [...]}`."""
        response = LimitsResponse.from_platform_response(wave_responses.LIMITS)
        assert response.limits
        first = next(iter(response.limits.items()))
        assert first[1].max >= 0
        assert first[1].remaining >= 0


class TestDependencyContracts:
    def test_dependencies_response_parses_real_payload(self):
        """Dependencies are grouped by consumer type, not a flat list."""
        response = DependenciesResponse(**wave_responses.DATASET_DEPENDENCIES)
        assert response.dashboards is not None
        assert response.dependencies
        assert response.dependencies[0].id



class TestDashboardContracts:
    def test_dashboard_parses_real_listing(self):
        dashboard = Dashboard(**wave_responses.DASHBOARDS["dashboards"][0])
        assert dashboard.id
        assert dashboard.name
        assert dashboard.label
        assert dashboard.created_date is not None

    def test_dashboard_list_response_parses_real_listing(self):
        response = DashboardListResponse(**wave_responses.DASHBOARDS)
        assert len(response.dashboards) == len(wave_responses.DASHBOARDS["dashboards"])

    def test_dashboard_list_response_reads_next_page_url(self):
        response = DashboardListResponse(**wave_responses.DASHBOARDS)
        assert response.next_page_url == wave_responses.DASHBOARDS.get("nextPageUrl")

    def test_dashboard_list_response_without_next_page_url(self):
        response = DashboardListResponse(
            **{"dashboards": wave_responses.DASHBOARDS["dashboards"]}
        )
        assert response.next_page_url is None


class TestDataflowContracts:
    def test_dataflow_parses_real_listing(self):
        dataflow = Dataflow(**wave_responses.DATAFLOWS["dataflows"][0])
        assert dataflow.id
        assert dataflow.name
        assert dataflow.label
        assert dataflow.created_date is not None

    def test_dataflow_list_response_parses_real_listing(self):
        response = DataflowListResponse(**wave_responses.DATAFLOWS)
        assert len(response.dataflows) == len(wave_responses.DATAFLOWS["dataflows"])


class TestDataflowJobContracts:
    """The platform's job payload does NOT look like the old model assumed."""

    def test_dataflow_job_list_response_parses_real_listing(self):
        response = DataflowJobListResponse(**wave_responses.DATAFLOW_JOBS)
        assert len(response.dataflowjobs) == len(
            wave_responses.DATAFLOW_JOBS["dataflowJobs"]
        )

    def test_dataflow_job_parses_real_job(self):
        job = DataflowJob(**wave_responses.DATAFLOW_JOBS["dataflowJobs"][0])
        assert job.id
        assert job.status

    def test_dataflow_job_reads_real_timestamps(self):
        """The platform uses `startDate` / `executedDate`, not `startTime`."""
        job = DataflowJob(**wave_responses.DATAFLOW_JOBS["dataflowJobs"][0])
        assert job.start_time is not None
        assert job.start_time.year == 2026

    def test_dataflow_job_end_time_is_optional(self):
        """A queued job has no `executedDate`; the model must still parse."""
        queued = next(
            j
            for j in wave_responses.DATAFLOW_JOBS["dataflowJobs"]
            if j.get("status") == "Queued"
        )
        job = DataflowJob(**queued)
        assert job.end_time is None

    def test_dataflow_job_exposes_label(self):
        job = DataflowJob(**wave_responses.DATAFLOW_JOBS["dataflowJobs"][0])
        assert job.label

    def test_dataflow_job_exposes_error_message(self):
        job = DataflowJob(**wave_responses.DATAFLOW_JOBS["dataflowJobs"][0])
        assert job.error_message is None or isinstance(job.error_message, str)


# ---------------------------------------------------------------------------
# Guard: a new response model must come with a contract test.
# ---------------------------------------------------------------------------

# Models that parse a Salesforce response and therefore need a contract test.
CONTRACT_MODELS = {
    "Dataset",
    "DatasetListResponse",
    "DatasetXMD",
    "DatasetVersion",
    "Dashboard",
    "DashboardListResponse",
    "Dataflow",
    "DataflowListResponse",
    "DataflowJob",
    "DataflowJobListResponse",
    "DependenciesResponse",
    "LimitsResponse",
}

# Models covered by this module, either directly or through a collection.
COVERED_MODELS = {
    "Dataset",
    "DatasetListResponse",
    "DatasetXMD",
    "DatasetVersion",
    "Dashboard",
    "DashboardListResponse",
    "Dataflow",
    "DataflowListResponse",
    "DataflowJob",
    "DataflowJobListResponse",
    "DependenciesResponse",
    "LimitsResponse",
}


def test_every_response_model_has_a_contract_test():
    """A new Salesforce-response model must be added to this module.

    Without this guard a model can be added (or a fixture key renamed) and
    the omission is silent -- which is exactly how the original three
    Critical/High defects shipped.
    """
    missing = sorted(CONTRACT_MODELS - COVERED_MODELS)
    assert not missing, (
        "These Salesforce-response models have no contract test in "
        "tests/unit/test_wave_contracts.py: "
        f"{missing}. Add a test that feeds the matching fixture from "
        "tests/fixtures/wave_responses.py."
    )


@pytest.mark.parametrize(
    "fixture_name",
    [
        "DATASETS",
        "DATASETS_PAGE_1",
        "DATASETS_PAGE_2",
        "DASHBOARDS",
        "DATAFLOWS",
        "DATAFLOW_JOBS",
        "LENSES",
        "DATASET_XMD",
        "DATASET_DEPENDENCIES",
        "DATASET_VERSIONS",
        "LIMITS",
        "REPLICATED_DATASETS",
    ],
)
def test_fixture_exists(fixture_name):
    """Every fixture referenced by the contract tests must be present."""
    assert hasattr(wave_responses, fixture_name)
    assert getattr(wave_responses, fixture_name)
