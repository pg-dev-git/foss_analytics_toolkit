"""Unit tests for the 401 replay policy.

The policy decides which requests may be sent a second time after the
platform rejects them with a 401. Getting it wrong in the permissive
direction starts a dataflow twice; getting it wrong in the strict
direction turns a recoverable session expiry into a hard failure.
"""

import pytest

from asftool.core.replay import has_explicit_asset_id, is_replayable

# A real Salesforce id, as captured from a live org and sanitized.
DATASET_ID = "0Fb010000000000000"
DASHBOARD_ID = "0Fb020000000000000"
EXTERNAL_DATA_ID = "0Fb030000000000AAA"


class TestAssetIdDetection:
    """A path qualifies only when its last segment is a Salesforce id."""

    @pytest.mark.parametrize(
        "path",
        [
            f"/wave/datasets/{DATASET_ID}",
            f"/wave/dashboards/{DASHBOARD_ID}",
            f"/sobjects/InsightsExternalData/{EXTERNAL_DATA_ID}",
            f"https://org.my.salesforce.com/services/data/v60.0/wave/datasets/{DATASET_ID}",
        ],
    )
    def test_a_path_with_an_id_qualifies(self, path):
        assert has_explicit_asset_id(path) is True

    @pytest.mark.parametrize(
        "path",
        [
            "/wave/datasets",
            "/wave/dashboards",
            "/wave/dataflowjobs",
            "/wave/dataflows",
            "/wave/replicatedDatasets",
            "/sobjects/InsightsExternalData",
            "/sobjects/InsightsExternalDataPart",
            "",
            "/",
        ],
    )
    def test_a_path_without_an_id_does_not_qualify(self, path):
        assert has_explicit_asset_id(path) is False

    def test_a_trailing_slash_does_not_hide_the_id(self):
        assert has_explicit_asset_id(f"/wave/datasets/{DATASET_ID}/") is True

    def test_a_15_character_name_without_digits_is_not_an_id(self):
        """The digit requirement separates an id from a collection name."""
        assert has_explicit_asset_id("/wave/abcdefghijklmno") is False

    def test_a_short_segment_is_not_an_id(self):
        assert has_explicit_asset_id("/wave/datasets/123") is False

    def test_a_16_character_segment_is_not_an_id(self):
        """Salesforce ids are 15 or 18 characters, never 16."""
        assert has_explicit_asset_id("/wave/0Fb0100000000000") is False

    def test_a_19_character_segment_is_not_an_id(self):
        assert has_explicit_asset_id("/wave/0Fb0100000000000000") is False

    def test_a_segment_with_punctuation_is_not_an_id(self):
        assert has_explicit_asset_id("/wave/datasets/0Fb0100000000-000") is False

    def test_the_id_must_be_the_last_segment(self):
        """An id in the middle of a path does not make the path an asset path."""
        assert has_explicit_asset_id(f"/wave/datasets/{DATASET_ID}/versions") is False


class TestReplayPolicy:
    """The set of requests that may be repeated."""

    @pytest.mark.parametrize("method", ["GET", "HEAD"])
    def test_reads_are_always_replayable(self, method):
        assert is_replayable(method, "/wave/datasets") is True

    def test_a_get_on_a_collection_is_replayable(self):
        assert is_replayable("GET", "/wave/datasets") is True

    def test_a_get_on_an_asset_is_replayable(self):
        assert is_replayable("GET", f"/wave/datasets/{DATASET_ID}") is True

    @pytest.mark.parametrize("method", ["POST", "PATCH"])
    def test_actions_are_never_replayed(self, method):
        """A POST could start a dataflow twice; a PATCH could process a job twice."""
        assert is_replayable(method, "/wave/dataflowjobs") is False

    def test_a_post_starting_a_dataflow_is_not_replayed(self):
        assert is_replayable("POST", "/wave/dataflowjobs") is False

    def test_a_post_creating_a_dashboard_is_not_replayed(self):
        assert is_replayable("POST", "/wave/dashboards") is False

    def test_a_post_uploading_a_data_part_is_not_replayed(self):
        assert is_replayable("POST", "/sobjects/InsightsExternalDataPart") is False

    def test_a_patch_processing_a_job_is_not_replayed(self):
        """The path carries an id, but the action is a side effect."""
        assert is_replayable("PATCH", f"/sobjects/InsightsExternalData/{EXTERNAL_DATA_ID}") is False

    def test_a_delete_with_an_id_is_replayable(self):
        assert is_replayable("DELETE", f"/wave/datasets/{DATASET_ID}") is True

    def test_a_delete_without_an_id_is_not_replayable(self):
        assert is_replayable("DELETE", "/wave/datasets") is False

    def test_a_put_with_an_id_is_replayable(self):
        assert is_replayable("PUT", f"/wave/dashboards/{DASHBOARD_ID}") is True

    def test_a_put_without_an_id_is_not_replayable(self):
        assert is_replayable("PUT", "/wave/dashboards") is False

    def test_the_method_is_case_insensitive(self):
        assert is_replayable("get", "/wave/datasets") is True
        assert is_replayable("post", "/wave/dataflowjobs") is False

    def test_an_unknown_method_is_not_replayed(self):
        """We cannot know what an unknown method does, so we do not repeat it."""
        assert is_replayable("TRACE", "/wave/datasets") is False
