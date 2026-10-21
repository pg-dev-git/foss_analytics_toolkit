"""Tests for the shared Wave API pagination helpers.

The cursor is base64-encoded and URL-encoded, so a naive ``str.split`` on the
query string mis-parses any value containing ``+`` or ``%``. The three
separate implementations this replaced had already drifted: two decoded the
value and one did not.
"""

from asftool.core.pagination import (
    LEGACY_PAGE_PARAM,
    PAGE_PARAM,
    build_page_params,
    extract_page_token,
)


class TestExtractPageToken:
    def test_empty_url(self):
        assert extract_page_token("") is None
        assert extract_page_token(None) is None

    def test_url_without_cursor(self):
        assert extract_page_token("/wave/datasets?other=x") is None

    def test_extracts_page_parameter(self):
        url = "/services/data/v68.0/wave/datasets?page=abc123"
        assert extract_page_token(url) == "abc123"

    def test_extracts_page_parameter_with_more_params(self):
        url = "/wave/datasets?page=abc123&sort=Mru"
        assert extract_page_token(url) == "abc123"

    def test_decodes_plus_sign(self):
        """A base64 cursor can contain '+', which means a space once decoded."""
        url = "/wave/datasets?page=ab%2Bc%3D%3D"
        assert extract_page_token(url) == "ab+c=="

    def test_decodes_percent_sequences(self):
        url = "/wave/datasets?page=abc%252Fdef"
        assert extract_page_token(url) == "abc%2Fdef"

    def test_decodes_real_cursor(self):
        """A cursor captured from a live org round-trips exactly."""
        url = (
            "/services/data/v68.0/wave/datasets"
            "?page=eyJwYWdlU2l6ZSI6NSwibGFzdElkIjoiMEZifQ%3D%3D"
        )
        assert extract_page_token(url) == "eyJwYWdlU2l6ZSI6NSwibGFzdElkIjoiMEZifQ=="

    def test_accepts_legacy_page_token_parameter(self):
        """A stored or hand-written URL may use the legacy spelling."""
        url = "/wave/datasets?pageToken=abc"
        assert extract_page_token(url) == "abc"


class TestBuildPageParams:
    def test_without_cursor(self):
        params = build_page_params(None, page_size=50, sort="Mru")
        assert params == {"pageSize": 50, "sort": "Mru"}

    def test_uses_page_parameter_not_page_token(self):
        """The platform ignores `pageToken` and returns page 1 again."""
        params = build_page_params("abc", page_size=25, sort="Mru")
        assert params[PAGE_PARAM] == "abc"
        assert LEGACY_PAGE_PARAM not in params
        assert "pageToken" not in params

    def test_keeps_page_size_and_sort(self):
        params = build_page_params("abc", page_size=10, sort="Asc")
        assert params["pageSize"] == 10
        assert params["sort"] == "Asc"
