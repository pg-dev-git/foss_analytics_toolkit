"""Shared helpers for the Salesforce Wave API's pagination.

The Wave API returns a ``nextPageUrl`` like::

    /services/data/v68.0/wave/datasets?page=eyJwYWdlU2l6ZSI6NSwi...

The cursor lives in the ``page`` query parameter, base64-encoded. Two details
matter and both were previously got wrong in three separate places:

* The parameter is ``page``, **not** ``pageToken``. The platform ignores an
  unknown query parameter and returns page 1 again -- silently. A client that
  sends ``pageToken`` therefore never advances.
* The cursor is URL-encoded base64, so it must be URL-decoded before being
  put back into a query string. A cursor containing ``+`` or ``%`` is
  mis-parsed by a naive ``str.split("page=")``.
"""

from urllib.parse import parse_qs, urlparse

#: The query parameter the Wave API uses for the pagination cursor.
PAGE_PARAM = "page"

#: Legacy parameter name some documentation refers to. The platform does
#: not accept it; sending it makes the API return page 1 again.
LEGACY_PAGE_PARAM = "pageToken"


def extract_page_token(url: str | None) -> str | None:
    """Extract the pagination cursor from a ``nextPageUrl``.

    Args:
        url: The ``nextPageUrl`` returned by the platform, or None.

    Returns:
        The decoded cursor, or None when the URL carries no cursor.

    The cursor is URL-decoded, so a value containing ``+`` or ``%`` survives
    the round trip. Both the current ``page`` parameter and the legacy
    ``pageToken`` spelling are accepted on read, because a stored or
    hand-written URL may use either.
    """
    if not url:
        return None

    params = parse_qs(urlparse(url).query)
    for key in (PAGE_PARAM, LEGACY_PAGE_PARAM):
        values = params.get(key)
        if values:
            return values[0]
    return None


def build_page_params(
    page_token: str | None,
    page_size: int = 50,
    sort: str = "Mru",
) -> dict[str, str | int]:
    """Build the query parameters for a paginated Wave listing.

    The cursor is sent as ``page`` -- the parameter the platform actually
    honours.
    """
    params: dict[str, str | int] = {"pageSize": page_size, "sort": sort}
    if page_token:
        params[PAGE_PARAM] = page_token
    return params
