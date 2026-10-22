"""Which requests may be replayed after a 401.

A 401 means the platform rejected the request. The obvious response is to
refresh the token and send the request again -- but "send it again" is only
safe for some requests. Replaying ``POST /wave/dataflowjobs`` could start
a dataflow twice; replaying ``DELETE /wave/datasets/{id}`` could act on an
id the platform has since recycled.

The rule implemented here is deliberately narrow:

* ``GET`` and ``HEAD`` are always safe to repeat. They have no side
  effect, so a second attempt either returns the same data or a fresh one.
* ``PUT`` and ``DELETE`` are safe to repeat only when the path carries an
  explicit asset id. A request aimed at one specific object cannot
  accidentally target a different one.
* ``POST`` and ``PATCH`` are never replayed. Both are used for actions
  rather than for assignments: starting a dataflow, creating a dashboard,
  processing an external-data job.

See ``docs/decisions/0002-replay-policy.md`` for the reasoning behind the
choices.
"""

import re

#: Methods that can be repeated without any risk of a duplicate side effect.
SAFE_METHODS = frozenset({"GET", "HEAD"})

#: Methods that can be repeated only against a specific, identified asset.
IDEMPOTENT_WITH_ID_METHODS = frozenset({"PUT", "DELETE"})

#: Methods that are never replayed.
NEVER_REPLAYED_METHODS = frozenset({"POST", "PATCH"})

#: A Salesforce id is 15 or 18 characters of letters and digits. The
#: digit requirement is what separates an id from a collection name: a
#: 15-character segment with no digits in it is a resource name, not an id.
_SALESFORCE_ID = re.compile(r"^(?=.*[0-9])[A-Za-z0-9]{15}(?:[A-Za-z0-9]{3})?$")


def _last_path_segment(path: str) -> str:
    """Return the last non-empty segment of a URL path."""
    return path.rstrip("/").rsplit("/", 1)[-1]


def has_explicit_asset_id(path: str) -> bool:
    """Report whether a path ends in a Salesforce asset id.

    A path qualifies when its last segment is a 15- or 18-character
    alphanumeric id that contains at least one digit.

    Args:
        path: The request path, absolute or relative.

    Returns:
        True when the path names one specific asset.

    Examples:
        >>> has_explicit_asset_id("/wave/datasets/0Fb010000000000000")
        True
        >>> has_explicit_asset_id("/wave/datasets")
        False
    """
    segment = _last_path_segment(path)
    if not segment:
        return False
    return bool(_SALESFORCE_ID.match(segment))


def is_replayable(method: str, path: str) -> bool:
    """Report whether a request may be sent again after a 401.

    Args:
        method: The HTTP method, case-insensitive.
        path: The request path.

    Returns:
        True when repeating the request is safe.

    Examples:
        >>> is_replayable("GET", "/wave/datasets")
        True
        >>> is_replayable("DELETE", "/wave/datasets/0Fb010000000000000")
        True
        >>> is_replayable("POST", "/wave/dataflowjobs")
        False
    """
    normalized = method.upper()

    if normalized in NEVER_REPLAYED_METHODS:
        return False

    if normalized in SAFE_METHODS:
        return True

    if normalized in IDEMPOTENT_WITH_ID_METHODS:
        return has_explicit_asset_id(path)

    # An unknown method is not replayed: we cannot know what it does.
    return False
