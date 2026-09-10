"""Field matching: exact and fuzzy comparison for Analytics REST API field names/labels."""

import difflib
from typing import Any

from thefuzz import fuzz

from asftool.core.models import MatchMode, MatchType


class FieldMatcher:
    """Compares a search term against field API names and labels.

    Supports both exact matching (case-insensitive default) and fuzzy
    matching using thefuzz/RapidFuzz scorers.
    """

    def __init__(self, threshold: int = 85):
        """Initialize the matcher.

        Args:
            threshold: Fuzzy match threshold (0-100). Scores at or above
                the threshold count as a fuzzy match.
        """
        self.threshold = max(0, min(100, threshold))

    def exact_match(
        self, search_term: str, target: str, case_sensitive: bool = False
    ) -> bool:
        """Check if search_term exactly matches target.

        Args:
            search_term: The term to search for.
            target: The candidate string to compare against.
            case_sensitive: If True, comparison is case-sensitive.
        """
        if not search_term or not target:
            return False
        if case_sensitive:
            return search_term == target
        return search_term.lower() == target.lower()

    def fuzzy_match(
        self,
        search_term: str,
        target: str,
        threshold: int | None = None,
        scorer: str = "token_set_ratio",
    ) -> tuple[bool, int]:
        """Check if search_term fuzzy-matches target.

        Args:
            search_term: The term to search for.
            target: The candidate string to compare against.
            threshold: Override the instance threshold for this call.
            scorer: Which thefuzz scorer to use. One of:
                - "ratio" (default SequenceMatcher ratio)
                - "partial_ratio" (best partial match)
                - "token_sort_ratio" (sorted tokens, then ratio)
                - "token_set_ratio" (intersection + remainder)

        Returns:
            (matched, score) tuple. matched=True if score >= threshold.
        """
        if not search_term or not target:
            return False, 0

        effective_threshold = self.threshold if threshold is None else threshold
        effective_threshold = max(0, min(100, effective_threshold))

        score = self._score(search_term, target, scorer)
        return score >= effective_threshold, score

    def _score(self, search_term: str, target: str, scorer: str) -> int:
        """Compute a fuzzy score in [0, 100]."""
        s1 = search_term.strip()
        s2 = target.strip()
        if not s1 or not s2:
            return 0

        try:
            if scorer == "ratio":
                return int(fuzz.ratio(s1, s2))
            if scorer == "partial_ratio":
                return int(fuzz.partial_ratio(s1, s2))
            if scorer == "token_sort_ratio":
                return int(fuzz.token_sort_ratio(s1, s2))
            if scorer == "token_set_ratio":
                return int(fuzz.token_set_ratio(s1, s2))
        except Exception:
            return int(difflib.SequenceMatcher(None, s1.lower(), s2.lower()).ratio() * 100)

        return int(fuzz.token_set_ratio(s1, s2))

    def best_score(self, search_term: str, target: str) -> int:
        """Return the best score across all scorers, used when picking the
        strongest match among api_name and label candidates.
        """
        if not search_term or not target:
            return 0
        scores = [
            self._score(search_term, target, "ratio"),
            self._score(search_term, target, "partial_ratio"),
            self._score(search_term, target, "token_sort_ratio"),
            self._score(search_term, target, "token_set_ratio"),
        ]
        return max(scores)

    def match_field(
        self,
        search_term: str,
        field_api_name: str,
        field_label: str | None = None,
        mode: MatchMode = MatchMode.BOTH,
        case_sensitive: bool = False,
    ) -> tuple[MatchType | None, int, str | None]:
        """Match a search term against a field's api name and optional label.

        Args:
            search_term: The term being searched.
            field_api_name: The field's API name (required).
            field_label: The field's user-facing label (optional).
            mode: Which match modes to consider.
            case_sensitive: For exact matching only.

        Returns:
            (match_type, score, matched_against) or (None, 0, None) if no match.
            matched_against is "api_name" or "label" indicating which side matched.
        """
        if not search_term:
            return None, 0, None

        # Try exact first
        if mode in (MatchMode.EXACT, MatchMode.BOTH):
            if self.exact_match(search_term, field_api_name, case_sensitive):
                return MatchType.EXACT, 100, "api_name"
            if field_label and self.exact_match(search_term, field_label, case_sensitive):
                return MatchType.EXACT, 100, "label"

        # Then fuzzy
        if mode in (MatchMode.FUZZY, MatchMode.BOTH):
            api_score = self.best_score(search_term, field_api_name)
            label_score = 0
            if field_label:
                label_score = self.best_score(search_term, field_label)

            best_score = max(api_score, label_score)
            best_source = "label" if label_score > api_score else "api_name"
            if best_score >= self.threshold:
                return MatchType.FUZZY, best_score, best_source

        return None, 0, None


def extract_field_strings(obj: Any, path: str = "") -> list[tuple[str, str]]:
    """Walk a nested dict/list and yield (field_path, value) for any string.

    Used by the crawler to find field-name-like strings buried in dashboard
    JSON / dataflow recipe JSON without depending on a known schema.
    """
    results: list[tuple[str, str]] = []
    if isinstance(obj, dict):
        for key, val in obj.items():
            new_path = f"{path}.{key}" if path else key
            if isinstance(val, str):
                if _looks_like_field_name(key) and val:
                    results.append((new_path, val))
            else:
                results.extend(extract_field_strings(val, new_path))
    elif isinstance(obj, list):
        for idx, val in enumerate(obj):
            new_path = f"{path}[{idx}]"
            results.extend(extract_field_strings(val, new_path))
    return results


def _looks_like_field_name(key: str) -> bool:
    """Heuristic: keys that typically hold field names in Analytics REST API JSON."""
    k = key.lower()
    return k in {
        "field", "fieldname", "name", "column",
        "sourcefield", "targetfield", "fieldid",
    } or k.endswith("field") or k.endswith("name")
