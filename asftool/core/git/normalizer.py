"""CRMA JSON Normalizer for clean Git diffs.

Strips volatile runtime attributes and enforces deterministic
UTF-8 JSON output to ensure git diff only shows true structural
or logic changes.
"""

import json
from copy import deepcopy
from pathlib import Path
from typing import Any, Optional

from pydantic import BaseModel, Field


class NormalizerConfig(BaseModel):
    """Configuration for CRMANormalizer."""

    model_config = {"extra": "forbid"}

    # Fields to always strip (volatile runtime attributes)
    strip_fields: list[str] = Field(
        default_factory=lambda: [
            # Timestamps
            "lastModifiedDate",
            "createdDate",
            "refreshDate",
            "nextScheduledDate",
            "lastQueriedDate",
            "lastViewedDate",
            "lastReferencedDate",
            "systemModstamp",
            # User references
            "createdBy",
            "createdById",
            "lastModifiedBy",
            "lastModifiedById",
            "ownerId",
            # Org-specific IDs (18-char Salesforce IDs)
            "id",  # Will be handled specially - keep for some asset types
            # Version/ETag
            "version",
            "etag",
            # Runtime state
            "state",
            "status",
            "executionStatus",
            "refreshStatus",
            # UI state
            "uiState",
            "viewState",
            # Metrics that change on every refresh
            "rowCount",
            "cellCount",
            "columnCount",
        ],
        description="Field names to strip from JSON (case-insensitive)",
    )

    # Fields to strip only from specific asset types
    strip_fields_by_type: dict[str, list[str]] = Field(
        default_factory=lambda: {
            "dashboard": [
                "applicationId",
                "folderId",
                "folderName",
                "runningUserId",
            ],
            "recipe": [
                "dataflowId",
                "dataflowName",
            ],
            "dataflow": [
                "applicationId",
            ],
            "lens": [
                "datasetId",
                "datasetName",
                "applicationId",
            ],
            "dataset": [
                "applicationId",
                "dataflowId",
            ],
            "xmd": [
                "datasetId",
            ],
        },
        description="Additional fields to strip per asset type",
    )

    # Fields to NEVER strip (preserve identity)
    preserve_fields: list[str] = Field(
        default_factory=lambda: [
            "developerName",
            "label",
            "name",
            "apiName",
            "namespace",
        ],
        description="Fields to always preserve",
    )

    # JSON output settings
    indent: int = Field(default=2, description="JSON indentation")
    sort_keys: bool = Field(default=True, description="Sort keys for deterministic output")
    ensure_ascii: bool = Field(default=False, description="Allow UTF-8 in output")

    # Special handling
    normalize_timestamps: bool = Field(
        default=True,
        description="Convert timestamps to ISO format placeholder",
    )
    timestamp_placeholder: str = Field(
        default="<TIMESTAMP>",
        description="Placeholder for stripped timestamps",
    )
    id_placeholder: str = Field(
        default="<ID>",
        description="Placeholder for stripped Salesforce IDs",
    )


class NormalizationResult(BaseModel):
    """Result of normalization operation."""

    model_config = {"extra": "forbid"}

    normalized_json: str
    stripped_fields: list[str] = Field(default_factory=list)
    preserved_fields: list[str] = Field(default_factory=list)
    asset_type: str


class CRMANormalizer:
    """Normalizes CRMA asset JSON for clean Git diffs.

    Strips volatile runtime attributes, sorts keys deterministically,
    and enforces UTF-8 encoding to ensure git diff only highlights
    true structural or logic changes.
    """

    # Salesforce 18-char ID pattern
    SF_ID_PATTERN = r"^[a-zA-Z0-9]{15,18}$"

    def __init__(self, config: Optional[NormalizerConfig] = None):
        """Initialize with optional config."""
        self.config = config or NormalizerConfig()
        self._strip_fields_set = {f.lower() for f in self.config.strip_fields}
        self._preserve_fields_set = {f.lower() for f in self.config.preserve_fields}

    def normalize(
        self,
        asset_json: dict[str, Any],
        asset_type: str,
    ) -> NormalizationResult:
        """Normalize CRMA asset JSON.

        Args:
            asset_json: Raw asset JSON from CRMA REST API
            asset_type: Asset type (dashboard, recipe, dataflow, lens, dataset, xmd)

        Returns:
            NormalizationResult with normalized JSON and metadata
        """
        # Deep copy to avoid mutating original
        data = deepcopy(asset_json)

        stripped = []
        preserved = []

        # Get type-specific strip fields
        type_strip_fields = {
            f.lower() for f in self.config.strip_fields_by_type.get(asset_type, [])
        }
        all_strip_fields = self._strip_fields_set | type_strip_fields

        # Recursively normalize
        data = self._normalize_object(
            data,
            all_strip_fields,
            stripped,
            preserved,
            path=[],
        )

        # Generate deterministic JSON output
        normalized_json = json.dumps(
            data,
            indent=self.config.indent,
            sort_keys=self.config.sort_keys,
            ensure_ascii=self.config.ensure_ascii,
        )

        # Ensure UTF-8 encoding (explicit)
        if not isinstance(normalized_json, str):
            normalized_json = normalized_json.decode("utf-8")

        return NormalizationResult(
            normalized_json=normalized_json,
            stripped_fields=sorted(set(stripped)),
            preserved_fields=sorted(set(preserved)),
            asset_type=asset_type,
        )

    def normalize_bytes(
        self,
        asset_bytes: bytes,
        asset_type: str,
    ) -> NormalizationResult:
        """Normalize from bytes (explicit UTF-8 handling)."""
        # Explicit UTF-8 decode
        try:
            asset_str = asset_bytes.decode("utf-8")
        except UnicodeDecodeError as e:
            # Try with replacement chars as fallback
            asset_str = asset_bytes.decode("utf-8", errors="replace")

        asset_json = json.loads(asset_str)
        return self.normalize(asset_json, asset_type)

    def _normalize_object(
        self,
        obj: Any,
        strip_fields: set[str],
        stripped: list[str],
        preserved: list[str],
        path: list[str],
    ) -> Any:
        """Recursively normalize JSON object."""
        if isinstance(obj, dict):
            result = {}
            for key, value in obj.items():
                key_lower = key.lower()
                current_path = path + [key]

                # Check if we should strip this field
                if key_lower in strip_fields and key_lower not in self._preserve_fields_set:
                    stripped.append(".".join(current_path))
                    # Completely omit the field (don't add to result)
                    continue

                # Preserve field
                if key_lower in self._preserve_fields_set:
                    preserved.append(".".join(current_path))

                # Recurse
                result[key] = self._normalize_object(
                    value, strip_fields, stripped, preserved, current_path
                )

            # Remove empty objects that resulted from stripping
            return {k: v for k, v in result.items() if v is not None and v != {} and v != []}

        elif isinstance(obj, list):
            # Normalize each item in list
            normalized_list = []
            for i, item in enumerate(obj):
                normalized_item = self._normalize_object(
                    item, strip_fields, stripped, preserved, path + [str(i)]
                )
                if normalized_item is not None and normalized_item != {} and normalized_item != []:
                    normalized_list.append(normalized_item)
            return normalized_list

        else:
            # Primitive value - return as-is
            return obj

    @staticmethod
    def _looks_like_sf_id(value: str) -> bool:
        """Check if string looks like a Salesforce 15/18-char ID."""
        import re
        return bool(re.match(r"^[a-zA-Z0-9]{15,18}$", value))

    @staticmethod
    def _looks_like_timestamp(value: Any) -> bool:
        """Check if value looks like a timestamp."""
        if isinstance(value, (int, float)):
            # Unix timestamp (seconds or milliseconds)
            # Reasonable range: year 2000 to 2100
            if 946684800 <= value <= 4102444800:  # seconds
                return True
            if 946684800000 <= value <= 4102444800000:  # milliseconds
                return True
        elif isinstance(value, str):
            # ISO format or similar
            import re
            iso_pattern = r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:?\d{2})?$"
            if re.match(iso_pattern, value):
                return True
            # Date only
            date_pattern = r"^\d{4}-\d{2}-\d{2}$"
            if re.match(date_pattern, value):
                return True
        return False

    def normalize_file(
        self,
        input_path: Path,
        output_path: Optional[Path] = None,
        asset_type: Optional[str] = None,
    ) -> NormalizationResult:
        """Normalize JSON file in place or to output path."""
        # Infer asset type from filename if not provided
        if asset_type is None:
            asset_type = self._infer_asset_type(input_path)

        with open(input_path, "r", encoding="utf-8") as f:
            asset_json = json.load(f)

        result = self.normalize(asset_json, asset_type)

        # Write output
        write_path = output_path or input_path
        write_path.parent.mkdir(parents=True, exist_ok=True)
        with open(write_path, "w", encoding="utf-8") as f:
            f.write(result.normalized_json)

        return result

    @staticmethod
    def _infer_asset_type(path: Path) -> str:
        """Infer asset type from file path."""
        name = path.name.lower()
        parent = path.parent.name.lower()

        # Check parent directory name
        if "dashboard" in parent:
            return "dashboard"
        if "recipe" in parent:
            return "recipe"
        if "dataflow" in parent:
            return "dataflow"
        if "lens" in parent:
            return "lens"
        if "dataset" in parent:
            return "dataset"
        if "xmd" in parent:
            return "xmd"

        # Check filename
        if "dashboard" in name:
            return "dashboard"
        if "recipe" in name:
            return "recipe"
        if "dataflow" in name:
            return "dataflow"
        if "lens" in name:
            return "lens"
        if "dataset" in name:
            return "dataset"
        if "xmd" in name:
            return "xmd"

        # Default
        return "unknown"

    def create_normalized_bundle(
        self,
        asset_json: dict[str, Any],
        asset_type: str,
    ) -> dict[str, Any]:
        """Create a normalized bundle ready for PUT /wave/<assetType>/<id>/bundle.

        This normalizes and wraps in the expected bundle format.
        """
        result = self.normalize(asset_json, asset_type)

        # Parse back to dict for bundle creation
        normalized_dict = json.loads(result.normalized_json)

        # Bundle format expected by CRMA REST API
        bundle = {
            "asset": normalized_dict,
            "metadata": {
                "normalized": True,
                "assetType": asset_type,
                "strippedFields": result.stripped_fields,
            },
        }

        return bundle


def create_normalizer(
    strip_fields: Optional[list[str]] = None,
    preserve_fields: Optional[list[str]] = None,
    indent: int = 2,
    sort_keys: bool = True,
) -> CRMANormalizer:
    """Factory function to create normalizer with custom config."""
    config = NormalizerConfig(
        strip_fields=strip_fields or NormalizerConfig().strip_fields,
        preserve_fields=preserve_fields or NormalizerConfig().preserve_fields,
        indent=indent,
        sort_keys=sort_keys,
    )
    return CRMANormalizer(config)