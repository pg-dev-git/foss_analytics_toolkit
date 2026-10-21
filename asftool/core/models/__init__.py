"""Pydantic models for ASFTool domain objects."""

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from asftool.core.models.field_impact import (
    AssetDependency,
    AssetDependencyGraph,
    AssetType,
    DashboardFieldAnalysisResult,
    DashboardFieldReference,
    DataflowFieldAnalysisResult,
    DataflowFieldReference,
    DatasetFieldAnalysisResult,
    DatasetFieldReference,
    FieldImpactDetail,
    FieldImpactReport,
    FieldImpactScope,
    FieldImpactSummary,
    MatchMode,
    MatchType,
    ReplicatedDatasetField,
    ReplicatedDatasetFieldAnalysisResult,
)


# Progress Models (used by UI)
@dataclass
class ExtractionProgress:
    """Progress information for dataset extraction."""
    total_rows: int
    processed_rows: int
    current_chunk: int
    total_chunks: int
    status: str


@dataclass
class UploadProgress:
    """Progress information for CSV upload."""
    total_rows: int
    uploaded_rows: int
    current_part: int
    total_parts: int
    status: str


# Auth Models
class OAuthToken(BaseModel):
    """OAuth token response from Salesforce."""
    access_token: str
    refresh_token: str | None = None
    instance_url: str
    id: str
    token_type: str = "Bearer"
    issued_at: str | None = None
    signature: str | None = None
    scope: str | None = None

    model_config = ConfigDict(extra="allow")


class ConnectedAppConfig(BaseModel):
    """Connected App configuration for JWT Bearer flow."""
    client_id: str
    client_secret: str
    username: str
    domain: str = "login"

    model_config = ConfigDict(extra="allow")


class WebOAuthConfig(BaseModel):
    """Web OAuth configuration for PKCE flow."""
    client_id: str
    client_secret: str
    redirect_uri: str
    domain: str = "login"
    scopes: list[str] = Field(default_factory=lambda: ["api", "refresh_token", "web"])

    model_config = ConfigDict(extra="allow")


class DeviceFlowConfig(BaseModel):
    """Device Authorization Flow configuration."""
    client_id: str
    domain: str = "login"

    model_config = ConfigDict(extra="allow")


class DeviceAuthorizationResponse(BaseModel):
    """Response from device authorization endpoint."""
    device_code: str
    user_code: str
    verification_uri: str
    verification_uri_complete: str | None = None
    expires_in: int
    interval: int

    model_config = ConfigDict(extra="allow")


# Dataset Models
class DatasetField(BaseModel):
    """Dataset field definition."""
    field: str
    label: str
    type: str
    is_system: bool = False
    is_unique: bool = False
    is_nillable: bool = True
    precision: int | None = None
    scale: int | None = None
    default_value: Any = None

    model_config = ConfigDict(extra="allow")


class DatasetXMD(BaseModel):
    """Dataset Extended Metadata (XMD)."""
    measures: list[dict[str, Any]] = Field(default_factory=list)
    dimensions: list[dict[str, Any]] = Field(default_factory=list)
    dates: list[dict[str, Any]] = Field(default_factory=list)

    model_config = ConfigDict(extra="allow")


class DatasetVersion(BaseModel):
    """Dataset version information.

    The platform returns the owning dataset as a nested ``dataset`` object
    (not a flat ``datasetId``) and has no ``versionNumber``/``status``.
    """

    id: str
    created_date: datetime | None = Field(default=None, alias="createdDate")
    last_modified_date: datetime | None = Field(default=None, alias="lastModifiedDate")
    dataset: dict | None = None
    total_row_count: int | None = Field(default=None, alias="totalRowCount")
    type: str | None = None
    url: str | None = None
    xmds_url: str | None = Field(default=None, alias="xmdsUrl")
    xmd: DatasetXMD | None = None

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    @property
    def dataset_id(self) -> str | None:
        """Get the owning dataset id from the nested ``dataset`` object."""
        if self.dataset and isinstance(self.dataset, dict):
            return self.dataset.get("id")
        return None


class Dataset(BaseModel):
    """Dataset model."""
    id: str
    name: str
    label: str
    description: str | None = None
    current_version_id: str | None = Field(default=None, alias="currentVersionId")
    current_version_url: str | None = Field(default=None, alias="currentVersionUrl")
    versions_url: str | None = Field(default=None, alias="versionsUrl")
    histories_url: str | None = Field(default=None, alias="historiesUrl")
    created_date: datetime = Field(alias="createdDate")
    created_by: dict | None = Field(default=None, alias="createdBy")
    last_modified_date: datetime = Field(alias="lastModifiedDate")
    last_modified_by: dict | None = Field(default=None, alias="lastModifiedBy")
    row_count: int | None = Field(default=None, alias="rowCount")
    status: str = "Active"
    type: str = "Edgemart"
    data_refresh_date: datetime | None = Field(default=None, alias="dataRefreshDate")
    dataset_type: str | None = Field(default=None, alias="datasetType")
    folder: dict | None = Field(default=None, alias="folder")
    last_accessed_date: datetime | None = Field(default=None, alias="lastAccessedDate")
    last_queried_date: datetime | None = Field(default=None, alias="lastQueriedDate")
    permissions: dict | None = Field(default=None, alias="permissions")
    client_shards_url: str | None = Field(default=None, alias="clientShardsUrl")
    visibility: str | None = Field(default=None, alias="visibility")

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    @property
    def created_by_id(self) -> str | None:
        """Get createdBy ID from the createdBy object."""
        if self.created_by and isinstance(self.created_by, dict):
            return self.created_by.get("id")
        return None

    @property
    def last_modified_by_id(self) -> str | None:
        """Get lastModifiedBy ID from the lastModifiedBy object."""
        if self.last_modified_by and isinstance(self.last_modified_by, dict):
            return self.last_modified_by.get("id")
        return None


class DatasetListResponse(BaseModel):
    """Response for dataset listing."""
    datasets: list[Dataset]
    next_page_url: str | None = Field(default=None, alias="nextPageUrl")

    model_config = ConfigDict(extra="allow", populate_by_name=True)


class ExtractionJob(BaseModel):
    """Dataset extraction job status."""
    id: str
    dataset_id: str
    status: Literal["pending", "running", "completed", "failed"]
    total_rows: int = 0
    processed_rows: int = 0
    current_chunk: int = 0
    total_chunks: int = 0
    result_path: str | None = None
    error: str | None = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    completed_at: datetime | None = None

    model_config = ConfigDict(extra="allow")


class UploadJob(BaseModel):
    """CSV upload job status."""
    id: str
    dataset_id: str
    edgemart_alias: str
    file_path: str
    operation: Literal["Overwrite", "Append"] = "Overwrite"
    status: Literal["pending", "uploading", "processing", "completed", "failed"]
    total_rows: int = 0
    uploaded_rows: int = 0
    current_part: int = 0
    total_parts: int = 0
    external_data_id: str | None = None
    error: str | None = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    completed_at: datetime | None = None

    model_config = ConfigDict(extra="allow")


# Dashboard Models
class Dashboard(BaseModel):
    """Dashboard model."""
    id: str
    name: str
    label: str
    description: str | None = None
    folder_id: str | None = Field(default=None, alias="folderId")
    folder_name: str | None = Field(default=None, alias="folderName")
    created_date: datetime = Field(alias="createdDate")
    created_by: dict | None = Field(default=None, alias="createdBy")
    last_modified_date: datetime = Field(alias="lastModifiedDate")
    last_modified_by: dict | None = Field(default=None, alias="lastModifiedBy")
    histories_url: str | None = Field(default=None, alias="historiesUrl")
    datasets_url: str | None = Field(default=None, alias="datasetsUrl")
    allow_preview: bool | None = Field(default=None, alias="allowPreview")
    asset_sharing_url: str | None = Field(default=None, alias="assetSharingUrl")
    datasets: list[dict] | None = Field(default=None, alias="datasets")
    date_version: int | None = Field(default=None, alias="dateVersion")
    files: list | None = Field(default=None, alias="files")
    folder: dict | None = Field(default=None, alias="folder")
    mobile_disabled: bool | None = Field(default=None, alias="mobileDisabled")
    permissions: dict | None = Field(default=None, alias="permissions")
    refresh_date: datetime | None = Field(default=None, alias="refreshDate")
    template_asset_source_name: str | None = Field(default=None, alias="templateAssetSourceName")
    template_source_id: str | None = Field(default=None, alias="templateSourceId")
    type: str | None = Field(default=None, alias="type")
    visibility: str | None = Field(default=None, alias="visibility")

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    @property
    def created_by_id(self) -> str | None:
        """Get createdBy ID from the createdBy object."""
        if self.created_by and isinstance(self.created_by, dict):
            return self.created_by.get("id")
        return None

    @property
    def last_modified_by_id(self) -> str | None:
        """Get lastModifiedBy ID from the lastModifiedBy object."""
        if self.last_modified_by and isinstance(self.last_modified_by, dict):
            return self.last_modified_by.get("id")
        return None


class DashboardListResponse(BaseModel):
    """Response for dashboard listing."""
    dashboards: list[Dashboard]
    next_page_url: str | None = Field(default=None, alias="nextPageUrl")

    model_config = ConfigDict(extra="allow", populate_by_name=True)


class DashboardDataset(BaseModel):
    """Dataset reference in a dashboard."""
    id: str
    name: str
    label: str
    version_id: str | None = None

    model_config = ConfigDict(extra="allow")


class DashboardBackup(BaseModel):
    """Dashboard JSON backup."""
    dashboard_id: str
    dashboard_name: str
    dashboard_label: str
    json_definition: dict[str, Any]
    backed_up_at: datetime = Field(default_factory=datetime.utcnow)

    model_config = ConfigDict(extra="allow")


# Dataflow Models
class Dataflow(BaseModel):
    """Dataflow model."""
    id: str
    name: str
    label: str
    description: str | None = Field(default=None, alias="description")
    status: str | None = None  # Not present in list response
    created_date: datetime = Field(alias="createdDate")
    created_by: dict | None = Field(default=None, alias="createdBy")
    last_modified_date: datetime = Field(alias="lastModifiedDate")
    last_modified_by: dict | None = Field(default=None, alias="lastModifiedBy")
    histories_url: str | None = Field(default=None, alias="historiesUrl")
    email_notification_level: str | None = Field(default=None, alias="emailNotificationLevel")
    type: str | None = Field(default=None, alias="type")

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    @property
    def created_by_id(self) -> str | None:
        """Get createdBy ID from the createdBy object."""
        if self.created_by and isinstance(self.created_by, dict):
            return self.created_by.get("id")
        return None

    @property
    def last_modified_by_id(self) -> str | None:
        """Get lastModifiedBy ID from the lastModifiedBy object."""
        if self.last_modified_by and isinstance(self.last_modified_by, dict):
            return self.last_modified_by.get("id")
        return None


class DataflowListResponse(BaseModel):
    """Response for dataflow listing."""
    dataflows: list[Dataflow]

    model_config = ConfigDict(extra="allow", populate_by_name=True)


class DataflowJob(BaseModel):
    """Dataflow job execution.

    The platform does not return ``dataflowId``/``dataflowName``/``command``
    on a job -- it returns the job's own ``label`` and timestamps under
    ``startDate``/``executedDate``. ``dataflow_id`` and ``dataflow_name``
    are therefore optional and only populated by callers that know which
    dataflow they asked for (e.g. the response to a start/stop request).
    """

    id: str
    status: Literal["Queued", "Running", "Success", "Failed", "Cancelled", "NotStarted"]
    label: str | None = None
    job_type: str | None = Field(default=None, alias="jobType")
    dataflow_id: str | None = None
    dataflow_name: str | None = None
    command: Literal["start", "stop"] | None = None
    start_time: datetime | None = Field(default=None, alias="startDate")
    end_time: datetime | None = Field(default=None, alias="executedDate")
    duration: int | None = None
    progress: float | None = None
    retry_count: int | None = Field(default=None, alias="retryCount")
    wait_time: int | None = Field(default=None, alias="waitTime")
    error_message: str | None = Field(default=None, alias="message")
    nodes_url: str | None = Field(default=None, alias="nodesUrl")
    url: str | None = None

    model_config = ConfigDict(extra="allow", populate_by_name=True)


class DataflowJobListResponse(BaseModel):
    """Response for dataflow job listing."""
    dataflowjobs: list[DataflowJob] = Field(default_factory=list, alias="dataflowJobs")

    model_config = ConfigDict(extra="allow", populate_by_name=True)


# Limits Models
class APILimit(BaseModel):
    """API limit information.

    ``/limits`` returns a flat mapping of limit name -> ``{Max, Remaining}``,
    not a list, so the name lives on the response rather than on the entry.
    """

    max: int = Field(alias="Max")
    remaining: int = Field(alias="Remaining")

    model_config = ConfigDict(extra="allow", populate_by_name=True)


class LimitsResponse(BaseModel):
    """API limits response.

    The platform returns ``{"LimitName": {"Max": N, "Remaining": N}, ...}``.
    """

    limits: dict[str, APILimit] = Field(default_factory=dict)

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    @classmethod
    def from_platform_response(cls, payload: dict[str, Any]) -> "LimitsResponse":
        """Build a response from the platform's flat ``{name: {Max, Remaining}}``."""
        return cls(limits={k: APILimit(**v) for k, v in payload.items()})


# Dependency Models
class Dependency(BaseModel):
    """Dependency reference."""
    id: str
    name: str
    type: str
    label: str | None = None

    model_config = ConfigDict(extra="allow", populate_by_name=True)


class DependenciesResponse(BaseModel):
    """Dependencies response.

    ``/wave/dependencies/{id}`` groups dependencies by consumer type, e.g.
    ``{"dashboards": {"dependencies": [...], "totalSize": N}}``. ``dependencies``
    is a convenience view over every group.
    """

    dashboards: dict[str, Any] | None = None
    recipes: dict[str, Any] | None = None
    lenses: dict[str, Any] | None = None
    dataflows: dict[str, Any] | None = None
    id: str | None = None
    name: str | None = None
    type: str | None = None
    url: str | None = None

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    @property
    def dependencies(self) -> list[Dependency]:
        """All dependencies across every consumer group."""
        result: list[Dependency] = []
        for group in (self.dashboards, self.recipes, self.lenses, self.dataflows):
            if not group:
                continue
            for item in group.get("dependencies", []) or []:
                result.append(Dependency(**item))
        return result


__all__ = [
    # Progress Models
    "ExtractionProgress",
    "UploadProgress",
    # Auth Models
    "OAuthToken",
    "ConnectedAppConfig",
    "WebOAuthConfig",
    "DeviceFlowConfig",
    "DeviceAuthorizationResponse",
    # Dataset Models
    "DatasetField",
    "DatasetXMD",
    "DatasetVersion",
    "Dataset",
    "DatasetListResponse",
    "ExtractionJob",
    "UploadJob",
    # Dashboard Models
    "Dashboard",
    "DashboardListResponse",
    "DashboardDataset",
    "DashboardBackup",
    # Dataflow Models
    "Dataflow",
    "DataflowListResponse",
    "DataflowJob",
    "DataflowJobListResponse",
    # Limits Models
    "APILimit",
    "LimitsResponse",
    # Dependency Models
    "Dependency",
    "DependenciesResponse",
    # Field Impact Models
    "AssetDependency",
    "AssetDependencyGraph",
    "AssetType",
    "DashboardFieldAnalysisResult",
    "DashboardFieldReference",
    "DatasetFieldAnalysisResult",
    "DatasetFieldReference",
    "DataflowFieldAnalysisResult",
    "DataflowFieldReference",
    "FieldImpactDetail",
    "FieldImpactReport",
    "FieldImpactScope",
    "FieldImpactSummary",
    "MatchMode",
    "MatchType",
    "ReplicatedDatasetField",
    "ReplicatedDatasetFieldAnalysisResult",
]
