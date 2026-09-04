"""Pydantic models for ASFTool domain objects."""

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


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
    """Dataset version information."""
    id: str
    dataset_id: str
    version_number: int
    created_date: datetime
    created_by_id: str
    status: str
    row_count: int | None = None
    xmd: DatasetXMD | None = None

    model_config = ConfigDict(extra="allow")


class Dataset(BaseModel):
    """Dataset model."""
    id: str
    name: str
    label: str
    description: str | None = None
    current_version_id: str | None = None
    current_version_url: str | None = None
    versions_url: str | None = None
    histories_url: str | None = None
    created_date: datetime
    created_by_id: str
    last_modified_date: datetime
    last_modified_by_id: str
    row_count: int | None = None
    status: str = "Active"
    type: str = "Edgemart"

    model_config = ConfigDict(extra="allow")


class DatasetListResponse(BaseModel):
    """Response for dataset listing."""
    datasets: list[Dataset]
    next_page_url: str | None = None

    model_config = ConfigDict(extra="allow")


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
    folder_id: str | None = None
    folder_name: str | None = None
    created_date: datetime
    created_by_id: str
    last_modified_date: datetime
    last_modified_by_id: str
    histories_url: str | None = None
    datasets_url: str | None = None

    model_config = ConfigDict(extra="allow")


class DashboardListResponse(BaseModel):
    """Response for dashboard listing."""
    dashboards: list[Dashboard]
    next_page_url: str | None = None

    model_config = ConfigDict(extra="allow")


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
    description: str | None = None
    status: str
    created_date: datetime
    created_by_id: str
    last_modified_date: datetime
    last_modified_by_id: str
    histories_url: str | None = None

    model_config = ConfigDict(extra="allow")


class DataflowListResponse(BaseModel):
    """Response for dataflow listing."""
    dataflows: list[Dataflow]

    model_config = ConfigDict(extra="allow")


class DataflowJob(BaseModel):
    """Dataflow job execution."""
    id: str
    dataflow_id: str
    dataflow_name: str
    command: Literal["start", "stop"]
    status: Literal["Queued", "Running", "Success", "Failed", "Cancelled"]
    start_time: datetime | None = None
    end_time: datetime | None = None
    error_message: str | None = None

    model_config = ConfigDict(extra="allow")


class DataflowJobListResponse(BaseModel):
    """Response for dataflow job listing."""
    dataflowjobs: list[DataflowJob]

    model_config = ConfigDict(extra="allow")


# Limits Models
class APILimit(BaseModel):
    """API limit information."""
    name: str
    remaining: int
    max: int

    model_config = ConfigDict(extra="allow")


class LimitsResponse(BaseModel):
    """API limits response."""
    limits: list[APILimit]

    model_config = ConfigDict(extra="allow")


# Dependency Models
class Dependency(BaseModel):
    """Dependency reference."""
    id: str
    name: str
    type: str
    label: str | None = None

    model_config = ConfigDict(extra="allow")


class DependenciesResponse(BaseModel):
    """Dependencies response."""
    dependencies: list[Dependency]

    model_config = ConfigDict(extra="allow")
