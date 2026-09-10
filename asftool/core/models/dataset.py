"""Dataset Pydantic schemas mapping Salesforce Analytics REST endpoints."""
from datetime import datetime
from pydantic import BaseModel, ConfigDict, Field


class DatasetModel(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    id: str | None = None
    name: str | None = None
    label: str | None = None
    current_version_id: str | None = Field(default=None, alias="currentVersionId")
    created_date: datetime | None = Field(default=None, alias="createdDate")
    last_modified_date: datetime | None = Field(default=None, alias="lastModifiedDate")
    created_by_id: str | None = Field(default=None, alias="createdById")
    last_modified_by_id: str | None = Field(default=None, alias="lastModifiedById")
    row_count: int | None = Field(default=None, alias="rowCount")
    status: str | None = None
    dataset_type: str | None = Field(default=None, alias="type")


class DatasetVersionModel(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    id: str | None = None
    dataset_id: str | None = Field(default=None, alias="datasetId")
    version_number: str | None = Field(default=None, alias="versionNumber")
    created_date: datetime | None = Field(default=None, alias="createdDate")


class DatasetCollectionModel(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    datasets: list[DatasetModel] = Field(default_factory=list)
    total: int = 0


class XmdMainModel(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    dataset_id: str | None = Field(default=None, alias="datasetId")
    version_id: str | None = Field(default=None, alias="versionId")
    main: dict | None = None
