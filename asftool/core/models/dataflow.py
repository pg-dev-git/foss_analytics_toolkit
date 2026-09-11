"""Dataflow Pydantic schemas."""
from pydantic import BaseModel, ConfigDict, Field

class DataflowModel(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    id: str | None = None
    name: str | None = None
    label: str | None = None
    dataflow_type: str | None = Field(default=None, alias="type")
    status: str | None = None

class DataflowJobModel(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    id: str | None = None
    dataflow_id: str | None = Field(default=None, alias="dataflowId")
    command: str | None = None
    status: str | None = None

class DataflowJobNodeModel(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    node_id: str | None = Field(default=None, alias="nodeId")
    node_type: str | None = Field(default=None, alias="nodeType")
    status: str | None = None
