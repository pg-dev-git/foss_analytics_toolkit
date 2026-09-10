"""Dashboard Pydantic schemas."""
from pydantic import BaseModel, ConfigDict, Field

class DashboardModel(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    id: str | None = None
    name: str | None = None
    label: str | None = None
    state: str | None = None
    folder_name: str | None = Field(default=None, alias="folderName")

class DashboardStateModel(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    state: str | None = None
    status: str | None = None
