"""LangChain tool wrappers for ASFToolSDK."""

from langchain_core.tools import BaseTool
from pydantic import BaseModel, Field


class CRMAListDatasetsInput(BaseModel):
    page_size: int = Field(default=50)


class CRMAListDatasetsTool(BaseTool):
    name: str = "crma_list_datasets"
    description: str = "List Salesforce Analytics datasets"
    args_schema: type[BaseModel] = CRMAListDatasetsInput

    def _run(self, page_size: int = 50) -> str:
        return f"Listing datasets with page_size={page_size}"
