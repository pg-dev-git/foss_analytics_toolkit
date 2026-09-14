"""Authentication token models for SF CLI session integration."""

from pydantic import BaseModel, Field


class AuthTokens(BaseModel):
    """Typed container for Salesforce authentication tokens."""

    model_config = {"frozen": True}

    access_token: str = Field(..., description="OAuth/access token for REST API")
    instance_url: str = Field(..., description="Salesforce instance URL")
    username: str | None = Field(None, description="Authenticated username")
    alias: str = Field(default="default", description="SF CLI alias")
    token_expired: bool = Field(default=False, description="Whether token has expired")
