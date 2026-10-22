"""Core SDK module for ASFTool."""

from asftool.core.auth import (
    SFCLIAuthError,
    SFCLIAuthResult,
    SFCLIAuthService,
    SFCLIError,
    SFCLIManager,
    SFCLINotFoundError,
    StoredToken,
    TokenStore,
)
from asftool.core.client import SalesforceClient, create_client, create_client_from_sf_cli
from asftool.core.config import Settings, get_settings
from asftool.core.crypto import CryptoManager, EncryptedData
from asftool.core.exceptions import (
    ASFToolError,
    AuthenticationError,
    CryptoError,
    DashboardError,
    DataflowError,
    DatasetError,
    RateLimitExceededError,
    ResourceNotFoundError,
    SalesforceAPIError,
    SalesforceAuthError,
    SalesforceNotFoundError,
    SalesforceRateLimitError,
    TokenRefreshError,
    TokenStoreUnavailableError,
    UploadError,
    ValidationError,
)
from asftool.core.replay import has_explicit_asset_id, is_replayable
from asftool.core.storage import StorageManager, get_storage_manager

__all__ = [
    "Settings",
    "get_settings",
    "CryptoManager",
    "EncryptedData",
    "StorageManager",
    "get_storage_manager",
    "has_explicit_asset_id",
    "is_replayable",
    "SalesforceClient",
    "create_client",
    "create_client_from_sf_cli",
    "ASFToolError",
    "AuthenticationError",
    "CryptoError",
    "RateLimitExceededError",
    "ResourceNotFoundError",
    "SalesforceAPIError",
    "SalesforceAuthError",
    "SalesforceRateLimitError",
    "SalesforceNotFoundError",
    "TokenRefreshError",
    "TokenStoreUnavailableError",
    "ValidationError",
    "DatasetError",
    "DashboardError",
    "DataflowError",
    "UploadError",
    "SFCLIAuthService",
    "SFCLIAuthError",
    "TokenStore",
    "StoredToken",
    "SFCLIManager",
    "SFCLIAuthResult",
    "SFCLIError",
    "SFCLINotFoundError",
]
