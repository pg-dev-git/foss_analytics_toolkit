"""Core SDK module for TCRM Toolkit."""

from tcrm_toolkit.core.config import Settings, get_settings
from tcrm_toolkit.core.crypto import CryptoManager, EncryptedData
from tcrm_toolkit.core.client import SalesforceClient, create_client
from tcrm_toolkit.core.exceptions import (
    TCRMToolkitError,
    ConfigurationError,
    CryptoError,
    SalesforceAPIError,
    SalesforceAuthError,
    SalesforceRateLimitError,
    SalesforceNotFoundError,
    OAuthError,
    TokenExpiredError,
    TokenNotFoundError,
    ValidationError,
    DatasetError,
    DashboardError,
    DataflowError,
    UploadError,
)

__all__ = [
    "Settings",
    "get_settings",
    "CryptoManager",
    "EncryptedData",
    "SalesforceClient",
    "create_client",
    "TCRMToolkitError",
    "ConfigurationError",
    "CryptoError",
    "SalesforceAPIError",
    "SalesforceAuthError",
    "SalesforceRateLimitError",
    "SalesforceNotFoundError",
    "OAuthError",
    "TokenExpiredError",
    "TokenNotFoundError",
    "ValidationError",
    "DatasetError",
    "DashboardError",
    "DataflowError",
    "UploadError",
]