"""Custom exceptions for TCRM Toolkit."""


class TCRMToolkitError(Exception):
    """Base exception for TCRM Toolkit."""

    def __init__(self, message: str, *args: object) -> None:
        super().__init__(message, *args)
        self.message = message


class ConfigurationError(TCRMToolkitError):
    """Raised when configuration is invalid or missing."""

    pass


class CryptoError(TCRMToolkitError):
    """Raised when encryption/decryption fails."""

    pass


class SalesforceAPIError(TCRMToolkitError):
    """Raised when Salesforce API returns an error."""

    def __init__(
        self,
        message: str,
        status_code: int | None = None,
        error_code: str | None = None,
        *args: object,
    ) -> None:
        super().__init__(message, *args)
        self.status_code = status_code
        self.error_code = error_code


class SalesforceAuthError(SalesforceAPIError):
    """Raised when authentication fails or token expires."""

    def __init__(self, message: str = "Authentication failed or token expired", *args: object) -> None:
        super().__init__(message, status_code=401, *args)


class SalesforceRateLimitError(SalesforceAPIError):
    """Raised when rate limit is exceeded."""

    def __init__(
        self,
        message: str = "Rate limit exceeded",
        retry_after: int = 60,
        *args: object,
    ) -> None:
        super().__init__(message, status_code=429, *args)
        self.retry_after = retry_after


class SalesforceNotFoundError(SalesforceAPIError):
    """Raised when a resource is not found."""

    def __init__(self, message: str = "Resource not found", *args: object) -> None:
        super().__init__(message, status_code=404, *args)


class OAuthError(TCRMToolkitError):
    """Raised when OAuth flow fails."""

    def __init__(
        self,
        message: str,
        error_code: str | None = None,
        *args: object,
    ) -> None:
        super().__init__(message, *args)
        self.error_code = error_code


class TokenExpiredError(OAuthError):
    """Raised when OAuth token has expired and cannot be refreshed."""

    pass


class TokenNotFoundError(OAuthError):
    """Raised when no token is found for the user."""

    pass


class ValidationError(TCRMToolkitError):
    """Raised when input validation fails."""

    pass


class DatasetError(TCRMToolkitError):
    """Raised when dataset operations fail."""

    pass


class DashboardError(TCRMToolkitError):
    """Raised when dashboard operations fail."""

    pass


class DataflowError(TCRMToolkitError):
    """Raised when dataflow operations fail."""

    pass


class UploadError(TCRMToolkitError):
    """Raised when file upload fails."""

    pass
