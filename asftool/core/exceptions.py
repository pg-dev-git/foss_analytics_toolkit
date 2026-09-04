"""Custom exceptions for ASFTool."""


class ASFToolError(Exception):
    """Base exception for ASFTool."""

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message


class ConfigurationError(ASFToolError):
    """Raised when configuration is invalid or missing."""

    pass


class CryptoError(ASFToolError):
    """Raised when encryption/decryption fails."""

    pass


class SalesforceAPIError(ASFToolError):
    """Raised when Salesforce API returns an error."""

    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        error_code: str | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.error_code = error_code


class SalesforceAuthError(SalesforceAPIError):
    """Raised when authentication fails or token expires."""

    def __init__(self, message: str = "Authentication failed or token expired") -> None:
        super().__init__(message, status_code=401)


class SalesforceRateLimitError(SalesforceAPIError):
    """Raised when rate limit is exceeded."""

    def __init__(
        self,
        message: str = "Rate limit exceeded",
        *,
        retry_after: int = 60,
    ) -> None:
        super().__init__(message, status_code=429)
        self.retry_after = retry_after


class SalesforceNotFoundError(SalesforceAPIError):
    """Raised when a resource is not found."""

    def __init__(self, message: str = "Resource not found") -> None:
        super().__init__(message, status_code=404)


class OAuthError(ASFToolError):
    """Raised when OAuth flow fails."""

    def __init__(
        self,
        message: str,
        *,
        error_code: str | None = None,
    ) -> None:
        super().__init__(message)
        self.error_code = error_code


class TokenExpiredError(OAuthError):
    """Raised when OAuth token has expired and cannot be refreshed."""

    pass


class TokenNotFoundError(OAuthError):
    """Raised when no token is found for the user."""

    pass


class ValidationError(ASFToolError):
    """Raised when input validation fails."""

    pass


class DatasetError(ASFToolError):
    """Raised when dataset operations fail."""

    pass


class DashboardError(ASFToolError):
    """Raised when dashboard operations fail."""

    pass


class DataflowError(ASFToolError):
    """Raised when dataflow operations fail."""

    pass


class UploadError(ASFToolError):
    """Raised when file upload fails."""

    pass
