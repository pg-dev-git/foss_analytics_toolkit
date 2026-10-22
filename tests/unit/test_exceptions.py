"""Tests for the exception hierarchy and its public exports.

The module used to define sixteen exception classes, of which five were
never raised or caught anywhere -- they existed only as re-exports from
``asftool.core``. They are removed, and these tests make sure the set does
not drift back: every exported name must exist, and every exception
class in the module must be exported.
"""

import inspect

import pytest

from asftool.core import exceptions as exc_module


class TestPublicExports:
    def test_every_exported_name_exists(self):
        """`asftool.core.__all__` must not name anything that is missing."""
        import asftool.core as core

        missing = [name for name in core.__all__ if not hasattr(core, name)]
        assert not missing, f"asftool.core.__all__ names missing attributes: {missing}"

    def test_every_exception_class_is_exported(self):
        """A new exception must be exported, or it is a private one."""
        import asftool.core as core

        defined = {
            name
            for name, obj in vars(exc_module).items()
            if inspect.isclass(obj)
            and issubclass(obj, Exception)
            and obj.__module__ == exc_module.__name__
        }
        not_exported = sorted(defined - set(core.__all__))
        assert not not_exported, (
            f"exception classes not exported from asftool.core: {not_exported}"
        )

    def test_no_stale_exports(self):
        """The removed classes must not come back."""
        import asftool.core as core

        removed = {
            "ConfigurationError",
            "OAuthError",
            "TokenExpiredError",
            "TokenNotFoundError",
        }
        assert not (removed & set(core.__all__))

    def test_field_impact_error_is_gone(self):
        """It had zero references in the codebase."""
        assert not hasattr(exc_module, "FieldImpactError")


class TestHierarchy:
    @pytest.mark.parametrize(
        "name",
        [
            "CryptoError",
            "SalesforceAPIError",
            "TokenStoreUnavailableError",
            "TokenRefreshError",
            "ValidationError",
            "DatasetError",
            "DashboardError",
            "DataflowError",
            "UploadError",
        ],
    )
    def test_every_error_derives_from_the_base(self, name):
        cls = getattr(exc_module, name)
        assert issubclass(cls, exc_module.ASFToolError)

    @pytest.mark.parametrize(
        "name",
        [
            "SalesforceAuthError",
            "SalesforceRateLimitError",
            "SalesforceNotFoundError",
        ],
    )
    def test_salesforce_errors_derive_from_the_api_error(self, name):
        cls = getattr(exc_module, name)
        assert issubclass(cls, exc_module.SalesforceAPIError)

    def test_rate_limit_error_carries_the_retry_after(self):
        error = exc_module.SalesforceRateLimitError("slow down", retry_after=300)
        assert error.retry_after == 300
        assert error.status_code == 429

    def test_api_error_carries_the_status_and_code(self):
        error = exc_module.SalesforceAPIError(
            "nope", status_code=403, error_code="INSUFFICIENT_ACCESS"
        )
        assert error.status_code == 403
        assert error.error_code == "INSUFFICIENT_ACCESS"

    def test_base_error_keeps_its_message(self):
        assert exc_module.ASFToolError("boom").message == "boom"
