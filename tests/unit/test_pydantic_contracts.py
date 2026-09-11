"""Unit tests for Pydantic contracts and exception hierarchy."""
import pytest


@pytest.mark.asyncio
async def test_dataset_model_contract():
    from asftool.core.models.dataset import DatasetModel
    data = {"id": "0Fb123", "name": "Test", "label": "Test Label"}
    model = DatasetModel(**data)
    assert model.id == "0Fb123"
    print("DatasetModel validated")


@pytest.mark.asyncio
async def test_exception_hierarchy():
    from asftool.core.exceptions import AuthenticationError, RateLimitExceededError, ResourceNotFoundError, ValidationError
    auth = AuthenticationError()
    assert auth.status_code == 401
    rate = RateLimitExceededError(retry_after_seconds=30)
    assert rate.retry_after_seconds == 30
    print("Exception hierarchy validated")
