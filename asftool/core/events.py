"""Observable event emitter for SDK and service layer."""

from __future__ import annotations

from typing import Any, Callable
from pydantic import BaseModel, ConfigDict, Field


class RetryEvent(BaseModel):
    model_config = ConfigDict(extra="allow")
    attempt: int
    exception_type: str
    delay_before_retry: float


class RateLimitEvent(BaseModel):
    model_config = ConfigDict(extra="allow")
    retry_after_seconds: int
    status_code: int = 429


class ProgressEvent(BaseModel):
    model_config = ConfigDict(extra="allow")
    current_chunk: int
    total_chunks: int
    processed_rows: int
    total_rows: int


class ApiRequestEvent(BaseModel):
    model_config = ConfigDict(extra="allow")
    method: str
    url: str
    status_code: int | None = None


class EventEmitter:
    def __init__(self):
        self._listeners: dict[str, list[Callable]] = {}

    def on(self, event_type: str, callback: Callable) -> None:
        self._listeners.setdefault(event_type, []).append(callback)

    async def emit(self, event_type: str, event: BaseModel) -> None:
        for cb in self._listeners.get(event_type, []):
            await cb(event)
