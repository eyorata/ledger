"""
src/models/events.py
Pydantic event models + stored event wrappers to match checklist.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Optional
from uuid import UUID

from pydantic import BaseModel, Field

from ledger.schema.events import *  # re-export all canonical event types


class StoredEvent(BaseModel):
    event_id: UUID
    stream_id: str
    stream_position: int
    global_position: int | None = None
    event_type: str
    event_version: int = 1
    payload: dict
    metadata: dict = Field(default_factory=dict)
    recorded_at: datetime


class StreamMetadata(BaseModel):
    stream_id: str
    aggregate_type: str
    current_version: int
    created_at: datetime | None = None
    archived_at: datetime | None = None
    metadata: dict = Field(default_factory=dict)


class DomainErrorType(BaseModel):
    error_type: str = "DomainError"
    message: str


class OptimisticConcurrencyErrorType(BaseModel):
    error_type: str = "OptimisticConcurrencyError"
    message: str
    stream_id: str
    expected_version: int
    actual_version: int
    suggested_action: str = "reload_stream_and_retry"


class PreconditionFailedType(BaseModel):
    error_type: str = "PreconditionFailed"
    message: str
    suggested_action: Optional[str] = None


__all__ = [
    "StoredEvent",
    "StreamMetadata",
    "DomainErrorType",
    "OptimisticConcurrencyErrorType",
    "PreconditionFailedType",
] + [name for name in globals().keys() if name[0].isupper()]
