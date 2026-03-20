"""Projection base classes."""
from __future__ import annotations
from abc import ABC, abstractmethod
from datetime import datetime


class Projection(ABC):
    name: str
    subscribed_event_types: set[str]

    def __init__(self):
        self.last_processed_position: int = 0
        self.last_processed_at: datetime | None = None
        self._lag_ms: float | None = None

    @abstractmethod
    async def handle(self, event: dict, store) -> None:
        raise NotImplementedError

    def set_lag(self, lag_ms: float | None) -> None:
        self._lag_ms = lag_ms

    def get_lag(self) -> float | None:
        return self._lag_ms
