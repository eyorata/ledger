"""
src/models/events.py
Thin wrapper for spec path compatibility.
"""
from ledger.schema.events import *  # noqa: F401,F403
from ledger.event_store import OptimisticConcurrencyError  # noqa: F401
from ledger.domain.errors import DomainError  # noqa: F401

__all__ = [  # type: ignore[var-annotated]
    *globals().get("__all__", []),
    "OptimisticConcurrencyError",
    "DomainError",
]
