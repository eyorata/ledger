"""
src/event_store.py
Wrapper to match Phase 1 checklist structure.
"""
from ledger.event_store import EventStore, InMemoryEventStore, OptimisticConcurrencyError

__all__ = ["EventStore", "InMemoryEventStore", "OptimisticConcurrencyError"]
