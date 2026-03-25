"""
src/aggregates/audit_ledger.py
Minimal AuditLedgerAggregate for spec path compatibility.
"""
from __future__ import annotations
from dataclasses import dataclass

from ledger.domain.errors import DomainError


@dataclass
class AuditLedgerAggregate:
    stream_id: str
    last_event_id: str | None = None

    @classmethod
    async def load(cls, store, entity_type: str, entity_id: str) -> "AuditLedgerAggregate":
        stream_id = f"audit-{entity_type}-{entity_id}"
        events = await store.load_stream(stream_id)
        agg = cls(stream_id=stream_id)
        for event in events:
            agg._apply(event)
        return agg

    def _apply(self, event: dict) -> None:
        if event.get("event_id"):
            self.last_event_id = str(event.get("event_id"))

    def assert_append_only(self, previous_event_id: str | None) -> None:
        if previous_event_id and self.last_event_id and previous_event_id != self.last_event_id:
            raise DomainError("Audit ledger is append-only; unexpected previous_event_id.")
