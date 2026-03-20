"""
ledger/domain/aggregates/audit_ledger.py
AuditLedger aggregate with append-only and causal ordering checks.
"""
from __future__ import annotations
from dataclasses import dataclass

from ledger.domain.errors import DomainError


@dataclass
class AuditLedgerAggregate:
    entity_type: str
    entity_id: str
    stream_id: str
    version: int = -1

    @classmethod
    async def load(cls, store, entity_type: str, entity_id: str) -> "AuditLedgerAggregate":
        stream_id = f"audit-{entity_type}-{entity_id}"
        events = await store.load_stream(stream_id)
        agg = cls(entity_type=entity_type, entity_id=entity_id, stream_id=stream_id)
        for event in events:
            agg._apply(event)
        return agg

    def _apply(self, event: dict) -> None:
        if "stream_position" in event:
            self.version = event["stream_position"]
        else:
            self.version += 1

    def assert_append_only(self, expected_version: int) -> None:
        if self.version != expected_version:
            raise DomainError(
                f"Append-only violation: expected version {expected_version}, actual {self.version}"
            )

    async def assert_causation_exists(self, store, causation_id: str | None) -> None:
        if not causation_id:
            return
        ev = await store.get_event(causation_id)
        if not ev:
            raise DomainError(f"Causation event {causation_id} not found.")
