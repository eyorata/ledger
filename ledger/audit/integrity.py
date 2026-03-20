"""
ledger/audit/integrity.py
=========================
Phase 4: Cryptographic audit chain for tamper-evident logs.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from hashlib import sha256
from typing import Iterable

from ledger.schema.events import AuditIntegrityCheckRun


@dataclass
class IntegrityCheckResult:
    events_verified: int
    chain_valid: bool
    tamper_detected: bool


def _hash_payload(payload: dict) -> str:
    data = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return sha256(data).hexdigest()


def _chain_hash(previous_hash: str | None, event_hashes: Iterable[str]) -> str:
    base = previous_hash or ""
    joined = base + "".join(event_hashes)
    return sha256(joined.encode("utf-8")).hexdigest()


async def run_integrity_check(store, entity_type: str, entity_id: str) -> IntegrityCheckResult:
    """
    1. Load all events for the entity's primary stream
    2. Load all prior AuditIntegrityCheckRun events for the audit stream
    3. Verify the existing audit chain against current events
    4. Hash payloads of all events since the last check
    5. Append new AuditIntegrityCheckRun to audit-{entity_type}-{entity_id}
    """
    primary_stream = f"{entity_type}-{entity_id}"
    audit_stream = f"audit-{entity_type}-{entity_id}"

    primary_events = await store.load_stream(primary_stream)
    audit_events = await store.load_stream(audit_stream)

    chain_valid = True
    tamper_detected = False
    cursor = 0
    previous_hash: str | None = None

    # Validate existing audit chain
    for audit_event in audit_events:
        payload = audit_event.get("payload", {})
        count = int(payload.get("events_verified_count", 0))
        segment = primary_events[cursor:cursor + count]
        event_hashes = [_hash_payload(e.get("payload", {})) for e in segment]
        expected = payload.get("integrity_hash")
        computed = _chain_hash(previous_hash, event_hashes)
        if expected != computed:
            chain_valid = False
            tamper_detected = True
        previous_hash = expected
        cursor += count

    # Hash remaining events since last check
    remaining = primary_events[cursor:]
    remaining_hashes = [_hash_payload(e.get("payload", {})) for e in remaining]
    new_hash = _chain_hash(previous_hash, remaining_hashes)

    integrity_event = AuditIntegrityCheckRun(
        entity_type=entity_type,
        entity_id=entity_id,
        check_timestamp=datetime.now(timezone.utc),
        events_verified_count=len(remaining),
        integrity_hash=new_hash,
        previous_hash=previous_hash,
        chain_valid=chain_valid,
        tamper_detected=tamper_detected,
    )

    expected_version = await store.stream_version(audit_stream)
    await store.append(
        stream_id=audit_stream,
        events=[integrity_event],
        expected_version=expected_version,
    )

    return IntegrityCheckResult(
        events_verified=len(remaining),
        chain_valid=chain_valid,
        tamper_detected=tamper_detected,
    )
