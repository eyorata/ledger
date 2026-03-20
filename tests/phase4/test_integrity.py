import pytest

from ledger.audit.integrity import run_integrity_check
from ledger.event_store import InMemoryEventStore


@pytest.mark.asyncio
async def test_audit_hash_chain_detects_tamper():
    store = InMemoryEventStore()
    stream_id = "loan-ABC-1"

    await store.append(stream_id, [{"event_type": "E1", "payload": {"x": 1}}], expected_version=-1)
    await store.append(stream_id, [{"event_type": "E2", "payload": {"x": 2}}], expected_version=0)

    result1 = await run_integrity_check(store, "loan", "ABC-1")
    assert result1.events_verified == 2
    assert result1.chain_valid is True
    assert result1.tamper_detected is False

    # Tamper with a historical event payload
    store._streams[stream_id][0]["payload"]["x"] = 999

    result2 = await run_integrity_check(store, "loan", "ABC-1")
    assert result2.chain_valid is False
    assert result2.tamper_detected is True


@pytest.mark.asyncio
async def test_audit_hash_chain_progresses():
    store = InMemoryEventStore()
    stream_id = "loan-ABC-2"

    await store.append(stream_id, [{"event_type": "E1", "payload": {"x": 1}}], expected_version=-1)
    await store.append(stream_id, [{"event_type": "E2", "payload": {"x": 2}}], expected_version=0)

    await run_integrity_check(store, "loan", "ABC-2")
    await store.append(stream_id, [{"event_type": "E3", "payload": {"x": 3}}], expected_version=1)

    result = await run_integrity_check(store, "loan", "ABC-2")
    assert result.events_verified == 1
    assert result.chain_valid is True
    assert result.tamper_detected is False
