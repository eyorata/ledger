import pytest
"""
tests/test_event_store.py
=========================
Phase 1 tests: EventStore implementation.
These tests FAIL until you implement EventStore. That is expected.
When all pass, your event store is correct.

Run: pytest tests/test_event_store.py -v
"""
import asyncio, pytest, sys, os
from pathlib import Path; sys.path.insert(0, str(Path(__file__).parent.parent))
from ledger.event_store import EventStore, OptimisticConcurrencyError
from ledger.schema.events import CreditAnalysisCompleted, CreditDecision, RiskTier
from dotenv import load_dotenv

load_dotenv()
DB_URL = os.getenv("DATABASE_URL", "postgresql://localhost/apex_ledger")

@pytest.fixture
async def store():
    s = EventStore(DB_URL); await s.connect()
    async with s._pool.acquire() as conn:
        await conn.execute("DELETE FROM outbox WHERE event_id IN (SELECT event_id FROM events WHERE stream_id LIKE 'test-%')")
        await conn.execute("DELETE FROM events WHERE stream_id LIKE 'test-%'")
        await conn.execute("DELETE FROM event_streams WHERE stream_id LIKE 'test-%'")
    yield s
    await s.close()

def _event(etype, n=1):
    return [{"event_type":etype,"event_version":1,"payload":{"seq":i,"test":True}} for i in range(n)]

@pytest.mark.asyncio
async def test_append_new_stream(store):
    new_version = await store.append("test-new-001", _event("TestEvent"), expected_version=-1)
    assert new_version == 1

@pytest.mark.asyncio
async def test_append_existing_stream(store):
    await store.append("test-exist-001", _event("TestEvent"), expected_version=-1)
    new_version = await store.append("test-exist-001", _event("TestEvent2"), expected_version=1)
    assert new_version == 2

@pytest.mark.asyncio
async def test_occ_wrong_version_raises(store):
    await store.append("test-occ-001", _event("E"), expected_version=-1)
    with pytest.raises(OptimisticConcurrencyError) as exc:
        await store.append("test-occ-001", _event("E"), expected_version=99)
    assert exc.value.expected == 99; assert exc.value.actual == 1

@pytest.mark.asyncio
async def test_concurrent_double_append_exactly_one_succeeds(store):
    """The critical OCC test: two concurrent appends, exactly one wins."""
    await store.append("test-concurrent-001", _event("Init"), expected_version=-1)
    results = await asyncio.gather(
        store.append("test-concurrent-001", _event("A"), expected_version=1),
        store.append("test-concurrent-001", _event("B"), expected_version=1),
        return_exceptions=True,
    )
    successes = [r for r in results if isinstance(r, int)]
    errors = [r for r in results if isinstance(r, OptimisticConcurrencyError)]
    assert len(successes) == 1, f"Expected exactly 1 success, got {len(successes)}"
    assert len(errors) == 1

@pytest.mark.asyncio
async def test_load_stream_ordered(store):
    await store.append("test-load-001", _event("E",3), expected_version=-1)
    events = await store.load_stream("test-load-001")
    assert len(events) == 3
    positions = [e["stream_position"] for e in events]
    assert positions == sorted(positions)

@pytest.mark.asyncio
async def test_stream_version(store):
    await store.append("test-ver-001", _event("E",4), expected_version=-1)
    assert await store.stream_version("test-ver-001") == 4

@pytest.mark.asyncio
async def test_stream_version_nonexistent(store):
    assert await store.stream_version("test-does-not-exist") == -1

@pytest.mark.asyncio
async def test_load_all_yields_in_global_order(store):
    await store.append("test-global-A", _event("E",2), expected_version=-1)
    await store.append("test-global-B", _event("E",2), expected_version=-1)
    all_events = [e async for e in store.load_all(from_global_position=0)]
    positions = [e["global_position"] for e in all_events]
    assert positions == sorted(positions)

@pytest.mark.asyncio
async def test_double_decision_occ(store):
    await store.append("test-credit-001", _event("Init", 3), expected_version=-1)

    decision = CreditDecision(
        risk_tier=RiskTier.LOW,
        recommended_limit_usd=100000,
        confidence=0.9,
        rationale="Strong cash flow",
    )
    ev = CreditAnalysisCompleted(
        application_id="APP-001",
        session_id="S-001",
        decision=decision,
        model_version="v1",
        model_deployment_id="deploy-1",
        input_data_hash="hash",
        analysis_duration_ms=1234,
        completed_at=__import__("datetime").datetime.utcnow(),
    )

    async def attempt():
        return await store.append("test-credit-001", [ev], expected_version=3)

    results = await asyncio.gather(attempt(), attempt(), return_exceptions=True)
    successes = [r for r in results if isinstance(r, int)]
    errors = [r for r in results if isinstance(r, OptimisticConcurrencyError)]
    assert len(successes) == 1
    assert len(errors) == 1

    events = await store.load_stream("test-credit-001")
    assert len(events) == 4
    positions = [e["stream_position"] for e in events]
    assert positions[-1] == 4
