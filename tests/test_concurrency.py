import asyncio
import os

import pytest

from ledger.event_store import EventStore, OptimisticConcurrencyError


DB_URL = os.getenv("DATABASE_URL", "postgresql://localhost/apex_ledger")


@pytest.fixture
async def store():
    s = EventStore(DB_URL)
    await s.connect()
    async with s._pool.acquire() as conn:
        await conn.execute(
            "DELETE FROM outbox WHERE event_id IN (SELECT event_id FROM events WHERE stream_id LIKE 'test-concurrency-%')"
        )
        await conn.execute("DELETE FROM events WHERE stream_id LIKE 'test-concurrency-%'")
        await conn.execute("DELETE FROM event_streams WHERE stream_id LIKE 'test-concurrency-%'")
    yield s
    await s.close()


def _event(etype, n=1):
    return [{"event_type": etype, "event_version": 1, "payload": {"seq": i}} for i in range(n)]


@pytest.mark.asyncio
async def test_double_decision_concurrency(store):
    """
    Two concurrent tasks append to the same stream at expected_version=3.
    Exactly one succeeds, one raises OptimisticConcurrencyError, stream length = 4.
    """
    await store.append("test-concurrency-001", _event("Init", 3), expected_version=-1)

    async def attempt():
        return await store.append(
            "test-concurrency-001",
            _event("CreditAnalysisCompleted"),
            expected_version=3,
        )

    results = await asyncio.gather(attempt(), attempt(), return_exceptions=True)
    successes = [r for r in results if isinstance(r, int)]
    errors = [r for r in results if isinstance(r, OptimisticConcurrencyError)]
    print(f"Success count: {len(successes)} | Successes: {successes}")
    if errors:
        err = errors[0]
        print(
            "OCC error:",
            f"stream_id={err.stream_id}",
            f"expected={err.expected}",
            f"actual={err.actual}",
        )
    assert len(successes) == 1
    assert len(errors) == 1

    events = await store.load_stream("test-concurrency-001")
    print(f"Total stream length: {len(events)}")
    if events:
        print(f"Winning event stream_position: {events[-1]['stream_position']}")
    assert len(events) == 4
    assert events[-1]["stream_position"] == 4
