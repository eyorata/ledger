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
        await conn.execute("DELETE FROM outbox WHERE event_id IN (SELECT event_id FROM events WHERE stream_id LIKE 'test-concurrency-%')")
        await conn.execute("DELETE FROM events WHERE stream_id LIKE 'test-concurrency-%'")
        await conn.execute("DELETE FROM event_streams WHERE stream_id LIKE 'test-concurrency-%'")
    yield s
    await s.close()


def _event(etype, n=1):
    return [{"event_type": etype, "event_version": 1, "payload": {"seq": i}} for i in range(n)]


@pytest.mark.asyncio
async def test_double_decision_concurrency():
    """
    Two concurrent tasks append to the same stream at expected_version=3.
    Exactly one succeeds, one raises OptimisticConcurrencyError, stream length = 4.
    """
    store = EventStore(DB_URL)
    await store.connect()
    try:
        await store.append("test-concurrency-001", _event("Init", 3), expected_version=-1)

        async def attempt():
            return await store.append("test-concurrency-001", _event("CreditAnalysisCompleted"), expected_version=3)

        results = await asyncio.gather(attempt(), attempt(), return_exceptions=True)
        successes = [r for r in results if isinstance(r, int)]
        errors = [r for r in results if isinstance(r, OptimisticConcurrencyError)]
        assert len(successes) == 1
        assert len(errors) == 1

        events = await store.load_stream("test-concurrency-001")
        assert len(events) == 4
        assert events[-1]["stream_position"] == 4
    finally:
        await store.close()
