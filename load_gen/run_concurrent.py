"""
load_gen/run_concurrent.py
==========================
Concurrent append load generator with OCC collision tracking.
"""
from __future__ import annotations
import argparse
import asyncio
import os
from datetime import datetime
from uuid import uuid4

from ledger.event_store import EventStore, OptimisticConcurrencyError
from ledger.schema.events import ApplicationSubmitted, DecisionRequested


async def _ensure_application(store: EventStore, app_id: str) -> None:
    stream_id = f"loan-{app_id}"
    if await store.stream_version(stream_id) >= 0:
        return
    ev = ApplicationSubmitted(
        application_id=app_id,
        applicant_id="COMP-001",
        requested_amount_usd=100000,
        loan_purpose="working_capital",
        loan_term_months=12,
        submission_channel="load_gen",
        contact_email="loadgen@example.com",
        contact_name="Load Gen",
        submitted_at=datetime.now(),
        application_reference=app_id,
    )
    await store.append(stream_id, [ev], expected_version=-1)


async def _append_with_retry(store: EventStore, stream_id: str, event) -> int:
    collisions = 0
    for _ in range(10):
        expected = await store.stream_version(stream_id)
        try:
            await store.append(stream_id, [event], expected_version=expected)
            return collisions
        except OptimisticConcurrencyError:
            collisions += 1
            await asyncio.sleep(0)
    return collisions


async def worker(store: EventStore, app_ids: list[str]) -> int:
    collisions = 0
    for app_id in app_ids:
        stream_id = f"loan-{app_id}"
        ev = DecisionRequested(
            application_id=app_id,
            requested_at=datetime.now(),
            all_analyses_complete=False,
            triggered_by_event_id=str(uuid4()),
        )
        collisions += await _append_with_retry(store, stream_id, ev)
    return collisions


async def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--applications", type=int, default=15)
    p.add_argument("--concurrency", type=int, default=6)
    p.add_argument("--db-url", default=os.getenv("DATABASE_URL", "postgresql://localhost/apex_ledger"))
    args = p.parse_args()

    store = EventStore(args.db_url)
    await store.connect()

    app_ids = [f"LOAD-{i:04d}" for i in range(1, args.applications + 1)]
    for app_id in app_ids:
        await _ensure_application(store, app_id)

    # Distribute apps across workers
    chunks = [app_ids[i::args.concurrency] for i in range(args.concurrency)]
    tasks = [worker(store, chunk) for chunk in chunks]
    results = await asyncio.gather(*tasks)
    collisions = sum(results)

    # Write OCC collision report
    os.makedirs("artifacts", exist_ok=True)
    with open(os.path.join("artifacts", "occ_collision_report.txt"), "w", encoding="utf-8") as f:
        f.write(f"applications={args.applications} concurrency={args.concurrency}\n")
        f.write(f"occ_collisions={collisions}\n")

    # Projection lag report
    latest_pos, latest_at = await store.latest_event_info()
    async with store._pool.acquire() as conn:
        rows = await conn.fetch("SELECT projection_name, last_position, updated_at FROM projection_checkpoints")
    with open(os.path.join("artifacts", "projection_lag_report.txt"), "w", encoding="utf-8") as f:
        f.write(f"latest_global_position={latest_pos} recorded_at={latest_at}\n")
        for r in rows:
            if latest_at and r["updated_at"]:
                lag_ms = (latest_at - r["updated_at"]).total_seconds() * 1000
            else:
                lag_ms = None
            f.write(f"{r['projection_name']}: lag_ms={lag_ms}\n")

    await store.close()


if __name__ == "__main__":
    asyncio.run(main())
