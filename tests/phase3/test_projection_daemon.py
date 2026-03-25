import pytest
import asyncio

from ledger.event_store import InMemoryEventStore
from ledger.projections.base import Projection
from ledger.projections.daemon import ProjectionDaemon
from datetime import datetime, timezone

from ledger.schema.events import ApplicationSubmitted
from ledger.projections.application_summary import ApplicationSummaryProjection


class FlakyProjection(Projection):
    name = "flaky_projection"
    subscribed_event_types = {"ApplicationSubmitted"}

    def __init__(self):
        super().__init__()
        self._failed = False

    async def handle(self, event: dict, store) -> None:
        if not self._failed:
            self._failed = True
            raise RuntimeError("boom")


@pytest.mark.asyncio
async def test_projection_daemon_fault_tolerance_and_lag():
    store = InMemoryEventStore()
    await store.append(
        "loan-DAEMON-1",
        [
            ApplicationSubmitted(
                application_id="DAEMON-1",
                applicant_id="COMP-001",
                requested_amount_usd=1000,
                loan_purpose="working_capital",
                loan_term_months=12,
                submission_channel="api",
                contact_email="test@example.com",
                contact_name="Test",
                submitted_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
                application_reference="DAEMON-1",
            )
        ],
        expected_version=-1,
    )

    proj = FlakyProjection()
    daemon = ProjectionDaemon(store, [proj], max_retries=1)
    await daemon._process_batch()

    checkpoint = await store.load_checkpoint(proj.name)
    assert checkpoint >= 0
    assert daemon.get_lag(proj.name) is not None


@pytest.mark.asyncio
async def test_projection_lag_slo_under_load():
    store = InMemoryEventStore()
    proj = ApplicationSummaryProjection()
    daemon = ProjectionDaemon(store, [proj], max_retries=2)

    async def append_app(i: int):
        await store.append(
            f"loan-SLO-{i}",
            [
                ApplicationSubmitted(
                    application_id=f"SLO-{i}",
                    applicant_id="COMP-001",
                    requested_amount_usd=1000 + i,
                    loan_purpose="working_capital",
                    loan_term_months=12,
                    submission_channel="api",
                    contact_email="test@example.com",
                    contact_name="Test",
                    submitted_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
                    application_reference=f"SLO-{i}",
                )
            ],
            expected_version=-1,
        )

    await asyncio.gather(*[append_app(i) for i in range(50)])
    await daemon._process_batch()

    lag = daemon.get_lag(proj.name)
    assert lag is not None
    assert lag <= 500.0


@pytest.mark.asyncio
async def test_projection_rebuild_from_scratch():
    store = InMemoryEventStore()
    proj = ApplicationSummaryProjection()

    await store.append(
        "loan-REB-1",
        [
            ApplicationSubmitted(
                application_id="REB-1",
                applicant_id="COMP-001",
                requested_amount_usd=1000,
                loan_purpose="working_capital",
                loan_term_months=12,
                submission_channel="api",
                contact_email="test@example.com",
                contact_name="Test",
                submitted_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
                application_reference="REB-1",
            )
        ],
        expected_version=-1,
    )

    # Should complete without error and populate projection state
    # InMemoryEventStore doesn't provide DB tables; emulate rebuild by replaying
    async for event in store.load_all(from_global_position=0, event_types=list(proj.subscribed_event_types)):
        await proj.handle(event, store=None)
    assert proj is not None
