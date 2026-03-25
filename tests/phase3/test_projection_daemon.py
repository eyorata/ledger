import pytest

from ledger.event_store import InMemoryEventStore
from ledger.projections.base import Projection
from ledger.projections.daemon import ProjectionDaemon
from datetime import datetime, timezone

from ledger.schema.events import ApplicationSubmitted


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
