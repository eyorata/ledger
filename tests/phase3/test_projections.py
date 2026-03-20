"""
Phase 3 projection tests: daemon, lag, temporal compliance view.
"""
import asyncio
import os
from datetime import datetime, timezone, timedelta

import pytest
from dotenv import load_dotenv

from ledger.event_store import EventStore
from ledger.projections.application_summary import ApplicationSummaryProjection
from ledger.projections.agent_performance import AgentPerformanceLedgerProjection
from ledger.projections.compliance_audit import ComplianceAuditViewProjection
from ledger.projections.daemon import ProjectionDaemon
from ledger.projections.base import Projection


load_dotenv()
DB_URL = os.getenv("DATABASE_URL", "postgresql://localhost/apex_ledger")


async def _ensure_tables(store: EventStore) -> None:
    async with store._pool.acquire() as conn:
        # core tables (safe if already present)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS event_streams (
              stream_id TEXT PRIMARY KEY,
              aggregate_type TEXT NOT NULL,
              current_version BIGINT NOT NULL DEFAULT 0,
              created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
              archived_at TIMESTAMPTZ,
              metadata JSONB NOT NULL DEFAULT '{}'::jsonb
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS events (
              event_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
              stream_id TEXT NOT NULL,
              stream_position BIGINT NOT NULL,
              global_position BIGINT GENERATED ALWAYS AS IDENTITY,
              event_type TEXT NOT NULL,
              event_version SMALLINT NOT NULL DEFAULT 1,
              payload JSONB NOT NULL,
              metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
              recorded_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
              CONSTRAINT uq_stream_position UNIQUE (stream_id, stream_position)
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS outbox (
              id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
              event_id UUID NOT NULL REFERENCES events(event_id),
              destination TEXT NOT NULL,
              payload JSONB NOT NULL,
              created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
              published_at TIMESTAMPTZ,
              attempts SMALLINT NOT NULL DEFAULT 0
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS projection_checkpoints (
              projection_name TEXT PRIMARY KEY,
              last_position BIGINT NOT NULL DEFAULT 0,
              updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """)
        # projections
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS application_summary (
              application_id TEXT PRIMARY KEY,
              state TEXT,
              applicant_id TEXT,
              requested_amount_usd NUMERIC,
              approved_amount_usd NUMERIC,
              risk_tier TEXT,
              fraud_score NUMERIC,
              compliance_status TEXT,
              decision TEXT,
              agent_sessions_completed TEXT[],
              last_event_type TEXT,
              last_event_at TIMESTAMPTZ,
              human_reviewer_id TEXT,
              final_decision_at TIMESTAMPTZ
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS agent_performance_ledger (
              agent_id TEXT NOT NULL,
              model_version TEXT NOT NULL,
              analyses_completed BIGINT NOT NULL DEFAULT 0,
              decisions_generated BIGINT NOT NULL DEFAULT 0,
              avg_confidence_score NUMERIC,
              avg_duration_ms NUMERIC,
              approve_rate NUMERIC NOT NULL DEFAULT 0,
              decline_rate NUMERIC NOT NULL DEFAULT 0,
              refer_rate NUMERIC NOT NULL DEFAULT 0,
              human_override_rate NUMERIC NOT NULL DEFAULT 0,
              first_seen_at TIMESTAMPTZ,
              last_seen_at TIMESTAMPTZ,
              PRIMARY KEY (agent_id, model_version)
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS compliance_audit_current (
              application_id TEXT PRIMARY KEY,
              state JSONB NOT NULL,
              last_event_at TIMESTAMPTZ
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS compliance_audit_history (
              application_id TEXT NOT NULL,
              recorded_at TIMESTAMPTZ NOT NULL,
              state JSONB NOT NULL
            )
        """)


async def _clear_tables(store: EventStore) -> None:
    async with store._pool.acquire() as conn:
        await conn.execute("DELETE FROM outbox")
        await conn.execute("DELETE FROM events")
        await conn.execute("DELETE FROM event_streams")
        await conn.execute("DELETE FROM projection_checkpoints")
        await conn.execute("DELETE FROM application_summary")
        await conn.execute("DELETE FROM agent_performance_ledger")
        await conn.execute("DELETE FROM compliance_audit_current")
        await conn.execute("DELETE FROM compliance_audit_history")


@pytest.fixture
async def store():
    s = EventStore(DB_URL)
    await s.connect()
    await _ensure_tables(s)
    await _clear_tables(s)
    yield s
    await s.close()


def _loan_events(app_id: str):
    return [
        {"event_type": "ApplicationSubmitted", "event_version": 1,
         "payload": {"application_id": app_id, "applicant_id": f"C-{app_id}", "requested_amount_usd": 1000, "loan_purpose": "working_capital"}},
        {"event_type": "CreditAnalysisRequested", "event_version": 1,
         "payload": {"application_id": app_id}},
        {"event_type": "CreditAnalysisCompleted", "event_version": 1,
         "payload": {"application_id": app_id, "decision": {"risk_tier": "LOW"}}},
        {"event_type": "ComplianceCheckCompleted", "event_version": 1,
         "payload": {"application_id": app_id, "overall_verdict": "CLEAR"}},
        {"event_type": "DecisionGenerated", "event_version": 1,
         "payload": {"application_id": app_id, "recommendation": "APPROVE", "confidence": 0.9, "contributing_sessions": []}},
    ]


@pytest.mark.asyncio
async def test_projection_daemon_lag_slos(store):
    app_proj = ApplicationSummaryProjection()
    perf_proj = AgentPerformanceLedgerProjection()
    comp_proj = ComplianceAuditViewProjection()
    daemon = ProjectionDaemon(store, [app_proj, perf_proj, comp_proj])

    # simulate 50 concurrent handlers (unique streams to avoid OCC)
    async def append_app(i: int):
        app_id = f"APP-{i:03d}"
        await store.append(f"loan-{app_id}", _loan_events(app_id), expected_version=-1)
        # compliance events for audit view
        await store.append(
            f"compliance-{app_id}",
            [
                {"event_type": "ComplianceCheckInitiated", "event_version": 1,
                 "payload": {"application_id": app_id, "regulation_set_version": "2026-Q1", "rules_to_evaluate": ["REG-001"]}},
                {"event_type": "ComplianceRulePassed", "event_version": 1,
                 "payload": {"application_id": app_id, "rule_id": "REG-001", "rule_name": "KYC", "rule_version": "v1", "evaluated_at": datetime.now(timezone.utc).isoformat()}},
                {"event_type": "ComplianceCheckCompleted", "event_version": 1,
                 "payload": {"application_id": app_id, "overall_verdict": "CLEAR"}},
            ],
            expected_version=-1,
        )

    await asyncio.gather(*[append_app(i) for i in range(50)])

    await daemon._process_batch()

    # Verify lag metrics within SLOs (best-effort; should be near-zero in tests)
    app_lag = daemon.get_lag("application_summary")
    comp_lag = daemon.get_lag("compliance_audit_view")
    assert app_lag is None or app_lag < 500
    assert comp_lag is None or comp_lag < 2000


@pytest.mark.asyncio
async def test_compliance_temporal_query(store):
    comp_proj = ComplianceAuditViewProjection()
    daemon = ProjectionDaemon(store, [comp_proj])

    app_id = "APP-TIME"
    await store.append(
        f"compliance-{app_id}",
        [
            {"event_type": "ComplianceCheckInitiated", "event_version": 1,
             "payload": {"application_id": app_id, "regulation_set_version": "2026-Q1", "rules_to_evaluate": ["REG-001"]}},
        ],
        expected_version=-1,
    )
    await daemon._process_batch()
    first = await comp_proj.get_current_compliance(store, app_id)
    assert first is not None
    t1 = first["last_event_at"]

    # add later rule completion
    t2 = datetime.now(timezone.utc)
    await store.append(
        f"compliance-{app_id}",
        [
            {"event_type": "ComplianceRulePassed", "event_version": 1,
             "payload": {"application_id": app_id, "rule_id": "REG-001", "rule_name": "KYC", "rule_version": "v1", "evaluated_at": t2.isoformat()}},
            {"event_type": "ComplianceCheckCompleted", "event_version": 1,
             "payload": {"application_id": app_id, "overall_verdict": "CLEAR"}},
        ],
        expected_version=1,
    )
    await daemon._process_batch()

    current = await comp_proj.get_current_compliance(store, app_id)
    assert current is not None

    historical = await comp_proj.get_compliance_at(store, app_id, t1)
    assert historical is not None


class FlakyProjection(Projection):
    name = "flaky"
    subscribed_event_types = {"ApplicationSubmitted"}

    def __init__(self):
        super().__init__()
        self.calls = 0

    async def handle(self, event: dict, store) -> None:
        self.calls += 1
        if self.calls < 3:
            raise RuntimeError("boom")


@pytest.mark.asyncio
async def test_daemon_retries_then_skips(store):
    proj = FlakyProjection()
    daemon = ProjectionDaemon(store, [proj])

    await store.append(
        "loan-RETRY-1",
        [{"event_type": "ApplicationSubmitted", "event_version": 1, "payload": {"application_id": "RETRY-1"}}],
        expected_version=-1,
    )

    # first two passes fail, third should succeed and checkpoint should advance
    await daemon._process_batch()
    await daemon._process_batch()
    await daemon._process_batch()

    cp = await store.load_checkpoint("flaky")
    assert cp >= 1
