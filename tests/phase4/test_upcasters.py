import os
import json
from datetime import datetime, timezone

import pytest

from ledger.event_store import EventStore


DB_URL = os.getenv("DATABASE_URL", "postgresql://localhost/apex_ledger")


@pytest.fixture
async def store():
    s = EventStore(DB_URL)
    await s.connect()
    async with s._pool.acquire() as conn:
        await conn.execute("DELETE FROM outbox WHERE event_id IN (SELECT event_id FROM events WHERE stream_id LIKE 'test-upcast-%')")
        await conn.execute("DELETE FROM events WHERE stream_id LIKE 'test-upcast-%'")
        await conn.execute("DELETE FROM event_streams WHERE stream_id LIKE 'test-upcast-%'")
    yield s
    await s.close()


@pytest.mark.asyncio
async def test_upcast_credit_analysis_immutability(store):
    stream_id = "test-upcast-credit-001"
    v1_event = {
        "event_type": "CreditAnalysisCompleted",
        "event_version": 1,
        "payload": {
            "application_id": "APP-IMM-1",
            "session_id": "S-IMM-1",
            "decision": {"risk_tier": "LOW", "recommended_limit_usd": 100000, "confidence": 0.75, "rationale": "ok"},
            "model_deployment_id": "legacy-deploy",
            "input_data_hash": "hash",
            "analysis_duration_ms": 123,
            "completed_at": datetime.now(timezone.utc).isoformat(),
        },
    }

    await store.append(stream_id, [v1_event], expected_version=-1)

    async with store._pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT payload, event_version FROM events WHERE stream_id=$1 ORDER BY stream_position ASC LIMIT 1",
            stream_id,
        )
        raw_payload = row["payload"]
        if isinstance(raw_payload, str):
            raw_payload = json.loads(raw_payload)
        raw_version = row["event_version"]

    loaded = await store.load_stream(stream_id)
    assert loaded[0]["event_version"] == 2
    assert "model_version" in loaded[0]["payload"]
    assert "confidence_score" in loaded[0]["payload"]
    assert "regulatory_basis" in loaded[0]["payload"]

    async with store._pool.acquire() as conn:
        row2 = await conn.fetchrow(
            "SELECT payload, event_version FROM events WHERE stream_id=$1 ORDER BY stream_position ASC LIMIT 1",
            stream_id,
        )
        assert row2["event_version"] == raw_version
        payload2 = row2["payload"]
        if isinstance(payload2, str):
            payload2 = json.loads(payload2)
        assert payload2 == raw_payload


@pytest.mark.asyncio
async def test_upcast_decision_generated_model_versions(store):
    session_id = "S-UP-1"
    agent_stream = f"test-upcast-agent-{session_id}"
    await store.append(
        agent_stream,
        [
            {
                "event_type": "AgentSessionStarted",
                "event_version": 1,
                "payload": {
                    "session_id": session_id,
                    "agent_type": "credit_analysis",
                    "agent_id": "agent-1",
                    "application_id": "APP-UP-1",
                    "model_version": "model-v2.3",
                    "langgraph_graph_version": "g1",
                    "context_source": "registry",
                    "context_token_count": 123,
                    "started_at": datetime.now(timezone.utc).isoformat(),
                },
            }
        ],
        expected_version=-1,
    )

    loan_stream = "test-upcast-decision-001"
    await store.append(
        loan_stream,
        [
            {
                "event_type": "DecisionGenerated",
                "event_version": 1,
                "payload": {
                    "application_id": "APP-UP-1",
                    "orchestrator_session_id": "S-ORCH-1",
                    "recommendation": "APPROVE",
                    "confidence": 0.9,
                    "executive_summary": "ok",
                    "contributing_sessions": [session_id],
                    "generated_at": datetime.now(timezone.utc).isoformat(),
                },
            }
        ],
        expected_version=-1,
    )

    loaded = await store.load_stream(loan_stream)
    payload = loaded[0]["payload"]
    assert loaded[0]["event_version"] == 2
    assert payload["model_versions"][session_id] == "model-v2.3"
