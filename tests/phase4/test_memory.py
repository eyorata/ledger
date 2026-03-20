from datetime import datetime, timezone

import pytest

from ledger.agents.memory import reconstruct_agent_context
from ledger.event_store import InMemoryEventStore


@pytest.mark.asyncio
async def test_reconstruct_agent_context_needs_reconciliation():
    store = InMemoryEventStore()
    stream_id = "agent-document_processing-S-CRASH-1"

    events = [
        {
            "event_type": "AgentSessionStarted",
            "event_version": 1,
            "payload": {
                "session_id": "S-CRASH-1",
                "agent_type": "document_processing",
                "agent_id": "agent-1",
                "application_id": "APP-CRASH-1",
                "model_version": "model-v1",
                "langgraph_graph_version": "g1",
                "context_source": "registry",
                "context_token_count": 123,
                "started_at": datetime.now(timezone.utc).isoformat(),
            },
        },
        {
            "event_type": "AgentInputValidated",
            "event_version": 1,
            "payload": {
                "session_id": "S-CRASH-1",
                "agent_type": "document_processing",
                "application_id": "APP-CRASH-1",
                "inputs_validated": ["income_statement", "balance_sheet"],
                "validation_duration_ms": 10,
                "validated_at": datetime.now(timezone.utc).isoformat(),
            },
        },
        {
            "event_type": "AgentNodeExecuted",
            "event_version": 1,
            "payload": {
                "session_id": "S-CRASH-1",
                "agent_type": "document_processing",
                "node_name": "extract_income",
                "node_sequence": 1,
                "input_keys": ["income_statement"],
                "output_keys": ["facts"],
                "llm_called": False,
                "duration_ms": 45,
                "executed_at": datetime.now(timezone.utc).isoformat(),
            },
        },
        {
            "event_type": "AgentOutputWritten",
            "event_version": 1,
            "payload": {
                "session_id": "S-CRASH-1",
                "agent_type": "document_processing",
                "application_id": "APP-CRASH-1",
                "events_written": [{"event_type": "ExtractionCompleted"}],
                "output_summary": "Partial output written",
                "written_at": datetime.now(timezone.utc).isoformat(),
            },
        },
        {
            "event_type": "AgentNodeExecuted",
            "event_version": 1,
            "payload": {
                "session_id": "S-CRASH-1",
                "agent_type": "document_processing",
                "node_name": "extract_balance",
                "node_sequence": 2,
                "input_keys": ["balance_sheet"],
                "output_keys": ["facts"],
                "llm_called": False,
                "duration_ms": 40,
                "executed_at": datetime.now(timezone.utc).isoformat(),
            },
        },
    ]

    await store.append(stream_id, events, expected_version=-1)

    ctx = await reconstruct_agent_context(store, agent_id="agent-1", session_id="S-CRASH-1")
    assert ctx.last_event_position == 4
    assert "AgentOutputWritten" in ctx.context_text
    assert ctx.pending_work
    assert ctx.session_health_status == "NEEDS_RECONCILIATION"
