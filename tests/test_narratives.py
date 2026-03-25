"""
tests/test_narratives.py
========================
Narrative scenario tests aligned to Section 7 of the challenge document.
These tests validate required event sequences and invariants.
"""
import asyncio
import pytest, sys
from pathlib import Path
from datetime import datetime, timezone

sys.path.insert(0, str(Path(__file__).parent.parent))

from ledger.event_store import InMemoryEventStore, OptimisticConcurrencyError


@pytest.mark.asyncio
async def test_narr01_concurrent_occ_collision():
    app_id = "NARR-01"
    store = InMemoryEventStore()
    stream_id = f"credit-{app_id}"

    await store.append(
        stream_id,
        [{"event_type": "CreditRecordOpened", "event_version": 1, "payload": {"application_id": app_id}}],
        expected_version=-1,
    )

    async def agent_a():
        await store.append(
            stream_id,
            [{"event_type": "CreditAnalysisCompleted", "event_version": 1, "payload": {"application_id": app_id}}],
            expected_version=0,
        )

    async def agent_b():
        try:
            await store.append(
                stream_id,
                [{"event_type": "CreditAnalysisCompleted", "event_version": 1, "payload": {"application_id": app_id}}],
                expected_version=0,
            )
        except OptimisticConcurrencyError:
            events = await store.load_stream(stream_id)
            first = next(e for e in events if e.get("event_type") == "CreditAnalysisCompleted")
            await store.append(
                stream_id,
                [{"event_type": "CreditAnalysisCompleted", "event_version": 1, "payload": {"application_id": app_id}}],
                expected_version=1,
                causation_id=str(first.get("event_id")),
            )

    await asyncio.gather(agent_a(), agent_b())

    events = await store.load_stream(stream_id)
    ca = [e for e in events if e.get("event_type") == "CreditAnalysisCompleted"]
    assert len(ca) == 2
    assert ca[0]["stream_position"] == 1
    assert ca[1]["stream_position"] == 2
    assert ca[1]["metadata"].get("causation_id") == ca[0]["event_id"]


@pytest.mark.asyncio
async def test_narr02_document_extraction_failure():
    app_id = "NARR-02"
    store = InMemoryEventStore()

    await store.append(
        f"docpkg-{app_id}",
        [
            {
                "event_type": "ExtractionCompleted",
                "event_version": 1,
                "payload": {
                    "application_id": app_id,
                    "document_id": "doc-1",
                    "document_type": "income_statement",
                    "facts": {"ebitda": None, "total_revenue": 1000, "net_income": 100},
                    "field_confidence": {"ebitda": 0.0},
                    "extraction_notes": ["ebitda missing"],
                    "completed_at": datetime.now(timezone.utc).isoformat(),
                },
            },
            {
                "event_type": "QualityAssessmentCompleted",
                "event_version": 1,
                "payload": {
                    "application_id": app_id,
                    "document_id": "doc-1",
                    "overall_confidence": 0.6,
                    "is_coherent": True,
                    "critical_missing_fields": ["ebitda"],
                    "reextraction_recommended": False,
                    "auditor_notes": "missing ebitda",
                    "assessed_at": datetime.now(timezone.utc).isoformat(),
                },
            },
        ],
        expected_version=-1,
    )

    await store.append(
        f"loan-{app_id}",
        [
            {
                "event_type": "CreditAnalysisCompleted",
                "event_version": 1,
                "payload": {
                    "application_id": app_id,
                    "decision": {
                        "confidence": 0.75,
                        "data_quality_caveats": ["ebitda missing"],
                    },
                },
            }
        ],
        expected_version=-1,
    )

    doc_events = await store.load_stream(f"docpkg-{app_id}")
    extraction = next(e for e in doc_events if e.get("event_type") == "ExtractionCompleted")
    assert extraction["payload"]["facts"]["ebitda"] is None
    assert extraction["payload"]["field_confidence"]["ebitda"] == 0.0
    assert "ebitda" in " ".join(extraction["payload"]["extraction_notes"]).lower()

    quality = next(e for e in doc_events if e.get("event_type") == "QualityAssessmentCompleted")
    assert "ebitda" in [f.lower() for f in quality["payload"]["critical_missing_fields"]]

    loan_events = await store.load_stream(f"loan-{app_id}")
    ca = next(e for e in loan_events if e.get("event_type") == "CreditAnalysisCompleted")
    assert ca["payload"]["decision"]["confidence"] <= 0.75
    assert ca["payload"]["decision"]["data_quality_caveats"]


@pytest.mark.asyncio
async def test_narr03_agent_crash_recovery():
    app_id = "NARR-03"
    store = InMemoryEventStore()
    crashed_session = "S-CRASH-1"
    recovery_session = "S-RECOVER-1"

    await store.append(
        f"agent-fraud_detection-{crashed_session}",
        [
            {
                "event_type": "AgentContextLoaded",
                "event_version": 1,
                "payload": {
                    "session_id": crashed_session,
                    "agent_type": "fraud_detection",
                    "agent_id": "agent-1",
                    "application_id": app_id,
                    "model_version": "model-v1",
                    "langgraph_graph_version": "g1",
                    "context_source": "fresh",
                    "context_token_count": 100,
                    "loaded_at": datetime.now(timezone.utc).isoformat(),
                },
            },
            {
                "event_type": "AgentNodeExecuted",
                "event_version": 1,
                "payload": {
                    "session_id": crashed_session,
                    "agent_type": "fraud_detection",
                    "node_name": "load_facts",
                    "node_sequence": 1,
                    "input_keys": ["application_id"],
                    "output_keys": ["extracted_facts"],
                    "llm_called": False,
                    "duration_ms": 10,
                    "executed_at": datetime.now(timezone.utc).isoformat(),
                },
            },
            {
                "event_type": "AgentSessionFailed",
                "event_version": 1,
                "payload": {
                    "session_id": crashed_session,
                    "agent_type": "fraud_detection",
                    "application_id": app_id,
                    "error_type": "SimulatedCrash",
                    "error_message": "boom",
                    "last_successful_node": "load_facts",
                    "recoverable": True,
                    "failed_at": datetime.now(timezone.utc).isoformat(),
                },
            },
        ],
        expected_version=-1,
    )

    await store.append(
        f"agent-fraud_detection-{recovery_session}",
        [
            {
                "event_type": "AgentContextLoaded",
                "event_version": 1,
                "payload": {
                    "session_id": recovery_session,
                    "agent_type": "fraud_detection",
                    "agent_id": "agent-1",
                    "application_id": app_id,
                    "model_version": "model-v1",
                    "langgraph_graph_version": "g1",
                    "context_source": f"prior_session_replay:{crashed_session}",
                    "context_token_count": 100,
                    "loaded_at": datetime.now(timezone.utc).isoformat(),
                },
            },
            {
                "event_type": "AgentSessionRecovered",
                "event_version": 1,
                "payload": {
                    "session_id": recovery_session,
                    "agent_type": "fraud_detection",
                    "application_id": app_id,
                    "recovered_from_session_id": crashed_session,
                    "recovery_point": "load_facts",
                    "recovered_at": datetime.now(timezone.utc).isoformat(),
                },
            },
            {
                "event_type": "AgentNodeExecuted",
                "event_version": 1,
                "payload": {
                    "session_id": recovery_session,
                    "agent_type": "fraud_detection",
                    "node_name": "cross_reference_registry",
                    "node_sequence": 2,
                    "input_keys": ["applicant_id"],
                    "output_keys": ["historical_financials"],
                    "llm_called": False,
                    "duration_ms": 12,
                    "executed_at": datetime.now(timezone.utc).isoformat(),
                },
            },
        ],
        expected_version=-1,
    )

    await store.append(
        f"fraud-{app_id}",
        [
            {
                "event_type": "FraudScreeningCompleted",
                "event_version": 1,
                "payload": {"application_id": app_id, "fraud_score": 0.2},
            }
        ],
        expected_version=-1,
    )

    fraud_events = await store.load_stream(f"fraud-{app_id}")
    assert len([e for e in fraud_events if e.get("event_type") == "FraudScreeningCompleted"]) == 1

    rec_events = await store.load_stream(f"agent-fraud_detection-{recovery_session}")
    ctx = rec_events[0]["payload"]["context_source"]
    assert ctx.startswith("prior_session_replay:")
    assert any(e.get("event_type") == "AgentSessionRecovered" for e in rec_events)

    all_agent_events = (
        await store.load_stream(f"agent-fraud_detection-{crashed_session}")
        + await store.load_stream(f"agent-fraud_detection-{recovery_session}")
    )
    load_facts_nodes = [
        e for e in all_agent_events
        if e.get("event_type") == "AgentNodeExecuted"
        and (e.get("payload") or {}).get("node_name") == "load_facts"
    ]
    assert len(load_facts_nodes) == 1


@pytest.mark.asyncio
async def test_narr04_compliance_hard_block():
    app_id = "NARR-04"
    store = InMemoryEventStore()

    await store.append(
        f"compliance-{app_id}",
        [
            {
                "event_type": "ComplianceRulePassed",
                "event_version": 1,
                "payload": {"application_id": app_id, "rule_id": "REG-001"},
            },
            {
                "event_type": "ComplianceRulePassed",
                "event_version": 1,
                "payload": {"application_id": app_id, "rule_id": "REG-002"},
            },
            {
                "event_type": "ComplianceRuleFailed",
                "event_version": 1,
                "payload": {"application_id": app_id, "rule_id": "REG-003", "is_hard_block": True},
            },
            {
                "event_type": "ComplianceCheckCompleted",
                "event_version": 1,
                "payload": {"application_id": app_id, "overall_verdict": "BLOCKED"},
            },
        ],
        expected_version=-1,
    )

    await store.append(
        f"loan-{app_id}",
        [
            {
                "event_type": "ApplicationDeclined",
                "event_version": 1,
                "payload": {
                    "application_id": app_id,
                    "decline_reasons": ["REG-003 jurisdiction hard block"],
                    "adverse_action_notice_required": True,
                },
            }
        ],
        expected_version=-1,
    )

    compliance_events = await store.load_stream(f"compliance-{app_id}")
    rule_events = [e for e in compliance_events if e.get("event_type") in {"ComplianceRulePassed", "ComplianceRuleFailed"}]
    assert len(rule_events) == 3
    assert any(e.get("payload", {}).get("rule_id") == "REG-003" for e in rule_events)

    loan_events = await store.load_stream(f"loan-{app_id}")
    assert not any(e.get("event_type") == "DecisionGenerated" for e in loan_events)
    declined = next(e for e in loan_events if e.get("event_type") == "ApplicationDeclined")
    assert any("REG-003" in r for r in declined["payload"].get("decline_reasons", []))
    assert declined["payload"].get("adverse_action_notice_required") is True


@pytest.mark.asyncio
async def test_narr05_human_override():
    app_id = "NARR-05"
    store = InMemoryEventStore()

    await store.append(
        f"loan-{app_id}",
        [
            {
                "event_type": "DecisionGenerated",
                "event_version": 1,
                "payload": {"application_id": app_id, "recommendation": "DECLINE", "confidence": 0.82},
            },
            {
                "event_type": "HumanReviewRequested",
                "event_version": 1,
                "payload": {"application_id": app_id, "reason": "auto_refer"},
            },
            {
                "event_type": "HumanReviewCompleted",
                "event_version": 1,
                "payload": {
                    "application_id": app_id,
                    "override": True,
                    "reviewer_id": "LO-Sarah-Chen",
                    "final_decision": "APPROVE",
                    "override_reason": "15-year customer, prior repayment history, collateral offered",
                },
            },
            {
                "event_type": "ApplicationApproved",
                "event_version": 1,
                "payload": {
                    "application_id": app_id,
                    "approved_amount_usd": 750000,
                    "conditions": ["Monthly revenue reporting for 12 months", "Personal guarantee from CEO"],
                },
            },
        ],
        expected_version=-1,
    )

    events = await store.load_stream(f"loan-{app_id}")
    decision = next(e for e in events if e.get("event_type") == "DecisionGenerated")
    assert decision["payload"]["recommendation"] == "DECLINE"

    review = next(e for e in events if e.get("event_type") == "HumanReviewCompleted")
    assert review["payload"]["override"] is True
    assert review["payload"]["reviewer_id"] == "LO-Sarah-Chen"

    approved = next(e for e in events if e.get("event_type") == "ApplicationApproved")
    assert approved["payload"]["approved_amount_usd"] == 750000
    assert len(approved["payload"]["conditions"]) == 2
