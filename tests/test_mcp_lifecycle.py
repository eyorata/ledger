"""
tests/test_mcp_lifecycle.py
===========================
Phase 5: MCP lifecycle test via tool calls.
"""
import os
import pytest
from datetime import datetime
from uuid import uuid4

from ledger.event_store import EventStore
from ledger.mcp_server import (
    tool_submit_application,
    tool_start_agent_session,
    tool_record_credit_analysis,
    tool_record_fraud_screening,
    tool_record_compliance_check,
    tool_generate_decision,
    tool_record_human_review,
    resource_compliance,
)
from ledger.projections.compliance_audit import ComplianceAuditViewProjection


def _db_url() -> str:
    return os.getenv("DATABASE_URL", "postgresql://localhost/apex_ledger")


@pytest.mark.asyncio
async def test_mcp_full_lifecycle():
    store = EventStore(_db_url())
    await store.connect()

    app_id = f"APEX-MCP-{uuid4().hex[:6]}"
    applicant_id = "COMP-001"

    # 1) Submit application
    result = await tool_submit_application(
        {
            "application_id": app_id,
            "applicant_id": applicant_id,
            "requested_amount_usd": 500000,
            "loan_purpose": "working_capital",
            "loan_term_months": 36,
            "submission_channel": "api",
            "contact_email": "test@example.com",
            "contact_name": "Test User",
            "application_reference": app_id,
        },
        store,
    )
    assert "stream_id" in result

    # 2) Start agent session (credit)
    credit_session = await tool_start_agent_session(
        {
            "agent_type": "credit_analysis",
            "agent_id": "credit-1",
            "application_id": app_id,
            "model_version": "test-model",
            "langgraph_graph_version": "1.0.0",
            "context_source": "mcp-test",
            "context_token_count": 100,
        },
        store,
    )

    # 3) Record credit analysis
    credit_resp = await tool_record_credit_analysis(
        {
            "application_id": app_id,
            "agent_type": "credit_analysis",
            "agent_id": "credit-1",
            "session_id": credit_session["session_id"],
            "model_version": "test-model",
            "model_deployment_id": "dep-test",
            "input_data_hash": "hash",
            "analysis_duration_ms": 1000,
            "decision": {
                "risk_tier": "MEDIUM",
                "recommended_limit_usd": 250000,
                "confidence": 0.72,
                "rationale": "Stubbed decision.",
                "key_concerns": [],
                "data_quality_caveats": [],
                "policy_overrides_applied": [],
            },
        },
        store,
    )
    assert "event_id" in credit_resp

    # 4) Start agent session (fraud)
    fraud_session = await tool_start_agent_session(
        {
            "agent_type": "fraud_detection",
            "agent_id": "fraud-1",
            "application_id": app_id,
            "model_version": "test-model",
            "langgraph_graph_version": "1.0.0",
            "context_source": "mcp-test",
            "context_token_count": 100,
        },
        store,
    )

    # 5) Record fraud screening
    fraud_resp = await tool_record_fraud_screening(
        {
            "application_id": app_id,
            "agent_type": "fraud_detection",
            "agent_id": "fraud-1",
            "session_id": fraud_session["session_id"],
            "fraud_score": 0.2,
            "risk_level": "LOW",
            "anomalies_found": 0,
            "recommendation": "PROCEED",
            "screening_model_version": "test-model",
            "input_data_hash": "hash",
        },
        store,
    )
    assert "event_id" in fraud_resp

    # 6) Start agent session (compliance)
    comp_session = await tool_start_agent_session(
        {
            "agent_type": "compliance",
            "agent_id": "compliance-1",
            "application_id": app_id,
            "model_version": "test-model",
            "langgraph_graph_version": "1.0.0",
            "context_source": "mcp-test",
            "context_token_count": 100,
        },
        store,
    )

    # 7) Record compliance check (rule pass)
    comp_resp = await tool_record_compliance_check(
        {
            "application_id": app_id,
            "agent_type": "compliance",
            "agent_id": "compliance-1",
            "session_id": comp_session["session_id"],
            "regulation_set_version": "2026-Q1-v1",
            "rule_id": "REG-001",
            "passed": True,
            "evaluation_notes": "ok",
        },
        store,
    )
    assert "new_stream_version" in comp_resp

    # 8) Start agent session (orchestrator)
    orch_session = await tool_start_agent_session(
        {
            "agent_type": "decision_orchestrator",
            "agent_id": "orch-1",
            "application_id": app_id,
            "model_version": "test-model",
            "langgraph_graph_version": "1.0.0",
            "context_source": "mcp-test",
            "context_token_count": 100,
        },
        store,
    )

    # 9) Generate decision
    decision_resp = await tool_generate_decision(
        {
            "application_id": app_id,
            "orchestrator_session_id": orch_session["session_id"],
            "agent_type": "decision_orchestrator",
            "agent_id": "orch-1",
            "recommendation": "DECLINE",
            "confidence": 0.82,
            "approved_amount_usd": None,
            "conditions": [],
            "executive_summary": "Decline due to policy.",
            "key_risks": ["Risk"],
            "contributing_sessions": [credit_session["session_id"]],
            "model_versions": {},
        },
        store,
    )
    assert "decision_id" in decision_resp

    # 10) Record human review override (approve)
    hr_resp = await tool_record_human_review(
        {
            "application_id": app_id,
            "reviewer_id": "LO-Sarah-Chen",
            "override": True,
            "original_recommendation": "DECLINE",
            "final_decision": "APPROVE",
            "override_reason": "15-year customer, prior repayment history, collateral offered",
            "approved_amount_usd": 750000,
            "interest_rate_pct": 7.5,
            "term_months": 36,
            "conditions": ["Monthly revenue reporting for 12 months", "Personal guarantee from CEO"],
        },
        store,
    )
    assert hr_resp.get("final_decision") == "APPROVE"

    # 11) Rebuild compliance projection then query resource
    projection = ComplianceAuditViewProjection()
    await projection.rebuild_from_scratch(store)

    # 12) Resource query: compliance
    compliance_view = await resource_compliance(app_id, None, store)
    assert compliance_view is not None

    await store.close()
