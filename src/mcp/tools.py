"""
src/mcp/tools.py
MCP tool registrations (command side).
"""
from __future__ import annotations
from typing import Any, Awaitable, Callable

from ledger.mcp_server import (
    tool_submit_application,
    tool_start_agent_session,
    tool_record_credit_analysis,
    tool_record_fraud_screening,
    tool_record_compliance_check,
    tool_generate_decision,
    tool_record_human_review,
    tool_run_integrity_check,
)


GetStore = Callable[[], Awaitable[Any]]


def register_tools(mcp: Any, get_store: GetStore) -> None:
    @mcp.tool(
        name="submit_application",
        description=(
            "Submit a new loan application. Validates schema and rejects duplicate application_id."
        ),
    )
    async def submit_application(**payload):
        store = await get_store()
        return await tool_submit_application(payload, store)

    @mcp.tool(
        name="start_agent_session",
        description=(
            "Start an agent session and load context. "
            "Precondition: must be called before any agent decision tools."
        ),
    )
    async def start_agent_session(**payload):
        store = await get_store()
        return await tool_start_agent_session(payload, store)

    @mcp.tool(
        name="record_credit_analysis",
        description=(
            "Record CreditAnalysisCompleted. "
            "Precondition: active agent session created by start_agent_session. "
            "If CreditAnalysisRequested is missing, this tool will append it."
        ),
    )
    async def record_credit_analysis(**payload):
        store = await get_store()
        return await tool_record_credit_analysis(payload, store)

    @mcp.tool(
        name="record_fraud_screening",
        description=(
            "Record FraudScreeningCompleted. "
            "Precondition: active agent session created by start_agent_session. "
            "fraud_score must be 0.0–1.0."
        ),
    )
    async def record_fraud_screening(**payload):
        store = await get_store()
        return await tool_record_fraud_screening(payload, store)

    @mcp.tool(
        name="record_compliance_check",
        description=(
            "Record a compliance rule pass/fail. "
            "Precondition: active agent session created by start_agent_session. "
            "rule_id must exist in active regulation_set_version."
        ),
    )
    async def record_compliance_check(**payload):
        store = await get_store()
        return await tool_record_compliance_check(payload, store)

    @mcp.tool(
        name="generate_decision",
        description=(
            "Generate DecisionGenerated. "
            "Precondition: active orchestrator session created by start_agent_session, "
            "and all analyses (credit, fraud, compliance) present. "
            "If ComplianceCheckCompleted or DecisionRequested is missing, this tool will append them."
        ),
    )
    async def generate_decision(**payload):
        store = await get_store()
        return await tool_generate_decision(payload, store)

    @mcp.tool(
        name="record_human_review",
        description=(
            "Record HumanReviewCompleted and finalize application state. "
            "Precondition: reviewer_id required; if override=True, override_reason required."
        ),
    )
    async def record_human_review(**payload):
        store = await get_store()
        return await tool_record_human_review(payload, store)

    @mcp.tool(
        name="run_integrity_check",
        description=(
            "Run audit integrity check. "
            "Precondition: compliance role only, 1/min per entity."
        ),
    )
    async def run_integrity(**payload):
        store = await get_store()
        return await tool_run_integrity_check(payload, store)


__all__ = ["register_tools"]
