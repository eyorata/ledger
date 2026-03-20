"""
src/mcp/tools.py
Command-side MCP tools wrapper.
"""
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

__all__ = [
    "tool_submit_application",
    "tool_start_agent_session",
    "tool_record_credit_analysis",
    "tool_record_fraud_screening",
    "tool_record_compliance_check",
    "tool_generate_decision",
    "tool_record_human_review",
    "tool_run_integrity_check",
]
