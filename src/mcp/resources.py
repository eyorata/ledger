"""
src/mcp/resources.py
Query-side MCP resources wrapper.
"""
from ledger.mcp_server import (
    resource_application_summary,
    resource_compliance,
    resource_audit_trail,
    resource_agent_performance,
    resource_agent_session,
    resource_health,
)

__all__ = [
    "resource_application_summary",
    "resource_compliance",
    "resource_audit_trail",
    "resource_agent_performance",
    "resource_agent_session",
    "resource_health",
]
