"""
src/mcp/resources.py
MCP resource registrations (query side).
"""
from __future__ import annotations
from typing import Any, Awaitable, Callable

from ledger.mcp_server import (
    resource_application_summary,
    resource_compliance,
    resource_audit_trail,
    resource_agent_performance,
    resource_agent_session,
    resource_health,
)


GetStore = Callable[[], Awaitable[Any]]


def register_resources(mcp: Any, get_store: GetStore) -> None:
    @mcp.resource("ledger://applications/{application_id}")
    async def application_summary(application_id: str):
        store = await get_store()
        return await resource_application_summary(application_id, store)

    @mcp.resource("ledger://applications/{application_id}/compliance")
    async def compliance_view(application_id: str, as_of: str | None = None):
        store = await get_store()
        return await resource_compliance(application_id, as_of, store)

    @mcp.resource("ledger://applications/{application_id}/audit-trail")
    async def audit_trail(application_id: str, from_ts: str | None = None, to_ts: str | None = None):
        store = await get_store()
        return await resource_audit_trail(application_id, from_ts, to_ts, store)

    @mcp.resource("ledger://agents/{agent_id}/performance")
    async def agent_performance(agent_id: str):
        store = await get_store()
        return await resource_agent_performance(agent_id, store)

    @mcp.resource("ledger://agents/{agent_id}/sessions/{session_id}")
    async def agent_sessions(agent_id: str, session_id: str):
        store = await get_store()
        return await resource_agent_session(agent_id, session_id, store)

    @mcp.resource("ledger://ledger/health")
    async def ledger_health():
        store = await get_store()
        return await resource_health(store)


__all__ = ["register_resources"]
