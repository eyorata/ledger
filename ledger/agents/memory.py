"""
ledger/agents/memory.py
=======================
Phase 4: Gas Town agent memory reconstruction.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime


@dataclass
class AgentContext:
    context_text: str
    last_event_position: int
    pending_work: list[str]
    session_health_status: str


async def _find_agent_stream(store, agent_id: str, session_id: str) -> str | None:
    # Postgres-backed store
    if hasattr(store, "_pool") and store._pool:
        async with store._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT stream_id FROM events "
                "WHERE event_type IN ('AgentContextLoaded','AgentSessionStarted') "
                "AND payload->>'agent_id'=$1 "
                "AND payload->>'session_id'=$2 "
                "ORDER BY recorded_at ASC LIMIT 1",
                agent_id,
                session_id,
            )
            if row:
                return row["stream_id"]

    # In-memory fallback
    if hasattr(store, "_global"):
        for e in store._global:
            if e.get("event_type") not in ("AgentContextLoaded", "AgentSessionStarted"):
                continue
            payload = e.get("payload", {})
            if payload.get("agent_id") == agent_id and payload.get("session_id") == session_id:
                return e.get("stream_id")
    return None


def _summarize_event_counts(events: list[dict]) -> str:
    counts: dict[str, int] = {}
    for e in events:
        et = e.get("event_type", "Unknown")
        counts[et] = counts.get(et, 0) + 1
    parts = [f"{k} x{v}" for k, v in sorted(counts.items())]
    return ", ".join(parts)


def _truncate(text: str, max_chars: int) -> str:
    if len(text) <= max_chars:
        return text
    return text[: max_chars - 3] + "..."


async def reconstruct_agent_context(
    store,
    agent_id: str,
    session_id: str,
    token_budget: int = 8000,
) -> AgentContext:
    """
    Rebuilds agent context from the event store after a crash.
    """
    stream_id = await _find_agent_stream(store, agent_id, session_id)
    if not stream_id:
        return AgentContext(
            context_text=f"No session stream found for agent_id={agent_id}, session_id={session_id}.",
            last_event_position=-1,
            pending_work=["Locate or restart session."],
            session_health_status="ERROR",
        )

    events = await store.load_stream(stream_id)
    if not events:
        return AgentContext(
            context_text=f"Session {stream_id} exists but contains no events.",
            last_event_position=-1,
            pending_work=["Initialize session context."],
            session_health_status="ERROR",
        )

    last_event = events[-1]
    last_pos = int(last_event.get("stream_position", -1))
    last_type = last_event.get("event_type")

    completed = any(e.get("event_type") == "AgentSessionCompleted" for e in events)
    error_events = {
        "AgentInputValidationFailed",
        "AgentSessionFailed",
    }
    preserved_indices = set(range(max(len(events) - 3, 0), len(events)))
    for i, e in enumerate(events):
        if e.get("event_type") in error_events:
            preserved_indices.add(i)

    preserved = [events[i] for i in sorted(preserved_indices)]
    older = [e for i, e in enumerate(events) if i not in preserved_indices]

    summary_lines = [
        f"Session stream: {stream_id}",
        f"Total events: {len(events)}",
    ]
    if older:
        summary_lines.append("Earlier events summary: " + _summarize_event_counts(older))

    preserved_lines = ["Preserved recent/error events (verbatim):"]
    for e in preserved:
        payload = e.get("payload", {})
        preserved_lines.append(f"- {e.get('event_type')} @pos {e.get('stream_position')}: {json.dumps(payload, sort_keys=True)}")

    context_text = "\n".join(summary_lines + [""] + preserved_lines)
    context_text = _truncate(context_text, max_chars=token_budget * 4)

    pending_work: list[str] = []
    session_health_status = "HEALTHY"

    if last_type in error_events:
        session_health_status = "ERROR"
        error_type = last_event.get("payload", {}).get("error_type", "unknown_error")
        pending_work.append(f"Recover from error: {error_type}")

    needs_recon = (not completed) and last_type in ("AgentOutputWritten", "AgentNodeExecuted")
    if needs_recon:
        session_health_status = "NEEDS_RECONCILIATION"
        pending_work.append("Reconcile partial output before continuing.")

    if not completed and last_type not in error_events:
        pending_work.append(f"Resume session after {last_type}.")

    return AgentContext(
        context_text=context_text,
        last_event_position=last_pos,
        pending_work=pending_work,
        session_health_status=session_health_status,
    )
