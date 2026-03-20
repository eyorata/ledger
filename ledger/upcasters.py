"""
ledger/upcasters.py — UpcasterRegistry
======================================
Upcasters transform old event versions to the current version ON READ.
They NEVER write to the events table. Immutability is non-negotiable.
"""
from __future__ import annotations
from datetime import datetime
from typing import Callable, Awaitable


class UpcasterRegistry:
    def __init__(self, store=None):
        self._upcasters: dict[tuple[str, int], Callable] = {}
        self._store = store

    def register(self, event_type: str, from_version: int):
        """Decorator. Registers fn as upcaster from event_type@from_version."""
        def decorator(fn: Callable[[dict, dict], dict] | Callable[[dict, dict], Awaitable[dict]]):
            self._upcasters[(event_type, from_version)] = fn
            return fn
        return decorator

    async def upcast(self, event: dict) -> dict:
        """Apply all registered upcasters for this event type in version order."""
        current = dict(event)
        v = current.get("event_version", 1)
        et = current.get("event_type")
        while (et, v) in self._upcasters:
            fn = self._upcasters[(et, v)]
            payload = dict(current.get("payload", {}))
            maybe = fn(payload, current)
            if hasattr(maybe, "__await__"):
                new_payload = await maybe
            else:
                new_payload = maybe
            current["payload"] = new_payload
            v += 1
            current["event_version"] = v
        return current

    async def _resolve_model_version(self, session_id: str) -> str:
        if not self._store:
            return "unknown"
        # Postgres-backed store
        if hasattr(self._store, "_pool") and self._store._pool:
            async with self._store._pool.acquire() as conn:
                row = await conn.fetchrow(
                    "SELECT payload->>'model_version' AS model_version "
                    "FROM events WHERE event_type='AgentSessionStarted' "
                    "AND payload->>'session_id'=$1 LIMIT 1",
                    session_id,
                )
                if row and row["model_version"]:
                    return row["model_version"]
        # In-memory store fallback
        if hasattr(self._store, "_global"):
            for e in self._store._global:
                if e.get("event_type") == "AgentSessionStarted":
                    p = e.get("payload", {})
                    if p.get("session_id") == session_id:
                        return p.get("model_version", "unknown")
        return "unknown"


def build_default_registry(store=None) -> UpcasterRegistry:
    reg = UpcasterRegistry(store=store)

    @reg.register("CreditAnalysisCompleted", from_version=1)
    def upcast_credit_v1_to_v2(payload: dict, event: dict) -> dict:
        recorded_at = event.get("recorded_at")
        ts = None
        if isinstance(recorded_at, str):
            try:
                ts = datetime.fromisoformat(recorded_at)
            except Exception:
                ts = None
        if ts and ts.year >= 2026:
            model_version = "legacy-2026"
            regulatory_basis = ["2026-Q1"]
        else:
            model_version = "legacy-pre-2026"
            regulatory_basis = ["2025-Q4"]
        return {
            **payload,
            "model_version": model_version,
            "confidence_score": None,
            "regulatory_basis": regulatory_basis,
        }

    @reg.register("DecisionGenerated", from_version=1)
    async def upcast_decision_v1_to_v2(payload: dict, event: dict) -> dict:
        sessions = payload.get("contributing_sessions") or []
        model_versions = {}
        for sid in sessions:
            model_versions[sid] = await reg._resolve_model_version(str(sid))
        return {
            **payload,
            "model_versions": model_versions,
        }

    return reg
