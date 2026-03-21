"""
ledger/domain/aggregates/agent_session.py
=========================================
AgentSession aggregate with Gas Town context enforcement and model version checks.
"""
from __future__ import annotations
from dataclasses import dataclass

from ledger.domain.errors import DomainError


DECISION_EVENT_TYPES = {
    "CreditAnalysisCompleted",
    "FraudScreeningCompleted",
    "ComplianceCheckCompleted",
    "DecisionGenerated",
}


@dataclass
class AgentSessionAggregate:
    stream_id: str
    agent_type: str
    session_id: str
    model_version: str | None = None
    context_loaded: bool = False
    first_event_type: str | None = None

    @classmethod
    async def load(cls, store, agent_type: str, session_id: str) -> "AgentSessionAggregate":
        stream_id = f"agent-{agent_type}-{session_id}"
        events = await store.load_stream(stream_id)
        agg = cls(stream_id=stream_id, agent_type=agent_type, session_id=session_id)
        for event in events:
            agg._apply(event)
        return agg

    def _apply(self, event: dict) -> None:
        et = event.get("event_type")
        if self.first_event_type is None:
            self.first_event_type = et
        handler = getattr(self, f"_on_{et}", None)
        if handler:
            handler(event)

    def _on_AgentSessionStarted(self, event: dict) -> None:
        payload = event.get("payload", {})
        self.model_version = payload.get("model_version") or self.model_version
        self.context_loaded = True

    def _on_AgentContextLoaded(self, event: dict) -> None:
        payload = event.get("payload", {})
        self.model_version = payload.get("model_version") or self.model_version
        self.context_loaded = True

    def assert_context_loaded(self) -> None:
        if not self.context_loaded or self.first_event_type not in ("AgentSessionStarted", "AgentContextLoaded"):
            raise DomainError("Agent context must be loaded as the first event")

    def assert_model_version_current(self, model_version: str) -> None:
        if self.model_version and self.model_version != model_version:
            raise DomainError(f"Model version mismatch: {self.model_version} != {model_version}")

    async def has_decision_event_for_application(self, store, application_id: str) -> bool:
        events = await store.load_stream(self.stream_id)
        for event in events:
            if event.get("event_type") != "AgentOutputWritten":
                continue
            payload = event.get("payload", {})
            if payload.get("application_id") != application_id:
                continue
            for e in payload.get("events_written", []) or []:
                if e.get("event_type") in DECISION_EVENT_TYPES:
                    return True
        return False
