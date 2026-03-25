"""
ledger/mcp_server.py
====================
Phase 5 MCP server: CQRS interface for tools (commands) and resources (queries).
"""
from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from dotenv import load_dotenv
from pydantic import BaseModel, Field, ValidationError

from ledger.audit.integrity import run_integrity_check
from ledger.domain.aggregates.agent_session import AgentSessionAggregate
from ledger.domain.aggregates.loan_application import LoanApplicationAggregate
from ledger.domain.commands import (
    CreditAnalysisCompletedCommand,
    DecisionGeneratedCommand,
    ApplicationApprovedCommand,
)
from ledger.domain.errors import DomainError
from ledger.domain.handlers import (
    handle_credit_analysis_completed,
    handle_decision_generated,
    handle_application_approved,
)
from ledger.event_store import EventStore, OptimisticConcurrencyError
from ledger.schema.events import (
    ApplicationSubmitted,
    AgentSessionStarted,
    CreditDecision,
    CreditAnalysisRequested,
    FraudScreeningCompleted,
    ComplianceRulePassed,
    ComplianceRuleFailed,
    ComplianceRuleNoted,
    ComplianceCheckCompleted,
    ComplianceCheckRequested,
    DecisionRequested,
    AgentOutputWritten,
    HumanReviewCompleted,
    ApplicationApproved,
    ApplicationDeclined,
)
from ledger.agents.stub_agents import REGULATIONS

try:
    from fastmcp import FastMCP
except Exception:  # pragma: no cover - optional dependency
    FastMCP = None

load_dotenv()
DB_URL = os.getenv("DATABASE_URL", "postgresql://localhost/apex_ledger")

_store: EventStore | None = None
_store_lock = asyncio.Lock()
_last_integrity_check: dict[str, datetime] = {}


class PreconditionFailed(Exception):
    pass


def _error(error_type: str, message: str, **kwargs: Any) -> dict:
    return {"error_type": error_type, "message": message, **kwargs}


async def _get_store() -> EventStore:
    global _store
    if _store:
        return _store
    async with _store_lock:
        if _store:
            return _store
        _store = EventStore(DB_URL)
        await _store.connect()
        return _store


async def _ensure_agent_session(store: EventStore, agent_type: str, session_id: str, agent_id: str) -> None:
    stream_id = f"agent-{agent_type}-{session_id}"
    events = await store.load_stream(stream_id)
    if not events:
        raise PreconditionFailed("No active agent session. Call start_agent_session first.")
    first_payload = events[0].get("payload", {})
    if first_payload.get("agent_id") and first_payload.get("agent_id") != agent_id:
        raise PreconditionFailed("agent_id does not match session owner.")
    agent = await AgentSessionAggregate.load(store, agent_type, session_id)
    agent.assert_context_loaded()


def _parse_iso(ts: str | None) -> datetime | None:
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts)
    except Exception:
        return None


# ----------------------------
# Tool input models
# ----------------------------

class SubmitApplicationInput(BaseModel):
    application_id: str
    applicant_id: str
    requested_amount_usd: float
    loan_purpose: str
    loan_term_months: int
    submission_channel: str
    contact_email: str
    contact_name: str
    application_reference: str


class StartAgentSessionInput(BaseModel):
    agent_type: str
    agent_id: str
    application_id: str
    model_version: str
    langgraph_graph_version: str
    context_source: str
    context_token_count: int
    session_id: str | None = None


class CreditAnalysisInput(BaseModel):
    application_id: str
    agent_type: str
    agent_id: str
    session_id: str
    model_version: str
    model_deployment_id: str
    input_data_hash: str
    analysis_duration_ms: int
    decision: dict
    correlation_id: str | None = None
    causation_id: str | None = None


class FraudScreeningInput(BaseModel):
    application_id: str
    agent_type: str
    agent_id: str
    session_id: str
    fraud_score: float = Field(ge=0.0, le=1.0)
    risk_level: str
    anomalies_found: int
    recommendation: str
    screening_model_version: str
    input_data_hash: str
    correlation_id: str | None = None
    causation_id: str | None = None


class ComplianceCheckInput(BaseModel):
    application_id: str
    agent_type: str
    agent_id: str
    session_id: str
    regulation_set_version: str
    rule_id: str
    passed: bool
    evaluation_notes: str | None = None


class DecisionGeneratedInput(BaseModel):
    application_id: str
    orchestrator_session_id: str
    agent_type: str
    agent_id: str
    recommendation: str
    confidence: float
    approved_amount_usd: float | None = None
    conditions: list[str] = Field(default_factory=list)
    executive_summary: str
    key_risks: list[str] = Field(default_factory=list)
    contributing_sessions: list[str] = Field(default_factory=list)
    model_versions: dict[str, str] = Field(default_factory=dict)
    correlation_id: str | None = None
    causation_id: str | None = None


class HumanReviewInput(BaseModel):
    application_id: str
    reviewer_id: str
    override: bool
    original_recommendation: str
    final_decision: str
    override_reason: str | None = None
    decision_event_id: str | None = None
    approved_amount_usd: float | None = None
    interest_rate_pct: float | None = None
    term_months: int | None = None
    conditions: list[str] = Field(default_factory=list)
    decline_reasons: list[str] = Field(default_factory=list)


class IntegrityCheckInput(BaseModel):
    entity_type: str
    entity_id: str
    role: str


# ----------------------------
# Tool handlers (callable from tests)
# ----------------------------

async def tool_submit_application(payload: dict, store: EventStore) -> dict:
    try:
        data = SubmitApplicationInput(**payload)
    except ValidationError as exc:
        return _error("ValidationError", "Invalid application payload.", details=exc.errors())

    stream_id = f"loan-{data.application_id}"
    if await store.stream_version(stream_id) >= 0:
        return _error("DuplicateApplication", "application_id already exists.", stream_id=stream_id)

    ev = ApplicationSubmitted(
        application_id=data.application_id,
        applicant_id=data.applicant_id,
        requested_amount_usd=data.requested_amount_usd,
        loan_purpose=data.loan_purpose,
        loan_term_months=data.loan_term_months,
        submission_channel=data.submission_channel,
        contact_email=data.contact_email,
        contact_name=data.contact_name,
        submitted_at=datetime.now(timezone.utc),
        application_reference=data.application_reference,
    )
    new_version = await store.append(stream_id, [ev], expected_version=-1)
    return {"stream_id": stream_id, "initial_version": new_version}


async def tool_start_agent_session(payload: dict, store: EventStore) -> dict:
    try:
        data = StartAgentSessionInput(**payload)
    except ValidationError as exc:
        return _error("ValidationError", "Invalid session payload.", details=exc.errors())

    session_id = data.session_id or str(uuid4())
    stream_id = f"agent-{data.agent_type}-{session_id}"
    if await store.stream_version(stream_id) >= 0:
        return _error("DuplicateSession", "session_id already exists.", stream_id=stream_id)

    ev = AgentSessionStarted(
        session_id=session_id,
        agent_type=data.agent_type,
        agent_id=data.agent_id,
        application_id=data.application_id,
        model_version=data.model_version,
        langgraph_graph_version=data.langgraph_graph_version,
        context_source=data.context_source,
        context_token_count=data.context_token_count,
        started_at=datetime.now(timezone.utc),
    )
    new_version = await store.append(stream_id, [ev], expected_version=-1)
    return {"session_id": session_id, "context_position": new_version}


async def tool_record_credit_analysis(payload: dict, store: EventStore) -> dict:
    try:
        data = CreditAnalysisInput(**payload)
        decision = CreditDecision(**data.decision)
    except ValidationError as exc:
        return _error("ValidationError", "Invalid credit analysis payload.", details=exc.errors())

    try:
        await _ensure_agent_session(store, data.agent_type, data.session_id, data.agent_id)
        # Ensure CreditAnalysisRequested exists to satisfy aggregate preconditions
        loan_events = await store.load_stream(f"loan-{data.application_id}")
        if not any(e.get("event_type") == "CreditAnalysisRequested" for e in loan_events):
            expected = await store.stream_version(f"loan-{data.application_id}")
            await store.append(
                f"loan-{data.application_id}",
                [CreditAnalysisRequested(
                    application_id=data.application_id,
                    requested_at=datetime.now(timezone.utc),
                    requested_by="mcp:record_credit_analysis",
                    priority="NORMAL",
                )],
                expected_version=expected,
            )
        cmd = CreditAnalysisCompletedCommand(
            application_id=data.application_id,
            agent_type=data.agent_type,
            session_id=data.session_id,
            model_version=data.model_version,
            decision=decision,
            model_deployment_id=data.model_deployment_id,
            input_data_hash=data.input_data_hash,
            analysis_duration_ms=data.analysis_duration_ms,
            correlation_id=data.correlation_id,
            causation_id=data.causation_id,
        )
        await handle_credit_analysis_completed(cmd, store)

        # Record agent output in the session stream for traceability
        session_stream = f"agent-{data.agent_type}-{data.session_id}"
        expected = await store.stream_version(session_stream)
        await store.append(
            session_stream,
            [AgentOutputWritten(
                session_id=data.session_id,
                agent_type=data.agent_type,
                application_id=data.application_id,
                events_written=[{"stream_id": f"loan-{data.application_id}", "event_type": "CreditAnalysisCompleted"}],
                output_summary="credit_analysis_completed",
                written_at=datetime.now(timezone.utc),
            )],
            expected_version=expected,
        )
    except PreconditionFailed as exc:
        return _error("PreconditionFailed", str(exc), suggested_action="start_agent_session")
    except DomainError as exc:
        return _error("DomainError", str(exc))
    except OptimisticConcurrencyError as exc:
        return _error(
            "OptimisticConcurrencyError",
            str(exc),
            stream_id=exc.stream_id,
            expected_version=exc.expected,
            actual_version=exc.actual,
            suggested_action="reload_stream_and_retry",
        )

    # Load last event id from loan stream (DB or in-memory fallback)
    if hasattr(store, "_pool") and store._pool:
        async with store._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT event_id, stream_position FROM events WHERE stream_id=$1 ORDER BY stream_position DESC LIMIT 1",
                f"loan-{data.application_id}",
            )
        return {"event_id": str(row["event_id"]), "new_stream_version": row["stream_position"]}
    events = await store.load_stream(f"loan-{data.application_id}")
    last = events[-1]
    return {"event_id": str(last.get("event_id")), "new_stream_version": last.get("stream_position")}


async def tool_record_fraud_screening(payload: dict, store: EventStore) -> dict:
    try:
        data = FraudScreeningInput(**payload)
    except ValidationError as exc:
        return _error("ValidationError", "Invalid fraud screening payload.", details=exc.errors())

    try:
        await _ensure_agent_session(store, data.agent_type, data.session_id, data.agent_id)
    except PreconditionFailed as exc:
        return _error("PreconditionFailed", str(exc), suggested_action="start_agent_session")

    ev = FraudScreeningCompleted(
        application_id=data.application_id,
        session_id=data.session_id,
        fraud_score=data.fraud_score,
        risk_level=data.risk_level,
        anomalies_found=data.anomalies_found,
        recommendation=data.recommendation,
        screening_model_version=data.screening_model_version,
        input_data_hash=data.input_data_hash,
        completed_at=datetime.now(timezone.utc),
    )
    stream_id = f"fraud-{data.application_id}"
    expected = await store.stream_version(stream_id)
    try:
        new_version = await store.append(stream_id, [ev], expected_version=expected)
    except OptimisticConcurrencyError as exc:
        return _error(
            "OptimisticConcurrencyError",
            str(exc),
            stream_id=exc.stream_id,
            expected_version=exc.expected,
            actual_version=exc.actual,
            suggested_action="reload_stream_and_retry",
        )
    # Record agent output in the session stream for traceability
    session_stream = f"agent-{data.agent_type}-{data.session_id}"
    expected_session = await store.stream_version(session_stream)
    await store.append(
        session_stream,
        [AgentOutputWritten(
            session_id=data.session_id,
            agent_type=data.agent_type,
            application_id=data.application_id,
            events_written=[{"stream_id": stream_id, "event_type": "FraudScreeningCompleted"}],
            output_summary="fraud_screening_completed",
            written_at=datetime.now(timezone.utc),
        )],
        expected_version=expected_session,
    )
    return {"event_id": str(ev.event_id), "new_stream_version": new_version}


async def tool_record_compliance_check(payload: dict, store: EventStore) -> dict:
    try:
        data = ComplianceCheckInput(**payload)
    except ValidationError as exc:
        return _error("ValidationError", "Invalid compliance payload.", details=exc.errors())

    try:
        await _ensure_agent_session(store, data.agent_type, data.session_id, data.agent_id)
    except PreconditionFailed as exc:
        return _error("PreconditionFailed", str(exc), suggested_action="start_agent_session")

    reg = REGULATIONS.get(data.rule_id)
    if not reg:
        return _error("ValidationError", "rule_id does not exist in active regulations.", rule_id=data.rule_id)
    if reg.get("version") != data.regulation_set_version:
        return _error("ValidationError", "rule_id not in active regulation_set_version.", rule_id=data.rule_id)

    if reg.get("note_type"):
        ev = ComplianceRuleNoted(
            application_id=data.application_id,
            session_id=data.session_id,
            rule_id=data.rule_id,
            rule_name=reg.get("name"),
            note_type=reg.get("note_type"),
            note_text=reg.get("note_text"),
            evaluated_at=datetime.now(timezone.utc),
        )
        status = "NOTED"
    elif data.passed:
        ev = ComplianceRulePassed(
            application_id=data.application_id,
            session_id=data.session_id,
            rule_id=data.rule_id,
            rule_name=reg.get("name"),
            rule_version=reg.get("version"),
            evidence_hash=f"{data.rule_id}-{data.application_id}",
            evaluation_notes=data.evaluation_notes or "passed",
            evaluated_at=datetime.now(timezone.utc),
        )
        status = "PASSED"
    else:
        ev = ComplianceRuleFailed(
            application_id=data.application_id,
            session_id=data.session_id,
            rule_id=data.rule_id,
            rule_name=reg.get("name"),
            rule_version=reg.get("version"),
            failure_reason=reg.get("failure_reason") or "failed",
            is_hard_block=bool(reg.get("is_hard_block")),
            remediation_available=reg.get("remediation") is not None,
            remediation_description=reg.get("remediation"),
            evidence_hash=f"{data.rule_id}-{data.application_id}",
            evaluated_at=datetime.now(timezone.utc),
        )
        status = "BLOCKED" if reg.get("is_hard_block") else "FAILED"

    stream_id = f"compliance-{data.application_id}"
    expected = await store.stream_version(stream_id)
    try:
        new_version = await store.append(stream_id, [ev], expected_version=expected)
    except OptimisticConcurrencyError as exc:
        return _error(
            "OptimisticConcurrencyError",
            str(exc),
            stream_id=exc.stream_id,
            expected_version=exc.expected,
            actual_version=exc.actual,
            suggested_action="reload_stream_and_retry",
        )
    # Record agent output in the session stream for traceability
    session_stream = f"agent-{data.agent_type}-{data.session_id}"
    expected_session = await store.stream_version(session_stream)
    await store.append(
        session_stream,
        [AgentOutputWritten(
            session_id=data.session_id,
            agent_type=data.agent_type,
            application_id=data.application_id,
            events_written=[{"stream_id": stream_id, "event_type": ev.event_type}],
            output_summary=f"compliance_rule_{data.rule_id}_{status.lower()}",
            written_at=datetime.now(timezone.utc),
        )],
        expected_version=expected_session,
    )
    return {"check_id": str(ev.event_id), "compliance_status": status, "new_stream_version": new_version}


async def _assert_required_analyses(store: EventStore, application_id: str) -> None:
    loan_events = await store.load_stream(f"loan-{application_id}")
    if not any(e.get("event_type") == "CreditAnalysisCompleted" for e in loan_events):
        raise PreconditionFailed("Missing CreditAnalysisCompleted for application.")

    fraud_events = await store.load_stream(f"fraud-{application_id}")
    if not any(e.get("event_type") == "FraudScreeningCompleted" for e in fraud_events):
        raise PreconditionFailed("Missing FraudScreeningCompleted for application.")

    compliance_events = await store.load_stream(f"compliance-{application_id}")
    if not any(
        e.get("event_type") in {"ComplianceCheckCompleted", "ComplianceRulePassed", "ComplianceRuleFailed", "ComplianceRuleNoted"}
        for e in compliance_events
    ):
        raise PreconditionFailed("Missing compliance evaluation for application.")


async def _resolve_agent_session_stream(store: EventStore, session_id: str) -> str | None:
    if hasattr(store, "_pool") and store._pool:
        async with store._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT stream_id FROM events WHERE event_type='AgentSessionStarted' "
                "AND payload->>'session_id'=$1 ORDER BY recorded_at DESC LIMIT 1",
                session_id,
            )
        if row:
            return row["stream_id"]

    if hasattr(store, "_streams"):
        for stream_id, events in getattr(store, "_streams", {}).items():
            for event in events:
                if event.get("event_type") == "AgentSessionStarted":
                    payload = event.get("payload", {})
                    if payload.get("session_id") == session_id:
                        return stream_id
    return None


async def tool_generate_decision(payload: dict, store: EventStore) -> dict:
    try:
        data = DecisionGeneratedInput(**payload)
    except ValidationError as exc:
        return _error("ValidationError", "Invalid decision payload.", details=exc.errors())

    try:
        await _ensure_agent_session(store, data.agent_type, data.orchestrator_session_id, data.agent_id)
        await _assert_required_analyses(store, data.application_id)

        # Ensure ComplianceCheckCompleted exists if only per-rule events were recorded
        compliance_events = await store.load_stream(f"compliance-{data.application_id}")
        if not any(e.get("event_type") == "ComplianceCheckCompleted" for e in compliance_events):
            rules = [e for e in compliance_events if e.get("event_type") in {"ComplianceRulePassed","ComplianceRuleFailed","ComplianceRuleNoted"}]
            has_block = any(
                e.get("event_type") == "ComplianceRuleFailed" and (e.get("payload") or {}).get("is_hard_block")
                for e in rules
            )
            expected = await store.stream_version(f"compliance-{data.application_id}")
            await store.append(
                f"compliance-{data.application_id}",
                [ComplianceCheckCompleted(
                    application_id=data.application_id,
                    session_id=data.orchestrator_session_id,
                    rules_evaluated=len(rules),
                    rules_passed=sum(1 for e in rules if e.get("event_type") == "ComplianceRulePassed"),
                    rules_failed=sum(1 for e in rules if e.get("event_type") == "ComplianceRuleFailed"),
                    rules_noted=sum(1 for e in rules if e.get("event_type") == "ComplianceRuleNoted"),
                    has_hard_block=has_block,
                    overall_verdict="BLOCKED" if has_block else "CLEAR",
                    completed_at=datetime.now(timezone.utc),
                )],
                expected_version=expected,
            )

        # Ensure ComplianceCheckRequested + DecisionRequested exist to satisfy aggregate preconditions
        loan_stream = f"loan-{data.application_id}"
        loan_events = await store.load_stream(loan_stream)
        if not any(e.get("event_type") == "ComplianceCheckRequested" for e in loan_events):
            expected = await store.stream_version(loan_stream)
            await store.append(
                loan_stream,
                [ComplianceCheckRequested(
                    application_id=data.application_id,
                    requested_at=datetime.now(timezone.utc),
                    triggered_by_event_id="mcp:generate_decision",
                    regulation_set_version="2026-Q1-v1",
                    rules_to_evaluate=list(REGULATIONS.keys()),
                )],
                expected_version=expected,
            )
            loan_events.append({"event_type": "ComplianceCheckRequested"})

        if not any(e.get("event_type") == "DecisionRequested" for e in loan_events):
            expected = await store.stream_version(loan_stream)
            await store.append(
                loan_stream,
                [DecisionRequested(
                    application_id=data.application_id,
                    requested_at=datetime.now(timezone.utc),
                    all_analyses_complete=True,
                    triggered_by_event_id="mcp:generate_decision",
                )],
                expected_version=expected,
            )
        contributing_sessions = []
        for session in data.contributing_sessions or []:
            if session.startswith("agent-"):
                contributing_sessions.append(session)
                continue
            resolved = await _resolve_agent_session_stream(store, session)
            contributing_sessions.append(resolved or session)

        cmd = DecisionGeneratedCommand(
            application_id=data.application_id,
            orchestrator_session_id=data.orchestrator_session_id,
            agent_type=data.agent_type,
            recommendation=data.recommendation,
            confidence=data.confidence,
            approved_amount_usd=data.approved_amount_usd,
            conditions=data.conditions,
            executive_summary=data.executive_summary,
            key_risks=data.key_risks,
            contributing_sessions=contributing_sessions,
            model_versions=data.model_versions,
            correlation_id=data.correlation_id,
            causation_id=data.causation_id,
        )
        await handle_decision_generated(cmd, store)
    except PreconditionFailed as exc:
        return _error("PreconditionFailed", str(exc), suggested_action="complete_missing_analyses")
    except DomainError as exc:
        return _error("DomainError", str(exc))
    except OptimisticConcurrencyError as exc:
        return _error(
            "OptimisticConcurrencyError",
            str(exc),
            stream_id=exc.stream_id,
            expected_version=exc.expected,
            actual_version=exc.actual,
            suggested_action="reload_stream_and_retry",
        )

    if hasattr(store, "_pool") and store._pool:
        async with store._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT event_id, payload->>'recommendation' AS recommendation "
                "FROM events WHERE stream_id=$1 ORDER BY stream_position DESC LIMIT 1",
                f"loan-{data.application_id}",
            )
        return {"decision_id": str(row["event_id"]), "recommendation": row["recommendation"]}
    events = await store.load_stream(f"loan-{data.application_id}")
    last = events[-1]
    return {"decision_id": str(last.get("event_id")), "recommendation": last.get("payload", {}).get("recommendation")}


async def tool_record_human_review(payload: dict, store: EventStore) -> dict:
    try:
        data = HumanReviewInput(**payload)
    except ValidationError as exc:
        return _error("ValidationError", "Invalid human review payload.", details=exc.errors())

    if data.override and not data.override_reason:
        return _error("ValidationError", "override_reason required when override=True.")
    if not data.reviewer_id:
        return _error("AuthError", "reviewer_id is required.")

    stream_id = f"loan-{data.application_id}"
    expected = await store.stream_version(stream_id)

    review_event = HumanReviewCompleted(
        application_id=data.application_id,
        reviewer_id=data.reviewer_id,
        override=data.override,
        original_recommendation=data.original_recommendation,
        final_decision=data.final_decision,
        override_reason=data.override_reason,
        reviewed_at=datetime.now(timezone.utc),
    )

    events_to_append = [review_event]

    if data.final_decision == "APPROVE":
        if data.approved_amount_usd is None or data.interest_rate_pct is None or data.term_months is None:
            return _error("ValidationError", "approved_amount_usd, interest_rate_pct, term_months required for approval.")
        try:
            await store.append(stream_id, [review_event], expected_version=expected)
            cmd = ApplicationApprovedCommand(
                application_id=data.application_id,
                approved_amount_usd=data.approved_amount_usd,
                interest_rate_pct=data.interest_rate_pct,
                term_months=data.term_months,
                conditions=data.conditions,
                approved_by=data.reviewer_id,
                effective_date=datetime.now(timezone.utc).date().isoformat(),
            )
            await handle_application_approved(cmd, store)
        except DomainError as exc:
            return _error("DomainError", str(exc))
        except OptimisticConcurrencyError as exc:
            return _error(
                "OptimisticConcurrencyError",
                str(exc),
                stream_id=exc.stream_id,
                expected_version=exc.expected,
                actual_version=exc.actual,
                suggested_action="reload_stream_and_retry",
            )
        final_state = "FINAL_APPROVED"
    elif data.final_decision == "DECLINE":
        decline_event = ApplicationDeclined(
            application_id=data.application_id,
            decline_reasons=data.decline_reasons or ["human_review_decline"],
            declined_by=data.reviewer_id,
            adverse_action_notice_required=True,
            adverse_action_codes=[],
            declined_at=datetime.now(timezone.utc),
        )
        events_to_append.append(decline_event)
        final_state = "FINAL_DECLINED"
        try:
            await store.append(stream_id, events_to_append, expected_version=expected)
        except OptimisticConcurrencyError as exc:
            return _error(
                "OptimisticConcurrencyError",
                str(exc),
                stream_id=exc.stream_id,
                expected_version=exc.expected,
                actual_version=exc.actual,
                suggested_action="reload_stream_and_retry",
            )
    else:
        final_state = "PENDING_HUMAN_REVIEW"
        try:
            await store.append(stream_id, events_to_append, expected_version=expected)
        except OptimisticConcurrencyError as exc:
            return _error(
                "OptimisticConcurrencyError",
                str(exc),
                stream_id=exc.stream_id,
                expected_version=exc.expected,
                actual_version=exc.actual,
                suggested_action="reload_stream_and_retry",
            )

    return {"final_decision": data.final_decision, "application_state": final_state}


async def tool_run_integrity_check(payload: dict, store: EventStore) -> dict:
    try:
        data = IntegrityCheckInput(**payload)
    except ValidationError as exc:
        return _error("ValidationError", "Invalid integrity check payload.", details=exc.errors())

    if data.role.lower() != "compliance":
        return _error("AuthError", "Only compliance role may run integrity checks.")

    key = f"{data.entity_type}:{data.entity_id}"
    now = datetime.now(timezone.utc)
    last = _last_integrity_check.get(key)
    if last and (now - last).total_seconds() < 60:
        return _error("RateLimit", "Integrity checks are limited to 1 per minute per entity.")
    _last_integrity_check[key] = now

    result = await run_integrity_check(store, data.entity_type, data.entity_id)
    return {"check_result": result.events_verified, "chain_valid": result.chain_valid, "tamper_detected": result.tamper_detected}


# ----------------------------
# Resource handlers
# ----------------------------

async def resource_application_summary(application_id: str, store: EventStore) -> dict | None:
    async with store._pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM application_summary WHERE application_id=$1",
            application_id,
        )
        return dict(row) if row else None


async def resource_compliance(application_id: str, as_of: str | None, store: EventStore) -> dict | None:
    ts = _parse_iso(as_of) if as_of else None
    async with store._pool.acquire() as conn:
        if ts:
            row = await conn.fetchrow(
                "SELECT application_id, state, recorded_at FROM compliance_audit_history "
                "WHERE application_id=$1 AND recorded_at <= $2 "
                "ORDER BY recorded_at DESC LIMIT 1",
                application_id,
                ts,
            )
            return dict(row) if row else None
        row = await conn.fetchrow(
            "SELECT application_id, state, last_event_at FROM compliance_audit_current WHERE application_id=$1",
            application_id,
        )
        return dict(row) if row else None


async def resource_audit_trail(application_id: str, from_ts: str | None, to_ts: str | None, store: EventStore) -> list[dict]:
    stream_id = f"audit-loan-{application_id}"
    events = await store.load_stream(stream_id)
    start = _parse_iso(from_ts)
    end = _parse_iso(to_ts)
    result = []
    for e in events:
        recorded_at = e.get("recorded_at")
        if isinstance(recorded_at, str):
            recorded_at = _parse_iso(recorded_at)
        if start and recorded_at and recorded_at < start:
            continue
        if end and recorded_at and recorded_at > end:
            continue
        result.append(e)
    return result


async def resource_agent_performance(agent_id: str, store: EventStore) -> list[dict]:
    async with store._pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM agent_performance_ledger WHERE agent_id=$1",
            agent_id,
        )
        return [dict(r) for r in rows]


async def resource_agent_session(agent_id: str, session_id: str, store: EventStore) -> list[dict] | None:
    # Find stream id from AgentSessionStarted payload
    async with store._pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT stream_id FROM events WHERE event_type='AgentSessionStarted' "
            "AND payload->>'agent_id'=$1 AND payload->>'session_id'=$2 "
            "ORDER BY recorded_at ASC LIMIT 1",
            agent_id,
            session_id,
        )
    if not row:
        return None
    return await store.load_stream(row["stream_id"])


async def resource_health(store: EventStore) -> dict[str, float | None]:
    latest_pos, latest_at = await store.latest_event_info()
    async with store._pool.acquire() as conn:
        rows = await conn.fetch("SELECT projection_name, last_position, updated_at FROM projection_checkpoints")
    lags: dict[str, float | None] = {}
    for r in rows:
        if latest_at and r["updated_at"]:
            lag_ms = (latest_at - r["updated_at"]).total_seconds() * 1000
            lags[r["projection_name"]] = max(lag_ms, 0.0)
        else:
            lags[r["projection_name"]] = None
    lags["latest_global_position"] = latest_pos
    return lags


# ----------------------------
# MCP server registration
# ----------------------------

def create_mcp_server() -> Any:
    if FastMCP is None:
        raise RuntimeError("fastmcp is not installed. Add fastmcp to your environment.")

    mcp = FastMCP("ledger-mcp")

    @mcp.tool(
        name="submit_application",
        description="Submit a new loan application. Validates schema and rejects duplicate application_id."
    )
    async def submit_application(**payload):
        store = await _get_store()
        return await tool_submit_application(payload, store)

    @mcp.tool(
        name="start_agent_session",
        description=(
            "Start an agent session and load context. "
            "Precondition: must be called before any agent decision tools."
        ),
    )
    async def start_agent_session(**payload):
        store = await _get_store()
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
        store = await _get_store()
        return await tool_record_credit_analysis(payload, store)

    @mcp.tool(
        name="record_fraud_screening",
        description=(
            "Record FraudScreeningCompleted. "
            "Precondition: active agent session created by start_agent_session."
        ),
    )
    async def record_fraud_screening(**payload):
        store = await _get_store()
        return await tool_record_fraud_screening(payload, store)

    @mcp.tool(
        name="record_compliance_check",
        description=(
            "Record a compliance rule pass/fail. "
            "Precondition: active agent session created by start_agent_session."
        ),
    )
    async def record_compliance_check(**payload):
        store = await _get_store()
        return await tool_record_compliance_check(payload, store)

    @mcp.tool(
        name="generate_decision",
        description=(
            "Generate DecisionGenerated. Requires all analyses (credit, fraud, compliance) present. "
            "If ComplianceCheckCompleted or DecisionRequested is missing, this tool will append them."
        ),
    )
    async def generate_decision(**payload):
        store = await _get_store()
        return await tool_generate_decision(payload, store)

    @mcp.tool(
        name="record_human_review",
        description="Record HumanReviewCompleted and finalize application state."
    )
    async def record_human_review(**payload):
        store = await _get_store()
        return await tool_record_human_review(payload, store)

    @mcp.tool(
        name="run_integrity_check",
        description="Run audit integrity check. Precondition: compliance role only, 1/min per entity."
    )
    async def run_integrity(**payload):
        store = await _get_store()
        return await tool_run_integrity_check(payload, store)

    @mcp.resource("ledger://applications/{application_id}")
    async def application_summary(application_id: str):
        store = await _get_store()
        return await resource_application_summary(application_id, store)

    @mcp.resource("ledger://applications/{application_id}/compliance")
    async def compliance_view(application_id: str, as_of: str | None = None):
        store = await _get_store()
        return await resource_compliance(application_id, as_of, store)

    @mcp.resource("ledger://applications/{application_id}/audit-trail")
    async def audit_trail(application_id: str, from_ts: str | None = None, to_ts: str | None = None):
        store = await _get_store()
        return await resource_audit_trail(application_id, from_ts, to_ts, store)

    @mcp.resource("ledger://agents/{agent_id}/performance")
    async def agent_performance(agent_id: str):
        store = await _get_store()
        return await resource_agent_performance(agent_id, store)

    @mcp.resource("ledger://agents/{agent_id}/sessions/{session_id}")
    async def agent_sessions(agent_id: str, session_id: str):
        store = await _get_store()
        return await resource_agent_session(agent_id, session_id, store)

    @mcp.resource("ledger://ledger/health")
    async def ledger_health():
        store = await _get_store()
        return await resource_health(store)

    return mcp


__all__ = [
    "create_mcp_server",
    "tool_submit_application",
    "tool_start_agent_session",
    "tool_record_credit_analysis",
    "tool_record_fraud_screening",
    "tool_record_compliance_check",
    "tool_generate_decision",
    "tool_record_human_review",
    "tool_run_integrity_check",
    "resource_application_summary",
    "resource_compliance",
    "resource_audit_trail",
    "resource_agent_performance",
    "resource_agent_session",
    "resource_health",
]
