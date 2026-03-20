from __future__ import annotations
from datetime import datetime, timezone

from ledger.domain.aggregates.loan_application import LoanApplicationAggregate
from ledger.domain.aggregates.agent_session import AgentSessionAggregate
from ledger.domain.commands import (
    CreditAnalysisCompletedCommand,
    DecisionGeneratedCommand,
    ApplicationApprovedCommand,
)
from ledger.schema.events import CreditAnalysisCompleted, DecisionGenerated, ApplicationApproved


async def handle_credit_analysis_completed(
    cmd: CreditAnalysisCompletedCommand,
    store,
) -> None:
    app = await LoanApplicationAggregate.load(store, cmd.application_id)
    agent = await AgentSessionAggregate.load(store, cmd.agent_type, cmd.session_id)

    app.assert_awaiting_credit_analysis()
    app.assert_can_append_credit_analysis()
    agent.assert_context_loaded()
    agent.assert_model_version_current(cmd.model_version)

    new_event = CreditAnalysisCompleted(
        application_id=cmd.application_id,
        session_id=cmd.session_id,
        decision=cmd.decision,
        model_version=cmd.model_version,
        model_deployment_id=cmd.model_deployment_id,
        input_data_hash=cmd.input_data_hash,
        analysis_duration_ms=cmd.analysis_duration_ms,
        completed_at=datetime.now(timezone.utc),
    )

    await store.append(
        stream_id=f"loan-{cmd.application_id}",
        events=[new_event],
        expected_version=app.version,
        correlation_id=cmd.correlation_id,
        causation_id=cmd.causation_id,
    )


async def handle_decision_generated(
    cmd: DecisionGeneratedCommand,
    store,
) -> None:
    app = await LoanApplicationAggregate.load(store, cmd.application_id)
    agent = await AgentSessionAggregate.load(store, cmd.agent_type, cmd.orchestrator_session_id)

    app.assert_pending_decision()
    agent.assert_context_loaded()

    recommendation = cmd.recommendation
    if cmd.confidence < 0.6:
        recommendation = "REFER"

    app.assert_decision_confidence(cmd.confidence, recommendation)
    await app.assert_contributing_sessions_valid(store, cmd.contributing_sessions)

    new_event = DecisionGenerated(
        application_id=cmd.application_id,
        orchestrator_session_id=cmd.orchestrator_session_id,
        recommendation=recommendation,
        confidence=cmd.confidence,
        approved_amount_usd=cmd.approved_amount_usd,
        conditions=list(cmd.conditions),
        executive_summary=cmd.executive_summary,
        key_risks=list(cmd.key_risks),
        contributing_sessions=list(cmd.contributing_sessions),
        model_versions=cmd.model_versions,
        generated_at=datetime.now(timezone.utc),
    )

    await store.append(
        stream_id=f"loan-{cmd.application_id}",
        events=[new_event],
        expected_version=app.version,
        correlation_id=cmd.correlation_id,
        causation_id=cmd.causation_id,
    )


async def handle_application_approved(
    cmd: ApplicationApprovedCommand,
    store,
) -> None:
    app = await LoanApplicationAggregate.load(store, cmd.application_id)
    await app.assert_compliance_rules_passed(store)

    new_event = ApplicationApproved(
        application_id=cmd.application_id,
        approved_amount_usd=cmd.approved_amount_usd,
        interest_rate_pct=cmd.interest_rate_pct,
        term_months=cmd.term_months,
        conditions=list(cmd.conditions),
        approved_by=cmd.approved_by,
        effective_date=cmd.effective_date,
        approved_at=datetime.now(timezone.utc),
    )

    await store.append(
        stream_id=f"loan-{cmd.application_id}",
        events=[new_event],
        expected_version=app.version,
        correlation_id=cmd.correlation_id,
        causation_id=cmd.causation_id,
    )
