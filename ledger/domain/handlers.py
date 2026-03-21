from __future__ import annotations
from datetime import datetime, timezone

from ledger.domain.aggregates.loan_application import LoanApplicationAggregate
from ledger.domain.aggregates.agent_session import AgentSessionAggregate
from ledger.domain.commands import (
    SubmitApplicationCommand,
    CreditAnalysisCompletedCommand,
    FraudScreeningCompletedCommand,
    ComplianceCheckCommand,
    DecisionGeneratedCommand,
    HumanReviewCompletedCommand,
    StartAgentSessionCommand,
    ApplicationApprovedCommand,
)
from ledger.schema.events import (
    ApplicationSubmitted,
    CreditAnalysisCompleted,
    FraudScreeningCompleted,
    ComplianceRulePassed,
    ComplianceRuleFailed,
    ComplianceRuleNoted,
    DecisionGenerated,
    HumanReviewCompleted,
    AgentSessionStarted,
    ApplicationApproved,
)


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


async def handle_submit_application(
    cmd: SubmitApplicationCommand,
    store,
) -> None:
    app = await LoanApplicationAggregate.load(store, cmd.application_id)

    new_event = ApplicationSubmitted(
        application_id=cmd.application_id,
        applicant_id=cmd.applicant_id,
        requested_amount_usd=cmd.requested_amount_usd,
        loan_purpose=cmd.loan_purpose,
        loan_term_months=cmd.loan_term_months,
        submission_channel=cmd.submission_channel,
        contact_email=cmd.contact_email,
        contact_name=cmd.contact_name,
        submitted_at=datetime.now(timezone.utc),
        application_reference=cmd.application_reference,
    )
    await store.append(
        stream_id=f"loan-{cmd.application_id}",
        events=[new_event],
        expected_version=app.version,
        correlation_id=cmd.correlation_id,
        causation_id=cmd.causation_id,
    )


async def handle_fraud_screening_completed(
    cmd: FraudScreeningCompletedCommand,
    store,
) -> None:
    agent = await AgentSessionAggregate.load(store, cmd.agent_type, cmd.session_id)
    agent.assert_context_loaded()

    new_event = FraudScreeningCompleted(
        application_id=cmd.application_id,
        session_id=cmd.session_id,
        fraud_score=cmd.fraud_score,
        risk_level=cmd.risk_level,
        anomalies_found=cmd.anomalies_found,
        recommendation=cmd.recommendation,
        screening_model_version=cmd.screening_model_version,
        input_data_hash=cmd.input_data_hash,
        completed_at=datetime.now(timezone.utc),
    )
    stream_id = f"fraud-{cmd.application_id}"
    expected = await store.stream_version(stream_id)
    await store.append(
        stream_id=stream_id,
        events=[new_event],
        expected_version=expected,
        correlation_id=cmd.correlation_id,
        causation_id=cmd.causation_id,
    )


async def handle_compliance_check(
    cmd: ComplianceCheckCommand,
    store,
) -> None:
    agent = await AgentSessionAggregate.load(store, cmd.agent_type, cmd.session_id)
    agent.assert_context_loaded()

    if cmd.noted:
        new_event = ComplianceRuleNoted(
            application_id=cmd.application_id,
            session_id=cmd.session_id,
            rule_id=cmd.rule_id,
            rule_name=cmd.rule_name,
            note_type="REG_NOTE",
            note_text=cmd.evaluation_notes or "",
            evaluated_at=datetime.now(timezone.utc),
        )
    elif cmd.passed:
        new_event = ComplianceRulePassed(
            application_id=cmd.application_id,
            session_id=cmd.session_id,
            rule_id=cmd.rule_id,
            rule_name=cmd.rule_name,
            rule_version=cmd.rule_version,
            evidence_hash=f"{cmd.rule_id}-{cmd.application_id}",
            evaluation_notes=cmd.evaluation_notes or "passed",
            evaluated_at=datetime.now(timezone.utc),
        )
    else:
        new_event = ComplianceRuleFailed(
            application_id=cmd.application_id,
            session_id=cmd.session_id,
            rule_id=cmd.rule_id,
            rule_name=cmd.rule_name,
            rule_version=cmd.rule_version,
            failure_reason=cmd.evaluation_notes or "failed",
            is_hard_block=cmd.is_hard_block,
            remediation_available=cmd.remediation_available,
            remediation_description=cmd.remediation_description,
            evidence_hash=f"{cmd.rule_id}-{cmd.application_id}",
            evaluated_at=datetime.now(timezone.utc),
        )

    stream_id = f"compliance-{cmd.application_id}"
    expected = await store.stream_version(stream_id)
    await store.append(
        stream_id=stream_id,
        events=[new_event],
        expected_version=expected,
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


async def handle_human_review_completed(
    cmd: HumanReviewCompletedCommand,
    store,
) -> None:
    new_event = HumanReviewCompleted(
        application_id=cmd.application_id,
        reviewer_id=cmd.reviewer_id,
        override=cmd.override,
        original_recommendation=cmd.original_recommendation,
        final_decision=cmd.final_decision,
        override_reason=cmd.override_reason,
        reviewed_at=datetime.now(timezone.utc),
    )
    stream_id = f"loan-{cmd.application_id}"
    expected = await store.stream_version(stream_id)
    await store.append(
        stream_id=stream_id,
        events=[new_event],
        expected_version=expected,
        correlation_id=cmd.correlation_id,
        causation_id=cmd.causation_id,
    )


async def handle_start_agent_session(
    cmd: StartAgentSessionCommand,
    store,
) -> None:
    new_event = AgentSessionStarted(
        session_id=cmd.session_id,
        agent_type=cmd.agent_type,
        agent_id=cmd.agent_id,
        application_id=cmd.application_id,
        model_version=cmd.model_version,
        langgraph_graph_version=cmd.langgraph_graph_version,
        context_source=cmd.context_source,
        context_token_count=cmd.context_token_count,
        started_at=datetime.now(timezone.utc),
    )
    stream_id = f"agent-{cmd.agent_type}-{cmd.session_id}"
    expected = await store.stream_version(stream_id)
    await store.append(
        stream_id=stream_id,
        events=[new_event],
        expected_version=expected,
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
