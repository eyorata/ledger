"""
Phase 2 domain tests: aggregates + command handlers.
"""
import pytest
from ledger.domain.errors import DomainError
from ledger.domain.aggregates.loan_application import LoanApplicationAggregate, ApplicationState
from ledger.domain.aggregates.agent_session import AgentSessionAggregate
from ledger.domain.handlers import handle_credit_analysis_completed
from ledger.domain.commands import CreditAnalysisCompletedCommand
from ledger.schema.events import CreditDecision, RiskTier
from ledger.event_store import InMemoryEventStore


@pytest.mark.asyncio
async def test_gas_town_context_required():
    store = InMemoryEventStore()
    app_id = "A-001"
    session_id = "S-001"
    # ApplicationSubmitted -> CreditAnalysisRequested
    await store.append(
        f"loan-{app_id}",
        [
            {"event_type":"ApplicationSubmitted","event_version":1,"payload":{"application_id":app_id,"applicant_id":"C-1","requested_amount_usd":1000,"loan_purpose":"working_capital"}},
            {"event_type":"CreditAnalysisRequested","event_version":1,"payload":{"application_id":app_id}},
        ],
        expected_version=-1,
    )
    # Agent stream without context-loaded/started event
    await store.append(
        f"agent-credit_analysis-{session_id}",
        [{"event_type":"AgentOutputWritten","event_version":1,"payload":{"application_id":app_id,"events_written":[]}}],
        expected_version=-1,
    )

    decision = CreditDecision(
        risk_tier=RiskTier.LOW,
        recommended_limit_usd=1000,
        confidence=0.9,
        rationale="ok",
    )
    cmd = CreditAnalysisCompletedCommand(
        application_id=app_id,
        agent_type="credit_analysis",
        session_id=session_id,
        model_version="v1",
        decision=decision,
        model_deployment_id="d1",
        input_data_hash="h",
        analysis_duration_ms=10,
    )

    with pytest.raises(DomainError):
        await handle_credit_analysis_completed(cmd, store)


@pytest.mark.asyncio
async def test_confidence_floor_forces_refer():
    app_id = "A-002"
    agg = LoanApplicationAggregate(application_id=app_id)
    # simulate pending decision
    agg.state = ApplicationState.PENDING_DECISION
    with pytest.raises(DomainError):
        agg.assert_decision_confidence(0.5, "APPROVE")


@pytest.mark.asyncio
async def test_model_version_locking():
    app_id = "A-003"
    agg = LoanApplicationAggregate(application_id=app_id)
    agg.credit_analysis_completed = True
    agg.credit_analysis_overridden = False
    with pytest.raises(DomainError):
        agg.assert_can_append_credit_analysis()
