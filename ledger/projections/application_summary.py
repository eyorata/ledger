"""ApplicationSummary projection."""
from __future__ import annotations
from datetime import datetime
from typing import Iterable

from ledger.projections.base import Projection


STATE_BY_EVENT = {
    "ApplicationSubmitted": "SUBMITTED",
    "CreditAnalysisRequested": "AWAITING_ANALYSIS",
    "CreditAnalysisCompleted": "ANALYSIS_COMPLETE",
    "ComplianceCheckRequested": "COMPLIANCE_REVIEW",
    "ComplianceCheckCompleted": "PENDING_DECISION",
    "DecisionGenerated": "PENDING_DECISION",
    "HumanReviewRequested": "PENDING_HUMAN_REVIEW",
    "ApplicationApproved": "FINAL_APPROVED",
    "ApplicationDeclined": "FINAL_DECLINED",
}


class ApplicationSummaryProjection(Projection):
    name = "application_summary"
    subscribed_event_types = {
        "ApplicationSubmitted",
        "CreditAnalysisRequested",
        "CreditAnalysisCompleted",
        "FraudScreeningCompleted",
        "ComplianceCheckCompleted",
        "DecisionGenerated",
        "AgentSessionCompleted",
        "HumanReviewCompleted",
        "HumanReviewRequested",
        "ApplicationApproved",
        "ApplicationDeclined",
    }

    async def handle(self, event: dict, store) -> None:
        payload = event.get("payload", {})
        app_id = payload.get("application_id")
        if not app_id:
            return

        state = STATE_BY_EVENT.get(event.get("event_type"))
        applicant_id = payload.get("applicant_id")
        requested_amount = payload.get("requested_amount_usd")
        approved_amount = payload.get("approved_amount_usd")
        risk_tier = None
        fraud_score = None
        compliance_status = None
        decision = None
        human_reviewer_id = None
        final_decision_at = None
        agent_sessions_completed: Iterable[str] | None = None

        if event.get("event_type") == "CreditAnalysisCompleted":
            decision_payload = payload.get("decision") or {}
            risk_tier = decision_payload.get("risk_tier")
        elif event.get("event_type") == "FraudScreeningCompleted":
            fraud_score = payload.get("fraud_score")
        elif event.get("event_type") == "ComplianceCheckCompleted":
            compliance_status = payload.get("overall_verdict")
        elif event.get("event_type") == "DecisionGenerated":
            decision = payload.get("recommendation")
        elif event.get("event_type") == "AgentSessionCompleted":
            agent_sessions_completed = [payload.get("session_id")] if payload.get("session_id") else []
        elif event.get("event_type") == "HumanReviewCompleted":
            human_reviewer_id = payload.get("reviewer_id")
        elif event.get("event_type") in ("ApplicationApproved", "ApplicationDeclined"):
            final_decision_at = payload.get("approved_at") or payload.get("declined_at")

        last_event_type = event.get("event_type")
        last_event_at = event.get("recorded_at")
        if isinstance(last_event_at, str):
            last_event_at = datetime.fromisoformat(last_event_at)

        async with store._pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO application_summary("
                "application_id, state, applicant_id, requested_amount_usd, approved_amount_usd,"
                "risk_tier, fraud_score, compliance_status, decision, agent_sessions_completed,"
                "last_event_type, last_event_at, human_reviewer_id, final_decision_at)"
                " VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)"
                " ON CONFLICT (application_id) DO UPDATE SET"
                " state=COALESCE(EXCLUDED.state, application_summary.state),"
                " applicant_id=COALESCE(EXCLUDED.applicant_id, application_summary.applicant_id),"
                " requested_amount_usd=COALESCE(EXCLUDED.requested_amount_usd, application_summary.requested_amount_usd),"
                " approved_amount_usd=COALESCE(EXCLUDED.approved_amount_usd, application_summary.approved_amount_usd),"
                " risk_tier=COALESCE(EXCLUDED.risk_tier, application_summary.risk_tier),"
                " fraud_score=COALESCE(EXCLUDED.fraud_score, application_summary.fraud_score),"
                " compliance_status=COALESCE(EXCLUDED.compliance_status, application_summary.compliance_status),"
                " decision=COALESCE(EXCLUDED.decision, application_summary.decision),"
                " agent_sessions_completed=CASE WHEN EXCLUDED.agent_sessions_completed IS NULL"
                " THEN application_summary.agent_sessions_completed"
                " ELSE ARRAY(SELECT DISTINCT UNNEST(COALESCE(application_summary.agent_sessions_completed, ARRAY[]::text[]))"
                " || EXCLUDED.agent_sessions_completed) END,"
                " last_event_type=EXCLUDED.last_event_type,"
                " last_event_at=EXCLUDED.last_event_at,"
                " human_reviewer_id=COALESCE(EXCLUDED.human_reviewer_id, application_summary.human_reviewer_id),"
                " final_decision_at=COALESCE(EXCLUDED.final_decision_at, application_summary.final_decision_at)",
                app_id,
                state,
                applicant_id,
                requested_amount,
                approved_amount,
                risk_tier,
                fraud_score,
                compliance_status,
                decision,
                agent_sessions_completed,
                last_event_type,
                last_event_at,
                human_reviewer_id,
                final_decision_at,
            )
