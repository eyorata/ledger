"""
ledger/domain/aggregates/loan_application.py
=============================================
Loan application aggregate with event replay, state machine, and business rules.
"""
from __future__ import annotations
from dataclasses import dataclass, field
from enum import Enum
from typing import Iterable

from ledger.domain.errors import DomainError


class ApplicationState(str, Enum):
    NEW = "NEW"
    SUBMITTED = "SUBMITTED"
    AWAITING_ANALYSIS = "AWAITING_ANALYSIS"
    ANALYSIS_COMPLETE = "ANALYSIS_COMPLETE"
    COMPLIANCE_REVIEW = "COMPLIANCE_REVIEW"
    PENDING_DECISION = "PENDING_DECISION"
    APPROVED_PENDING_HUMAN = "APPROVED_PENDING_HUMAN"
    DECLINED_PENDING_HUMAN = "DECLINED_PENDING_HUMAN"
    PENDING_HUMAN_REVIEW = "PENDING_HUMAN_REVIEW"
    FINAL_APPROVED = "FINAL_APPROVED"
    FINAL_DECLINED = "FINAL_DECLINED"


VALID_TRANSITIONS = {
    ApplicationState.NEW: [ApplicationState.SUBMITTED],
    ApplicationState.SUBMITTED: [ApplicationState.AWAITING_ANALYSIS],
    ApplicationState.AWAITING_ANALYSIS: [ApplicationState.ANALYSIS_COMPLETE],
    ApplicationState.ANALYSIS_COMPLETE: [ApplicationState.COMPLIANCE_REVIEW],
    ApplicationState.COMPLIANCE_REVIEW: [ApplicationState.PENDING_DECISION],
    ApplicationState.PENDING_DECISION: [
        ApplicationState.APPROVED_PENDING_HUMAN,
        ApplicationState.DECLINED_PENDING_HUMAN,
        ApplicationState.PENDING_HUMAN_REVIEW,
    ],
    ApplicationState.APPROVED_PENDING_HUMAN: [ApplicationState.FINAL_APPROVED, ApplicationState.FINAL_DECLINED],
    ApplicationState.DECLINED_PENDING_HUMAN: [ApplicationState.FINAL_DECLINED, ApplicationState.FINAL_APPROVED],
    ApplicationState.PENDING_HUMAN_REVIEW: [ApplicationState.FINAL_APPROVED, ApplicationState.FINAL_DECLINED],
}


DECISION_EVENT_TYPES = {
    "CreditAnalysisCompleted",
    "FraudScreeningCompleted",
    "ComplianceCheckCompleted",
    "DecisionGenerated",
}


@dataclass
class LoanApplicationAggregate:
    application_id: str
    state: ApplicationState = ApplicationState.NEW
    applicant_id: str | None = None
    requested_amount_usd: float | None = None
    loan_purpose: str | None = None
    version: int = -1

    credit_analysis_completed: bool = False
    credit_analysis_overridden: bool = False

    compliance_required_rules: set[str] = field(default_factory=set)
    compliance_passed_rules: set[str] = field(default_factory=set)
    compliance_blocked: bool = False

    contributing_agent_sessions: set[str] = field(default_factory=set)

    @classmethod
    async def load(cls, store, application_id: str) -> "LoanApplicationAggregate":
        events = await store.load_stream(f"loan-{application_id}")
        agg = cls(application_id=application_id)
        for event in events:
            agg._apply(event)
        return agg

    def _apply(self, event: dict) -> None:
        handler = getattr(self, f"_on_{event.get('event_type')}", None)
        if handler:
            handler(event)
        if "stream_position" in event:
            self.version = event["stream_position"]
        else:
            self.version += 1

    def _transition(self, target: ApplicationState) -> None:
        allowed = VALID_TRANSITIONS.get(self.state, [])
        if target not in allowed:
            raise DomainError(f"Invalid transition {self.state} -> {target}. Allowed: {allowed}")
        self.state = target

    def _on_ApplicationSubmitted(self, event: dict) -> None:
        self._transition(ApplicationState.SUBMITTED)
        payload = event.get("payload", {})
        self.applicant_id = payload.get("applicant_id")
        self.requested_amount_usd = payload.get("requested_amount_usd")
        self.loan_purpose = payload.get("loan_purpose")

    def _on_CreditAnalysisRequested(self, event: dict) -> None:
        self._transition(ApplicationState.AWAITING_ANALYSIS)

    def _on_CreditAnalysisCompleted(self, event: dict) -> None:
        self._transition(ApplicationState.ANALYSIS_COMPLETE)
        self.credit_analysis_completed = True

    def _on_ComplianceCheckRequested(self, event: dict) -> None:
        self._transition(ApplicationState.COMPLIANCE_REVIEW)

    def _on_ComplianceCheckCompleted(self, event: dict) -> None:
        payload = event.get("payload", {})
        if payload.get("overall_verdict") == "BLOCKED":
            self.compliance_blocked = True
        self._transition(ApplicationState.PENDING_DECISION)

    def _on_DecisionRequested(self, event: dict) -> None:
        self._transition(ApplicationState.PENDING_DECISION)

    def _on_DecisionGenerated(self, event: dict) -> None:
        payload = event.get("payload", {})
        recommendation = payload.get("recommendation")
        confidence = payload.get("confidence", 1.0)
        if confidence < 0.6:
            recommendation = "REFER"
        sessions = payload.get("contributing_sessions") or []
        self.contributing_agent_sessions = set(sessions)
        if recommendation == "APPROVE":
            self._transition(ApplicationState.APPROVED_PENDING_HUMAN)
        elif recommendation == "DECLINE":
            self._transition(ApplicationState.DECLINED_PENDING_HUMAN)
        else:
            self._transition(ApplicationState.PENDING_HUMAN_REVIEW)

    def _on_HumanReviewRequested(self, event: dict) -> None:
        self._transition(ApplicationState.PENDING_HUMAN_REVIEW)

    def _on_HumanReviewCompleted(self, event: dict) -> None:
        payload = event.get("payload", {})
        if payload.get("override"):
            self.credit_analysis_overridden = True

    def _on_ApplicationApproved(self, event: dict) -> None:
        self._transition(ApplicationState.FINAL_APPROVED)

    def _on_ApplicationDeclined(self, event: dict) -> None:
        self._transition(ApplicationState.FINAL_DECLINED)

    def assert_awaiting_credit_analysis(self) -> None:
        if self.state != ApplicationState.AWAITING_ANALYSIS:
            raise DomainError(f"Expected AWAITING_ANALYSIS, got {self.state}")

    def assert_pending_decision(self) -> None:
        if self.state != ApplicationState.PENDING_DECISION:
            raise DomainError(f"Expected PENDING_DECISION, got {self.state}")

    def assert_can_append_credit_analysis(self) -> None:
        if self.credit_analysis_completed and not self.credit_analysis_overridden:
            raise DomainError("CreditAnalysisCompleted already recorded without override")

    def assert_decision_confidence(self, confidence: float, recommendation: str) -> None:
        if confidence < 0.6 and recommendation != "REFER":
            raise DomainError("confidence_score < 0.6 requires recommendation=REFER")

    async def assert_compliance_rules_passed(self, store) -> None:
        events = await store.load_stream(f"compliance-{self.application_id}")
        required: set[str] = set()
        passed: set[str] = set()
        for event in events:
            et = event.get("event_type")
            payload = event.get("payload", {})
            if et == "ComplianceCheckInitiated":
                required.update(payload.get("rules_to_evaluate") or [])
            elif et == "ComplianceRulePassed":
                rule_id = payload.get("rule_id")
                if rule_id:
                    passed.add(rule_id)
            elif et == "ComplianceRuleFailed" and payload.get("is_hard_block"):
                raise DomainError("Compliance hard block present; approval not allowed")
        if required and not required.issubset(passed):
            missing = sorted(required - passed)
            raise DomainError(f"Missing compliance passes: {missing}")

    async def assert_contributing_sessions_valid(self, store, session_stream_ids: Iterable[str]) -> None:
        for stream_id in session_stream_ids:
            events = await store.load_stream(stream_id)
            has_decision = False
            for event in events:
                if event.get("event_type") != "AgentOutputWritten":
                    continue
                payload = event.get("payload", {})
                if payload.get("application_id") != self.application_id:
                    continue
                for e in payload.get("events_written", []) or []:
                    if e.get("event_type") in DECISION_EVENT_TYPES:
                        has_decision = True
                        break
                if has_decision:
                    break
            if not has_decision:
                raise DomainError(f"Agent session {stream_id} has no decision event for application")
