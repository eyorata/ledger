from __future__ import annotations
from dataclasses import dataclass
from typing import Sequence

from ledger.schema.events import CreditDecision


@dataclass(frozen=True)
class SubmitApplicationCommand:
    application_id: str
    applicant_id: str
    requested_amount_usd: float
    loan_purpose: str
    loan_term_months: int
    submission_channel: str
    contact_email: str
    contact_name: str
    application_reference: str
    correlation_id: str | None = None
    causation_id: str | None = None


@dataclass(frozen=True)
class CreditAnalysisCompletedCommand:
    application_id: str
    agent_type: str
    session_id: str
    model_version: str
    decision: CreditDecision
    model_deployment_id: str
    input_data_hash: str
    analysis_duration_ms: int
    correlation_id: str | None = None
    causation_id: str | None = None


@dataclass(frozen=True)
class FraudScreeningCompletedCommand:
    application_id: str
    agent_type: str
    session_id: str
    fraud_score: float
    risk_level: str
    anomalies_found: int
    recommendation: str
    screening_model_version: str
    input_data_hash: str
    correlation_id: str | None = None
    causation_id: str | None = None


@dataclass(frozen=True)
class ComplianceCheckCommand:
    application_id: str
    agent_type: str
    session_id: str
    rule_id: str
    rule_name: str
    rule_version: str
    passed: bool
    noted: bool = False
    is_hard_block: bool = False
    remediation_available: bool = False
    remediation_description: str | None = None
    evaluation_notes: str | None = None
    correlation_id: str | None = None
    causation_id: str | None = None


@dataclass(frozen=True)
class DecisionGeneratedCommand:
    application_id: str
    orchestrator_session_id: str
    agent_type: str
    recommendation: str
    confidence: float
    approved_amount_usd: float | None
    conditions: Sequence[str]
    executive_summary: str
    key_risks: Sequence[str]
    contributing_sessions: Sequence[str]
    model_versions: dict[str, str]
    correlation_id: str | None = None
    causation_id: str | None = None


@dataclass(frozen=True)
class HumanReviewCompletedCommand:
    application_id: str
    reviewer_id: str
    override: bool
    original_recommendation: str
    final_decision: str
    override_reason: str | None = None
    decision_event_id: str | None = None
    correlation_id: str | None = None
    causation_id: str | None = None


@dataclass(frozen=True)
class StartAgentSessionCommand:
    agent_type: str
    agent_id: str
    application_id: str
    session_id: str
    model_version: str
    langgraph_graph_version: str
    context_source: str
    context_token_count: int
    correlation_id: str | None = None
    causation_id: str | None = None


@dataclass(frozen=True)
class ApplicationApprovedCommand:
    application_id: str
    approved_amount_usd: float
    interest_rate_pct: float
    term_months: int
    conditions: Sequence[str]
    approved_by: str
    effective_date: str
    correlation_id: str | None = None
    causation_id: str | None = None
