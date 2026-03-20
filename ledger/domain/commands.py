from __future__ import annotations
from dataclasses import dataclass
from typing import Sequence

from ledger.schema.events import CreditDecision


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
