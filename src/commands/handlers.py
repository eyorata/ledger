"""
src/commands/handlers.py
Wrapper for domain command handlers.
"""
from ledger.domain.handlers import (
    handle_submit_application,
    handle_credit_analysis_completed,
    handle_fraud_screening_completed,
    handle_compliance_check,
    handle_decision_generated,
    handle_human_review_completed,
    handle_start_agent_session,
    handle_application_approved,
)

__all__ = [
    "handle_submit_application",
    "handle_credit_analysis_completed",
    "handle_fraud_screening_completed",
    "handle_compliance_check",
    "handle_decision_generated",
    "handle_human_review_completed",
    "handle_start_agent_session",
    "handle_application_approved",
]
