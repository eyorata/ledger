"""
ledger/what_if.py
=================
Phase 6: What-If projector for counterfactual projections.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Iterable
from uuid import uuid4

from ledger.schema.events import BaseEvent


@dataclass
class WhatIfResult:
    real_outcome: dict
    counterfactual_outcome: dict
    divergence_events: list[dict]


class InMemoryApplicationSummaryProjection:
    """
    In-memory ApplicationSummary used for what-if evaluation.
    """
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

    def __init__(self):
        self.rows: dict[str, dict] = {}

    async def handle(self, event: dict, store=None) -> None:
        payload = event.get("payload", {})
        app_id = payload.get("application_id")
        if not app_id:
            return

        state_by_event = {
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

        row = self.rows.get(app_id, {})
        row["application_id"] = app_id
        row["state"] = state_by_event.get(event.get("event_type"), row.get("state"))
        row["applicant_id"] = payload.get("applicant_id", row.get("applicant_id"))
        row["requested_amount_usd"] = payload.get("requested_amount_usd", row.get("requested_amount_usd"))
        row["approved_amount_usd"] = payload.get("approved_amount_usd", row.get("approved_amount_usd"))

        if event.get("event_type") == "CreditAnalysisCompleted":
            decision_payload = payload.get("decision") or {}
            row["risk_tier"] = decision_payload.get("risk_tier", row.get("risk_tier"))
        if event.get("event_type") == "FraudScreeningCompleted":
            row["fraud_score"] = payload.get("fraud_score", row.get("fraud_score"))
        if event.get("event_type") == "ComplianceCheckCompleted":
            row["compliance_status"] = payload.get("overall_verdict", row.get("compliance_status"))
        if event.get("event_type") == "DecisionGenerated":
            row["decision"] = payload.get("recommendation", row.get("decision"))
        if event.get("event_type") == "HumanReviewCompleted":
            row["human_reviewer_id"] = payload.get("reviewer_id", row.get("human_reviewer_id"))
        if event.get("event_type") in ("ApplicationApproved", "ApplicationDeclined"):
            row["final_decision_at"] = payload.get("approved_at") or payload.get("declined_at")

        row["last_event_type"] = event.get("event_type")
        row["last_event_at"] = event.get("recorded_at")
        self.rows[app_id] = row

    def snapshot(self, application_id: str) -> dict:
        return dict(self.rows.get(application_id) or {})


class InMemoryComplianceAuditViewProjection:
    """
    In-memory ComplianceAuditView used for what-if evaluation.
    """
    name = "compliance_audit_view"
    subscribed_event_types = {
        "ComplianceCheckInitiated",
        "ComplianceRulePassed",
        "ComplianceRuleFailed",
        "ComplianceRuleNoted",
        "ComplianceCheckCompleted",
    }

    def __init__(self):
        self.current: dict[str, dict] = {}
        self.history: list[dict] = []

    async def handle(self, event: dict, store=None) -> None:
        if event.get("event_type") not in self.subscribed_event_types:
            return
        payload = event.get("payload", {})
        app_id = payload.get("application_id")
        if not app_id:
            return
        recorded_at = event.get("recorded_at")

        state = self.current.get(app_id) or {
            "rules": [],
            "overall_verdict": None,
            "regulation_set_version": None,
        }
        if event.get("event_type") == "ComplianceCheckInitiated":
            state["regulation_set_version"] = payload.get("regulation_set_version")
        elif event.get("event_type") in ("ComplianceRulePassed", "ComplianceRuleFailed", "ComplianceRuleNoted"):
            state["rules"].append(
                {
                    "rule_id": payload.get("rule_id"),
                    "rule_name": payload.get("rule_name"),
                    "rule_version": payload.get("rule_version"),
                    "result": event.get("event_type"),
                    "is_hard_block": payload.get("is_hard_block"),
                    "evaluated_at": payload.get("evaluated_at"),
                }
            )
        elif event.get("event_type") == "ComplianceCheckCompleted":
            state["overall_verdict"] = payload.get("overall_verdict")

        self.current[app_id] = state
        self.history.append(
            {
                "application_id": app_id,
                "recorded_at": recorded_at,
                "state": dict(state),
            }
        )

    def snapshot(self, application_id: str) -> dict:
        return dict(self.current.get(application_id) or {})


async def run_what_if(
    store,
    application_id: str,
    branch_at_event_type: str,
    counterfactual_events: list[BaseEvent],
    projections: list,
) -> WhatIfResult:
    """
    Evaluate a counterfactual branch without mutating the real store.
    """
    all_events = []
    async for ev in store.load_all(from_global_position=0):
        payload = ev.get("payload", {}) or {}
        if payload.get("application_id") == application_id:
            all_events.append(ev)

    all_events.sort(key=lambda e: e.get("global_position", 0))
    branch_index = next(
        (i for i, e in enumerate(all_events) if e.get("event_type") == branch_at_event_type),
        None,
    )
    if branch_index is None:
        raise ValueError(f"Branch event {branch_at_event_type} not found for application_id={application_id}")

    pre_branch = all_events[:branch_index]
    branch_event = all_events[branch_index]
    post_branch = all_events[branch_index + 1 :]

    injected_events = []
    for ev in counterfactual_events:
        payload = ev.to_store_dict().get("payload", {}) if isinstance(ev, BaseEvent) else ev.get("payload", {})
        injected_events.append(
            {
                "event_id": str(uuid4()),
                "event_type": ev.event_type if isinstance(ev, BaseEvent) else ev.get("event_type"),
                "event_version": getattr(ev, "event_version", 1),
                "payload": payload,
                "metadata": {"counterfactual": True},
                "recorded_at": datetime.now().isoformat(),
                "global_position": None,
            }
        )

    dependent_ids = {branch_event.get("event_id")}
    for e in injected_events:
        dependent_ids.add(e.get("event_id"))

    filtered_post = []
    for e in post_branch:
        causation = (e.get("metadata") or {}).get("causation_id")
        if causation and causation in dependent_ids:
            dependent_ids.add(e.get("event_id"))
            continue
        filtered_post.append(e)

    real_events = pre_branch + [branch_event] + post_branch
    counterfactual_events_seq = pre_branch + injected_events + filtered_post

    async def _apply(events: Iterable[dict], projection_list: list):
        for e in events:
            for p in projection_list:
                if p.subscribed_event_types and e.get("event_type") not in p.subscribed_event_types:
                    continue
                await p.handle(e, None)
        return {
            "application_summary": projection_list[0].snapshot(application_id)
            if projection_list else {}
        }

    real_proj = [type(p)() for p in projections]
    cf_proj = [type(p)() for p in projections]
    real_outcome = await _apply(real_events, real_proj)
    counter_outcome = await _apply(counterfactual_events_seq, cf_proj)

    return WhatIfResult(
        real_outcome=real_outcome,
        counterfactual_outcome=counter_outcome,
        divergence_events=[branch_event] + injected_events,
    )
