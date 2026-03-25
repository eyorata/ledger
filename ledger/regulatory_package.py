"""
ledger/regulatory_package.py
============================
Phase 6: Regulatory examination package generator.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from ledger.audit.integrity import run_integrity_check


@dataclass
class RegulatoryPackage:
    application_id: str
    examination_date: str
    events: list[dict]
    projections: dict[str, Any]
    audit_integrity: dict[str, Any]
    narrative: list[str]
    agent_decisions: list[dict]


def _event_sentence(event: dict) -> str:
    et = event.get("event_type")
    payload = event.get("payload", {}) or {}
    app_id = payload.get("application_id", "")
    if et == "ApplicationSubmitted":
        return f"Application {app_id} was submitted."
    if et == "CreditAnalysisCompleted":
        decision = payload.get("decision", {})
        return f"Credit analysis completed with risk tier {decision.get('risk_tier')}."
    if et == "FraudScreeningCompleted":
        return f"Fraud screening completed with score {payload.get('fraud_score')}."
    if et == "ComplianceCheckCompleted":
        return f"Compliance check completed with verdict {payload.get('overall_verdict')}."
    if et == "DecisionGenerated":
        return f"Decision generated: {payload.get('recommendation')}."
    if et == "HumanReviewCompleted":
        return f"Human review completed by {payload.get('reviewer_id')}."
    if et == "ApplicationApproved":
        return f"Application {app_id} approved for {payload.get('approved_amount_usd')}."
    if et == "ApplicationDeclined":
        return f"Application {app_id} declined."
    return f"Event {et} recorded."


async def generate_regulatory_package(
    store,
    application_id: str,
    examination_date: datetime,
    output_path: str | None = None,
) -> RegulatoryPackage:
    # Load all events related to this application_id
    events = []
    async for e in store.load_all(from_global_position=0):
        payload = e.get("payload", {}) or {}
        if payload.get("application_id") == application_id:
            events.append(e)
    events.sort(key=lambda ev: ev.get("global_position", 0))

    # Projection states as-of examination date
    projections: dict[str, Any] = {}
    if hasattr(store, "_pool") and store._pool:
        async with store._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM application_summary WHERE application_id=$1",
                application_id,
            )
            projections["application_summary"] = dict(row) if row else None

            row = await conn.fetchrow(
                "SELECT application_id, state, recorded_at FROM compliance_audit_history "
                "WHERE application_id=$1 AND recorded_at <= $2 "
                "ORDER BY recorded_at DESC LIMIT 1",
                application_id,
                examination_date,
            )
            projections["compliance_audit_view"] = dict(row) if row else None

    # Audit integrity check
    integrity = await run_integrity_check(store, "loan", application_id)
    audit_integrity = {
        "events_verified": integrity.events_verified,
        "chain_valid": integrity.chain_valid,
        "tamper_detected": integrity.tamper_detected,
    }

    # Narrative
    narrative = [_event_sentence(e) for e in events]

    # Agent decision metadata
    agent_decisions = []
    for e in events:
        et = e.get("event_type")
        payload = e.get("payload", {}) or {}
        if et == "CreditAnalysisCompleted":
            agent_decisions.append(
                {
                    "event_type": et,
                    "model_version": payload.get("model_version"),
                    "confidence": (payload.get("decision") or {}).get("confidence"),
                    "input_data_hash": payload.get("input_data_hash"),
                }
            )
        if et == "FraudScreeningCompleted":
            agent_decisions.append(
                {
                    "event_type": et,
                    "model_version": payload.get("screening_model_version"),
                    "confidence": None,
                    "input_data_hash": payload.get("input_data_hash"),
                }
            )
        if et == "DecisionGenerated":
            agent_decisions.append(
                {
                    "event_type": et,
                    "model_versions": payload.get("model_versions"),
                    "confidence": payload.get("confidence"),
                }
            )

    pkg = RegulatoryPackage(
        application_id=application_id,
        examination_date=examination_date.isoformat(),
        events=events,
        projections=projections,
        audit_integrity=audit_integrity,
        narrative=narrative,
        agent_decisions=agent_decisions,
    )

    if output_path:
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(pkg.__dict__, f, indent=2, default=str)

    return pkg
