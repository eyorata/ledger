"""
scripts/run_whatif.py
Run a what-if projection for a given application.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
from datetime import datetime

from dotenv import load_dotenv

from ledger.event_store import EventStore
from ledger.schema.events import CreditAnalysisCompleted, CreditDecision, RiskTier
from ledger.what_if import (
    run_what_if,
    InMemoryApplicationSummaryProjection,
)


async def main():
    load_dotenv()
    parser = argparse.ArgumentParser()
    parser.add_argument("--application", required=True)
    parser.add_argument("--substitute-credit-tier", required=True)
    parser.add_argument("--output", default="artifacts/counterfactual_narr05.json")
    args = parser.parse_args()

    if "→" in args.substitute_credit_tier:
        _, new_tier = args.substitute_credit_tier.split("→", 1)
    elif "->" in args.substitute_credit_tier:
        _, new_tier = args.substitute_credit_tier.split("->", 1)
    else:
        new_tier = args.substitute_credit_tier
    new_tier = new_tier.strip().upper()

    db_url = os.getenv("DATABASE_URL", "postgresql://localhost/apex_ledger")
    store = EventStore(db_url)
    await store.connect()

    counterfactual = CreditAnalysisCompleted(
        application_id=args.application,
        session_id="what-if",
        decision=CreditDecision(
            risk_tier=RiskTier(new_tier),
            recommended_limit_usd=0,
            confidence=0.65,
            rationale="Counterfactual credit tier override.",
        ),
        model_version="what-if-model",
        model_deployment_id="what-if",
        input_data_hash="what-if",
        analysis_duration_ms=0,
        completed_at=datetime.utcnow(),
    )

    result = await run_what_if(
        store,
        application_id=args.application,
        branch_at_event_type="CreditAnalysisCompleted",
        counterfactual_events=[counterfactual],
        projections=[InMemoryApplicationSummaryProjection()],
    )

    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(
            {
                "application_id": args.application,
                "real_outcome": result.real_outcome,
                "counterfactual_outcome": result.counterfactual_outcome,
                "divergence_events": result.divergence_events,
            },
            f,
            indent=2,
            default=str,
        )

    await store.close()


if __name__ == "__main__":
    asyncio.run(main())
