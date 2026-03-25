"""
scripts/demo_what_if.py
Run a simple what-if scenario for credit risk tier change.
"""
import asyncio
import os
from datetime import datetime

from dotenv import load_dotenv
from ledger.event_store import EventStore
from ledger.schema.events import CreditAnalysisCompleted, CreditDecision, RiskTier
from ledger.what_if import run_what_if, InMemoryApplicationSummaryProjection


async def main():
    load_dotenv()
    db_url = os.getenv("DATABASE_URL", "postgresql://localhost/apex_ledger")
    store = EventStore(db_url)
    await store.connect()

    application_id = os.getenv("WHAT_IF_APPLICATION_ID", "APEX-0021")
    counterfactual = CreditAnalysisCompleted(
        application_id=application_id,
        session_id="what-if",
        decision=CreditDecision(
            risk_tier=RiskTier.HIGH,
            recommended_limit_usd=0,
            confidence=0.65,
            rationale="Counterfactual high risk scenario.",
        ),
        model_version="what-if-model",
        model_deployment_id="what-if",
        input_data_hash="what-if",
        analysis_duration_ms=0,
        completed_at=datetime.utcnow(),
    )

    result = await run_what_if(
        store,
        application_id=application_id,
        branch_at_event_type="CreditAnalysisCompleted",
        counterfactual_events=[counterfactual],
        projections=[InMemoryApplicationSummaryProjection()],
    )

    print("Real outcome:", result.real_outcome)
    print("Counterfactual outcome:", result.counterfactual_outcome)

    await store.close()


if __name__ == "__main__":
    asyncio.run(main())
