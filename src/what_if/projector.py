"""
src/what_if/projector.py
Phase 6: What-If projector (counterfactual branch evaluation).
"""
from __future__ import annotations

from ledger.what_if import (
    run_what_if,
    WhatIfResult,
    InMemoryApplicationSummaryProjection,
    InMemoryComplianceAuditViewProjection,
)

__all__ = [
    "run_what_if",
    "WhatIfResult",
    "InMemoryApplicationSummaryProjection",
    "InMemoryComplianceAuditViewProjection",
]
