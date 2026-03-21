# DOMAIN_NOTES.md

## 1) EDA vs. ES distinction
Callback-based tracing (e.g., LangChain traces) is **EDA**, not ES: events are optional, can be dropped, and are not the system of record. **Event Sourcing** makes events the permanent record of truth; state is reconstructed by replay. Redesigning with The Ledger changes: (a) all decisions become immutable events in the events table, (b) aggregates rebuild state from streams, (c) projections provide queryable views. Gains: deterministic replay, auditability, and the ability for compliance to reconstruct state at any timestamp.

## 2) The aggregate question
Chosen boundaries: LoanApplication, DocumentPackage, AgentSession, ComplianceRecord. Alternative boundary rejected: **merge ComplianceRecord into LoanApplication**. That coupling would force every ComplianceRulePassed/Failed write to take a lock on the LoanApplication stream, creating write contention when multiple agents operate concurrently. It also creates a failure mode where a compliance retry blocks loan decisions. Separating ComplianceRecord isolates compliance write spikes and prevents cross‑aggregate contention.

## 3) Concurrency in practice
Sequence: both agents read stream at v3 → both call append with expected_version=3 → first writer acquires the stream lock / passes the version check and commits, advancing current_version to 4 → second writer fails the version check → losing agent receives OptimisticConcurrencyError(stream_id, expected=3, actual=4) → losing agent reloads the stream, inspects the new event, and decides whether to retry or abandon.

## 4) Projection lag and its consequences
With 200ms lag, a loan officer may see a stale limit immediately after a disbursement. The system responds by **displaying a lag indicator** (last_event_at timestamp) and, for critical actions, performing a **strong-consistency fallback** (re-read from stream or block until checkpoint catches up). The UI contract states: “Projections may be ~200ms behind; refreshing…” and treats this as an accepted trade‑off, not an error.

## 5) Upcasting scenario
Upcaster v1→v2 adds model_version, confidence_score, regulatory_basis. Strategy: **confidence_score remains null** because fabricating a score would corrupt analytics and regulatory records; null correctly signals “unknown.” **model_version** is inferred from recorded_at against a known deployment timeline and flagged as approximate. **regulatory_basis** is inferred from the rule versions active at recorded_at.

```
def upcast_v1_to_v2(event):
    ts = event["recorded_at"]
    payload = dict(event["payload"])
    payload["confidence_score"] = None
    payload["model_version"] = "legacy-pre-2026" if ts < "2026-01-01" else "legacy-2026"
    payload["regulatory_basis"] = ["2025-Q4"] if ts < "2026-01-01" else ["2026-Q1"]
    event["payload"] = payload
    event["event_version"] = 2
    return event
```

## 6) Marten async daemon parallel
To distribute projections, run multiple daemon workers with a **lease table or Postgres advisory locks**. Workers claim a projection or event‑range lease; this prevents two workers from processing the same batch (duplicate writes corrupt aggregated metrics). If the leader crashes, the lease expires and a follower resumes from the last checkpoint. This guards against double‑processing and lost checkpoints.
