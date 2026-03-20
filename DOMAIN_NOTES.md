# DOMAIN_NOTES.md

## 1) EDA vs. ES distinction
The component described (callbacks like LangChain traces) is Event-Driven Architecture, not Event Sourcing. It emits events for observability, but the events are not the system of record and cannot reconstruct state. If redesigned with The Ledger, each domain decision (and its inputs) would be appended to an immutable stream, and aggregates would rebuild state by replaying events. The gain is strong auditability, replayability, temporal queries, and deterministic reconstruction of how a decision was made.

## 2) The aggregate question
Chosen boundaries: LoanApplication, DocumentPackage, AgentSession, ComplianceRecord (per spec). Alternative boundary considered: collapsing AgentSession into LoanApplication. I rejected it because it couples agent operational telemetry to domain state and forces domain reads to scan large session histories. Separate AgentSession prevents cross-cutting concerns (cost, tool usage, retries) from bloating the core business aggregate.

## 3) Concurrency in practice
Sequence: both agents read stream at v3; both call append with expected_version=3. The store locks stream row, checks current_version. One transaction wins, inserts events, updates current_version to 4. The losing transaction sees current_version=4 and receives OptimisticConcurrencyError. The losing agent must reload the stream, reconcile the new state, and decide whether to retry or abort.

## 4) Projection lag and its consequences
With 200ms lag, a loan officer may see a stale limit just after a disbursement event. The system should surface a freshness indicator in the UI and, for critical actions, re-check the write model or wait until the projection catches up. UX should communicate: "State may be up to 200ms behind. Refreshing..." and disable unsafe actions until the projection is current.

## 5) Upcasting scenario
Upcaster from v1 to v2 adds model_version, confidence_score, regulatory_basis.
Strategy: infer defaults for historical events: model_version="unknown", confidence_score=None (or 0.0 if required by schema), regulatory_basis=["legacy"]. Keep original reason and decision unchanged. Example pseudo-code:

```
if event_version == 1:
    payload["model_version"] = "unknown"
    payload["confidence_score"] = payload.get("confidence_score")  # None
    payload["regulatory_basis"] = payload.get("regulatory_basis") or ["legacy"]
    event_version = 2
```

## 6) Marten async daemon parallel
To distribute projections, run multiple daemon workers with a shared coordination primitive (Postgres advisory locks or a lease table). Each worker claims a projection shard or event range. The lock prevents two workers from processing the same projection events. This guards against duplicate projection writes and race conditions when checkpoints advance.
