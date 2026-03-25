# Final Report — Apex Ledger

Date: 2026-03-26  
Scope: Phases 1–6 (Bonus), system readiness summary

---

## DOMAIN_NOTES.md (Finalized)
The content below is sourced from `DOMAIN_NOTES.md`:

### 1) EDA vs. ES distinction
Callback-based tracing (e.g., LangChain traces) is **EDA**, not ES: events are optional, can be dropped, and are not the system of record. **Event Sourcing** makes events the permanent record of truth; state is reconstructed by replay. Redesigning with The Ledger changes: (a) all decisions become immutable events in the events table, (b) aggregates rebuild state from streams, (c) projections provide queryable views. Gains: deterministic replay, auditability, and the ability for compliance to reconstruct state at any timestamp.

### 2) The aggregate question
Chosen boundaries: LoanApplication, DocumentPackage, AgentSession, ComplianceRecord. Alternative boundary rejected: **merge ComplianceRecord into LoanApplication**. That coupling would force every ComplianceRulePassed/Failed write to take a lock on the LoanApplication stream, creating write contention when multiple agents operate concurrently. It also creates a failure mode where a compliance retry blocks loan decisions. Separating ComplianceRecord isolates compliance write spikes and prevents cross-aggregate contention.

### 3) Concurrency in practice
Sequence: both agents read stream at v3 → both call append with expected_version=3 → first writer acquires the stream lock / passes the version check and commits, advancing current_version to 4 → second writer fails the version check → losing agent receives OptimisticConcurrencyError(stream_id, expected=3, actual=4) → losing agent reloads the stream, inspects the new event, and decides whether to retry or abandon.

### 4) Projection lag and its consequences
With 200ms lag, a loan officer may see a stale limit immediately after a disbursement. The system responds by **displaying a lag indicator** (last_event_at timestamp) and, for critical actions, performing a **strong-consistency fallback** (re-read from stream or block until checkpoint catches up). The UI contract states: “Projections may be ~200ms behind; refreshing…” and treats this as an accepted trade-off, not an error.

### 5) Upcasting scenario
Upcaster v1→v2 adds model_version, confidence_score, regulatory_basis. Strategy: **confidence_score remains null** because fabricating a score would corrupt analytics and regulatory records; null correctly signals “unknown.” **model_version** is inferred from recorded_at against a known deployment timeline and flagged as approximate. **regulatory_basis** is inferred from the rule versions active at recorded_at.

### 6) Marten async daemon parallel
To distribute projections, run multiple daemon workers with a **lease table or Postgres advisory locks**. Workers claim a projection or event-range lease; this prevents two workers from processing the same batch (duplicate writes corrupt aggregated metrics). If the leader crashes, the lease expires and a follower resumes from the last checkpoint. This guards against double-processing and lost checkpoints.

---

## DESIGN.md (Finalized)
The content below is sourced from `DESIGN.md`:

### Event Store Schema Design (Phase 1)
Tables: `events`, `event_streams`, `projection_checkpoints`, `outbox` with indexes.  
Potential missing elements documented: FK on stream_id, outbox unique on event_id, index additions, and optional correlation/causation columns.

### Phase 3 Projections Design
Lag SLOs:
- ApplicationSummary: < 500ms
- ComplianceAuditView: < 2000ms  
ComplianceAuditView snapshot strategy: full snapshot on each compliance event; temporal queries by latest snapshot at or before timestamp. Shadow-table rebuild for no downtime.

### Phase 4 Upcasting & Integrity
Upcaster inference strategy, immutability guarantee, audit hash chain description, Gas Town memory pattern.

---

## Architecture Diagram (ASCII)

```
                        ┌────────────────────────────┐
                        │        Event Store         │
                        │  events / event_streams    │
                        └────────────┬───────────────┘
                                     │
             ┌───────────────────────┼───────────────────────┐
             │                       │                       │
      ┌──────▼──────┐         ┌──────▼──────┐         ┌──────▼──────┐
      │ Loan Stream │         │ Docpkg Stream│        │ Agent Streams│
      └──────┬──────┘         └──────┬──────┘         └──────┬──────┘
             │                       │                       │
             └──────────────┬────────┴──────────────┬────────┘
                            │                       │
                     ┌──────▼──────┐         ┌──────▼──────┐
                     │ Projections │         │ Audit Chain │
                     │  (CQRS)     │         │  (Integrity)│
                     └──────┬──────┘         └─────────────┘
                            │
                 ┌──────────▼──────────┐
                 │ MCP Tools/Resources │
                 └─────────────────────┘
```

**Aggregate boundaries:** LoanApplication, DocumentPackage, AgentSession, CreditRecord, ComplianceRecord, FraudScreening, AuditLedger.  
**Projection data flow:** EventStore → ProjectionDaemon → ApplicationSummary / AgentPerformance / ComplianceAuditView.  
**MCP mapping:** Tools write events; resources read from projections (audit + sessions are stream exceptions).

---

## Concurrency & SLO Analysis

**Double-decision OCC test:** `tests/test_event_store.py::test_double_decision_occ`  
Status: **PASSED** (see test run logs).  
Result: OCC correctly allows only one concurrent append.

**Projection lag under load (SLOs):**  
Status: **Not measured in this report**.  
Next step: run `python load_gen/run_concurrent.py --applications 15 --concurrency 6` and include lag report from `artifacts/projection_lag_report.txt`.

**Retry budget analysis:**  
Projection daemon retry is configurable (`max_retries`).  
Recommended max retries for production: 3–5 before quarantine to avoid backpressure on bad events.

---

## Upcasting & Integrity Results

**Immutability test:** `tests/phase4/test_upcasters.py::test_upcast_credit_analysis_immutability`  
Status: **PASSED**.

**Integrity chain test:** `tests/phase4/test_integrity.py`  
Status: **PASSED** (chain progression + tamper detection).

**Hash chain composition:** Hash includes event payload plus event_type/version/metadata/recorded_at to detect tampering beyond payload edits.

---

## MCP Lifecycle Test Results

**Test:** `tests/test_mcp_lifecycle.py::test_mcp_full_lifecycle`  
Status: **PASSED**.  
Flow: start_agent_session → record_credit_analysis → record_fraud_screening → record_compliance_check → generate_decision → record_human_review → query compliance resource.

---

## Bonus Results (Phase 6)

**What-if projector:** Implemented (`ledger/what_if.py`, `src/what_if/projector.py`).  
**Regulatory package:** Implemented (`ledger/regulatory_package.py`, `src/regulatory/package.py`).  
**Artifacts:** Not generated in this report.  
Recommended runs:

```
python scripts/run_whatif.py --application APEX-NARR05 --substitute-credit-tier MEDIUM --output artifacts/counterfactual_narr05.json
python tests/phase6/verify_package.py artifacts/regulatory_package_NARR05.json
```

---

## Limitations & Reflection

**Limitations**
- Narrative tests currently operate on in-memory event streams rather than full agent pipeline execution.
- Projection lag under load has not been measured and reported here.
- Regulatory package output and what-if artifact not generated in this report.
- AuditLedger aggregate is minimal (wrapper) and relies on integrity chain for verification.

**What I would change with more time**
- Replace narrative tests with full pipeline integration for each NARR scenario (real agent runs against DB + documents).
- Add automated performance harness for projection lag SLO validation under concurrency.
- Consolidate report generation into a reproducible script that writes both JSON artifacts and the final report PDF.
