# Event Store Schema Design (Phase 1)

## Purpose
This schema supports an append-only event store with per-stream ordering, global ordering, projection checkpoints, and an outbox for downstream delivery.

## Table: events
- event_id: Primary key for immutable event identity and foreign key target for outbox.
- stream_id: Groups events by aggregate stream; required for replay.
- stream_position: Monotonic position within a stream; required for optimistic concurrency and deterministic ordering.
- global_position: Monotonic global order; required for projections and replay across streams.
- event_type: Discriminator for schema registry and consumer routing.
- event_version: Schema evolution version for upcasting.
- payload: Event body; JSONB to store structured data.
- metadata: Operational context such as correlation_id/causation_id and other trace data.
- recorded_at: Time of persistence; used for audit and replay windows.
- uq_stream_position: Enforces single writer per stream position.

Indexes:
- idx_events_stream_id: Fast stream replay.
- idx_events_global_pos: Fast global replay.
- idx_events_type: Fast filtering by type.
- idx_events_recorded: Time-window queries and audits.

## Table: event_streams
- stream_id: Primary key to identify the stream.
- aggregate_type: Partitioning by aggregate and diagnostics.
- current_version: Optimistic concurrency check.
- created_at: Stream creation timestamp.
- archived_at: Soft archive marker.
- metadata: Stream-level metadata (tenant, owner, tags).

## Table: projection_checkpoints
- projection_name: Unique projection identifier.
- last_position: Last processed global_position.
- updated_at: Operational observability for projection health.

## Table: outbox
- id: Primary key for outbox entry.
- event_id: Links to the source event.
- destination: Logical sink for routing.
- payload: Serialized message for transport.
- created_at: Outbox creation time.
- published_at: Acknowledged publish time.
- attempts: Retry counter for delivery.

## Potential Missing Elements (Recommended)
- Foreign key from events.stream_id to event_streams.stream_id for referential integrity. Consider performance impact on high write volumes.
- Unique constraint on outbox.event_id to prevent duplicate dispatch entries per event.
- Indexes on event_streams.archived_at and projection_checkpoints.updated_at for operational queries.
- Optional top-level columns for correlation_id and causation_id if you want indexable tracing without JSONB scans.
- CHECK constraints for non-negative stream_position, current_version, event_version, and attempts.
- Consider partitioning events by time or stream_id for large-scale datasets.

# Phase 3 Projections Design

## Projection SLOs (Lag Contract)
- ApplicationSummary lag SLO: < 500ms in normal operation.
- ComplianceAuditView lag SLO: < 2000ms in normal operation.
Lag is defined as the time difference between the latest event recorded_at in the event store and the last event processed by a projection.

## ComplianceAuditView Snapshot Strategy
We store a full compliance state snapshot on every compliance event in `compliance_audit_history` (append-only), and keep the latest snapshot in `compliance_audit_current` for fast reads. Temporal queries (`get_compliance_at`) select the most recent snapshot at or before a timestamp. This strategy is simple, deterministic, and supports regulatory time-travel with minimal complexity because compliance event volume is low relative to full system events. For rebuilds, a shadow-table swap is used to avoid downtime for live reads.

## Rebuild Without Downtime
`rebuild_from_scratch()` writes to shadow tables and swaps them into place in a transaction to keep reads available during replays.

# Phase 4 Upcasting & Integrity

## Upcaster Inference Strategy
- CreditAnalysisCompleted v1→v2: model_version inferred from recorded_at (2026+ → `legacy-2026`, else `legacy-pre-2026`); confidence_score set to `null` because fabrication would pollute auditability; regulatory_basis inferred from rule sets active at recorded_at (here, `2026-Q1` for 2026+ and `2025-Q4` for earlier events).
- DecisionGenerated v1→v2: model_versions reconstructed by loading each contributing agent session’s `AgentSessionStarted` event; performance implication is a lookup per session on read, acceptable for low-volume historical loads but should be cached or pre-joined for high-throughput replay.

## Immutability Guarantee
Upcasters run on read only. The immutability test verifies that the raw stored payload stays byte-identical while the loaded event is transparently upcasted to the latest version.

## Audit Hash Chain
Each `AuditIntegrityCheckRun` records a hash over the payloads of the next segment of events plus the previous integrity hash. We validate the existing chain by recomputing prior segments; any mismatch flags tampering. The audit stream is append-only and provides tamper-evident integrity checks.

## Gas Town Memory Pattern
Agent sessions are reconstructed by replaying the agent session stream, summarizing older events, and preserving verbatim the last three events plus any error events. If the last event indicates a partial output without completion, the context is flagged `NEEDS_RECONCILIATION` so the agent must resolve the partial state before proceeding.
