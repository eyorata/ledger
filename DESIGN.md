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
