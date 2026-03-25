-- Thin wrapper include for schema
-- This file mirrors the root schema.sql for spec path compatibility.
-- Keep in sync with ../../schema.sql
-- (Use root schema.sql as source of truth.)
-- Event store schema (Phase 1)
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE events (
  event_id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  stream_id        TEXT NOT NULL,
  stream_position  BIGINT NOT NULL,
  global_position  BIGINT GENERATED ALWAYS AS IDENTITY,
  event_type       TEXT NOT NULL,
  event_version    SMALLINT NOT NULL DEFAULT 1,
  payload          JSONB NOT NULL,
  metadata         JSONB NOT NULL DEFAULT '{}'::jsonb,
  recorded_at      TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  CONSTRAINT uq_stream_position UNIQUE (stream_id, stream_position)
);

CREATE INDEX idx_events_stream_id ON events (stream_id, stream_position);
CREATE INDEX idx_events_global_pos ON events (global_position);
CREATE INDEX idx_events_type ON events (event_type);
CREATE INDEX idx_events_recorded ON events (recorded_at);

CREATE TABLE event_streams (
  stream_id        TEXT PRIMARY KEY,
  aggregate_type   TEXT NOT NULL,
  current_version  BIGINT NOT NULL DEFAULT 0,
  created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  archived_at      TIMESTAMPTZ,
  metadata         JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE projection_checkpoints (
  projection_name  TEXT PRIMARY KEY,
  last_position    BIGINT NOT NULL DEFAULT 0,
  updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE outbox (
  id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  event_id         UUID NOT NULL REFERENCES events(event_id),
  destination      TEXT NOT NULL,
  payload          JSONB NOT NULL,
  created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  published_at     TIMESTAMPTZ,
  attempts         SMALLINT NOT NULL DEFAULT 0
);

-- Projections
CREATE TABLE application_summary (
  application_id          TEXT PRIMARY KEY,
  state                   TEXT,
  applicant_id            TEXT,
  requested_amount_usd    NUMERIC,
  approved_amount_usd     NUMERIC,
  risk_tier               TEXT,
  fraud_score             NUMERIC,
  compliance_status       TEXT,
  decision                TEXT,
  agent_sessions_completed TEXT[],
  last_event_type         TEXT,
  last_event_at           TIMESTAMPTZ,
  human_reviewer_id       TEXT,
  final_decision_at       TIMESTAMPTZ
);

CREATE TABLE agent_performance_ledger (
  agent_id                TEXT NOT NULL,
  model_version           TEXT NOT NULL,
  analyses_completed      BIGINT NOT NULL DEFAULT 0,
  decisions_generated     BIGINT NOT NULL DEFAULT 0,
  avg_confidence_score    NUMERIC,
  avg_duration_ms         NUMERIC,
  approve_rate            NUMERIC NOT NULL DEFAULT 0,
  decline_rate            NUMERIC NOT NULL DEFAULT 0,
  refer_rate              NUMERIC NOT NULL DEFAULT 0,
  human_override_rate     NUMERIC NOT NULL DEFAULT 0,
  first_seen_at           TIMESTAMPTZ,
  last_seen_at            TIMESTAMPTZ,
  PRIMARY KEY (agent_id, model_version)
);

CREATE TABLE compliance_audit_current (
  application_id          TEXT PRIMARY KEY,
  state                   JSONB NOT NULL,
  last_event_at           TIMESTAMPTZ
);

CREATE TABLE compliance_audit_history (
  application_id          TEXT NOT NULL,
  recorded_at             TIMESTAMPTZ NOT NULL,
  state                   JSONB NOT NULL
);

CREATE INDEX idx_compliance_audit_history_app_time ON compliance_audit_history (application_id, recorded_at);
