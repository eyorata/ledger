"""ComplianceAuditView projection with temporal queries."""
from __future__ import annotations
from datetime import datetime
from typing import Any

from ledger.projections.base import Projection


COMPLIANCE_EVENTS = {
    "ComplianceCheckInitiated",
    "ComplianceRulePassed",
    "ComplianceRuleFailed",
    "ComplianceRuleNoted",
    "ComplianceCheckCompleted",
}


class ComplianceAuditViewProjection(Projection):
    name = "compliance_audit_view"
    subscribed_event_types = COMPLIANCE_EVENTS

    def __init__(self):
        super().__init__()

    async def handle(self, event: dict, store) -> None:
        if event.get("event_type") not in COMPLIANCE_EVENTS:
            return
        payload = event.get("payload", {})
        app_id = payload.get("application_id")
        if not app_id:
            return
        recorded_at = event.get("recorded_at")
        if isinstance(recorded_at, str):
            recorded_at = datetime.fromisoformat(recorded_at)

        current = await self.get_current_compliance(store, app_id)
        state = current["state"] if current else {
            "rules": [],
            "overall_verdict": None,
            "regulation_set_version": None,
        }

        if event.get("event_type") == "ComplianceCheckInitiated":
            state["regulation_set_version"] = payload.get("regulation_set_version")
        elif event.get("event_type") in ("ComplianceRulePassed", "ComplianceRuleFailed", "ComplianceRuleNoted"):
            state["rules"].append(
                {
                    "rule_id": payload.get("rule_id"),
                    "rule_name": payload.get("rule_name"),
                    "rule_version": payload.get("rule_version"),
                    "result": event.get("event_type"),
                    "is_hard_block": payload.get("is_hard_block"),
                    "evaluated_at": payload.get("evaluated_at"),
                }
            )
        elif event.get("event_type") == "ComplianceCheckCompleted":
            state["overall_verdict"] = payload.get("overall_verdict")

        async with store._pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO compliance_audit_current(application_id, state, last_event_at)"
                " VALUES($1, $2::jsonb, $3)"
                " ON CONFLICT (application_id) DO UPDATE SET"
                " state = EXCLUDED.state, last_event_at = EXCLUDED.last_event_at",
                app_id,
                __import__("json").dumps(state),
                recorded_at,
            )
            await conn.execute(
                "INSERT INTO compliance_audit_history(application_id, recorded_at, state)"
                " VALUES($1, $2, $3::jsonb)",
                app_id,
                recorded_at,
                __import__("json").dumps(state),
            )

    async def get_current_compliance(self, store, application_id: str) -> dict[str, Any] | None:
        async with store._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT application_id, state, last_event_at FROM compliance_audit_current"
                " WHERE application_id=$1",
                application_id,
            )
            if not row:
                return None
            return {
                "application_id": row["application_id"],
                "state": row["state"],
                "last_event_at": row["last_event_at"],
            }

    async def get_compliance_at(self, store, application_id: str, ts: datetime) -> dict[str, Any] | None:
        async with store._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT application_id, state, recorded_at FROM compliance_audit_history"
                " WHERE application_id=$1 AND recorded_at <= $2"
                " ORDER BY recorded_at DESC LIMIT 1",
                application_id,
                ts,
            )
            if not row:
                return None
            return {
                "application_id": row["application_id"],
                "state": row["state"],
                "recorded_at": row["recorded_at"],
            }

    async def rebuild_from_scratch(self, store) -> None:
        # Shadow-table rebuild for no-downtime swap
        async with store._pool.acquire() as conn:
            await conn.execute("CREATE TABLE IF NOT EXISTS compliance_audit_current_rebuild (LIKE compliance_audit_current INCLUDING ALL)")
            await conn.execute("CREATE TABLE IF NOT EXISTS compliance_audit_history_rebuild (LIKE compliance_audit_history INCLUDING ALL)")
            await conn.execute("TRUNCATE compliance_audit_current_rebuild")
            await conn.execute("TRUNCATE compliance_audit_history_rebuild")

        async for event in store.load_all(from_global_position=0, event_types=list(COMPLIANCE_EVENTS)):
            await self._handle_to_tables(store, event, "compliance_audit_current_rebuild", "compliance_audit_history_rebuild")

        async with store._pool.acquire() as conn:
            await conn.execute("BEGIN")
            await conn.execute("ALTER TABLE compliance_audit_current RENAME TO compliance_audit_current_old")
            await conn.execute("ALTER TABLE compliance_audit_current_rebuild RENAME TO compliance_audit_current")
            await conn.execute("ALTER TABLE compliance_audit_history RENAME TO compliance_audit_history_old")
            await conn.execute("ALTER TABLE compliance_audit_history_rebuild RENAME TO compliance_audit_history")
            await conn.execute("DROP TABLE compliance_audit_current_old")
            await conn.execute("DROP TABLE compliance_audit_history_old")
            await conn.execute("COMMIT")

    async def _handle_to_tables(self, store, event: dict, current_table: str, history_table: str) -> None:
        payload = event.get("payload", {})
        app_id = payload.get("application_id")
        if not app_id:
            return
        recorded_at = event.get("recorded_at")
        if isinstance(recorded_at, str):
            recorded_at = datetime.fromisoformat(recorded_at)
        async with store._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"SELECT state FROM {current_table} WHERE application_id=$1",
                app_id,
            )
            if row:
                state = row["state"]
                if isinstance(state, str):
                    state = __import__("json").loads(state)
            else:
                state = {"rules": [], "overall_verdict": None, "regulation_set_version": None}

        if event.get("event_type") == "ComplianceCheckInitiated":
            state["regulation_set_version"] = payload.get("regulation_set_version")
        elif event.get("event_type") in ("ComplianceRulePassed", "ComplianceRuleFailed", "ComplianceRuleNoted"):
            state["rules"].append(
                {
                    "rule_id": payload.get("rule_id"),
                    "rule_name": payload.get("rule_name"),
                    "rule_version": payload.get("rule_version"),
                    "result": event.get("event_type"),
                    "is_hard_block": payload.get("is_hard_block"),
                    "evaluated_at": payload.get("evaluated_at"),
                }
            )
        elif event.get("event_type") == "ComplianceCheckCompleted":
            state["overall_verdict"] = payload.get("overall_verdict")

        async with store._pool.acquire() as conn:
            await conn.execute(
                f"INSERT INTO {current_table}(application_id, state, last_event_at)"
                " VALUES($1, $2::jsonb, $3)"
                " ON CONFLICT (application_id) DO UPDATE SET"
                " state = EXCLUDED.state, last_event_at = EXCLUDED.last_event_at",
                app_id,
                __import__("json").dumps(state),
                recorded_at,
            )
            await conn.execute(
                f"INSERT INTO {history_table}(application_id, recorded_at, state)"
                " VALUES($1, $2, $3::jsonb)",
                app_id,
                recorded_at,
                __import__("json").dumps(state),
            )
