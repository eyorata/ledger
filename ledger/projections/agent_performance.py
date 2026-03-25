"""AgentPerformanceLedger projection."""
from __future__ import annotations
from datetime import datetime

from ledger.projections.base import Projection


class AgentPerformanceLedgerProjection(Projection):
    name = "agent_performance_ledger"
    subscribed_event_types = {
        "AgentContextLoaded",
        "AgentSessionCompleted",
        "DecisionGenerated",
        "HumanReviewCompleted",
    }

    def __init__(self):
        super().__init__()
        self._session_map: dict[str, tuple[str, str]] = {}
        self._decision_map: dict[str, tuple[str, str]] = {}

    async def handle(self, event: dict, store) -> None:
        et = event.get("event_type")
        payload = event.get("payload", {})

        if et == "AgentContextLoaded":
            agent_id = payload.get("agent_id")
            model_version = payload.get("model_version")
            session_id = payload.get("session_id")
            if not (agent_id and model_version and session_id):
                return
            self._session_map[session_id] = (agent_id, model_version)
            await self._touch_row(store, agent_id, model_version, payload.get("loaded_at"))
            return

        if et == "AgentSessionCompleted":
            session_id = payload.get("session_id")
            agent_id, model_version = self._session_map.get(session_id, (None, None))
            if not agent_id or not model_version:
                return
            duration_ms = payload.get("total_duration_ms")
            await self._increment_analysis(store, agent_id, model_version, duration_ms)
            return

        if et == "DecisionGenerated":
            session_id = payload.get("orchestrator_session_id")
            agent_id, model_version = self._session_map.get(session_id, (None, None))
            if not agent_id or not model_version:
                return
            decision = payload.get("recommendation")
            confidence = payload.get("confidence")
            event_id = event.get("event_id")
            if event_id:
                self._decision_map[str(event_id)] = (agent_id, model_version)
            await self._increment_decision(store, agent_id, model_version, decision, confidence)
            return

        if et == "HumanReviewCompleted":
            if not payload.get("override"):
                return
            decision_event_id = payload.get("decision_event_id")
            key = self._decision_map.get(str(decision_event_id))
            if not key:
                return
            agent_id, model_version = key
            await self._increment_override(store, agent_id, model_version)

    async def _touch_row(self, store, agent_id: str, model_version: str, ts: str | None) -> None:
        dt = datetime.fromisoformat(ts) if isinstance(ts, str) else None
        async with store._pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO agent_performance_ledger("
                "agent_id, model_version, analyses_completed, decisions_generated,"
                "avg_confidence_score, avg_duration_ms, approve_rate, decline_rate, refer_rate,"
                "human_override_rate, first_seen_at, last_seen_at)"
                " VALUES($1,$2,0,0,NULL,NULL,0,0,0,0,$3,$3)"
                " ON CONFLICT (agent_id, model_version) DO UPDATE SET"
                " last_seen_at = COALESCE(EXCLUDED.last_seen_at, agent_performance_ledger.last_seen_at)",
                agent_id,
                model_version,
                dt,
            )

    async def _increment_analysis(self, store, agent_id: str, model_version: str, duration_ms: int | None) -> None:
        async with store._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT analyses_completed, avg_duration_ms FROM agent_performance_ledger"
                " WHERE agent_id=$1 AND model_version=$2",
                agent_id,
                model_version,
            )
            if not row:
                await self._touch_row(store, agent_id, model_version, None)
                analyses_completed = 0
                avg_duration = None
            else:
                analyses_completed = row["analyses_completed"]
                avg_duration = row["avg_duration_ms"]
            if duration_ms is not None:
                if avg_duration is None:
                    new_avg = duration_ms
                else:
                    new_avg = (avg_duration * analyses_completed + duration_ms) / (analyses_completed + 1)
            else:
                new_avg = avg_duration
            await conn.execute(
                "UPDATE agent_performance_ledger SET analyses_completed=$1, avg_duration_ms=$2, last_seen_at=NOW()"
                " WHERE agent_id=$3 AND model_version=$4",
                analyses_completed + 1,
                new_avg,
                agent_id,
                model_version,
            )

    async def _increment_decision(self, store, agent_id: str, model_version: str, decision: str, confidence: float | None) -> None:
        async with store._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT decisions_generated, avg_confidence_score, approve_rate, decline_rate, refer_rate"
                " FROM agent_performance_ledger WHERE agent_id=$1 AND model_version=$2",
                agent_id,
                model_version,
            )
            if not row:
                await self._touch_row(store, agent_id, model_version, None)
                decisions = 0
                avg_conf = None
                approve = decline = refer = 0
            else:
                decisions = row["decisions_generated"]
                avg_conf = row["avg_confidence_score"]
                approve = row["approve_rate"]
                decline = row["decline_rate"]
                refer = row["refer_rate"]

            if confidence is not None:
                if avg_conf is None:
                    new_avg_conf = confidence
                else:
                    new_avg_conf = (avg_conf * decisions + confidence) / (decisions + 1)
            else:
                new_avg_conf = avg_conf

            approve_count = approve * decisions
            decline_count = decline * decisions
            refer_count = refer * decisions
            if decision == "APPROVE":
                approve_count += 1
            elif decision == "DECLINE":
                decline_count += 1
            else:
                refer_count += 1
            new_total = decisions + 1

            await conn.execute(
                "UPDATE agent_performance_ledger SET decisions_generated=$1, avg_confidence_score=$2,"
                " approve_rate=$3, decline_rate=$4, refer_rate=$5, last_seen_at=NOW()"
                " WHERE agent_id=$6 AND model_version=$7",
                new_total,
                new_avg_conf,
                approve_count / new_total,
                decline_count / new_total,
                refer_count / new_total,
                agent_id,
                model_version,
            )

    async def _increment_override(self, store, agent_id: str, model_version: str) -> None:
        async with store._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT decisions_generated, human_override_rate FROM agent_performance_ledger"
                " WHERE agent_id=$1 AND model_version=$2",
                agent_id,
                model_version,
            )
            if not row:
                return
            decisions = row["decisions_generated"]
            override_rate = row["human_override_rate"]
            override_count = override_rate * decisions
            override_count += 1
            if decisions > 0:
                new_rate = override_count / decisions
            else:
                new_rate = 1.0
            await conn.execute(
                "UPDATE agent_performance_ledger SET human_override_rate=$1, last_seen_at=NOW()"
                " WHERE agent_id=$2 AND model_version=$3",
                new_rate,
                agent_id,
                model_version,
            )

    async def rebuild_from_scratch(self, store) -> None:
        # Reset projection state and reapply all events
        self._session_map.clear()
        self._decision_map.clear()
        async with store._pool.acquire() as conn:
            await conn.execute("TRUNCATE agent_performance_ledger")
        async for event in store.load_all(from_global_position=0, event_types=list(self.subscribed_event_types)):
            await self.handle(event, store)
