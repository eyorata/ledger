"""Async projection daemon."""
from __future__ import annotations
import asyncio
import logging
from collections import defaultdict
from datetime import datetime

logger = logging.getLogger(__name__)


class ProjectionDaemon:
    def __init__(self, store, projections: list, max_retries: int = 3):
        self._store = store
        self._projections = {p.name: p for p in projections}
        self._running = False
        self._retry_counts = defaultdict(int)
        self._max_retries = max_retries

    async def run_forever(self, poll_interval_ms: int = 100) -> None:
        self._running = True
        while self._running:
            await self._process_batch()
            await asyncio.sleep(poll_interval_ms / 1000)

    def stop(self) -> None:
        self._running = False

    async def _process_batch(self) -> None:
        checkpoints = {}
        for name, proj in self._projections.items():
            cp = await self._store.load_checkpoint(name)
            proj.last_processed_position = cp
            checkpoints[name] = cp

        min_checkpoint = min(checkpoints.values()) if checkpoints else 0
        latest_global_pos, latest_recorded_at = await self._store.latest_event_info()

        async for event in self._store.load_all(from_global_position=min_checkpoint):
            event_gp = event.get("global_position")
            for name, proj in self._projections.items():
                if event_gp <= proj.last_processed_position:
                    continue
                if proj.subscribed_event_types and event.get("event_type") not in proj.subscribed_event_types:
                    proj.last_processed_position = event_gp
                    await self._store.save_checkpoint(name, proj.last_processed_position)
                    continue

                key = (name, event.get("event_id") or event_gp)
                try:
                    await proj.handle(event, self._store)
                    proj.last_processed_position = event_gp
                    proj.last_processed_at = event.get("recorded_at")
                    await self._store.save_checkpoint(name, proj.last_processed_position)
                except Exception:
                    logger.exception(
                        "Projection %s failed on event %s (gp=%s)",
                        name,
                        event.get("event_type"),
                        event_gp,
                    )
                    self._retry_counts[key] += 1
                    if self._retry_counts[key] >= self._max_retries:
                        proj.last_processed_position = event_gp
                        proj.last_processed_at = event.get("recorded_at")
                        await self._store.save_checkpoint(name, proj.last_processed_position)
                        continue
                    continue

        # update lag metrics
        for proj in self._projections.values():
            if latest_recorded_at and proj.last_processed_at:
                if isinstance(latest_recorded_at, str):
                    latest_recorded_at = datetime.fromisoformat(latest_recorded_at)
                if isinstance(proj.last_processed_at, str):
                    proj.last_processed_at = datetime.fromisoformat(proj.last_processed_at)
                lag_ms = (latest_recorded_at - proj.last_processed_at).total_seconds() * 1000
                proj.set_lag(max(lag_ms, 0.0))
            else:
                if latest_recorded_at and proj.last_processed_position >= 0:
                    proj.set_lag(0.0)
                else:
                    proj.set_lag(None)

    def get_lag(self, projection_name: str) -> float | None:
        proj = self._projections.get(projection_name)
        return proj.get_lag() if proj else None

    def get_projection_lag(self) -> dict[str, float | None]:
        return {name: proj.get_lag() for name, proj in self._projections.items()}
