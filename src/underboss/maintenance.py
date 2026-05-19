"""Maintenance sweeps — recover timed-out jobs and delete old ones.

Ported from pg-boss's manager maintenance. Two sweeps run on an interval:
``fail_expired_jobs`` returns jobs abandoned by a dead worker to retry/failed,
and ``delete_old_jobs`` purges jobs past their retention window. Both sweeps are
idempotent, so overlapping runs across nodes are safe (a per-node debounce is a
later refinement).
"""

from __future__ import annotations

import asyncio
import contextlib
import logging

from underboss import sql
from underboss.db import Database

_log = logging.getLogger("underboss.maintenance")


class Maintenance:
    """Periodically recovers timed-out jobs and deletes expired ones."""

    def __init__(self, db: Database, schema: str, *, interval_seconds: float = 60.0) -> None:
        self._db = db
        self._interval = interval_seconds
        self._timeout_sql = sql.fail_expired_jobs(schema)
        self._deletion_sql = sql.delete_old_jobs(schema)
        self._task: asyncio.Task[None] | None = None
        self._stopping = asyncio.Event()

    async def start(self) -> None:
        """Begin sweeping on the configured interval."""
        self._task = asyncio.create_task(self._run(), name="underboss-maintenance")

    async def stop(self) -> None:
        """Stop sweeping."""
        self._stopping.set()
        if self._task is None:
            return
        done, _ = await asyncio.wait({self._task}, timeout=30.0)
        if not done:
            self._task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self._task
        self._task = None

    async def sweep(self) -> None:
        """Run both maintenance sweeps once."""
        await self._db.execute(self._timeout_sql)
        await self._db.execute(self._deletion_sql)

    async def _run(self) -> None:
        while not self._stopping.is_set():
            try:
                await self.sweep()
            except Exception:
                _log.exception("underboss maintenance: sweep failed")
            await self._idle(self._interval)

    async def _idle(self, seconds: float) -> None:
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(self._stopping.wait(), timeout=seconds)
