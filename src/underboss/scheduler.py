"""The cron scheduler — underboss's timekeeper.

Ported from pg-boss's ``timekeeper.ts``. The scheduler ticks on an interval; on
each tick one node "wins" the tick (via ``try_set_cron_time``, debounced through
``version.cron_on``) and enqueues a job into the internal send-it queue for
every schedule whose cron fired within the last minute. A worker on that queue
then performs the real ``send``. Send-it jobs are throttled
(``singleton_seconds=60``), so a schedule fires at most once per minute even if
ticks overlap across nodes.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from collections.abc import Awaitable, Callable, Mapping
from datetime import UTC, datetime
from typing import Any
from zoneinfo import ZoneInfo

import asyncpg
from croniter import croniter

from underboss import sql
from underboss.db import Database
from underboss.errors import QueueNotFoundError
from underboss.types import Job, SendOptions, WorkOptions
from underboss.worker import Worker

_log = logging.getLogger("underboss.scheduler")

#: Internal queue that decouples cron evaluation from the real sends.
SEND_IT_QUEUE = "__underboss__send-it"

#: A schedule is due when its cron last fired within this many seconds.
_DUE_WINDOW_SECONDS = 60.0

#: A callable with the shape of :meth:`Underboss.send`.
SendFn = Callable[..., Awaitable[Any]]


def validate_cron(cron: str) -> None:
    """Raise :class:`ValueError` if ``cron`` is not a valid cron expression."""
    if not croniter.is_valid(cron):
        raise ValueError(f"invalid cron expression: {cron!r}")


def _should_send(cron: str, timezone: str | None) -> bool:
    """True when ``cron`` last fired within the due window."""
    zone = ZoneInfo(timezone) if timezone else UTC
    now = datetime.now(zone)
    previous = croniter(cron, now).get_prev(datetime)
    return (now - previous).total_seconds() < _DUE_WINDOW_SECONDS


class Scheduler:
    """Evaluates cron schedules and enqueues their jobs."""

    def __init__(
        self,
        db: Database,
        schema: str,
        send: SendFn,
        *,
        tick_interval_seconds: float = 60.0,
    ) -> None:
        self._db = db
        self._schema = schema
        self._send = send
        self._tick_interval = tick_interval_seconds
        self._sendit_poll = min(tick_interval_seconds, 2.0)
        self._worker: Worker | None = None
        self._task: asyncio.Task[None] | None = None
        self._stopping = asyncio.Event()

    async def start(self) -> None:
        """Create the send-it queue, start its worker, and begin ticking."""
        await self._db.execute(
            sql.create_queue(self._schema), SEND_IT_QUEUE, json.dumps({"policy": "standard"})
        )
        self._worker = Worker(
            self._db,
            self._schema,
            SEND_IT_QUEUE,
            self._on_send_it,
            WorkOptions(batch_size=50, poll_interval_seconds=self._sendit_poll),
        )
        self._worker.start()
        self._task = asyncio.create_task(self._run(), name="underboss-scheduler")

    async def stop(self) -> None:
        """Stop ticking and drain the send-it worker."""
        self._stopping.set()
        if self._task is not None:
            done, _ = await asyncio.wait({self._task}, timeout=30.0)
            if not done:
                self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None
        if self._worker is not None:
            await self._worker.stop()
            self._worker = None

    async def upsert(
        self,
        name: str,
        key: str,
        cron: str,
        timezone: str | None,
        data: Mapping[str, Any] | None,
    ) -> None:
        """Create or replace a schedule."""
        validate_cron(cron)
        try:
            await self._db.execute(
                sql.upsert_schedule(self._schema), name, key, cron, timezone, data, {}
            )
        except asyncpg.ForeignKeyViolationError as exc:
            raise QueueNotFoundError(f"queue {name!r} does not exist") from exc

    async def delete(self, name: str, key: str) -> None:
        """Remove a schedule."""
        await self._db.execute(sql.delete_schedule(self._schema), name, key)

    async def _run(self) -> None:
        while not self._stopping.is_set():
            try:
                await self._tick()
            except Exception:
                _log.exception("underboss scheduler: tick failed")
            await self._idle(self._tick_interval)

    async def _tick(self) -> None:
        won = await self._db.fetchval(sql.try_set_cron_time(self._schema), self._tick_interval)
        if won:
            await self._enqueue_due()

    async def _enqueue_due(self) -> None:
        for row in await self._db.fetch(sql.get_schedules(self._schema)):
            if not _should_send(row["cron"], row["timezone"]):
                continue
            envelope = {"name": row["name"], "data": row["data"], "options": row["options"]}
            await self._send(
                SEND_IT_QUEUE,
                envelope,
                SendOptions(
                    singleton_key=f"{row['name']}__{row['key']}",
                    singleton_seconds=int(_DUE_WINDOW_SECONDS),
                ),
            )

    async def _on_send_it(self, jobs: list[Job]) -> None:
        for job in jobs:
            envelope = job.data
            try:
                await self._send(envelope["name"], envelope["data"])
            except Exception:
                _log.exception("underboss scheduler: scheduled send failed")

    async def _idle(self, seconds: float) -> None:
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(self._stopping.wait(), timeout=seconds)
