"""The job worker — the consumer side of underboss."""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import Mapping
from typing import Any
from uuid import uuid4

from underboss import failure, sql
from underboss.db import Database
from underboss.types import Job, JobState, WorkHandler, WorkOptions

_log = logging.getLogger("underboss.worker")

#: After a notify(), an empty fetch is retried this soon. A notify means a job
#: was just enqueued; an empty result usually means CockroachDB has not yet
#: resolved that job's write intent, so FOR UPDATE SKIP LOCKED skipped it.
_NOTIFY_RETRY_SECONDS = 0.05


def _row_to_job(row: Any, *, include_metadata: bool) -> Job:
    """Build a :class:`Job` from a fetched row."""
    metadata: dict[str, Any] = {}
    if include_metadata:
        metadata = {
            "state": JobState(row["state"]),
            "priority": row["priority"],
            "retry_limit": row["retry_limit"],
            "retry_count": row["retry_count"],
            "started_on": row["started_on"],
            "created_on": row["created_on"],
        }
    return Job(
        id=str(row["id"]),
        name=row["name"],
        data=row["data"],
        expire_in_seconds=row["expire_seconds"],
        group_id=row["group_id"],
        group_tier=row["group_tier"],
        **metadata,
    )


class Worker:
    """Polls one queue and dispatches batches of jobs to a handler.

    The lifecycle is claim → execute → settle: ``fetch_next_job`` claims a batch
    in a single auto-committed statement (releasing the row lock immediately),
    the handler runs with no lock held, and the batch is then completed or
    failed. A handler that returns settles the batch as completed; one that
    raises settles it as failed — jobs with retries remaining return to ``retry``.
    """

    def __init__(
        self,
        db: Database,
        schema: str,
        name: str,
        handler: WorkHandler,
        options: WorkOptions,
    ) -> None:
        self.id = str(uuid4())
        self.name = name
        self._db = db
        self._schema = schema
        self._handler = handler
        self._options = options
        self._fetch_sql = sql.fetch_next_job(schema, include_metadata=options.include_metadata)
        self._complete_sql = sql.complete_jobs(schema)
        self._task: asyncio.Task[None] | None = None
        self._stopping = asyncio.Event()
        self._wake = asyncio.Event()

    def start(self) -> None:
        """Spawn the poll loop as a background task."""
        if self._task is not None:
            raise RuntimeError("worker already started")
        self._task = asyncio.create_task(self._run(), name=f"underboss-worker-{self.name}")

    async def stop(self, *, timeout: float = 30.0) -> None:
        """Signal the poll loop to stop and wait for the in-flight batch to finish."""
        self._stopping.set()
        self._wake.set()
        task = self._task
        if task is None:
            return
        done, _ = await asyncio.wait({task}, timeout=timeout)
        if not done:
            task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
        self._task = None

    def notify(self) -> None:
        """Wake the poll loop so it fetches immediately, skipping the poll delay.

        Ported from pg-boss's ``worker.notify()`` — an in-process nudge so a job
        sent in this process is picked up without waiting for the next tick.
        """
        self._wake.set()

    async def _run(self) -> None:
        poll = self._options.poll_interval_seconds
        while not self._stopping.is_set():
            notified = self._wake.is_set()
            self._wake.clear()
            try:
                rows = await self._db.fetch(self._fetch_sql, self.name, self._options.batch_size)
            except Exception:
                _log.exception("underboss worker for %r: fetch failed", self.name)
                await self._idle(poll)
                continue
            if not rows:
                # An empty fetch right after a notify usually means the just-
                # enqueued job's write intent is not resolved yet (SKIP LOCKED
                # skipped it) — retry soon instead of waiting a full poll.
                await self._idle(min(poll, _NOTIFY_RETRY_SECONDS) if notified else poll)
                continue
            await self._process(rows)

    async def _process(self, rows: list[Any]) -> None:
        ids = [row["id"] for row in rows]
        jobs = [_row_to_job(row, include_metadata=self._options.include_metadata) for row in rows]
        try:
            result = await self._handler(jobs)
        except Exception as exc:  # any handler error fails the whole batch
            _log.exception("underboss worker for %r: handler raised", self.name)
            await self._fail(ids, {"message": str(exc), "type": type(exc).__name__})
        else:
            output = result if isinstance(result, Mapping) else None
            await self._complete(ids, output)

    async def _complete(self, ids: list[Any], output: Any) -> None:
        try:
            await self._db.execute(self._complete_sql, self.name, ids, output)
        except Exception:
            _log.exception("underboss worker for %r: completing batch failed", self.name)

    async def _fail(self, ids: list[Any], output: dict[str, Any]) -> None:
        try:
            await failure.fail_by_id(self._db, self._schema, self.name, ids, output)
        except Exception:
            _log.exception("underboss worker for %r: failing batch failed", self.name)

    async def _idle(self, seconds: float) -> None:
        """Sleep up to ``seconds``, waking early on a stop or a notify().

        Both :meth:`notify` and :meth:`stop` set ``_wake``; the poll loop clears
        it at the top of each iteration.
        """
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(self._wake.wait(), timeout=seconds)
