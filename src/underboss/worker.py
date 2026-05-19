"""The job worker — the consumer side of underboss."""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import Mapping
from typing import Any
from uuid import uuid4

from underboss import sql
from underboss.db import Database
from underboss.types import Job, JobState, WorkHandler, WorkOptions

_log = logging.getLogger("underboss.worker")


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
        self._handler = handler
        self._options = options
        self._fetch_sql = sql.fetch_next_job(schema, include_metadata=options.include_metadata)
        self._complete_sql = sql.complete_jobs(schema)
        self._fail_sql = sql.fail_jobs(schema)
        self._task: asyncio.Task[None] | None = None
        self._stopping = asyncio.Event()

    def start(self) -> None:
        """Spawn the poll loop as a background task."""
        if self._task is not None:
            raise RuntimeError("worker already started")
        self._task = asyncio.create_task(self._run(), name=f"underboss-worker-{self.name}")

    async def stop(self, *, timeout: float = 30.0) -> None:
        """Signal the poll loop to stop and wait for the in-flight batch to finish."""
        self._stopping.set()
        task = self._task
        if task is None:
            return
        done, _ = await asyncio.wait({task}, timeout=timeout)
        if not done:
            task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
        self._task = None

    async def _run(self) -> None:
        poll = self._options.poll_interval_seconds
        while not self._stopping.is_set():
            try:
                rows = await self._db.fetch(self._fetch_sql, self.name, self._options.batch_size)
            except Exception:
                _log.exception("underboss worker for %r: fetch failed", self.name)
                await self._idle(poll)
                continue
            if not rows:
                await self._idle(poll)
                continue
            await self._process(rows)

    async def _process(self, rows: list[Any]) -> None:
        ids = [row["id"] for row in rows]
        jobs = [_row_to_job(row, include_metadata=self._options.include_metadata) for row in rows]
        try:
            result = await self._handler(jobs)
        except Exception as exc:  # any handler error fails the whole batch
            _log.exception("underboss worker for %r: handler raised", self.name)
            await self._settle(
                self._fail_sql, ids, {"message": str(exc), "type": type(exc).__name__}
            )
        else:
            output = result if isinstance(result, Mapping) else None
            await self._settle(self._complete_sql, ids, output)

    async def _settle(self, query: str, ids: list[Any], output: Any) -> None:
        try:
            await self._db.execute(query, self.name, ids, output)
        except Exception:
            _log.exception("underboss worker for %r: settling batch failed", self.name)

    async def _idle(self, seconds: float) -> None:
        """Sleep up to ``seconds``, waking early if a stop was requested."""
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(self._stopping.wait(), timeout=seconds)
