"""The :class:`Underboss` class — underboss's public entry point."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from types import TracebackType
from typing import Any

import asyncpg

from underboss import schema as ddl
from underboss import sql
from underboss.db import Database
from underboss.errors import MigrationRequiredError, NotStartedError
from underboss.maintenance import Maintenance
from underboss.scheduler import Scheduler
from underboss.schema import DEFAULT_SCHEMA, SCHEMA_VERSION
from underboss.types import (
    Job,
    JobInsert,
    Queue,
    QueueOptions,
    QueuePolicy,
    SendOptions,
    WorkHandler,
    WorkOptions,
)
from underboss.worker import Worker, _row_to_job


def _isoformat_z(value: datetime) -> str:
    """Render a datetime as an ISO-8601 string ending in ``Z`` (UTC)."""
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _encode_start_after(value: datetime | int | str | None) -> str | None:
    """Encode a ``start_after`` value the way the insert query expects it.

    A datetime becomes a ``Z``-suffixed ISO string (parsed as an absolute time);
    an int or str becomes a relative interval (e.g. ``"30"`` → 30s from now).
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        return _isoformat_z(value)
    return str(value)


def _queue_options_payload(options: QueueOptions) -> dict[str, Any]:
    """Translate :class:`QueueOptions` into the camelCase JSON ``create_queue`` expects."""
    payload: dict[str, Any] = {
        "policy": options.policy.value,
        "retryLimit": options.retry_limit,
        "retryDelay": options.retry_delay,
        "retryBackoff": options.retry_backoff,
        "expireInSeconds": options.expire_in_seconds,
        "retentionSeconds": options.retention_seconds,
        "deleteAfterSeconds": options.delete_after_seconds,
        "warningQueueSize": options.warning_queue_size,
    }
    if options.retry_delay_max is not None:
        payload["retryDelayMax"] = options.retry_delay_max
    if options.dead_letter is not None:
        payload["deadLetter"] = options.dead_letter
    if options.heartbeat_seconds is not None:
        payload["heartbeatSeconds"] = options.heartbeat_seconds
    return payload


def _job_payload(data: Mapping[str, Any] | None, options: SendOptions) -> dict[str, Any]:
    """Translate a job's data and :class:`SendOptions` into the insert query's job spec."""
    payload: dict[str, Any] = {"priority": options.priority}
    if data is not None:
        payload["data"] = data
    start_after = _encode_start_after(options.start_after)
    if start_after is not None:
        payload["startAfter"] = start_after
    if options.singleton_key is not None:
        payload["singletonKey"] = options.singleton_key
    if options.singleton_seconds is not None:
        payload["singletonSeconds"] = options.singleton_seconds
        if options.singleton_next_slot:
            payload["singletonOffset"] = options.singleton_seconds
    if options.dead_letter is not None:
        payload["deadLetter"] = options.dead_letter
    if options.group is not None:
        payload["groupId"] = options.group.id
        if options.group.tier is not None:
            payload["groupTier"] = options.group.tier
    if options.retry_limit is not None:
        payload["retryLimit"] = options.retry_limit
    if options.retry_delay is not None:
        payload["retryDelay"] = options.retry_delay
    if options.retry_backoff is not None:
        payload["retryBackoff"] = options.retry_backoff
    if options.expire_in_seconds is not None:
        payload["expireInSeconds"] = options.expire_in_seconds
    return payload


def _job_insert_payload(job: JobInsert) -> dict[str, Any]:
    """Translate a :class:`JobInsert` into the insert query's job spec."""
    payload: dict[str, Any] = {"priority": job.priority}
    if job.id is not None:
        payload["id"] = job.id
    if job.data is not None:
        payload["data"] = job.data
    start_after = _encode_start_after(job.start_after)
    if start_after is not None:
        payload["startAfter"] = start_after
    if job.singleton_key is not None:
        payload["singletonKey"] = job.singleton_key
    if job.dead_letter is not None:
        payload["deadLetter"] = job.dead_letter
    if job.retry_limit is not None:
        payload["retryLimit"] = job.retry_limit
    if job.retry_delay is not None:
        payload["retryDelay"] = job.retry_delay
    if job.retry_backoff is not None:
        payload["retryBackoff"] = job.retry_backoff
    if job.expire_in_seconds is not None:
        payload["expireInSeconds"] = job.expire_in_seconds
    if job.group is not None:
        payload["groupId"] = job.group.id
        if job.group.tier is not None:
            payload["groupTier"] = job.group.tier
    return payload


def _row_to_queue(row: Any) -> Queue:
    """Build a :class:`Queue` from a fetched ``queue`` row."""
    return Queue(
        name=row["name"],
        options=QueueOptions(
            policy=QueuePolicy(row["policy"]),
            retry_limit=row["retry_limit"],
            retry_delay=row["retry_delay"],
            retry_backoff=row["retry_backoff"],
            retry_delay_max=row["retry_delay_max"],
            expire_in_seconds=row["expire_seconds"],
            retention_seconds=row["retention_seconds"],
            delete_after_seconds=row["deletion_seconds"],
            dead_letter=row["dead_letter"],
            warning_queue_size=row["warning_queued"],
            heartbeat_seconds=row["heartbeat_seconds"],
        ),
    )


class Underboss:
    """An async, Postgres-backed job queue.

    Create an instance with a DSN (underboss owns the connection pool) or an
    existing :class:`asyncpg.Pool`, then :meth:`start` it::

        boss = await Underboss("postgresql://localhost/mydb").start()
        ...
        await boss.stop()

    It also works as an async context manager.
    """

    def __init__(
        self,
        dsn: str | None = None,
        *,
        pool: asyncpg.Pool | None = None,
        schema: str = DEFAULT_SCHEMA,
        min_pool_size: int = 2,
        max_pool_size: int = 10,
        scheduling: bool = True,
        cron_interval_seconds: float = 60.0,
        maintenance: bool = True,
        maintenance_interval_seconds: float = 60.0,
    ) -> None:
        self._schema = schema
        self._db = Database(dsn, pool=pool, min_size=min_pool_size, max_size=max_pool_size)
        self._workers: dict[str, Worker] = {}
        self._scheduler: Scheduler | None = (
            Scheduler(self._db, schema, self.send, tick_interval_seconds=cron_interval_seconds)
            if scheduling
            else None
        )
        self._maintenance: Maintenance | None = (
            Maintenance(self._db, schema, interval_seconds=maintenance_interval_seconds)
            if maintenance
            else None
        )
        self._started = False

    @property
    def schema(self) -> str:
        """The Postgres schema (namespace) underboss is installed in."""
        return self._schema

    @property
    def started(self) -> bool:
        """Whether :meth:`start` has completed."""
        return self._started

    async def start(self) -> Underboss:
        """Open the connection pool, install the schema, and start background tasks."""
        await self._db.open()
        await self._provision()
        self._started = True
        if self._scheduler is not None:
            await self._scheduler.start()
        if self._maintenance is not None:
            await self._maintenance.start()
        return self

    async def stop(self) -> None:
        """Stop background tasks and every worker, then close the connection pool."""
        if self._maintenance is not None:
            await self._maintenance.stop()
        if self._scheduler is not None:
            await self._scheduler.stop()
        workers = list(self._workers.values())
        self._workers.clear()
        for worker in workers:
            await worker.stop()
        await self._db.close()
        self._started = False

    async def __aenter__(self) -> Underboss:
        return await self.start()

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.stop()

    async def _provision(self) -> None:
        """Install the schema, or verify an existing install is the right version."""
        installed = await self._db.fetchval(ddl.version_table_exists(self._schema))
        if not installed:
            await self._db.run_script(ddl.build_schema(self._schema, SCHEMA_VERSION))
            return
        version = await self._db.fetchval(ddl.get_version(self._schema))
        if version is not None and int(version) != SCHEMA_VERSION:
            raise MigrationRequiredError(
                f"database schema '{self._schema}' is at version {version}, "
                f"but this build of underboss expects {SCHEMA_VERSION}"
            )

    def _require_started(self) -> None:
        if not self._started:
            raise NotStartedError("Underboss is not started; call start() first")

    # ----------------------------------------------------------------------
    # Producer / worker API — stubbed; implemented incrementally toward 0.1.0.
    # ----------------------------------------------------------------------
    async def create_queue(
        self,
        name: str,
        options: QueueOptions | None = None,
        *,
        connection: asyncpg.Connection | None = None,
    ) -> None:
        """Create a queue, or do nothing if a queue with this name already exists.

        Pass ``connection`` to run inside a caller-supplied transaction.
        """
        self._require_started()
        payload = _queue_options_payload(options or QueueOptions())
        await self._db.execute(
            sql.create_queue(self._schema), name, payload, connection=connection
        )

    async def send(
        self,
        name: str,
        data: Mapping[str, Any] | None = None,
        options: SendOptions | None = None,
        *,
        connection: asyncpg.Connection | None = None,
    ) -> str | None:
        """Enqueue a job on queue ``name``.

        Returns the new job's id, or ``None`` when a queue-policy index
        suppressed it as a duplicate. Pass ``connection`` to enqueue inside a
        caller-supplied transaction — the job commits or rolls back with it.
        """
        self._require_started()
        payload = _job_payload(data, options or SendOptions())
        # fetch (not fetchrow): fetchrow leaves a row-limited suspended portal,
        # which CockroachDB rejects inside a transaction (crdb issue #40195).
        rows = await self._db.fetch(
            sql.insert_jobs(self._schema), [payload], name, connection=connection
        )
        return str(rows[0]["id"]) if rows else None

    async def work(
        self,
        name: str,
        handler: WorkHandler,
        options: WorkOptions | None = None,
    ) -> str:
        """Start a worker that polls ``name`` and dispatches jobs to ``handler``.

        Returns the worker's id. The worker runs in the background until
        :meth:`stop_worker` or :meth:`stop` is called.
        """
        self._require_started()
        worker = Worker(self._db, self._schema, name, handler, options or WorkOptions())
        worker.start()
        self._workers[worker.id] = worker
        return worker.id

    async def stop_worker(self, worker_id: str) -> None:
        """Stop a single worker by id."""
        worker = self._workers.pop(worker_id, None)
        if worker is not None:
            await worker.stop()

    def notify_worker(self, worker_id: str) -> None:
        """Nudge a worker to poll immediately instead of waiting for its next tick."""
        self._require_started()
        worker = self._workers.get(worker_id)
        if worker is not None:
            worker.notify()

    async def schedule(
        self,
        name: str,
        cron: str,
        *,
        data: Mapping[str, Any] | None = None,
        key: str = "",
        timezone: str | None = None,
    ) -> None:
        """Attach a cron schedule to a queue."""
        self._require_started()
        if self._scheduler is None:
            raise RuntimeError("scheduling is disabled on this Underboss instance")
        await self._scheduler.upsert(name, key, cron, timezone, data)

    async def unschedule(self, name: str, key: str = "") -> None:
        """Remove a cron schedule from a queue."""
        self._require_started()
        if self._scheduler is None:
            raise RuntimeError("scheduling is disabled on this Underboss instance")
        await self._scheduler.delete(name, key)

    async def insert(
        self, name: str, jobs: list[JobInsert], *, connection: asyncpg.Connection | None = None
    ) -> list[str]:
        """Bulk-insert jobs into ``name``; returns the ids of jobs actually inserted.

        Pass ``connection`` to insert inside a caller-supplied transaction.
        """
        self._require_started()
        payloads = [_job_insert_payload(job) for job in jobs]
        rows = await self._db.fetch(
            sql.insert_jobs(self._schema), payloads, name, connection=connection
        )
        return [str(row["id"]) for row in rows]

    async def fetch(
        self,
        name: str,
        *,
        batch_size: int = 1,
        include_metadata: bool = False,
        group_concurrency: int | None = None,
    ) -> list[Job]:
        """Claim up to ``batch_size`` jobs from ``name`` without running a worker.

        With ``group_concurrency``, at most that many jobs per ``group_id`` are
        claimed across all jobs already active.
        """
        self._require_started()
        query = sql.fetch_next_job(
            self._schema, include_metadata=include_metadata, group_concurrency=group_concurrency
        )
        if group_concurrency is None:
            args: tuple[Any, ...] = (name, batch_size)
        else:
            args = (name, batch_size, group_concurrency)
        rows = await self._db.fetch(query, *args)
        return [_row_to_job(row, include_metadata=include_metadata) for row in rows]

    async def get_job(self, name: str, job_id: str) -> Job | None:
        """Return a job by id, or ``None`` if it does not exist."""
        self._require_started()
        row = await self._db.fetchrow(sql.get_job_by_id(self._schema), name, job_id)
        return None if row is None else _row_to_job(row, include_metadata=True)

    async def complete(
        self, name: str, job_id: str, output: Mapping[str, Any] | None = None
    ) -> None:
        """Mark a job completed."""
        self._require_started()
        await self._db.execute(sql.complete_jobs(self._schema), name, [job_id], output)

    async def cancel(self, name: str, job_id: str) -> None:
        """Cancel a queued or active job."""
        self._require_started()
        await self._db.execute(sql.cancel_jobs(self._schema), name, [job_id])

    async def resume(self, name: str, job_id: str) -> None:
        """Return a cancelled job to its queue."""
        self._require_started()
        await self._db.execute(sql.resume_jobs(self._schema), name, [job_id])

    async def retry(self, name: str, job_id: str) -> None:
        """Re-queue a failed job."""
        self._require_started()
        await self._db.execute(sql.retry_jobs(self._schema), name, [job_id])

    async def delete_job(self, name: str, job_id: str) -> None:
        """Delete a job by id."""
        self._require_started()
        await self._db.execute(sql.delete_jobs(self._schema), name, [job_id])

    async def get_queue(self, name: str) -> Queue | None:
        """Return a queue's configuration, or ``None`` if it does not exist."""
        self._require_started()
        row = await self._db.fetchrow(sql.get_queue(self._schema), name)
        return None if row is None else _row_to_queue(row)

    async def get_queues(self) -> list[Queue]:
        """Return every queue's configuration."""
        self._require_started()
        rows = await self._db.fetch(sql.get_queues(self._schema))
        return [_row_to_queue(row) for row in rows]
